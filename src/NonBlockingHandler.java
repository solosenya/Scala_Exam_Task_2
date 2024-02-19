import java.time.Duration;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NonBlockingHandler implements Handler {
    private final Client client;
    private final ScheduledThreadPoolExecutor executorService;

    public NonBlockingHandler(Client client, int threadsAmount) {
        this.client = client;
        executorService = new ScheduledThreadPoolExecutor(threadsAmount);
    }

    @Override
    public Duration timeout() {
        return null;
    }

    @Override
    public void performOperation() {
        Event event = client.readData();
        Payload payload = event.payload();

        for (Address address : event.recipients()) {
            executorService.schedule(() -> {
                Result result = client.sendData(address, payload);
                if (result.equals(Result.REJECTED)) {
                    scheduleTask(address, payload);
                }
            }, 0, TimeUnit.MILLISECONDS);

        }
    }

    private void scheduleTask(Address address, Payload payload) {
        executorService.schedule(() -> {
            Result result = client.sendData(address, payload);
            if (result.equals(Result.REJECTED)) {
                scheduleTask(address, payload);
            }
        }, timeout().toMillis(), TimeUnit.MILLISECONDS);
    }
}
