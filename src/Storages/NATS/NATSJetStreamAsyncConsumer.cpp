#include <Storages/NATS/NATSJetStreamAsyncConsumer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_NATS;
    extern const int INVALID_STATE;
}

NATSJetStreamAsyncConsumer::NATSJetStreamAsyncConsumer(
    NATSConnectionPtr connection,
    String stream_name_,
    String consumer_name_,
    const std::vector<String> & subjects,
    const String & subscribe_queue_name,
    LoggerPtr log,
    uint32_t queue_size,
    const std::atomic<bool> & stopped)
    : NATSJetStreamConsumer(
        std::move(connection), 
        std::move(stream_name_),
        std::move(consumer_name_),
        subjects, 
        subscribe_queue_name, 
        log, queue_size, 
        stopped)
{
}

NATSSubscriptionPtr NATSJetStreamAsyncConsumer::subscribeToSubject(const String & subject)
{
    if (consumer_name.empty())
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "To use NATS jet stream consumers, you must specify `nats_consumer_name` setting");

    natsSubscription * subscription;
    auto status = js_PullSubscribeAsync(
        &subscription, 
        jet_stream_ctx.get(),
        subject.c_str(), 
        consumer_name.c_str(),
        onMsg,
        static_cast<void *>(this),
        &jet_stream_options,
        &subscribe_options,
        nullptr);
    if (status != NATS_OK)
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_NATS, 
            "Failed to subscribe consumer {} to subject {}. Error: {} {}", static_cast<void*>(this), subject, natsStatus_GetText(status), nats_GetLastError(nullptr));
    
    NATSSubscriptionPtr result(subscription, &natsSubscription_Destroy);
    natsSubscription_SetPendingLimits(result.get(), -1, -1);

    return result;
}

}
