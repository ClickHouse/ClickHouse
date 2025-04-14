#include <Storages/NATS/NATSJetStreamConsumer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_NATS;
    extern const int INVALID_STATE;
}

NATSJetStreamConsumer::NATSJetStreamConsumer(
    NATSConnectionPtr connection,
    String stream_name_,
    String consumer_name_,
    const std::vector<String> & subjects,
    const String & subscribe_queue_name,
    LoggerPtr log,
    uint32_t queue_size,
    const std::atomic<bool> & stopped)
    : INATSConsumer(std::move(connection), subjects, subscribe_queue_name, log, queue_size, stopped)
    , stream_name(std::move(stream_name_))
    , consumer_name(std::move(consumer_name_))
    , jet_stream_ctx(nullptr, &jsCtx_Destroy)
{
}

void NATSJetStreamConsumer::subscribe()
{
    if (isSubscribed())
        return;

    auto er = jsOptions_Init(&jet_stream_options);
    if (er != NATS_OK)
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_NATS, 
            "Failed to create NATS jet stream options for {}. Nats last error: {}", getConnection()->connectionInfoForLog(), natsStatus_GetText(er));
    
    jsCtx * new_jet_stream_ctx = nullptr;
    er = natsConnection_JetStream(&new_jet_stream_ctx, getNativeConnection(), &jet_stream_options);
    if (er != NATS_OK)
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_NATS, 
            "Failed to create NATS jet stream ctx for {}. Nats last error: {}", getConnection()->connectionInfoForLog(), natsStatus_GetText(er));
    jet_stream_ctx.reset(new_jet_stream_ctx);

    er = jsSubOptions_Init(&subscribe_options);
    if (er != NATS_OK)
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_NATS, 
            "Failed to create NATS jet stream subscribe options for {}. Error: {}", getConnection()->connectionInfoForLog(), natsStatus_GetText(er));

    subscribe_options.Stream = stream_name.c_str();
    subscribe_options.Consumer = consumer_name.c_str();
    
    if (!getQueueName().empty())
        subscribe_options.Queue = getQueueName().c_str();

    std::vector<NATSSubscriptionPtr> created_subscriptions;
    created_subscriptions.reserve(getSubjects().size());

    for (const auto & subject : getSubjects())
    {
        created_subscriptions.emplace_back(subscribeToSubject(subject));
        LOG_DEBUG(getLogger(), "Subscribed to subject {}", subject);
    }
    LOG_DEBUG(getLogger(), "Consumer {} subscribed to {} subjects", static_cast<void*>(this), created_subscriptions.size());

    setSubscriptions(std::move(created_subscriptions));
}

}
