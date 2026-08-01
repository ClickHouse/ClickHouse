#pragma once

#include <Storages/NATS/INATSConsumer.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class NATSJetStreamConsumer : public INATSConsumer
{
public:
    NATSJetStreamConsumer(
        NATSConnectionPtr connection,
        String stream_name_,
        String consumer_name_,
        const std::vector<String> & subjects,
        const String & subscribe_queue_name,
        LoggerPtr log,
        uint32_t queue_size,
        const std::atomic<bool> & stopped);

    bool needsAck() const override { return true; }

    /// A subscription whose fetch the client terminated never consumes again, and JetStream
    /// redelivers unacked messages, so re-subscribing is safe. Requires all of them to be closed:
    /// teardown is per consumer, so a live sibling must not be dropped.
    bool needsResubscribe() const override { return isSubscribed() && allSubscriptionsClosed(); }

protected:
    void subscribeImpl() override;

    void nackMessage(natsMsg * msg) override;

    NATSSubscriptionPtr subscribeToSubject(const String & subject);

    const String stream_name;
    const String consumer_name;

    std::unique_ptr<jsCtx, decltype(&jsCtx_Destroy)> jet_stream_ctx;
    jsOptions jet_stream_options{};
    jsSubOptions subscribe_options{};
};

}
