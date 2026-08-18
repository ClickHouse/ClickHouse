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

    /// An asynchronous pull subscription is renewed only when a message is delivered, so it consumes
    /// nothing more once the pull request it is waiting for is gone. That happens two ways: the
    /// client closes the subscription when the broker answers the request while shutting down, and a
    /// reconnect resends the `SUB` line but not the request, leaving a subscription that still looks
    /// healthy with nothing parked on the broker. JetStream redelivers unacked messages, so
    /// re-subscribing is safe here.
    bool needsResubscribe() const override
    {
        return isSubscribed() && (hasClosedSubscription() || hasConnectionReconnected());
    }

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
