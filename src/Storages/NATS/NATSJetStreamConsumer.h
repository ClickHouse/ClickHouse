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
    /// Reported only while the client is connected. The client lets a subscription be created while
    /// it is reconnecting: the consumer lookup times out and is tolerated for a bound pull consumer,
    /// the `SUB` line and the pull request wait in the pending buffer, and the reconnect that then
    /// completes sends them. Such a subscription is sound, but it was created against a reconnect
    /// count that predates that reconnect, so it would read as stale once more and be replaced,
    /// handing back whatever the broker had just delivered to it. Waiting for the connection costs
    /// nothing: a stale subscription receives nothing in the meantime, and both conditions keep
    /// reporting until they have been acted on.
    bool needsResubscribe() const override
    {
        return isSubscribed() && isConnectionConnected() && (hasClosedSubscription() || hasConnectionReconnected());
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
