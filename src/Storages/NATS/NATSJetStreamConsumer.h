#pragma once

#include <Storages/NATS/INATSConsumer.h>

#include <chrono>

namespace DB
{

/// JetStream: pulls messages of an existing durable pull consumer with the requests of the JetStream API,
/// and acknowledges them after they are inserted. One pull request is outstanding at a time, and only while
/// the consumer is read from, so that the server does not deliver many messages that nobody reads.
class NATSJetStreamConsumer : public INATSConsumer
{
public:
    NATSJetStreamConsumer(
        const NATSConfiguration & configuration,
        String stream_name_,
        String consumer_name_,
        const std::vector<String> & subjects,
        const String & subscribe_queue_name,
        LoggerPtr log,
        const std::atomic<bool> & stopped);

    [[nodiscard]] bool needsAck() const override { return true; }

protected:
    void subscribeImpl() override;
    void unsubscribeImpl() override;
    std::optional<NATSClient::Message> receiveImpl(UInt64 timeout_ms) override;
    std::vector<String> drainBeforeUnsubscribe() override;

private:
    void requestPull();

    const String stream_name;
    const String consumer_name;

    String inbox;
    UInt64 sid = 0;

    bool pull_outstanding = false;
    size_t pull_remaining = 0;
    std::chrono::steady_clock::time_point pull_expires_at;
};

}
