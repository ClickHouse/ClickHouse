#pragma once

#include <Storages/NATS/NATSJetStreamConsumer.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class NATSJetStreamAsyncConsumer : public NATSJetStreamConsumer
{
public:
    NATSJetStreamAsyncConsumer(
        NATSConnectionPtr connection,
        String stream_name_,
        String consumer_name_,
        const std::vector<String> & subjects,
        const String & subscribe_queue_name,
        LoggerPtr log,
        uint32_t queue_size,
        const std::atomic<bool> & stopped);

private:
    virtual NATSSubscriptionPtr subscribeToSubject(const String & subject) override;
};

}
