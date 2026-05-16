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

    void subscribe() override;

protected:
    NATSSubscriptionPtr subscribeToSubject(const String & subject);

    const String stream_name;
    const String consumer_name;

    std::unique_ptr<jsCtx, decltype(&jsCtx_Destroy)> jet_stream_ctx;
    jsOptions jet_stream_options;
    jsSubOptions subscribe_options;
};

}
