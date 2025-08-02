#pragma once

#include <Storages/NATS/INATSConsumer.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class NATSCoreConsumer : public INATSConsumer
{
public:
    NATSCoreConsumer(
        NATSConnectionPtr connection,
        const std::vector<String> & subjects,
        const String & subscribe_queue_name,
        LoggerPtr log,
        uint32_t queue_size,
        const std::atomic<bool> & stopped);

    void subscribe() override;
};

}
