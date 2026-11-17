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
    using INATSConsumer::INATSConsumer;

    void subscribe() override;
};

}
