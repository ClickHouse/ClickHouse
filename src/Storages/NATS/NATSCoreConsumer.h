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

    /// Plain `Subscribe` and `QueueSubscribe` subscriptions are auto-restored
    /// by libnats after a reconnect, so no application-level re-subscription
    /// is required.
    bool needsResubscribeOnReconnect() const override { return false; }
};

}
