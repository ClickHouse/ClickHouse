#pragma once

#include <Storages/NATS/INATSConsumer.h>

namespace DB
{

/// Core NATS: a subscription per subject, in the queue group of the table. Messages are not acknowledged.
class NATSCoreConsumer : public INATSConsumer
{
public:
    using INATSConsumer::INATSConsumer;

protected:
    void subscribeImpl() override;
    void unsubscribeImpl() override;
    std::optional<NATSClient::Message> receiveImpl(UInt64 timeout_ms) override;
    std::optional<NATSClient::Message> acceptMessage(NATSClient::Message message) override;

private:
    std::vector<UInt64> sids;
};

}
