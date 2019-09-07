#pragma once

#include <ACL/Quota2.h>
#include <ACL/IACLAttributesStorage.h>
#include <Poco/Net/IPAddress.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <vector>


namespace DB
{
class IACLAttributableManager;


/// Consumes quotas and stores information how much amount of resources have been consumed and how mush are left.
/// This class is thread-safe.
class QuotaConsumer
{
public:
    using ResourceType = Quota2::ResourceType;
    using ResourceAmount = Quota2::ResourceAmount;
    using IPAddress = Poco::Net::IPAddress;

    QuotaConsumer(const std::vector<UUID> & quotas_, IACLAttributableManager & manager,
                  const String & user_name_, const IPAddress & ip_address_, const String & custom_consumption_key_);
    ~QuotaConsumer();

    void consume(ResourceType resource_type, ResourceAmount amount);
    void consume(ResourceType resource_type, ResourceAmount amount, std::chrono::system_clock::time_point current_time);

private:
    struct Interval;
    class Intervals;
    class ConsumptionMap;
    struct ExceedInfo;

    const std::vector<UUID> quotas;
    const String user_name;
    const IPAddress ip_address;
    const String custom_consumption_key;

    using SubscriptionPtr = IACLAttributesStorage::SubscriptionPtr;

    std::unique_ptr<std::atomic<Intervals *>[]> intervals_for_quotas;
    std::unique_ptr<SubscriptionPtr[]> subscriptions;
};

using QuotaConsumerPtr = std::shared_ptr<QuotaConsumer>;
}
