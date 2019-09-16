#pragma once

#include <ACL/Quota2.h>
#include <ACL/IControlAttributesStorage.h>
#include <Poco/Net/IPAddress.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <vector>


namespace DB
{
class IControlAttributesDrivenManager;


/// Consumes quotas and stores information how much amount of resources have been consumed and how mush are left.
/// This class is thread-safe.
class QuotasConsumer
{
public:
    using ResourceType = Quota2::ResourceType;
    using ResourceAmount = Quota2::ResourceAmount;
    using IPAddress = Poco::Net::IPAddress;

    QuotasConsumer(
        const std::vector<Quota2> & quotas_,
        const String & user_name_,
        const IPAddress & ip_address_,
        const String & custom_consumption_key_);
    ~QuotasConsumer();

    const std::vector<Quota2> & getQuotas() const { return quotas; }
    const String & getUserName() const { return user_name; }
    const IPAddress & getIPAddress() const { return ip_address; }
    const String & getCustomConsumptionKey() const { return custom_consumption_key; }

    void consume(ResourceType resource_type, ResourceAmount amount);
    void consume(ResourceType resource_type, ResourceAmount amount, std::chrono::system_clock::time_point current_time);

private:
    struct Interval;
    class Intervals;
    class ConsumptionMap;
    struct ExceedInfo;

    const std::vector<Quota2> quotas;
    const String user_name;
    const IPAddress ip_address;
    const String custom_consumption_key;

    using AtomicIntervalPtr = std::atomic<Intervals *>;
    using SubscriptionPtr = IControlAttributesStorage::SubscriptionPtr;

    std::unique_ptr<AtomicIntervalPtr[]> intervals_for_quotas;
    std::unique_ptr<SubscriptionPtr[]> subscriptions;
};

using QuotasConsumerPtr = std::shared_ptr<QuotasConsumer>;
}
