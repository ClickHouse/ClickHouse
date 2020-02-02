#pragma once

#include <Access/Quota.h>
#include <Core/UUID.h>
#include <Poco/Net/IPAddress.h>
#include <ext/shared_ptr_helper.h>
#include <boost/noncopyable.hpp>
#include <atomic>
#include <chrono>
#include <memory>


namespace DB
{
struct QuotaUsageInfo;


/// Instances of `QuotaContext` are used to track resource consumption.
class QuotaContext : public boost::noncopyable
{
public:
    using ResourceType = Quota::ResourceType;
    using ResourceAmount = Quota::ResourceAmount;

    /// Default constructors makes an unlimited quota.
    QuotaContext();

    ~QuotaContext();

    /// Tracks resource consumption. If the quota exceeded and `check_exceeded == true`, throws an exception.
    void used(ResourceType resource_type, ResourceAmount amount, bool check_exceeded = true);
    void used(const std::pair<ResourceType, ResourceAmount> & resource, bool check_exceeded = true);
    void used(const std::pair<ResourceType, ResourceAmount> & resource1, const std::pair<ResourceType, ResourceAmount> & resource2, bool check_exceeded = true);
    void used(const std::pair<ResourceType, ResourceAmount> & resource1, const std::pair<ResourceType, ResourceAmount> & resource2, const std::pair<ResourceType, ResourceAmount> & resource3, bool check_exceeded = true);
    void used(const std::vector<std::pair<ResourceType, ResourceAmount>> & resources, bool check_exceeded = true);

    /// Checks if the quota exceeded. If so, throws an exception.
    void checkExceeded();
    void checkExceeded(ResourceType resource_type);

    /// Returns the information about this quota context.
    QuotaUsageInfo getUsageInfo() const;

private:
    friend class QuotaContextFactory;
    friend struct ext::shared_ptr_helper<QuotaContext>;

    /// Instances of this class are created by QuotaContextFactory.
    QuotaContext(const String & user_name_, const Poco::Net::IPAddress & address_, const String & client_key_);

    static constexpr size_t MAX_RESOURCE_TYPE = Quota::MAX_RESOURCE_TYPE;

    struct Interval
    {
        mutable std::atomic<ResourceAmount> used[MAX_RESOURCE_TYPE];
        ResourceAmount max[MAX_RESOURCE_TYPE];
        std::chrono::seconds duration;
        bool randomize_interval;
        mutable std::atomic<std::chrono::system_clock::duration> end_of_interval;

        Interval() {}
        Interval(const Interval & src) { *this = src; }
        Interval & operator =(const Interval & src);
    };

    struct Intervals
    {
        std::vector<Interval> intervals;
        UUID quota_id;
        String quota_name;
        String quota_key;

        QuotaUsageInfo getUsageInfo(std::chrono::system_clock::time_point current_time) const;
    };

    struct Impl;

    const String user_name;
    const Poco::Net::IPAddress address;
    const String client_key;
    std::shared_ptr<const Intervals> atomic_intervals; /// atomically changed by QuotaUsageManager
};

using QuotaContextPtr = std::shared_ptr<QuotaContext>;


/// The information about a quota context.
struct QuotaUsageInfo
{
    using ResourceType = Quota::ResourceType;
    using ResourceAmount = Quota::ResourceAmount;
    static constexpr size_t MAX_RESOURCE_TYPE = Quota::MAX_RESOURCE_TYPE;

    struct Interval
    {
        ResourceAmount used[MAX_RESOURCE_TYPE];
        ResourceAmount max[MAX_RESOURCE_TYPE];
        std::chrono::seconds duration = std::chrono::seconds::zero();
        bool randomize_interval = false;
        std::chrono::system_clock::time_point end_of_interval;
        Interval();
    };

    std::vector<Interval> intervals;
    UUID quota_id;
    String quota_name;
    String quota_key;
    QuotaUsageInfo();
};
}
