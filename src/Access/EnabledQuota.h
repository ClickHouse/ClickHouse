#pragma once

#include <Access/Common/QuotaDefs.h>
#include <Core/UUID.h>
#include <Poco/Net/IPAddress.h>
#include <boost/container/flat_set.hpp>
#include <boost/noncopyable.hpp>
#include <boost/smart_ptr/atomic_shared_ptr.hpp>
#include <atomic>
#include <chrono>
#include <memory>
#include <optional>


namespace DB
{
struct QuotaUsage;


/// Instances of `EnabledQuota` are used to track resource consumption.
class EnabledQuota : public boost::noncopyable
{
public:
    struct Params
    {
        UUID user_id;
        String user_name;
        boost::container::flat_set<UUID> enabled_roles;
        Poco::Net::IPAddress client_address;
        String forwarded_address;
        String client_key;

        auto toTuple() const { return std::tie(user_id, enabled_roles, user_name, client_address, forwarded_address, client_key); }
        friend bool operator ==(const Params & lhs, const Params & rhs) { return lhs.toTuple() == rhs.toTuple(); }
        friend bool operator !=(const Params & lhs, const Params & rhs) { return !(lhs == rhs); }
        friend bool operator <(const Params & lhs, const Params & rhs) { return lhs.toTuple() < rhs.toTuple(); }
        friend bool operator >(const Params & lhs, const Params & rhs) { return rhs < lhs; }
        friend bool operator <=(const Params & lhs, const Params & rhs) { return !(rhs < lhs); }
        friend bool operator >=(const Params & lhs, const Params & rhs) { return !(lhs < rhs); }
    };

    ~EnabledQuota();

    /// Tracks resource consumption. If the quota exceeded and `check_exceeded == true`, throws an exception.
    void used(QuotaType quota_type, QuotaValue value, bool check_exceeded = true) const;
    void used(const std::pair<QuotaType, QuotaValue> & usage1, bool check_exceeded = true) const;
    void used(const std::pair<QuotaType, QuotaValue> & usage1, const std::pair<QuotaType, QuotaValue> & usage2, bool check_exceeded = true) const;
    void used(const std::pair<QuotaType, QuotaValue> & usage1, const std::pair<QuotaType, QuotaValue> & usage2, const std::pair<QuotaType, QuotaValue> & usage3, bool check_exceeded = true) const;
    void used(const std::vector<std::pair<QuotaType, QuotaValue>> & usages, bool check_exceeded = true) const;

    /// Checks if the quota exceeded. If so, throws an exception.
    void checkExceeded() const;
    void checkExceeded(QuotaType quota_type) const;

    /// Returns the information about quota consumption.
    std::optional<QuotaUsage> getUsage() const;

    /// Returns an instance of EnabledQuota which is never exceeded.
    static std::shared_ptr<const EnabledQuota> getUnlimitedQuota();

private:
    friend class QuotaCache;
    explicit EnabledQuota(const Params & params_);
    EnabledQuota() {} /// NOLINT

    const String & getUserName() const { return params.user_name; }

    struct Interval
    {
        mutable std::atomic<QuotaValue> used[static_cast<size_t>(QuotaType::MAX)];
        QuotaValue max[static_cast<size_t>(QuotaType::MAX)];
        std::chrono::seconds duration = std::chrono::seconds::zero();
        bool randomize_interval = false;
        mutable std::atomic<std::chrono::system_clock::duration> end_of_interval;

        Interval(std::chrono::seconds duration_, bool randomize_interval_, std::chrono::system_clock::time_point current_time_);

        Interval(const Interval & src) { *this = src; }
        Interval & operator =(const Interval & src);

        std::chrono::system_clock::time_point getEndOfInterval(std::chrono::system_clock::time_point current_time) const;
        std::chrono::system_clock::time_point getEndOfInterval(std::chrono::system_clock::time_point current_time, bool & counters_were_reset) const;
    };

    struct Intervals
    {
        std::vector<Interval> intervals;
        std::optional<UUID> quota_id;
        String quota_name;
        String quota_key;

        std::optional<QuotaUsage> getUsage(std::chrono::system_clock::time_point current_time) const;
    };

    struct Impl;

    const Params params;
    boost::atomic_shared_ptr<const Intervals> intervals; /// atomically changed by QuotaUsageManager
};

}
