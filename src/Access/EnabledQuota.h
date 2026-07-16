#pragma once

#include <Access/Common/QuotaDefs.h>
#include <Common/HashTable/HashMap.h>
#include <Core/UUID.h>
#include <Poco/Net/IPAddress.h>
#include <boost/container/flat_set.hpp>
#include <boost/noncopyable.hpp>
#include <boost/smart_ptr/atomic_shared_ptr.hpp>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
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

    /// Tracks per-normalized-query-hash quota. Increments the per-hash counter and checks
    /// against the `QUERIES_PER_NORMALIZED_HASH` limit.
    void usedPerNormalizedHash(UInt64 normalized_query_hash) const;

    /// For `NORMALIZED_QUERY_HASH` keyed quotas: resolves intervals for a specific hash,
    /// then tracks resource consumption against the resolved intervals.
    void usedForNormalizedQuery(UInt64 normalized_query_hash, QuotaType quota_type, QuotaValue value, bool check_exceeded = true) const;

    /// Checks if the quota exceeded. If so, throws an exception.
    void checkExceeded() const;
    void checkExceeded(QuotaType quota_type) const;

    void reset(QuotaType quota_type) const;

    /// Returns the information about quota consumption.
    std::optional<QuotaUsage> getUsage() const;

    /// Returns true if this quota is keyed by `NORMALIZED_QUERY_HASH`.
    /// When true, callers should use `usedForNormalizedQuery` instead of `used`.
    bool isKeyedByNormalizedQueryHash() const
    {
        std::lock_guard lock(resolved_intervals_mutex);
        return interval_resolver != nullptr;
    }


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

        /// Per-normalized-query-hash counters for `QUERIES_PER_NORMALIZED_HASH`.
        mutable std::mutex per_hash_mutex;
        mutable HashMap<UInt64, QuotaValue> per_hash_used;

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

    /// Callback type for resolving intervals lazily per normalized query hash.
    /// Used when the quota is keyed by `NORMALIZED_QUERY_HASH`.
    using IntervalResolver = std::function<boost::shared_ptr<const Intervals>(const String & key)>;

    struct Impl;

    const Params params;
    boost::atomic_shared_ptr<const Intervals> intervals; /// atomically changed by QuotaUsageManager
    std::atomic<bool> empty = false; /// Use a separate flag to avoid loading intervals, which is way more expensive than an atomic bool

    /// For `NORMALIZED_QUERY_HASH` keyed quotas: callback to resolve intervals per hash.
    IntervalResolver interval_resolver;
    /// Cache of resolved intervals per normalized query hash.
    mutable std::mutex resolved_intervals_mutex;
    mutable HashMap<UInt64, boost::shared_ptr<const Intervals>> resolved_intervals_cache;
};

}
