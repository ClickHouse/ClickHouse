#pragma once

#include <cstring>
#include <unordered_map>
#include <memory>
#include <pcg_random.hpp>

#include <Poco/Timespan.h>

#include <Poco/Util/Application.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <Poco/Net/IPAddress.h>

#include <Core/Types.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>


namespace DB
{

/** Quota for resources consumption for specific interval.
  * Used to limit resource usage by user.
  * Quota is applied "softly" - could be slightly exceed, because it is checked usually only on each block of processed data.
  * Accumulated values are not persisted and are lost on server restart.
  * Quota is local to server,
  *  but for distributed queries, accumulated values for read rows and bytes
  *  are collected from all participating servers and accumulated locally.
  */

/// Used both for maximum allowed values and for counters of current accumulated values.
template <typename Counter>        /// either size_t or std::atomic<size_t>
struct QuotaValues
{
    /// Zero values (for maximums) means no limit.
    Counter queries;                /// Number of queries.
    Counter errors;                 /// Number of queries with exceptions.
    Counter result_rows;            /// Number of rows returned as result.
    Counter result_bytes;           /// Number of bytes returned as result.
    Counter read_rows;              /// Number of rows read from tables.
    Counter read_bytes;             /// Number of bytes read from tables.
    Counter execution_time_usec;    /// Total amount of query execution time in microseconds.

    QuotaValues()
    {
        clear();
    }

    QuotaValues(const QuotaValues & rhs)
    {
        tuple() = rhs.tuple();
    }

    QuotaValues & operator=(const QuotaValues & rhs)
    {
        tuple() = rhs.tuple();
        return *this;
    }

    void clear()
    {
        tuple() = std::make_tuple(0, 0, 0, 0, 0, 0, 0);
    }

    void initFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config);

    bool operator== (const QuotaValues & rhs) const
    {
        return tuple() == rhs.tuple();
    }

private:
    auto tuple()
    {
        return std::forward_as_tuple(queries, errors, result_rows, result_bytes, read_rows, read_bytes, execution_time_usec);
    }

    auto tuple() const
    {
        return std::make_tuple(queries, errors, result_rows, result_bytes, read_rows, read_bytes, execution_time_usec);
    }
};

template <>
inline auto QuotaValues<std::atomic<size_t>>::tuple() const
{
    return std::make_tuple(
        queries.load(std::memory_order_relaxed),
        errors.load(std::memory_order_relaxed),
        result_rows.load(std::memory_order_relaxed),
        result_bytes.load(std::memory_order_relaxed),
        read_rows.load(std::memory_order_relaxed),
        read_bytes.load(std::memory_order_relaxed),
        execution_time_usec.load(std::memory_order_relaxed));
}


/// Time, rounded down to start of interval; limits for that interval and accumulated values.
struct QuotaForInterval
{
    std::atomic<time_t> rounded_time {0};
    size_t duration = 0;
    bool randomize = false;
    time_t offset = 0;        /// Offset of interval for randomization (to avoid DoS if intervals for many users end at one time).
    QuotaValues<size_t> max;
    QuotaValues<std::atomic<size_t>> used;

    QuotaForInterval() = default;
    QuotaForInterval(time_t duration_) : duration(duration_) {}

    void initFromConfig(const String & config_elem, time_t duration_, bool randomize_, time_t offset_, Poco::Util::AbstractConfiguration & config);

    /// Increase current value.
    void addQuery() noexcept;
    void addError() noexcept;

    /// Check if quota is already exceeded. If that, throw an exception.
    void checkExceeded(time_t current_time, const String & quota_name, const String & user_name);

    /// Check corresponding value. If exceeded, throw an exception. Otherwise, increase that value.
    void checkAndAddResultRowsBytes(time_t current_time, const String & quota_name, const String & user_name, size_t rows, size_t bytes);
    void checkAndAddReadRowsBytes(time_t current_time, const String & quota_name, const String & user_name, size_t rows, size_t bytes);
    void checkAndAddExecutionTime(time_t current_time, const String & quota_name, const String & user_name, Poco::Timespan amount);

    /// Get a text, describing what quota is exceeded.
    String toString() const;

    /// Only compare configuration, not accumulated (used) values or random offsets.
    bool operator== (const QuotaForInterval & rhs) const
    {
        return randomize == rhs.randomize
            && duration == rhs.duration
            && max == rhs.max;
    }

    QuotaForInterval & operator= (const QuotaForInterval & rhs)
    {
        rounded_time.store(rhs.rounded_time.load(std::memory_order_relaxed));
        duration = rhs.duration;
        randomize = rhs.randomize;
        offset = rhs.offset;
        max = rhs.max;
        used = rhs.used;
        return *this;
    }

    QuotaForInterval(const QuotaForInterval & rhs)
    {
        *this = rhs;
    }

private:
    /// Reset counters of used resources, if interval for quota is expired.
    void updateTime(time_t current_time);
    void check(size_t max_amount, size_t used_amount,
        const String & quota_name, const String & user_name, const char * resource_name);
};


struct Quota;

/// Length of interval -> quota: maximum allowed and currently accumulated values for that interval (example: 3600 -> values for current hour).
class QuotaForIntervals
{
private:
    /// While checking, will walk through intervals in order of decreasing size - from largest to smallest.
    /// To report first about largest interval on what quota was exceeded.
    using Container = std::map<size_t, QuotaForInterval>;
    Container cont;

    std::string quota_name;
    std::string user_name;    /// user name is set only for current counters for user, not for object that contain maximum values (limits).

public:
    QuotaForIntervals(const std::string & quota_name_, const std::string & user_name_)
        : quota_name(quota_name_), user_name(user_name_) {}

    QuotaForIntervals(const QuotaForIntervals & other, const std::string & user_name_)
        : QuotaForIntervals(other)
    {
        user_name = user_name_;
    }

    QuotaForIntervals() = default;
    QuotaForIntervals(const QuotaForIntervals & other) = default;

    /// Is there at least one interval for counting quota?
    bool empty() const
    {
        return cont.empty();
    }

    void initFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config, pcg64 & rng);

    /// Set maximum values (limits) from passed argument.
    /// Remove intervals that does not exist in argument. Add intervals from argument, that we don't have.
    void setMax(const QuotaForIntervals & quota);

    void addQuery() noexcept;
    void addError() noexcept;

    void checkExceeded(time_t current_time);

    void checkAndAddResultRowsBytes(time_t current_time, size_t rows, size_t bytes);
    void checkAndAddReadRowsBytes(time_t current_time, size_t rows, size_t bytes);
    void checkAndAddExecutionTime(time_t current_time, Poco::Timespan amount);

    /// Get text, describing what part of quota has been exceeded.
    String toString() const;

    bool hasEqualConfiguration(const QuotaForIntervals & rhs) const
    {
        return cont == rhs.cont && quota_name == rhs.quota_name;
    }
};

using QuotaForIntervalsPtr = std::shared_ptr<QuotaForIntervals>;


/// Quota key -> quotas (max and current values) for intervals. If quota doesn't have keys, then values stored at key 0.
struct Quota
{
    using Container = std::unordered_map<UInt64, QuotaForIntervalsPtr>;

    String name;

    /// Maximum values from config.
    QuotaForIntervals max;
    /// Maximum and accumulated values for different keys.
    /// For all keys, maximum values are the same and taken from 'max'.
    Container quota_for_keys;
    std::mutex mutex;

    bool is_keyed = false;

    /// If the quota is not keyed, but the user passed some key, ignore it instead of throwing exception.
    /// For transitional periods, when you want to enable quota keys
    ///  - first, enable passing keys from your application, then make quota keyed in ClickHouse users config.
    bool ignore_key_if_not_keyed = false;

    bool keyed_by_ip = false;

    void loadFromConfig(const String & config_elem, const String & name_, Poco::Util::AbstractConfiguration & config, pcg64 & rng);
    QuotaForIntervalsPtr get(const String & quota_key, const String & user_name, const Poco::Net::IPAddress & ip);
};


class Quotas
{
private:
    /// Name of quota -> quota.
    using Container = std::unordered_map<String, Quota>;
    Container cont;

public:
    void loadFromConfig(Poco::Util::AbstractConfiguration & config);
    QuotaForIntervalsPtr get(const String & name, const String & quota_key,
        const String & user_name, const Poco::Net::IPAddress & ip);
};

}
