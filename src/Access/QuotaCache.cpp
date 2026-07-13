#include <Access/EnabledQuota.h>
#include <Access/Quota.h>
#include <Access/QuotaCache.h>
#include <Access/QuotaUsage.h>
#include <Access/AccessControl.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <base/range.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/lower_bound.hpp>
#include <boost/range/algorithm/stable_sort.hpp>
#include <boost/smart_ptr/make_shared.hpp>


namespace ProfileEvents
{
    extern const Event QuotaCacheRecalculations;
    extern const Event QuotaCacheRecalculationMicroseconds;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int QUOTA_REQUIRES_CLIENT_KEY;
    extern const int LOGICAL_ERROR;
}


void QuotaCache::QuotaInfo::setQuota(const QuotaPtr & quota_, const UUID & quota_id_)
{
    quota = quota_;
    quota_id = quota_id_;
    roles = &quota->to_roles;
    rebuildAllIntervals();
}


String QuotaCache::QuotaInfo::calculateKey(const EnabledQuota & enabled, bool throw_if_client_key_empty) const
{
    const auto & params = enabled.params;
    auto mask_address = [this](const Poco::Net::IPAddress & addr) -> String
    {
        using Family = Poco::Net::IPAddress::Family;
        /// An IPv4-mapped IPv6 address (such as `::ffff:192.0.2.10`) represents an IPv4 client,
        /// so it is governed by `IPV4_PREFIX_BITS` and never by `IPV6_PREFIX_BITS`.
        if (addr.family() == Family::IPv6 && addr.isIPv4Mapped())
        {
            /// A /0 prefix is also valid: it puts every IP into a single shared bucket.
            if (quota->ipv4_prefix_bits)
            {
                /// Normalize to a native IPv4 address so that masked mapped clients share quota
                /// buckets with plain IPv4 clients.
                Poco::Net::IPAddress native(reinterpret_cast<const char *>(addr.addr()) + 12, 4);
                Poco::Net::IPAddress mask(static_cast<unsigned>(*quota->ipv4_prefix_bits), Family::IPv4);
                return (native & mask).toString();
            }
            /// No IPv4 prefix configured: keep the original representation to preserve the
            /// pre-prefix-bits quota key. In particular, do not let `IPV6_PREFIX_BITS` mask an
            /// IPv4 client.
            return addr.toString();
        }

        const auto fam = addr.family();
        /// A /0 prefix is also valid: it puts every IP into a single shared bucket.
        if (fam == Family::IPv4)
        {
            if (quota->ipv4_prefix_bits)
            {
                Poco::Net::IPAddress mask(static_cast<unsigned>(*quota->ipv4_prefix_bits), Family::IPv4);
                Poco::Net::IPAddress masked = addr & mask;
                return masked.toString();
            }
        }
        else
        {
            if (quota->ipv6_prefix_bits)
            {
                Poco::Net::IPAddress mask(static_cast<unsigned>(*quota->ipv6_prefix_bits), Family::IPv6);
                Poco::Net::IPAddress masked = addr & mask;
                return masked.toString();
            }
        }
        return addr.toString();
    };
    switch (quota->key_type)
    {
        case QuotaKeyType::NONE:
        {
            return "";
        }
        case QuotaKeyType::USER_NAME:
        {
            return params.user_name;
        }
        case QuotaKeyType::IP_ADDRESS:
        {
            return mask_address(params.client_address);
        }
        case QuotaKeyType::FORWARDED_IP_ADDRESS:
        {
            /// Fast path: when no prefix masking is configured, return the raw address
            /// without parsing into `Poco::Net::IPAddress`. This matches pre-prefix-bits
            /// behavior and avoids extra work on the per-query hot path for users who
            /// did not opt into prefix masking.
            if (!quota->ipv4_prefix_bits && !quota->ipv6_prefix_bits)
                return params.forwarded_address;

            if (!params.forwarded_address.empty())
            {
                try
                {
                    Poco::Net::IPAddress forwarded_ip(params.forwarded_address);
                    return mask_address(forwarded_ip);
                }
                catch (...) /// Ok: a malformed X-Forwarded-For value should not fail the query; fall back to using the raw string as the quota key, matching pre-prefix-bits behavior.
                {
                    return params.forwarded_address;
                }
            }
            return params.forwarded_address;
        }
        case QuotaKeyType::CLIENT_KEY:
        {
            if (!params.client_key.empty())
                return params.client_key;

            if (throw_if_client_key_empty)
                throw Exception(
                    ErrorCodes::QUOTA_REQUIRES_CLIENT_KEY,
                    "Quota {} (for user {}) requires a client supplied key.",
                    quota->getName(),
                    params.user_name);
            return ""; // Authentication quota has no client key at time of authentication.
        }
        case QuotaKeyType::CLIENT_KEY_OR_USER_NAME:
        {
            if (!params.client_key.empty())
                return params.client_key;
            return params.user_name;
        }
        case QuotaKeyType::CLIENT_KEY_OR_IP_ADDRESS:
        {
            if (!params.client_key.empty())
                return params.client_key;
            return mask_address(params.client_address);
        }
        case QuotaKeyType::NORMALIZED_QUERY_HASH:
        {
            /// For NORMALIZED_QUERY_HASH, the key is resolved per-query via IntervalResolver.
            /// Return a placeholder key for the shared session-level intervals.
            return params.user_name;
        }
        case QuotaKeyType::MAX: break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected quota key type: {}", static_cast<int>(quota->key_type));
}


boost::shared_ptr<const EnabledQuota::Intervals> QuotaCache::QuotaInfo::getOrBuildIntervals(const String & key)
{
    auto it = key_to_intervals.find(key);
    if (it != key_to_intervals.end())
        return it->second;
    return rebuildIntervals(key, std::chrono::system_clock::now());
}


void QuotaCache::QuotaInfo::rebuildAllIntervals()
{
    if (key_to_intervals.empty())
        return;
    auto current_time = std::chrono::system_clock::now();
    for (const String & key : key_to_intervals | boost::adaptors::map_keys)
        rebuildIntervals(key, current_time);
}


boost::shared_ptr<const EnabledQuota::Intervals> QuotaCache::QuotaInfo::rebuildIntervals(const String & key, std::chrono::system_clock::time_point current_time)
{
    auto new_intervals = boost::make_shared<Intervals>();
    new_intervals->quota_name = quota->getName();
    new_intervals->quota_id = quota_id;
    new_intervals->quota_key = key;
    auto & intervals = new_intervals->intervals;
    intervals.reserve(quota->all_limits.size());
    for (const auto & limits : quota->all_limits)
    {
        intervals.emplace_back(limits.duration, limits.randomize_interval, current_time);
        auto & interval = intervals.back();
        for (auto quota_type : collections::range(QuotaType::MAX))
        {
            auto quota_type_i = static_cast<size_t>(quota_type);
            if (limits.max[quota_type_i])
                interval.max[quota_type_i] = *limits.max[quota_type_i];
            interval.used[quota_type_i] = 0;
        }
    }

    /// Order intervals by durations from largest to smallest.
    /// To report first about largest interval on what quota was exceeded.
    struct GreaterByDuration
    {
        bool operator()(const Interval & lhs, const Interval & rhs) const { return lhs.duration > rhs.duration; }
    };
    boost::range::stable_sort(intervals, GreaterByDuration{});

    auto it = key_to_intervals.find(key);
    if (it == key_to_intervals.end())
    {
        /// Just put new intervals into the map.
        key_to_intervals.try_emplace(key, new_intervals);
    }
    else
    {
        /// We need to keep usage information from the old intervals.
        const auto & old_intervals = it->second->intervals;
        for (auto & new_interval : new_intervals->intervals)
        {
            /// Check if an interval with the same duration is already in use.
            auto lower_bound = boost::range::lower_bound(old_intervals, new_interval, GreaterByDuration{});
            if ((lower_bound == old_intervals.end()) || (lower_bound->duration != new_interval.duration))
                continue;

            /// Found an interval with the same duration, we need to copy its usage information to `result`.
            const auto & current_interval = *lower_bound;
            for (auto quota_type : collections::range(QuotaType::MAX))
            {
                auto quota_type_i = static_cast<size_t>(quota_type);
                new_interval.used[quota_type_i].store(current_interval.used[quota_type_i].load());
                new_interval.end_of_interval.store(current_interval.end_of_interval.load());
            }
        }
        it->second = new_intervals;
    }

    return new_intervals;
}


QuotaCache::QuotaCache(const AccessControl & access_control_)
    : access_control(access_control_)
{
}

QuotaCache::~QuotaCache() = default;


std::shared_ptr<const EnabledQuota> QuotaCache::getEnabledQuota(
    const UUID & user_id,
    const String & user_name,
    const boost::container::flat_set<UUID> & enabled_roles,
    const std::shared_ptr<Poco::Net::IPAddress> & client_address,
    const String & forwarded_address,
    const String & client_key,
    bool throw_if_client_key_empty)
{
    std::lock_guard lock{mutex};
    ensureAllQuotasRead();

    EnabledQuota::Params params;
    params.user_id = user_id;
    params.user_name = user_name;
    params.enabled_roles = enabled_roles;
    params.client_address = *client_address;
    params.forwarded_address = forwarded_address;
    params.client_key = client_key;
    auto it = enabled_quotas.find(params);
    if (it != enabled_quotas.end())
    {
        auto from_cache = it->second.lock();
        if (from_cache)
            return from_cache;
        enabled_quotas.erase(it);
    }

    auto res = std::shared_ptr<EnabledQuota>(new EnabledQuota(params));
    enabled_quotas.emplace(std::move(params), res);
    chooseQuotaToConsumeFor(*res, throw_if_client_key_empty);
    return res;
}

void QuotaCache::ensureAllQuotasRead()
{
    /// `mutex` is already locked.
    if (all_quotas_read)
        return;

    subscription = access_control.subscribeForChanges<Quota>(
        [this](const std::vector<AccessChangesNotifier::Change> & changes)
        {
            std::lock_guard lock{mutex};
            for (const auto & change : changes)
            {
                if (change.entity)
                    quotaAddedOrChanged(change.id, typeid_cast<QuotaPtr>(change.entity));
                else
                    quotaRemoved(change.id);
            }
            chooseQuotaToConsumeIfNeeded();
        });

    /// Start clean: a previous attempt may have thrown mid-scan.
    all_quotas.clear();
    for (const UUID & quota_id : access_control.findAll<Quota>())
    {
        auto quota = access_control.tryRead<Quota>(quota_id);
        if (quota)
            all_quotas.emplace(quota_id, QuotaInfo(quota, quota_id));
    }

    /// Set only after the subscription and the initial read succeed.
    all_quotas_read = true;
}


void QuotaCache::quotaAddedOrChanged(const UUID & quota_id, const std::shared_ptr<const Quota> & new_quota)
{
    /// `mutex` is already locked.
    auto it = all_quotas.find(quota_id);
    if (it == all_quotas.end())
    {
        it = all_quotas.emplace(quota_id, QuotaInfo(new_quota, quota_id)).first;
    }
    else
    {
        if (it->second.quota == new_quota)
            return;
    }

    auto & info = it->second;
    info.setQuota(new_quota, quota_id);
    need_choose_quota = true;
}


void QuotaCache::quotaRemoved(const UUID & quota_id)
{
    /// `mutex` is already locked.
    all_quotas.erase(quota_id);
    need_choose_quota = true;
}


void QuotaCache::chooseQuotaToConsumeIfNeeded()
{
    /// `mutex` is already locked.
    if (!need_choose_quota)
        return;
    /// Clear the flag only after a successful rebuild, so a throwing recompute is retried next batch.
    chooseQuotaToConsume();
    need_choose_quota = false;
}


void QuotaCache::chooseQuotaToConsume()
{
    /// `mutex` is already locked.

    ProfileEvents::increment(ProfileEvents::QuotaCacheRecalculations);
    Stopwatch watch;
    for (auto i = enabled_quotas.begin(), e = enabled_quotas.end(); i != e;)
    {
        auto elem = i->second.lock();
        if (!elem)
            i = enabled_quotas.erase(i);
        else
        {
            chooseQuotaToConsumeFor(*elem, true);
            ++i;
        }
    }

    const auto elapsed_ms = watch.elapsedMilliseconds();
    ProfileEvents::increment(ProfileEvents::QuotaCacheRecalculationMicroseconds, watch.elapsedMicroseconds());
    /// O(enabled sets * quotas), under `mutex` that the ContextAccess build path also takes.
    if (elapsed_ms >= 1000)
        LOG_DEBUG(getLogger("QuotaCache"), "Re-chose quotas for {} enabled set(s) over {} quotas in {} ms", enabled_quotas.size(), all_quotas.size(), elapsed_ms);
    else
        LOG_TRACE(getLogger("QuotaCache"), "Re-chose quotas for {} enabled set(s) over {} quotas in {} ms", enabled_quotas.size(), all_quotas.size(), elapsed_ms);
}

void QuotaCache::chooseQuotaToConsumeFor(EnabledQuota & enabled, bool throw_if_client_key_empty)
{
    /// `mutex` is already locked.

    /// A user/context may be governed by several quotas at once. Collect every quota whose
    /// `APPLY TO` matches; all of them are enforced together by `EnabledQuota`.
    auto new_quotas = boost::make_shared<Quotas>();
    for (auto & info : all_quotas | boost::adaptors::map_values)
    {
        if (!info.roles->match(enabled.params.user_id, enabled.params.enabled_roles))
            continue;

        String key = info.calculateKey(enabled, throw_if_client_key_empty);
        auto single = std::make_unique<SingleQuota>();
        single->intervals = info.getOrBuildIntervals(key);

        /// For NORMALIZED_QUERY_HASH keyed quotas, set up a resolver callback
        /// so that EnabledQuota can lazily resolve intervals per query hash.
        if (info.quota->key_type == QuotaKeyType::NORMALIZED_QUERY_HASH)
        {
            UUID found_quota_id = info.quota_id;
            single->interval_resolver = [this, found_quota_id](const String & hash_key) -> boost::shared_ptr<const Intervals>
            {
                std::lock_guard lock(mutex);
                auto it = all_quotas.find(found_quota_id);
                if (it == all_quotas.end())
                    return nullptr;
                return it->second.getOrBuildIntervals(hash_key);
            };
        }

        new_quotas->push_back(std::move(single));
    }

    /// Publish the new set: store `quotas` (always non-null, possibly empty) before updating the
    /// `empty` flag, so a concurrent reader never observes `empty == false` with a stale set.
    bool is_empty = new_quotas->empty();
    enabled.quotas.store(new_quotas);
    enabled.empty = is_empty;
}


std::vector<QuotaUsage> QuotaCache::getAllQuotasUsage() const
{
    std::lock_guard lock{mutex};
    std::vector<QuotaUsage> all_usage;
    auto current_time = std::chrono::system_clock::now();
    for (const auto & info : all_quotas | boost::adaptors::map_values)
    {
        for (const auto & intervals : info.key_to_intervals | boost::adaptors::map_values)
        {
            auto usage = intervals->getUsage(current_time);
            if (usage)
                all_usage.push_back(std::move(usage).value());
        }
    }
    return all_usage;
}

}
