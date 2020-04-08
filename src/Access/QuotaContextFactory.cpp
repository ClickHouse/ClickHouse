#include <Access/QuotaContext.h>
#include <Access/QuotaContextFactory.h>
#include <Access/AccessControlManager.h>
#include <Common/Exception.h>
#include <Common/thread_local_rng.h>
#include <ext/range.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/lower_bound.hpp>
#include <boost/range/algorithm/stable_sort.hpp>
#include <boost/range/algorithm_ext/erase.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int QUOTA_REQUIRES_CLIENT_KEY;
}


namespace
{
    std::chrono::system_clock::duration randomDuration(std::chrono::seconds max)
    {
        auto count = std::chrono::duration_cast<std::chrono::system_clock::duration>(max).count();
        std::uniform_int_distribution<Int64> distribution{0, count - 1};
        return std::chrono::system_clock::duration(distribution(thread_local_rng));
    }
}


void QuotaContextFactory::QuotaInfo::setQuota(const QuotaPtr & quota_, const UUID & quota_id_)
{
    quota = quota_;
    quota_id = quota_id_;

    boost::range::copy(quota->roles, std::inserter(roles, roles.end()));
    all_roles = quota->all_roles;
    boost::range::copy(quota->except_roles, std::inserter(except_roles, except_roles.end()));

    rebuildAllIntervals();
}


bool QuotaContextFactory::QuotaInfo::canUseWithContext(const QuotaContext & context) const
{
    if (roles.count(context.user_name))
        return true;

    if (all_roles && !except_roles.count(context.user_name))
        return true;

    return false;
}


String QuotaContextFactory::QuotaInfo::calculateKey(const QuotaContext & context) const
{
    using KeyType = Quota::KeyType;
    switch (quota->key_type)
    {
        case KeyType::NONE:
            return "";
        case KeyType::USER_NAME:
            return context.user_name;
        case KeyType::IP_ADDRESS:
            return context.address.toString();
        case KeyType::CLIENT_KEY:
        {
            if (!context.client_key.empty())
                return context.client_key;
            throw Exception(
                "Quota " + quota->getName() + " (for user " + context.user_name + ") requires a client supplied key.",
                ErrorCodes::QUOTA_REQUIRES_CLIENT_KEY);
        }
        case KeyType::CLIENT_KEY_OR_USER_NAME:
        {
            if (!context.client_key.empty())
                return context.client_key;
            return context.user_name;
        }
        case KeyType::CLIENT_KEY_OR_IP_ADDRESS:
        {
            if (!context.client_key.empty())
                return context.client_key;
            return context.address.toString();
        }
    }
    __builtin_unreachable();
}


std::shared_ptr<const QuotaContext::Intervals> QuotaContextFactory::QuotaInfo::getOrBuildIntervals(const String & key)
{
    auto it = key_to_intervals.find(key);
    if (it != key_to_intervals.end())
        return it->second;
    return rebuildIntervals(key);
}


void QuotaContextFactory::QuotaInfo::rebuildAllIntervals()
{
    for (const String & key : key_to_intervals | boost::adaptors::map_keys)
        rebuildIntervals(key);
}


std::shared_ptr<const QuotaContext::Intervals> QuotaContextFactory::QuotaInfo::rebuildIntervals(const String & key)
{
    auto new_intervals = std::make_shared<Intervals>();
    new_intervals->quota_name = quota->getName();
    new_intervals->quota_id = quota_id;
    new_intervals->quota_key = key;
    auto & intervals = new_intervals->intervals;
    intervals.reserve(quota->all_limits.size());
    constexpr size_t MAX_RESOURCE_TYPE = Quota::MAX_RESOURCE_TYPE;
    for (const auto & limits : quota->all_limits)
    {
        intervals.emplace_back();
        auto & interval = intervals.back();
        interval.duration = limits.duration;
        std::chrono::system_clock::time_point end_of_interval{};
        interval.randomize_interval = limits.randomize_interval;
        if (limits.randomize_interval)
            end_of_interval += randomDuration(limits.duration);
        interval.end_of_interval = end_of_interval.time_since_epoch();
        for (auto resource_type : ext::range(MAX_RESOURCE_TYPE))
        {
            interval.max[resource_type] = limits.max[resource_type];
            interval.used[resource_type] = 0;
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
            auto & current_interval = *lower_bound;
            for (auto resource_type : ext::range(MAX_RESOURCE_TYPE))
            {
                new_interval.used[resource_type].store(current_interval.used[resource_type].load());
                new_interval.end_of_interval.store(current_interval.end_of_interval.load());
            }
        }
        it->second = new_intervals;
    }

    return new_intervals;
}


QuotaContextFactory::QuotaContextFactory(const AccessControlManager & access_control_manager_)
    : access_control_manager(access_control_manager_)
{
}


QuotaContextFactory::~QuotaContextFactory()
{
}


std::shared_ptr<QuotaContext> QuotaContextFactory::createContext(const String & user_name, const Poco::Net::IPAddress & address, const String & client_key)
{
    std::lock_guard lock{mutex};
    ensureAllQuotasRead();
    auto context = ext::shared_ptr_helper<QuotaContext>::create(user_name, address, client_key);
    contexts.push_back(context);
    chooseQuotaForContext(context);
    return context;
}


void QuotaContextFactory::ensureAllQuotasRead()
{
    /// `mutex` is already locked.
    if (all_quotas_read)
        return;
    all_quotas_read = true;

    subscription = access_control_manager.subscribeForChanges<Quota>(
        [&](const UUID & id, const AccessEntityPtr & entity)
        {
            if (entity)
                quotaAddedOrChanged(id, typeid_cast<QuotaPtr>(entity));
            else
                quotaRemoved(id);
        });

    for (const UUID & quota_id : access_control_manager.findAll<Quota>())
    {
        auto quota = access_control_manager.tryRead<Quota>(quota_id);
        if (quota)
            all_quotas.emplace(quota_id, QuotaInfo(quota, quota_id));
    }
}


void QuotaContextFactory::quotaAddedOrChanged(const UUID & quota_id, const std::shared_ptr<const Quota> & new_quota)
{
    std::lock_guard lock{mutex};
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
    chooseQuotaForAllContexts();
}


void QuotaContextFactory::quotaRemoved(const UUID & quota_id)
{
    std::lock_guard lock{mutex};
    all_quotas.erase(quota_id);
    chooseQuotaForAllContexts();
}


void QuotaContextFactory::chooseQuotaForAllContexts()
{
    /// `mutex` is already locked.
    boost::range::remove_erase_if(
        contexts,
        [&](const std::weak_ptr<QuotaContext> & weak)
        {
            auto context = weak.lock();
            if (!context)
                return true; // remove from the `contexts` list.
            chooseQuotaForContext(context);
            return false; // keep in the `contexts` list.
        });
}

void QuotaContextFactory::chooseQuotaForContext(const std::shared_ptr<QuotaContext> & context)
{
    /// `mutex` is already locked.
    std::shared_ptr<const Intervals> intervals;
    for (auto & info : all_quotas | boost::adaptors::map_values)
    {
        if (info.canUseWithContext(*context))
        {
            String key = info.calculateKey(*context);
            intervals = info.getOrBuildIntervals(key);
            break;
        }
    }

    if (!intervals)
        intervals = std::make_shared<Intervals>(); /// No quota == no limits.

    std::atomic_store(&context->atomic_intervals, intervals);
}


std::vector<QuotaUsageInfo> QuotaContextFactory::getUsageInfo() const
{
    std::lock_guard lock{mutex};
    std::vector<QuotaUsageInfo> all_infos;
    auto current_time = std::chrono::system_clock::now();
    for (const auto & info : all_quotas | boost::adaptors::map_values)
    {
        for (const auto & intervals : info.key_to_intervals | boost::adaptors::map_values)
            all_infos.push_back(intervals->getUsageInfo(current_time));
    }
    return all_infos;
}
}
