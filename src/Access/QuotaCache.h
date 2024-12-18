#pragma once

#include <Access/EnabledQuota.h>
#include <base/scope_guard.h>
#include <memory>
#include <mutex>
#include <map>
#include <unordered_map>


namespace DB
{
class AccessControl;
struct Quota;
using QuotaPtr = std::shared_ptr<const Quota>;
struct RolesOrUsersSet;

/// Stores information how much amount of resources have been consumed and how much are left.
class QuotaCache
{
public:
    explicit QuotaCache(const AccessControl & access_control_);
    ~QuotaCache();

    std::shared_ptr<const EnabledQuota> getEnabledQuota(
        const UUID & user_id,
        const String & user_name,
        const boost::container::flat_set<UUID> & enabled_roles,
        const Poco::Net::IPAddress & address,
        const String & forwarded_address,
        const String & client_key,
        bool throw_if_client_key_empty);

    std::vector<QuotaUsage> getAllQuotasUsage() const;

private:
    using Interval = EnabledQuota::Interval;
    using Intervals = EnabledQuota::Intervals;

    struct QuotaInfo
    {
        QuotaInfo(const QuotaPtr & quota_, const UUID & quota_id_) { setQuota(quota_, quota_id_); }
        void setQuota(const QuotaPtr & quota_, const UUID & quota_id_);

        String calculateKey(const EnabledQuota & enabled_quota, bool throw_if_client_key_empty) const;
        boost::shared_ptr<const Intervals> getOrBuildIntervals(const String & key);
        boost::shared_ptr<const Intervals> rebuildIntervals(const String & key, std::chrono::system_clock::time_point current_time);
        void rebuildAllIntervals();

        QuotaPtr quota;
        UUID quota_id;
        const RolesOrUsersSet * roles = nullptr;
        std::unordered_map<String /* quota key */, boost::shared_ptr<const Intervals>> key_to_intervals;
    };

    void ensureAllQuotasRead();
    void quotaAddedOrChanged(const UUID & quota_id, const std::shared_ptr<const Quota> & new_quota);
    void quotaRemoved(const UUID & quota_id);
    void chooseQuotaToConsume();
    void chooseQuotaToConsumeFor(EnabledQuota & enabled_quota, bool throw_if_client_key_empty);

    const AccessControl & access_control;
    mutable std::mutex mutex;
    std::unordered_map<UUID /* quota id */, QuotaInfo> all_quotas;
    bool all_quotas_read = false;
    scope_guard subscription;
    std::map<EnabledQuota::Params, std::weak_ptr<EnabledQuota>> enabled_quotas;
};
}
