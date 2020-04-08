#pragma once

#include <Access/QuotaContext.h>
#include <Access/IAccessStorage.h>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>


namespace DB
{
class AccessControlManager;


/// Stores information how much amount of resources have been consumed and how much are left.
class QuotaContextFactory
{
public:
    QuotaContextFactory(const AccessControlManager & access_control_manager_);
    ~QuotaContextFactory();

    QuotaContextPtr createContext(const String & user_name, const Poco::Net::IPAddress & address, const String & client_key);
    std::vector<QuotaUsageInfo> getUsageInfo() const;

private:
    using Interval = QuotaContext::Interval;
    using Intervals = QuotaContext::Intervals;

    struct QuotaInfo
    {
        QuotaInfo(const QuotaPtr & quota_, const UUID & quota_id_) { setQuota(quota_, quota_id_); }
        void setQuota(const QuotaPtr & quota_, const UUID & quota_id_);

        bool canUseWithContext(const QuotaContext & context) const;
        String calculateKey(const QuotaContext & context) const;
        std::shared_ptr<const Intervals> getOrBuildIntervals(const String & key);
        std::shared_ptr<const Intervals> rebuildIntervals(const String & key);
        void rebuildAllIntervals();

        QuotaPtr quota;
        UUID quota_id;
        std::unordered_set<String> roles;
        bool all_roles = false;
        std::unordered_set<String> except_roles;
        std::unordered_map<String /* quota key */, std::shared_ptr<const Intervals>> key_to_intervals;
    };

    void ensureAllQuotasRead();
    void quotaAddedOrChanged(const UUID & quota_id, const std::shared_ptr<const Quota> & new_quota);
    void quotaRemoved(const UUID & quota_id);
    void chooseQuotaForAllContexts();
    void chooseQuotaForContext(const std::shared_ptr<QuotaContext> & context);

    const AccessControlManager & access_control_manager;
    mutable std::mutex mutex;
    std::unordered_map<UUID /* quota id */, QuotaInfo> all_quotas;
    bool all_quotas_read = false;
    IAccessStorage::SubscriptionPtr subscription;
    std::vector<std::weak_ptr<QuotaContext>> contexts;
};
}
