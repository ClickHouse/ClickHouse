#pragma once

#include <Access/QueryMaskingRule.h>
#include <base/defines.h>
#include <base/scope_guard.h>
#include <mutex>
#include <unordered_map>


namespace DB
{
class AccessControl;

/// Publishes the query masking rules stored in the access storages to SensitiveDataMasker.
class QueryMaskingRulesCache
{
public:
    explicit QueryMaskingRulesCache(const AccessControl & access_control_);
    ~QueryMaskingRulesCache();

    /// Reads all the rules from the access storages, publishes them and keeps them published.
    /// Throws if a rule cannot be read.
    void ensureAllRulesRead();

private:
    void publishLocked() TSA_REQUIRES(mutex);

    const AccessControl & access_control;
    std::unordered_map<UUID, QueryMaskingRulePtr> rules TSA_GUARDED_BY(mutex);
    bool all_rules_read TSA_GUARDED_BY(mutex) = false;
    /// Stays set after a failed publish, so the next notification pass retries it.
    bool needs_publish TSA_GUARDED_BY(mutex) = false;
    std::mutex mutex;
    scope_guard subscription;
};

}
