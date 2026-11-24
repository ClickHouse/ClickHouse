#include <Access/QueryMaskingRulesCache.h>
#include <Access/AccessControl.h>
#include <Common/Exception.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/quoteString.h>
#include <base/sort.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int LOGICAL_ERROR;
}


QueryMaskingRulesCache::QueryMaskingRulesCache(const AccessControl & access_control_)
    : access_control(access_control_)
{
}

QueryMaskingRulesCache::~QueryMaskingRulesCache() = default;


void QueryMaskingRulesCache::ensureAllRulesRead()
{
    std::lock_guard lock{mutex};
    if (all_rules_read)
        return;

    subscription = access_control.subscribeForChanges<QueryMaskingRule>(
        [this](const std::vector<AccessChangesNotifier::Change> & changes)
        {
            /// The changed rules are already stored and outlive the query that changed them:
            /// the server memory tracker accounts for them, but no memory limit may refuse them.
            MemoryTrackerBlockerInThread not_refused_by_memory_limits(VariableContext::Global);
            std::lock_guard lock2{mutex};
            for (const auto & change : changes)
            {
                if (change.entity)
                    rules[change.id] = typeid_cast<QueryMaskingRulePtr>(change.entity);
                else
                    rules.erase(change.id);
            }

            if (!changes.empty())
                needs_publish = true;

            if (needs_publish)
                publishLocked();
        });

    std::unordered_map<UUID, QueryMaskingRulePtr> new_rules;
    for (const UUID & id : access_control.findAll<QueryMaskingRule>())
    {
        if (auto rule = access_control.tryRead<QueryMaskingRule>(id))
            new_rules.emplace(id, rule);
    }

    rules = std::move(new_rules);
    needs_publish = true;
    publishLocked();
    all_rules_read = true;
}


void QueryMaskingRulesCache::publishLocked()
{
    std::vector<std::pair<UUID, QueryMaskingRulePtr>> sorted_rules(rules.begin(), rules.end());
    ::sort(sorted_rules.begin(), sorted_rules.end(), [](const auto & lhs, const auto & rhs)
    {
        return std::tie(lhs.second->getName(), lhs.first) < std::tie(rhs.second->getName(), rhs.first);
    });

    auto masker = std::make_unique<SensitiveDataMasker>();
    for (const auto & [id, rule] : sorted_rules)
    {
        try
        {
            masker->addMaskingRule(rule->getName(), rule->regexp, rule->replace, /*throw_on_match=*/ false);
        }
        catch (const Exception & e)
        {
            /// The original message quotes the whole regexp.
            if (e.code() == ErrorCodes::CANNOT_COMPILE_REGEXP)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Query masking rule {} passed validation but cannot be compiled", backQuote(rule->getName()));
            throw;
        }
    }

    SensitiveDataMasker::setSQLDefinedRules(std::move(masker));
    needs_publish = false;
}

}
