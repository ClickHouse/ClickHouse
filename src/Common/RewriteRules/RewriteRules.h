#pragma once

#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <boost/noncopyable.hpp>

#include <Common/RewriteRules/RewriteRulesStorage.h>
#include <Common/RewriteRules/RewriteRuleObject.h>

namespace DB
{

class RewriteRules : boost::noncopyable
{
public:
    static RewriteRules & instance();

    ~RewriteRules();

    bool exists(const std::string & rule_name) const;

    RewriteRuleObjectPtr get(const std::string & rule_name) const;

    RewriteRuleObjectPtr tryGet(const std::string & rule_name) const;

    RewriteRuleObjectsMap getAll() const;

    void createRule(const ASTCreateRewriteRuleQuery & query);

    void removeRule(const ASTDropRewriteRuleQuery & query);

    void updateRule(const ASTAlterRewriteRuleQuery & query);

    void addLog(
        const std::string& original_query,
        const Array& applied_rules,
        const std::string& resulting_query
    );

    std::vector<MutableRewriteRuleLogPtr> getLogs() const;

    bool usesReplicatedStorage();

    bool loadIfNot();

    void reload();

    void shutdown();

protected:
    mutable RewriteRuleObjectsMap loaded_rewrite_rules;
    mutable std::mutex mutex;
    mutable std::vector<MutableRewriteRuleLogPtr> logs;

    const LoggerPtr log = getLogger("RewriteRule");

    bool loaded = false;
    std::atomic<bool> shutdown_called = false;
    std::unique_ptr<RewriteRulesStorage> storage;
    BackgroundSchedulePoolTaskHolder update_task;

    bool exists(
        const std::string & rule_name,
        std::lock_guard<std::mutex> & lock) const;

    MutableRewriteRuleObjectPtr getMutable(const std::string & rule_name, std::lock_guard<std::mutex> & lock) const;

    void add(const std::string & rule_name, MutableRewriteRuleObjectPtr rule, std::lock_guard<std::mutex> & lock);

    void add(RewriteRuleObjectsMap rules, std::lock_guard<std::mutex> & lock);

    void remove(const std::string & rule_name, std::lock_guard<std::mutex> & lock);

    MutableRewriteRuleObjectPtr tryGet(const std::string & rule_name, std::lock_guard<std::mutex> & lock) const;

    void updateFunc();
};

}
