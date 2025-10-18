#include <Common/RewriteRules/RewriteRules.h>
#include <Core/Settings.h>
#include <base/sleep.h>
#include <Common/FieldVisitorToString.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int REWRITE_RULE_DOESNT_EXIST;
    extern const int REWRITE_RULE_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
}

RewriteRules & RewriteRules::instance()
{
    static RewriteRules instance;
    return instance;
}

RewriteRules::~RewriteRules()
{
    shutdown();
}

void RewriteRules::shutdown()
{
    shutdown_called = true;
    if (update_task)
        update_task->deactivate();
    storage.reset();
}

bool RewriteRules::exists(const std::string & rule_name) const
{
    std::lock_guard lock(mutex);
    return exists(rule_name, lock);
}

RewriteRuleObjectPtr RewriteRules::get(const std::string & rule_name) const
{
    std::lock_guard lock(mutex);
    auto rule = tryGet(rule_name, lock);
    if (!rule)
    {
        throw Exception(
            ErrorCodes::REWRITE_RULE_DOESNT_EXIST,
            "There is no rewrite rule `{}`",
            rule_name);
    }
    return rule;
}

RewriteRuleObjectPtr RewriteRules::tryGet(const std::string & rule_name) const
{
    std::lock_guard lock(mutex);
    return tryGet(rule_name, lock);
}

RewriteRuleObjectsMap RewriteRules::getAll() const
{
    std::lock_guard lock(mutex);
    return loaded_rewrite_rules;
}

bool RewriteRules::exists(const std::string & rule_name, std::lock_guard<std::mutex> &) const
{
    return loaded_rewrite_rules.contains(rule_name);
}

MutableRewriteRuleObjectPtr RewriteRules::tryGet(
    const std::string & rule_name,
    std::lock_guard<std::mutex> &) const
{
    if (auto it = loaded_rewrite_rules.find(rule_name);
        it != loaded_rewrite_rules.end())
        return it->second;
    return nullptr;
}

MutableRewriteRuleObjectPtr RewriteRules::getMutable(
    const std::string & rule_name,
    std::lock_guard<std::mutex> & lock) const
{
    auto rule = tryGet(rule_name, lock);
    if (!rule)
    {
        throw Exception(
            ErrorCodes::REWRITE_RULE_DOESNT_EXIST,
            "There is no rewrite rule `{}`",
            rule_name);
    }
    return rule;
}

void RewriteRules::add(
    const std::string & rule_name,
    MutableRewriteRuleObjectPtr rule,
    std::lock_guard<std::mutex> &)
{
    auto [it, inserted] = loaded_rewrite_rules.emplace(rule_name, rule);
    if (!inserted)
    {
        throw Exception(
            ErrorCodes::REWRITE_RULE_ALREADY_EXISTS,
            "A rewrite rule `{}` already exists",
            rule_name);
    }
}

void RewriteRules::add(RewriteRuleObjectsMap rules, std::lock_guard<std::mutex> & lock)
{
    for (const auto & [rule_name, rule] : rules)
        add(rule_name, rule, lock);
}

void RewriteRules::remove(const std::string & rule_name, std::lock_guard<std::mutex> &)
{
    loaded_rewrite_rules.erase(rule_name);
}

void RewriteRules::createRule(const ASTCreateRewriteRuleQuery & query)
{
    std::lock_guard lock(mutex);
    if (exists(query.rule_name, lock))
    {
        throw Exception(
            ErrorCodes::REWRITE_RULE_ALREADY_EXISTS,
            "A rewrite rule `{}` already exists",
            query.rule_name);
    }
    auto ptr = RewriteRuleObject::create(query);
    storage->create(ptr);
    add(query.rule_name, std::move(ptr), lock);
}

void RewriteRules::removeRule(const ASTDropRewriteRuleQuery & query)
{
    std::lock_guard lock(mutex);
    if (!exists(query.rule_name, lock))
    {
        throw Exception(
            ErrorCodes::REWRITE_RULE_DOESNT_EXIST,
            "A rewrite rule `{}` doesn't exists",
            query.rule_name);
    }
    storage->remove(query.rule_name);
    remove(query.rule_name, lock);
}

void RewriteRules::updateRule(const ASTAlterRewriteRuleQuery & query)
{
    std::lock_guard lock(mutex);
    if (!exists(query.rule_name, lock))
    {
        throw Exception(
            ErrorCodes::REWRITE_RULE_DOESNT_EXIST,
            "A rewrite rule `{}` doesn't exists",
            query.rule_name);
    }

    auto it = loaded_rewrite_rules.find(query.rule_name);
    if (it == loaded_rewrite_rules.end())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The rewrite rule {} unexpectedly does not exist.",
            query.rule_name);
    }
    auto ptr = RewriteRuleObject::create(query);
    storage->update(ptr);
    it->second = std::move(ptr);
}

void RewriteRules::addLog(
    const std::string& original_query,
    const Array& applied_rules,
    const std::string& resulting_query
)
{
    std::lock_guard lock(mutex);
    if (applied_rules.empty())
    {
        return;
    }
    logs.push_back(RewriteRuleLog::create(original_query, applied_rules, resulting_query));
}

std::vector<MutableRewriteRuleLogPtr> RewriteRules::getLogs() const
{
    std::lock_guard lock(mutex);
    return logs;
}

bool RewriteRules::loadIfNot()
{
    std::lock_guard lock(mutex);
    if (loaded)
        return false;

    auto context = Context::getGlobalContextInstance();
    storage = RewriteRulesStorage::create(context);
    auto rules = storage->getAll();
    add(std::move(rules), lock);

    if (storage->isReplicated())
    {
        update_task = context->getSchedulePool().createTask("RewriteRuleReplicatedStorage", [this]{ updateFunc(); });
        update_task->activate();
        update_task->schedule();
    }

    loaded = true;
    return true;
}

void RewriteRules::reload()
{
    loadIfNot();
}

void RewriteRules::updateFunc()
{
    LOG_TRACE(log, "Rewrite/query rules background updating thread started");

    while (!shutdown_called.load())
    {
        if (storage->waitUpdate())
        {
            try
            {
                reload();
            }
            catch (const Coordination::Exception & e)
            {
                if (Coordination::isHardwareError(e.code))
                {
                    LOG_INFO(log, "Lost ZooKeeper connection, will try to connect again: {}",
                            DB::getCurrentExceptionMessage(true));

                    sleepForSeconds(1);
                }
                else
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                    chassert(false);
                }
                continue;
            }
            catch (...)
            {
                DB::tryLogCurrentException(__PRETTY_FUNCTION__);
                chassert(false);
                continue;
            }
        }
    }

    LOG_TRACE(log, "Rewrite/query rules background updating thread finished");
}

}
