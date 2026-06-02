#include <Common/RewriteRules/RewriteRules.h>
#include <algorithm>
#include <unordered_set>
#include <Core/Settings.h>
#include <Common/FieldVisitorToString.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTQueryParameter.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int REWRITE_RULE_DOESNT_EXIST;
    extern const int REWRITE_RULE_ALREADY_EXISTS;
    extern const int REWRITE_RULE_DUPLICATED_QUERY_PARAMETER;
    extern const int REWRITE_RULE_UNKNOWN_QUERY_PARAMETER;
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// Collects the names of all `{name:Type}` placeholders (`ASTQueryParameter`)
    /// reachable from `ast`, recording duplicates encountered along the way.
    void collectQueryParameters(const ASTPtr & ast, std::unordered_set<String> & names, std::vector<String> & duplicates)
    {
        if (!ast)
            return;
        if (const auto * query_parameter = ast->as<ASTQueryParameter>())
        {
            if (!names.insert(query_parameter->name).second)
                duplicates.push_back(query_parameter->name);
        }
        for (const auto & child : ast->children)
            collectQueryParameters(child, names, duplicates);
    }

    /// Validates a rule's source/result templates at DDL time so that invalid rule
    /// metadata is rejected on `CREATE RULE` / `ALTER RULE` instead of turning into
    /// runtime exceptions for every matching query later on.
    template <typename Query>
    void validateRuleTemplates(const Query & query)
    {
        std::unordered_set<String> source_parameters;
        std::vector<String> source_duplicates;
        collectQueryParameters(query.source_query, source_parameters, source_duplicates);

        /// A placeholder appearing more than once in the source template is accepted by
        /// the parser but later throws `REWRITE_RULE_DUPLICATED_QUERY_PARAMETER` during
        /// matching, so reject it up front.
        if (!source_duplicates.empty())
            throw Exception(
                ErrorCodes::REWRITE_RULE_DUPLICATED_QUERY_PARAMETER,
                "Rewrite rule `{}` has a duplicate query parameter `{}` in its source template",
                query.rule_name, source_duplicates.front());

        if (!query.rewrite())
            return;

        /// Every placeholder referenced by the result template must be captured by the
        /// source template, otherwise matching queries throw
        /// `REWRITE_RULE_UNKNOWN_QUERY_PARAMETER` at execution time.
        std::unordered_set<String> result_parameters;
        std::vector<String> result_duplicates;
        collectQueryParameters(query.resulting_query, result_parameters, result_duplicates);
        for (const auto & name : result_parameters)
            if (!source_parameters.contains(name))
                throw Exception(
                    ErrorCodes::REWRITE_RULE_UNKNOWN_QUERY_PARAMETER,
                    "Rewrite rule `{}` references query parameter `{}` in its result template "
                    "that is not captured by its source template",
                    query.rule_name, name);
    }
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
    {
        update_task->deactivate();
        update_task = BackgroundSchedulePoolTaskHolder{};
    }
    std::lock_guard lock(mutex);
    storage.reset();
}

bool RewriteRules::exists(const std::string & rule_name) const
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
    return exists(rule_name, lock);
}

RewriteRuleObjectPtr RewriteRules::get(const std::string & rule_name) const
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
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
    loadIfNot(lock);
    return tryGet(rule_name, lock);
}

RewriteRuleObjectsList RewriteRules::getAll() const
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
    return loaded_rewrite_rules;
}

bool RewriteRules::exists(const std::string & rule_name, std::lock_guard<std::mutex> &) const
{
    return std::any_of(
        loaded_rewrite_rules.begin(), loaded_rewrite_rules.end(),
        [&](const auto & entry) { return entry.first == rule_name; });
}

MutableRewriteRuleObjectPtr RewriteRules::tryGet(
    const std::string & rule_name,
    std::lock_guard<std::mutex> &) const
{
    auto it = std::find_if(
        loaded_rewrite_rules.begin(), loaded_rewrite_rules.end(),
        [&](const auto & entry) { return entry.first == rule_name; });
    if (it != loaded_rewrite_rules.end())
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
    std::lock_guard<std::mutex> & lock)
{
    if (exists(rule_name, lock))
    {
        throw Exception(
            ErrorCodes::REWRITE_RULE_ALREADY_EXISTS,
            "A rewrite rule `{}` already exists",
            rule_name);
    }
    loaded_rewrite_rules.emplace_back(rule_name, std::move(rule));
}

void RewriteRules::add(RewriteRuleObjectsList rules, std::lock_guard<std::mutex> & lock)
{
    for (auto & [rule_name, rule] : rules)
        add(rule_name, std::move(rule), lock);
}

void RewriteRules::remove(const std::string & rule_name, std::lock_guard<std::mutex> &)
{
    std::erase_if(
        loaded_rewrite_rules,
        [&](const auto & entry) { return entry.first == rule_name; });
}

void RewriteRules::createRule(const ASTCreateRewriteRuleQuery & query)
{
    validateRuleTemplates(query);
    std::lock_guard lock(mutex);
    loadIfNot(lock);
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
    loadIfNot(lock);
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
    validateRuleTemplates(query);
    std::lock_guard lock(mutex);
    loadIfNot(lock);
    if (!exists(query.rule_name, lock))
    {
        throw Exception(
            ErrorCodes::REWRITE_RULE_DOESNT_EXIST,
            "A rewrite rule `{}` doesn't exists",
            query.rule_name);
    }

    auto it = std::find_if(
        loaded_rewrite_rules.begin(), loaded_rewrite_rules.end(),
        [&](const auto & entry) { return entry.first == query.rule_name; });
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
    /// Bound the log size to avoid unbounded memory growth.
    static constexpr size_t max_logs = 1024;
    if (logs.size() >= max_logs)
        logs.erase(logs.begin(), logs.begin() + (logs.size() - max_logs + 1));
    logs.push_back(RewriteRuleLog::create(original_query, applied_rules, resulting_query));
}

std::vector<MutableRewriteRuleLogPtr> RewriteRules::getLogs() const
{
    std::lock_guard lock(mutex);
    return logs;
}

bool RewriteRules::loadIfNot(std::lock_guard<std::mutex> & lock) const
{
    if (loaded)
        return false;

    auto context = Context::getGlobalContextInstance();
    storage = RewriteRulesStorage::create(context);
    auto rules = storage->getAll();
    /// `add` is non-const but only mutates `mutable` `loaded_rewrite_rules`.
    const_cast<RewriteRules *>(this)->add(std::move(rules), lock);

    if (storage->isReplicated())
    {
        auto * self = const_cast<RewriteRules *>(this);
        update_task = context->getSchedulePool().createTask(
            StorageID::createEmpty(),
            "RewriteRuleReplicatedStorage",
            [self]{ self->updateFunc(); });
        update_task->activate();
        update_task->schedule();
    }

    loaded = true;
    return true;
}

bool RewriteRules::loadIfNot()
{
    std::lock_guard lock(mutex);
    return loadIfNot(lock);
}

void RewriteRules::reload()
{
    std::lock_guard lock(mutex);
    if (loadIfNot(lock))
        return;
    if (!storage)
        return;
    /// For replicated storage, updates are picked up by the background watcher
    /// thread (`updateFunc`). Refreshing here would race with `waitUpdate`,
    /// which mutates storage state outside of `mutex`.
    if (storage->isReplicated())
        return;
    reloadImpl(lock);
}

void RewriteRules::reloadImpl(std::lock_guard<std::mutex> & lock)
{
    if (!storage)
        return;
    auto rules = storage->getAll();
    loaded_rewrite_rules.clear();
    add(std::move(rules), lock);
}

void RewriteRules::updateFunc()
{
    LOG_TRACE(log, "Rewrite/query rules background updating thread started");

    try
    {
        RewriteRulesStorage * storage_ptr = nullptr;
        {
            std::lock_guard lock(mutex);
            if (shutdown_called.load() || !storage)
                return;
            storage_ptr = storage.get();
        }

        if (storage_ptr->waitUpdate())
        {
            std::lock_guard lock(mutex);
            if (shutdown_called.load() || !storage)
                return;
            reloadImpl(lock);
        }
    }
    catch (const Coordination::Exception & e)
    {
        if (Coordination::isHardwareError(e.code))
        {
            LOG_INFO(log, "Lost ZooKeeper connection, will try to connect again: {}",
                    DB::getCurrentExceptionMessage(true));
        }
        else
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            chassert(false);
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        chassert(false);
    }

    if (!shutdown_called.load() && update_task)
        update_task->scheduleAfter(1000);

    LOG_TRACE(log, "Rewrite/query rules background updating thread finished");
}

}
