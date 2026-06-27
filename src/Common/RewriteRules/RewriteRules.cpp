#include <Common/RewriteRules/RewriteRules.h>
#include <algorithm>
#include <array>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <Core/Settings.h>
#include <Common/FieldVisitorToString.h>
#include <Common/StringUtils.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTCreateRewriteRuleQuery.h>
#include <Parsers/ASTAlterRewriteRuleQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTWithAlias.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int REWRITE_RULE_DOESNT_EXIST;
    extern const int REWRITE_RULE_ALREADY_EXISTS;
    extern const int REWRITE_RULE_DUPLICATED_QUERY_PARAMETER;
    extern const int REWRITE_RULE_UNKNOWN_QUERY_PARAMETER;
    extern const int REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    /// The placeholder types understood by the rewrite-rule matcher
    /// (`RewriteRulesASTTraversal.cpp`). A rule placeholder uses the `{name:Type}`
    /// query-parameter syntax, but `Type` is a small custom matching vocabulary
    /// rather than an ordinary ClickHouse data type: `String` and `Int` capture
    /// literals, `Expression`/`ExpressionList` capture expression subtrees and
    /// `Subquery` captures a subquery. A placeholder with any other type (e.g.
    /// `{x:UInt64}` or `{d:Date}`) would be stored but never match a literal query,
    /// silently turning the rule into a no-op, so such types are rejected at DDL time.
    bool isSupportedQueryParameterType(std::string_view type)
    {
        static constexpr std::array supported{"String", "Int", "Expression", "ExpressionList", "Subquery"};
        return std::find(supported.begin(), supported.end(), type) != supported.end();
    }

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

    /// Returns the first placeholder in `ast` whose type is not understood by the
    /// matcher, as a `{name, type}` pair, or `std::nullopt` if all are supported.
    std::optional<std::pair<String, String>> findUnsupportedQueryParameter(const ASTPtr & ast)
    {
        if (!ast)
            return {};
        if (const auto * query_parameter = ast->as<ASTQueryParameter>())
        {
            auto type = query_parameter->type;
            trimLeft(type);
            trimRight(type);
            if (!isSupportedQueryParameterType(type))
                return std::make_pair(query_parameter->name, query_parameter->type);
        }
        for (const auto & child : ast->children)
            if (auto found = findUnsupportedQueryParameter(child))
                return found;
        return {};
    }

    /// `collectQueryParameters` / `findUnsupportedQueryParameter` above and the matcher in
    /// `RewriteRulesASTTraversal.cpp` only ever descend through `IAST::children`.
    /// `ASTCreateRewriteRuleQuery` / `ASTAlterRewriteRuleQuery` keep their own
    /// `source_query` / `resulting_query` templates outside `children`, so a placeholder
    /// embedded inside a *nested* rule-DDL template (for example
    /// `CREATE RULE outer AS (CREATE RULE inner AS (SELECT {x:String}) REWRITE TO (SELECT 1))
    /// REWRITE TO (SELECT 1)`) is unreachable by the matcher: it can be neither bound nor
    /// substituted, so the rule would silently never match as intended. Returns the name of
    /// the first such placeholder so it can be rejected at DDL time. `inside_nested_template`
    /// is true once the walk has descended into a nested rule's template fields.
    std::optional<String> findQueryParameterInNestedRuleTemplate(const ASTPtr & ast, bool inside_nested_template)
    {
        if (!ast)
            return {};

        if (inside_nested_template)
            if (const auto * query_parameter = ast->as<ASTQueryParameter>())
                return query_parameter->name;

        if (const auto * create_rule = ast->as<ASTCreateRewriteRuleQuery>())
        {
            if (auto found = findQueryParameterInNestedRuleTemplate(create_rule->source_query, true))
                return found;
            if (auto found = findQueryParameterInNestedRuleTemplate(create_rule->resulting_query, true))
                return found;
        }
        else if (const auto * alter_rule = ast->as<ASTAlterRewriteRuleQuery>())
        {
            if (auto found = findQueryParameterInNestedRuleTemplate(alter_rule->source_query, true))
                return found;
            if (auto found = findQueryParameterInNestedRuleTemplate(alter_rule->resulting_query, true))
                return found;
        }

        for (const auto & child : ast->children)
            if (auto found = findQueryParameterInNestedRuleTemplate(child, inside_nested_template))
                return found;

        return {};
    }

    /// Collects the declared type of every `{name:Type}` placeholder reachable from `ast`,
    /// keyed by name (whitespace-trimmed). The source template rejects duplicate names
    /// elsewhere, so its names are unique; for the result template the first occurrence is
    /// recorded, which is enough to detect a type that disagrees with the source.
    void collectQueryParameterTypes(const ASTPtr & ast, std::unordered_map<String, String> & types)
    {
        if (!ast)
            return;
        if (const auto * query_parameter = ast->as<ASTQueryParameter>())
        {
            auto type = query_parameter->type;
            trimLeft(type);
            trimRight(type);
            types.emplace(query_parameter->name, type);
        }
        for (const auto & child : ast->children)
            collectQueryParameterTypes(child, types);
    }

    struct ParameterTypeMismatch
    {
        String name;
        String result_type;
        String source_type;
    };

    /// Returns the first result placeholder whose declared type disagrees with the type the
    /// same-named placeholder uses in the source template. Substitution binds captures by
    /// name and ignores the result-side type, so a mismatching type would let, say, a
    /// `String` capture land in an `{x:Int}` position, producing an AST that fails at
    /// execution time. Only names present in the source are compared (unknown names are
    /// rejected separately).
    std::optional<ParameterTypeMismatch> findResultParameterTypeMismatch(
        const ASTPtr & ast, const std::unordered_map<String, String> & source_types)
    {
        if (!ast)
            return {};
        if (const auto * query_parameter = ast->as<ASTQueryParameter>())
        {
            auto type = query_parameter->type;
            trimLeft(type);
            trimRight(type);
            auto it = source_types.find(query_parameter->name);
            if (it != source_types.end() && it->second != type)
                return ParameterTypeMismatch{query_parameter->name, type, it->second};
        }
        for (const auto & child : ast->children)
            if (auto found = findResultParameterTypeMismatch(child, source_types))
                return found;
        return {};
    }

    /// Returns true if `ast` contains an `INSERT` query carrying inline data (a `VALUES` /
    /// `FORMAT` payload). Such a template is unsafe to persist: `ASTInsertQuery::clone` copies
    /// the raw `data` / `end` pointers, which point into the buffer of the original
    /// `CREATE RULE` query; that buffer is freed long before the stored rule is used, so a
    /// later rewrite to `INSERT ... VALUES` / `INSERT ... FORMAT ...` would read dangling
    /// memory. Inline data is also not part of `children` or the tree hash, so it cannot
    /// participate in matching either. Walks ordinary `children` and the template fields of
    /// any nested rule DDL (which live outside `children`).
    bool containsInsertWithInlinedData(const ASTPtr & ast)
    {
        if (!ast)
            return false;
        if (const auto * insert = ast->as<ASTInsertQuery>(); insert && insert->hasInlinedData())
            return true;
        if (const auto * create_rule = ast->as<ASTCreateRewriteRuleQuery>())
        {
            if (containsInsertWithInlinedData(create_rule->source_query)
                || containsInsertWithInlinedData(create_rule->resulting_query))
                return true;
        }
        else if (const auto * alter_rule = ast->as<ASTAlterRewriteRuleQuery>())
        {
            if (containsInsertWithInlinedData(alter_rule->source_query)
                || containsInsertWithInlinedData(alter_rule->resulting_query))
                return true;
        }
        for (const auto & child : ast->children)
            if (containsInsertWithInlinedData(child))
                return true;
        return false;
    }

    /// Returns the name of the first query parameter used as an alias (`expr AS {name:Type}`)
    /// reachable from `ast`, or `std::nullopt`. Such a parameter is stored in
    /// `ASTWithAlias::parametrised_alias`, which is NOT a child, so the placeholder walks above
    /// (and the matcher / substitution in `RewriteRulesASTTraversal.cpp`, which also follow only
    /// `children`) never see it: the rule would be stored with an alias placeholder that is
    /// never validated, bound or substituted. Walks ordinary `children` and the template fields
    /// of any nested rule DDL.
    std::optional<String> findParametrisedAlias(const ASTPtr & ast)
    {
        if (!ast)
            return {};
        if (const auto * with_alias = dynamic_cast<const ASTWithAlias *>(ast.get());
            with_alias && with_alias->parametrised_alias)
            return with_alias->parametrised_alias->name;
        if (const auto * create_rule = ast->as<ASTCreateRewriteRuleQuery>())
        {
            if (auto found = findParametrisedAlias(create_rule->source_query))
                return found;
            if (auto found = findParametrisedAlias(create_rule->resulting_query))
                return found;
        }
        else if (const auto * alter_rule = ast->as<ASTAlterRewriteRuleQuery>())
        {
            if (auto found = findParametrisedAlias(alter_rule->source_query))
                return found;
            if (auto found = findParametrisedAlias(alter_rule->resulting_query))
                return found;
        }
        for (const auto & child : ast->children)
            if (auto found = findParametrisedAlias(child))
                return found;
        return {};
    }

    /// Validates a rule's source/result templates at DDL time so that invalid rule
    /// metadata is rejected on `CREATE RULE` / `ALTER RULE` instead of turning into
    /// runtime exceptions for every matching query later on.
    template <typename Query>
    void validateRuleTemplates(const Query & query)
    {
        /// Placeholders inside a nested `CREATE RULE` / `ALTER RULE` template are
        /// unreachable by the matcher and the substitution (both walk only `children`),
        /// so a rule using them could never match or rewrite as intended. Reject them up
        /// front instead of silently storing a rule that does nothing.
        for (const auto & template_query : {query.source_query, query.resulting_query})
            if (auto nested = findQueryParameterInNestedRuleTemplate(template_query, false))
                throw Exception(
                    ErrorCodes::REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE,
                    "Rewrite rule `{}` uses query parameter `{}` inside a nested CREATE RULE / "
                    "ALTER RULE template; placeholders in nested rule templates are not supported",
                    query.rule_name, *nested);

        /// An `INSERT` query carrying inline data (`VALUES` / `FORMAT`) is unsafe to persist:
        /// its raw `data` / `end` pointers reference the original `CREATE RULE` query buffer,
        /// which is freed before the stored rule is ever used. The parser already rejects such a
        /// template (the inline data swallows the closing parenthesis), but reject it here too so
        /// the memory-safety invariant does not depend on that parser behaviour (the inline data
        /// also cannot participate in matching, being outside `children`).
        for (const auto & template_query : {query.source_query, query.resulting_query})
            if (containsInsertWithInlinedData(template_query))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Rewrite rule `{}` uses an INSERT query with inline data (VALUES / FORMAT) in "
                    "its template; INSERT templates with inline data are not supported",
                    query.rule_name);

        /// A query parameter used as an alias (`... AS {name:Type}`) is kept in
        /// `ASTWithAlias::parametrised_alias`, outside `children`, so it is neither validated,
        /// bound nor substituted by the matcher. Reject it instead of silently storing a rule
        /// with an alias placeholder that does nothing.
        for (const auto & template_query : {query.source_query, query.resulting_query})
            if (auto alias = findParametrisedAlias(template_query))
                throw Exception(
                    ErrorCodes::REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE,
                    "Rewrite rule `{}` uses query parameter `{}` as an alias; query parameters in "
                    "aliases are not supported in rewrite rule templates",
                    query.rule_name, *alias);

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

        /// A placeholder in the source template whose type the matcher does not
        /// understand (anything other than `String`, `Int`, `Expression`,
        /// `ExpressionList` or `Subquery`) would be stored but never match any query,
        /// silently turning the rule into a no-op. Reject such types up front.
        if (auto unsupported = findUnsupportedQueryParameter(query.source_query))
            throw Exception(
                ErrorCodes::REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE,
                "Rewrite rule `{}` uses query parameter `{}` with unsupported type `{}` in its source template. "
                "Supported placeholder types are: String, Int, Expression, ExpressionList, Subquery",
                query.rule_name, unsupported->first, unsupported->second);

        if (!query.rewrite())
            return;

        /// A placeholder in the result template whose type the substitution does not
        /// understand (anything other than `String`, `Int`, `Expression`, `ExpressionList`
        /// or `Subquery`) — for example `{t:Identifier}` — would be substituted as a raw
        /// captured node into an incompatible position, leaving a malformed AST that fails
        /// at execution time. Reject such types up front, symmetrically with the source
        /// template.
        if (auto unsupported = findUnsupportedQueryParameter(query.resulting_query))
            throw Exception(
                ErrorCodes::REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE,
                "Rewrite rule `{}` uses query parameter `{}` with unsupported type `{}` in its result template. "
                "Supported placeholder types are: String, Int, Expression, ExpressionList, Subquery",
                query.rule_name, unsupported->first, unsupported->second);

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

        /// A placeholder reused in the result template must declare the same type as in the
        /// source template. Matching captures by name and ignores the result-side type, so a
        /// disagreeing type (e.g. source `{x:String}`, result `{x:Int}`) would substitute a
        /// captured string literal into an `{x:Int}` position, producing an AST that fails at
        /// execution time. Reject such mismatches up front.
        std::unordered_map<String, String> source_types;
        collectQueryParameterTypes(query.source_query, source_types);
        if (auto mismatch = findResultParameterTypeMismatch(query.resulting_query, source_types))
            throw Exception(
                ErrorCodes::REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE,
                "Rewrite rule `{}` uses query parameter `{}` with type `{}` in its result template "
                "but type `{}` in its source template; a placeholder must use the same type in both",
                query.rule_name, mismatch->name, mismatch->result_type, mismatch->source_type);
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
    /// Only deactivate the task here; do not reset the holder. `deactivate` waits for any
    /// in-flight `updateFunc` to return and prevents further runs, but the watcher's tail still
    /// reads `update_task` (`operator bool` / `operator->` when rescheduling). Reassigning the
    /// holder here would race with that unsynchronized read. Destruction of the holder is left
    /// to the owner (the destructor, which runs after this `shutdown`), by which point no
    /// `updateFunc` can be running.
    if (update_task)
        update_task->deactivate();
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
