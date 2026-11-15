#include <Interpreters/WhatIf/HypotheticalObjectStore.h>

#include <Access/Common/AccessFlags.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Analyzer/MatcherNode.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>

#include <Poco/String.h>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

std::optional<HypotheticalBinding::Column> describeColumn(const String & name, const ColumnsDescription & columns)
{
    const auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, name);
    if (!column)
        return std::nullopt;
    HypotheticalBinding::Column result;
    result.name_in_storage = column->getNameInStorage();
    result.type = column->type->getName();
    if (const auto column_default = columns.getDefault(result.name_in_storage))
    {
        result.default_kind = column_default->kind;
        result.default_hash = column_default->expression->getTreeHash(/*ignore_aliases=*/false);
        result.default_text = column_default->expression->formatForErrorMessage();
    }
    return result;
}

String describe(const std::optional<HypotheticalBinding::Column> & column)
{
    if (!column)
        return "absent";
    if (!column->default_kind)
        return column->type;
    return fmt::format("{} {} {}", column->type, toString(*column->default_kind), column->default_text);
}

/// false when the column is bound already, or the name is not a column, such as `_part_offset`
bool bindColumn(const String & name, const ColumnsDescription & columns, HypotheticalBinding & binding)
{
    if (binding.columns.contains(name))
        return false;
    auto column = describeColumn(name, columns);
    if (!column)
        return false;
    binding.columns.emplace(name, std::move(*column));
    return true;
}

/// the storage columns that an expression reads, also through `ALIAS` columns
void collectReads(const IAST & ast, const ColumnsDescription & columns, NameSet & names)
{
    if (const auto * identifier = ast.as<ASTIdentifier>())
    {
        const auto column = describeColumn(identifier->name(), columns);
        if (column && names.insert(column->name_in_storage).second && column->default_kind == ColumnDefaultKind::Alias)
            collectReads(*columns.getDefault(column->name_in_storage)->expression, columns, names);
    }
    for (const auto & child : ast.children)
        collectReads(*child, columns, names);
}

/// the columns that the definition names, also inside `ALIAS` columns: constant folding can remove a column
/// from the analysis, such as `b` in `isNull(b)`, while its type decides the result;
/// as `ReplaceAliasByExpressionMatcher`, a lambda parameter masks a column of the same name
void bindNames(const IAST & ast, const ColumnsDescription & columns, const NameSet & masked, HypotheticalBinding & binding)
{
    if (const auto * function = ast.as<ASTFunction>(); function && function->name == "lambda")
    {
        NameSet inner = masked;
        for (auto & name : RequiredSourceColumnsMatcher::extractNamesFromLambda(*function))
            inner.insert(std::move(name));
        bindNames(*function->arguments->children[1], columns, inner, binding);
        return;
    }
    if (const auto * identifier = ast.as<ASTIdentifier>())
    {
        const auto & name = identifier->name();
        /// an `ALIAS` expression is written at table scope
        if (!masked.contains(name) && bindColumn(name, columns, binding) && columns.hasAlias(name))
            bindNames(*columns.getDefault(name)->expression, columns, {}, binding);
    }
    /// an `APPLY` lambda is not in `children`
    if (const auto * apply = ast.as<ASTColumnsApplyTransformer>(); apply && apply->lambda)
        bindNames(*apply->lambda, columns, masked, binding);
    for (const auto & child : ast.children)
        bindNames(*child, columns, masked, binding);
}

/// the analyzer reads a projection with every column as ordinary, so a matcher expands over all of them
Names expand(const IAST & matcher, const ColumnsDescription & columns)
{
    const auto * regexp = matcher.as<ASTColumnsRegexpMatcher>();
    MatcherNode node = regexp ? MatcherNode(regexp->getPattern()) : MatcherNode();
    Names names;
    for (const auto & column : columns.getAll())
        if (node.isMatchingColumn(column.name))
            names.push_back(column.name);
    return names;
}

/// as `resolveFunction`, the analyzer removes an unqualified matcher from the arguments of `count` and `countState`,
/// also with combinators that keep the argument types, so `count(*)` reads no column
bool removesMatchers(const ASTFunction & function)
{
    String name = function.name;
    while (const auto combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(name))
    {
        if (combinator->transformsArgumentTypes())
            return false;
        name.resize(name.size() - combinator->getName().size());
    }
    const auto lowercase = Poco::toLower(name);
    return (lowercase == "count" || lowercase == "countstate") && Poco::toLower(function.name) != "countdistinct";
}

void bindExpansions(const ASTPtr & ast, const ColumnsDescription & columns, HypotheticalBinding & binding)
{
    if (ast->as<ASTAsterisk>() || ast->as<ASTColumnsRegexpMatcher>())
    {
        auto names = expand(*ast, columns);
        for (const auto & name : names)
            bindColumn(name, columns, binding);
        binding.expansions.emplace_back(ast, std::move(names));
    }
    const auto * function = ast->as<ASTFunction>();
    const bool skip_matchers = function && function->arguments && removesMatchers(*function);
    for (const auto & child : ast->children)
    {
        if (skip_matchers && child == function->arguments)
        {
            for (const auto & argument : child->children)
                if (!argument->as<ASTAsterisk>() && !argument->as<ASTColumnsRegexpMatcher>())
                    bindExpansions(argument, columns, binding);
            continue;
        }
        bindExpansions(child, columns, binding);
    }
}

HypotheticalBinding bindDefinition(const ASTPtr & definition, const Names & required_columns, const StorageInMemoryMetadata & metadata)
{
    HypotheticalBinding binding;
    const auto & columns = metadata.getColumns();
    for (const auto & name : required_columns)
        bindColumn(name, columns, binding);
    bindNames(*definition, columns, {}, binding);
    bindExpansions(definition, columns, binding);
    if (metadata.hasSettingsChanges())
        binding.settings = metadata.getSettingsChanges()->formatWithSecretsOneLine();
    return binding;
}

bool sameTable(const StorageID & a, const StorageID & b)
{
    return a.uuid != UUIDHelpers::Nil && a.uuid == b.uuid;
}

/// an entry under the same database.table whose uuid is gone: the table was dropped and recreated,
/// so the entry can never be looked up again
template <typename EntryT>
bool isStale(const EntryT & e, const StorageID & table_id, const String & name)
{
    if (e.name() != name
        || e.table_id.getDatabaseName() != table_id.getDatabaseName()
        || e.table_id.getTableName() != table_id.getTableName())
        return false;

    return e.table_id.uuid == UUIDHelpers::Nil || !DatabaseCatalog::instance().tryGetByUUID(e.table_id.uuid).second;
}

/// false when if_not_exists suppressed the insert, throws when the name is taken
template <typename EntryT>
bool prepareInsert(
    std::vector<EntryT> & entries, const StorageID & table_id, const String & name, bool if_not_exists, const char * kind)
{
    for (const auto & entry : entries)
    {
        if (sameTable(entry.table_id, table_id) && entry.name() == name)
        {
            if (if_not_exists)
                return false;
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Hypothetical {} '{}' already exists on {}.{}",
                kind,
                name,
                table_id.getDatabaseName(),
                table_id.getTableName());
        }
    }

    std::erase_if(entries, [&](const EntryT & e) { return !sameTable(e.table_id, table_id) && isStale(e, table_id, name); });
    return true;
}

/// prefer the entry matching by uuid, fall back to a stale same-name entry
template <typename EntryT>
typename std::vector<EntryT>::iterator findForRemoval(
    std::vector<EntryT> & entries, const StorageID & table_id, const String & name)
{
    auto pos = std::find_if(
        entries.begin(), entries.end(), [&](const EntryT & e) { return e.name() == name && sameTable(e.table_id, table_id); });
    if (pos != entries.end())
        return pos;
    return std::find_if(entries.begin(), entries.end(), [&](const EntryT & e) { return isStale(e, table_id, name); });
}

template <typename EntryT>
bool eraseOrThrow(
    std::vector<EntryT> & entries, const StorageID & table_id, const String & name, bool if_exists, const char * kind)
{
    auto pos = findForRemoval(entries, table_id, name);
    if (pos == entries.end())
    {
        if (if_exists)
            return false;
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Hypothetical {} '{}' does not exist on {}.{}",
            kind,
            name,
            table_id.getDatabaseName(),
            table_id.getTableName());
    }

    entries.erase(pos);
    return true;
}

}

HypotheticalBinding HypotheticalBinding::of(const IndexDescription & index, const StorageInMemoryMetadata & metadata)
{
    return bindDefinition(index.definition_ast, index.expression->getRequiredColumns(), metadata);
}

HypotheticalBinding HypotheticalBinding::of(const ProjectionDescription & projection, const StorageInMemoryMetadata & metadata)
{
    return bindDefinition(projection.definition_ast, projection.required_columns, metadata);
}

String HypotheticalBinding::changeIn(const StorageInMemoryMetadata & metadata, const ContextPtr & context, const StorageID & table_id) const
{
    const auto & current = metadata.getColumns();
    String change;
    NameSet names;
    for (const auto & [name, before] : columns)
    {
        names.insert(before.name_in_storage);
        const auto now = describeColumn(name, current);
        if (now && now->default_kind == ColumnDefaultKind::Alias)
            collectReads(*current.getDefault(now->name_in_storage)->expression, current, names);
        if (!change.empty() || now == before)
            continue;
        /// defaults that differ only in masked text describe the same
        if (describe(before) == describe(now))
            change = fmt::format("the default of column {} changed", backQuote(name));
        else
            change = fmt::format("column {} was {}, now {}", backQuote(name), describe(before), describe(now));
    }
    for (const auto & [matcher, before] : expansions)
    {
        auto now = expand(*matcher, current);
        if (change.empty() && now != before)
            change = fmt::format(
                "the columns under `{}` were ({}), now ({})", matcher->formatForErrorMessage(), fmt::join(before, ", "), fmt::join(now, ", "));
        names.insert(now.begin(), now.end());
    }
    if (change.empty() && settings != (metadata.hasSettingsChanges() ? metadata.getSettingsChanges()->formatWithSecretsOneLine() : ""))
        change = "the table settings changed";
    /// the change describes bound columns, so it needs SELECT on them, as they were and as they are now
    if (!change.empty())
        context->checkAccess(AccessType::SELECT, table_id, Names(names.begin(), names.end()));
    return change;
}

bool HypotheticalObjectStore::add(
    const StorageID & table_id, const IndexDescription & index, const HypotheticalBinding & binding, bool if_not_exists)
{
    std::lock_guard lock(mutex);
    if (!prepareInsert(entries, table_id, index.name, if_not_exists, "index"))
        return false;
    entries.push_back({table_id, index, binding});
    return true;
}

bool HypotheticalObjectStore::remove(const StorageID & table_id, const String & index_name, bool if_exists)
{
    std::lock_guard lock(mutex);
    return eraseOrThrow(entries, table_id, index_name, if_exists, "index");
}

void HypotheticalObjectStore::clear()
{
    std::lock_guard lock(mutex);
    entries.clear();
}

std::vector<HypotheticalObjectStore::Entry> HypotheticalObjectStore::getForTable(const StorageID & table_id) const
{
    std::lock_guard lock(mutex);
    std::vector<Entry> result;
    for (const auto & entry : entries)
    {
        if (sameTable(entry.table_id, table_id))
            result.push_back(entry);
    }
    return result;
}

std::vector<HypotheticalObjectStore::Entry> HypotheticalObjectStore::getAll() const
{
    std::lock_guard lock(mutex);
    return entries;
}

bool HypotheticalObjectStore::addProjection(
    const StorageID & table_id, const ProjectionDescription & projection, const HypotheticalBinding & binding, bool if_not_exists)
{
    std::lock_guard lock(mutex);
    if (!prepareInsert(projection_entries, table_id, projection.name, if_not_exists, "projection"))
        return false;
    /// ProjectionDescription is move-only, so the caller's descriptor is cloned
    projection_entries.push_back({table_id, projection.clone(), binding});
    return true;
}

bool HypotheticalObjectStore::removeProjection(const StorageID & table_id, const String & projection_name, bool if_exists)
{
    std::lock_guard lock(mutex);
    return eraseOrThrow(projection_entries, table_id, projection_name, if_exists, "projection");
}

void HypotheticalObjectStore::clearProjections()
{
    std::lock_guard lock(mutex);
    projection_entries.clear();
}

std::vector<HypotheticalObjectStore::ProjectionEntry> HypotheticalObjectStore::getProjectionsForTable(const StorageID & table_id) const
{
    std::lock_guard lock(mutex);
    std::vector<ProjectionEntry> result;
    for (const auto & entry : projection_entries)
    {
        if (sameTable(entry.table_id, table_id))
            result.push_back({entry.table_id, entry.projection.clone(), entry.binding});
    }
    return result;
}

std::vector<HypotheticalObjectStore::ProjectionEntry> HypotheticalObjectStore::getAllProjections() const
{
    std::lock_guard lock(mutex);
    std::vector<ProjectionEntry> result;
    result.reserve(projection_entries.size());
    for (const auto & entry : projection_entries)
        result.push_back({entry.table_id, entry.projection.clone(), entry.binding});
    return result;
}

}
