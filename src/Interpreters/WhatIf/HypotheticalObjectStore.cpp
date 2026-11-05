#include <Interpreters/WhatIf/HypotheticalObjectStore.h>

#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

void bindColumn(const String & name, const ColumnsDescription & columns, HypotheticalBinding & binding);

void bindIdentifiers(const IAST & ast, const ColumnsDescription & columns, HypotheticalBinding & binding)
{
    if (const auto * identifier = ast.as<ASTIdentifier>())
        bindColumn(identifier->name(), columns, binding);
    else if (ast.as<ASTAsterisk>() || ast.as<ASTColumnsRegexpMatcher>())
    {
        /// the order decides positional arguments such as `GROUP BY 1`
        binding.wildcard = columns.getOrdinary().getNames();
        for (const auto & name : binding.wildcard)
            bindColumn(name, columns, binding);
    }
    for (const auto & child : ast.children)
        bindIdentifiers(*child, columns, binding);
}

void bindColumn(const String & name, const ColumnsDescription & columns, HypotheticalBinding & binding)
{
    auto [it, inserted] = binding.columns.try_emplace(name);
    if (!inserted)
        return;
    const auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, name);
    if (!column)
        return;
    binding.storage_columns.insert(column->getNameInStorage());
    it->second = column->type->getName();
    if (const auto column_default = columns.getDefault(column->getNameInStorage()))
    {
        it->second += fmt::format(" {} {}", toString(column_default->kind), column_default->expression->formatForErrorMessage());
        binding.defaults[name] = column_default->expression->getTreeHash(/*ignore_aliases=*/false);
        bindIdentifiers(*column_default->expression, columns, binding);
    }
}

String describeChange(const HypotheticalBinding & stored, const HypotheticalBinding & current)
{
    auto report = [](const String & name, const String & before, const String & after)
    {
        auto describe = [](const String & column) { return column.empty() ? String("absent") : column; };
        return fmt::format("column {} was {}, now {}", backQuote(name), describe(before), describe(after));
    };
    /// a name on one side only is reached through a column that changed, so report the columns on both sides first
    for (const auto & [name, before] : stored.columns)
        if (const auto it = current.columns.find(name); it != current.columns.end() && it->second != before)
            return report(name, before, it->second);
    for (const auto & [name, before] : stored.columns)
        if (!before.empty() && !current.columns.contains(name))
            return report(name, before, {});
    for (const auto & [name, after] : current.columns)
        if (!after.empty() && !stored.columns.contains(name))
            return report(name, {}, after);
    for (const auto & [name, before] : stored.defaults)
        if (const auto it = current.defaults.find(name); it != current.defaults.end() && it->second != before)
            return fmt::format("the default of column {} changed", backQuote(name));
    if (stored.wildcard != current.wildcard)
        return fmt::format("the columns under `*` were ({}), now ({})", fmt::join(stored.wildcard, ", "), fmt::join(current.wildcard, ", "));
    if (stored.settings != current.settings)
        return "the table settings changed";
    return {};
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

HypotheticalBinding HypotheticalBinding::of(const IAST & definition, const StorageInMemoryMetadata & metadata)
{
    HypotheticalBinding binding;
    bindIdentifiers(definition, metadata.getColumns(), binding);
    if (metadata.hasSettingsChanges())
        binding.settings = metadata.getSettingsChanges()->formatWithSecretsOneLine();
    return binding;
}

String HypotheticalBinding::changeTo(const HypotheticalBinding & current, const ContextPtr & context, const StorageID & table_id) const
{
    auto change = describeChange(*this, current);
    if (change.empty())
        return change;
    /// the change describes bound columns, so it needs SELECT on them, as they were at CREATE and as they are now
    NameSet names = storage_columns;
    names.insert(current.storage_columns.begin(), current.storage_columns.end());
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
