#include <Interpreters/WhatIf/HypotheticalObjectStore.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

void bindColumn(const String & name, const ColumnsDescription & columns, std::map<String, String> & bound);

void bindIdentifiers(const IAST & ast, const ColumnsDescription & columns, std::map<String, String> & bound)
{
    if (const auto * identifier = ast.as<ASTIdentifier>())
        bindColumn(identifier->name(), columns, bound);
    else if (ast.as<ASTAsterisk>() || ast.as<ASTColumnsRegexpMatcher>())
        for (const auto & column : columns.getOrdinary())
            bindColumn(column.name, columns, bound);
    for (const auto & child : ast.children)
        bindIdentifiers(*child, columns, bound);
}

void bindColumn(const String & name, const ColumnsDescription & columns, std::map<String, String> & bound)
{
    auto [it, inserted] = bound.try_emplace(name);
    if (!inserted)
        return;
    const auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, name);
    if (!column)
        return;
    it->second = column->type->getName();
    if (const auto column_default = columns.getDefault(column->getNameInStorage()))
    {
        it->second += fmt::format(" {} {}", toString(column_default->kind), column_default->expression->formatForErrorMessage());
        bindIdentifiers(*column_default->expression, columns, bound);
    }
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
    bindIdentifiers(definition, metadata.getColumns(), binding.columns);
    if (metadata.hasSettingsChanges())
        binding.settings = metadata.getSettingsChanges()->formatWithSecretsOneLine();
    return binding;
}

String HypotheticalBinding::changeTo(const HypotheticalBinding & current) const
{
    auto report = [](const String & name, const String & before, const String & after)
    {
        auto describe = [](const String & column) { return column.empty() ? String("absent") : column; };
        return fmt::format("column {} was {}, now {}", backQuote(name), describe(before), describe(after));
    };
    /// a name on one side only is reached through a column that changed, so report the columns on both sides first
    for (const auto & [name, before] : columns)
        if (const auto it = current.columns.find(name); it != current.columns.end() && it->second != before)
            return report(name, before, it->second);
    for (const auto & [name, before] : columns)
        if (!before.empty() && !current.columns.contains(name))
            return report(name, before, {});
    for (const auto & [name, after] : current.columns)
        if (!after.empty() && !columns.contains(name))
            return report(name, {}, after);
    if (settings != current.settings)
        return "the table settings changed";
    return {};
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
