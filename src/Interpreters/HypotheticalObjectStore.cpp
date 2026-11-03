#include <Interpreters/HypotheticalObjectStore.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

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

bool HypotheticalObjectStore::add(const StorageID & table_id, const IndexDescription & index, bool if_not_exists)
{
    std::lock_guard lock(mutex);
    if (!prepareInsert(entries, table_id, index.name, if_not_exists, "index"))
        return false;
    entries.push_back({table_id, index});
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

std::vector<IndexDescription> HypotheticalObjectStore::getForTable(const StorageID & table_id) const
{
    std::lock_guard lock(mutex);
    std::vector<IndexDescription> result;
    for (const auto & entry : entries)
    {
        if (sameTable(entry.table_id, table_id))
            result.push_back(entry.index);
    }
    return result;
}

std::vector<HypotheticalObjectStore::Entry> HypotheticalObjectStore::getAll() const
{
    std::lock_guard lock(mutex);
    return entries;
}

bool HypotheticalObjectStore::addProjection(
    const StorageID & table_id, const ProjectionDescription & projection, bool if_not_exists)
{
    std::lock_guard lock(mutex);
    if (!prepareInsert(projection_entries, table_id, projection.name, if_not_exists, "projection"))
        return false;
    /// ProjectionDescription is move-only, so the caller's descriptor is cloned
    projection_entries.push_back({table_id, projection.clone()});
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

std::vector<ProjectionDescription> HypotheticalObjectStore::getProjectionsForTable(const StorageID & table_id) const
{
    std::lock_guard lock(mutex);
    std::vector<ProjectionDescription> result;
    for (const auto & entry : projection_entries)
    {
        if (sameTable(entry.table_id, table_id))
            result.push_back(entry.projection.clone());
    }
    return result;
}

std::vector<HypotheticalObjectStore::ProjectionEntry> HypotheticalObjectStore::getAllProjections() const
{
    std::lock_guard lock(mutex);
    std::vector<ProjectionEntry> result;
    result.reserve(projection_entries.size());
    for (const auto & entry : projection_entries)
        result.push_back({entry.table_id, entry.projection.clone()});
    return result;
}

}
