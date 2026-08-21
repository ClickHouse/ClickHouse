#include <Interpreters/HypotheticalObjectStore.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

bool HypotheticalObjectStore::sameTable(const StorageID & a, const StorageID & b)
{
    return a.uuid != UUIDHelpers::Nil && a.uuid == b.uuid;
}

bool HypotheticalObjectStore::add(const StorageID & table_id, const IndexDescription & index, bool if_not_exists)
{
    std::lock_guard lock(mutex);
    for (const auto & entry : entries)
    {
        if (sameTable(entry.table_id, table_id) && entry.index.name == index.name)
        {
            if (if_not_exists)
                return false;
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Hypothetical index '{}' already exists on {}.{}",
                index.name,
                table_id.getDatabaseName(),
                table_id.getTableName());
        }
    }

    std::erase_if(entries, [&](const Entry & e)
    {
        if (e.index.name != index.name
            || e.table_id.getDatabaseName() != table_id.getDatabaseName()
            || e.table_id.getTableName() != table_id.getTableName()
            || sameTable(e.table_id, table_id))
            return false;

        if (e.table_id.uuid != UUIDHelpers::Nil
            && DatabaseCatalog::instance().tryGetByUUID(e.table_id.uuid).second)
            return false;

        return true;
    });

    entries.push_back({table_id, index});
    return true;
}

bool HypotheticalObjectStore::remove(const StorageID & table_id, const String & index_name, bool if_exists)
{
    std::lock_guard lock(mutex);

    auto by_uuid = std::find_if(entries.begin(), entries.end(), [&](const Entry & e)
    {
        return e.index.name == index_name && sameTable(e.table_id, table_id);
    });

    auto pos = by_uuid;
    if (pos == entries.end())
    {
        pos = std::find_if(entries.begin(), entries.end(), [&](const Entry & e)
        {
            if (e.index.name != index_name
                || e.table_id.getDatabaseName() != table_id.getDatabaseName()
                || e.table_id.getTableName() != table_id.getTableName())
                return false;

            if (e.table_id.uuid != UUIDHelpers::Nil
                && DatabaseCatalog::instance().tryGetByUUID(e.table_id.uuid).second)
                return false;

            return true;
        });
    }

    if (pos == entries.end())
    {
        if (if_exists)
            return false;
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Hypothetical index '{}' does not exist on {}.{}",
            index_name,
            table_id.getDatabaseName(),
            table_id.getTableName());
    }

    entries.erase(pos);
    return true;
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

bool HypotheticalObjectStore::addProjection(const StorageID & table_id, const ProjectionDescription & projection, bool if_not_exists)
{
    std::lock_guard lock(mutex);
    for (const auto & entry : projection_entries)
    {
        if (sameTable(entry.table_id, table_id) && entry.projection.name == projection.name)
        {
            if (if_not_exists)
                return false;
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Hypothetical projection '{}' already exists on {}.{}",
                projection.name,
                table_id.getDatabaseName(),
                table_id.getTableName());
        }
    }

    /// Same name on the same database.table but a dead UUID: the old table was dropped and
    /// recreated, so the stale entry can never be looked up again. Reap it.
    std::erase_if(projection_entries, [&](const ProjectionEntry & e)
    {
        if (e.projection.name != projection.name
            || e.table_id.getDatabaseName() != table_id.getDatabaseName()
            || e.table_id.getTableName() != table_id.getTableName()
            || sameTable(e.table_id, table_id))
            return false;

        if (e.table_id.uuid != UUIDHelpers::Nil
            && DatabaseCatalog::instance().tryGetByUUID(e.table_id.uuid).second)
            return false;

        return true;
    });

    /// ProjectionDescription is move-only, so the caller's descriptor is cloned into the entry
    projection_entries.push_back({table_id, projection.clone()});
    return true;
}

bool HypotheticalObjectStore::removeProjection(const StorageID & table_id, const String & projection_name, bool if_exists)
{
    std::lock_guard lock(mutex);

    auto pos = std::find_if(projection_entries.begin(), projection_entries.end(), [&](const ProjectionEntry & e)
    {
        return e.projection.name == projection_name && sameTable(e.table_id, table_id);
    });

    if (pos == projection_entries.end())
    {
        pos = std::find_if(projection_entries.begin(), projection_entries.end(), [&](const ProjectionEntry & e)
        {
            if (e.projection.name != projection_name
                || e.table_id.getDatabaseName() != table_id.getDatabaseName()
                || e.table_id.getTableName() != table_id.getTableName())
                return false;

            if (e.table_id.uuid != UUIDHelpers::Nil
                && DatabaseCatalog::instance().tryGetByUUID(e.table_id.uuid).second)
                return false;

            return true;
        });
    }

    if (pos == projection_entries.end())
    {
        if (if_exists)
            return false;
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Hypothetical projection '{}' does not exist on {}.{}",
            projection_name,
            table_id.getDatabaseName(),
            table_id.getTableName());
    }

    projection_entries.erase(pos);
    return true;
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
