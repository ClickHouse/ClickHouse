#include <Interpreters/HypotheticalIndexStore.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Matches by UUID when both sides have one (stable across rename, distinguishes
/// drop-and-recreate). Falls back to (database, table) names for legacy
/// Ordinary databases where the table has no UUID.
bool HypotheticalIndexStore::sameTable(const StorageID & a, const StorageID & b)
{
    if (a.uuid != UUIDHelpers::Nil && b.uuid != UUIDHelpers::Nil)
        return a.uuid == b.uuid;
    return a.getDatabaseName() == b.getDatabaseName() && a.getTableName() == b.getTableName();
}

bool HypotheticalIndexStore::add(const StorageID & table_id, const IndexDescription & index, bool if_not_exists)
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

    /// Drop any stale entry for the same `(database, table, name)` whose UUID
    /// no longer matches — left behind by `DROP TABLE; CREATE TABLE` on the
    /// same name (the new table gets a fresh UUID, so `sameTable` returns
    /// false and the old entry would otherwise linger forever).
    std::erase_if(entries, [&](const Entry & e)
    {
        return e.index.name == index.name
            && e.table_id.getDatabaseName() == table_id.getDatabaseName()
            && e.table_id.getTableName() == table_id.getTableName()
            && !sameTable(e.table_id, table_id);
    });

    entries.push_back({table_id, index});
    return true;
}

bool HypotheticalIndexStore::remove(const StorageID & table_id, const String & index_name, bool if_exists)
{
    std::lock_guard lock(mutex);

    /// Prefer a UUID-strict match (handles `ALTER RENAME` correctly), but fall
    /// back to a `(database, table)` name match so a stale entry left over from
    /// `DROP TABLE; CREATE TABLE` on the same name can still be removed by the
    /// user — the new table has a different UUID, so `sameTable` would miss it.
    auto by_uuid = std::find_if(entries.begin(), entries.end(), [&](const Entry & e)
    {
        return e.index.name == index_name && sameTable(e.table_id, table_id);
    });

    auto pos = by_uuid;
    if (pos == entries.end())
    {
        pos = std::find_if(entries.begin(), entries.end(), [&](const Entry & e)
        {
            return e.index.name == index_name
                && e.table_id.getDatabaseName() == table_id.getDatabaseName()
                && e.table_id.getTableName() == table_id.getTableName();
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

void HypotheticalIndexStore::clear()
{
    std::lock_guard lock(mutex);
    entries.clear();
}

std::vector<IndexDescription> HypotheticalIndexStore::getForTable(const StorageID & table_id) const
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

std::vector<HypotheticalIndexStore::Entry> HypotheticalIndexStore::getAll() const
{
    std::lock_guard lock(mutex);
    return entries;
}

bool HypotheticalIndexStore::empty() const
{
    std::lock_guard lock(mutex);
    return entries.empty();
}

}
