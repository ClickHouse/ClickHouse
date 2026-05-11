#include <Interpreters/HypotheticalIndexStore.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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
    entries.push_back({table_id, index});
    return true;
}

bool HypotheticalIndexStore::remove(const StorageID & table_id, const String & index_name, bool if_exists)
{
    std::lock_guard lock(mutex);
    auto pos = std::find_if(entries.begin(), entries.end(), [&](const Entry & e)
    {
        return sameTable(e.table_id, table_id) && e.index.name == index_name;
    });

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
