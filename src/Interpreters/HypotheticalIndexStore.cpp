#include <Interpreters/HypotheticalIndexStore.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

HypotheticalIndexStore::Key HypotheticalIndexStore::makeKey(const StorageID & table_id)
{
    return {table_id.getDatabaseName(), table_id.getTableName()};
}

void HypotheticalIndexStore::add(const StorageID & table_id, const IndexDescription & index)
{
    std::lock_guard lock(mutex);
    auto & vec = indexes[makeKey(table_id)];
    for (const auto & existing : vec)
    {
        if (existing.name == index.name)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Hypothetical index '{}' already exists on {}.{}",
                index.name,
                table_id.getDatabaseName(),
                table_id.getTableName());
    }
    vec.push_back(index);
}

void HypotheticalIndexStore::remove(const StorageID & table_id, const String & index_name)
{
    std::lock_guard lock(mutex);
    auto key = makeKey(table_id);
    auto it = indexes.find(key);
    if (it == indexes.end())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "No hypothetical indexes on {}.{}",
            table_id.getDatabaseName(),
            table_id.getTableName());

    auto & vec = it->second;
    auto pos = std::find_if(vec.begin(), vec.end(), [&](const IndexDescription & idx) { return idx.name == index_name; });
    if (pos == vec.end())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Hypothetical index '{}' does not exist on {}.{}",
            index_name,
            table_id.getDatabaseName(),
            table_id.getTableName());

    vec.erase(pos);
    if (vec.empty())
        indexes.erase(it);
}

void HypotheticalIndexStore::clear()
{
    std::lock_guard lock(mutex);
    indexes.clear();
}

std::vector<IndexDescription> HypotheticalIndexStore::getForTable(const StorageID & table_id) const
{
    std::lock_guard lock(mutex);
    auto it = indexes.find(makeKey(table_id));
    if (it == indexes.end())
        return {};
    return it->second;
}

std::vector<HypotheticalIndexStore::Entry> HypotheticalIndexStore::getAll() const
{
    std::lock_guard lock(mutex);
    std::vector<Entry> result;
    for (const auto & [key, vec] : indexes)
    {
        StorageID table_id(key.first, key.second);
        for (const auto & idx : vec)
            result.push_back({table_id, idx});
    }
    return result;
}

bool HypotheticalIndexStore::empty() const
{
    std::lock_guard lock(mutex);
    return indexes.empty();
}

}
