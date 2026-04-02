#pragma once

#include <Storages/IndicesDescription.h>
#include <Interpreters/StorageID.h>

#include <map>
#include <mutex>
#include <vector>

namespace DB
{

/// Session-scoped in-memory store for hypothetical indexes, used by EXPLAIN WHATIF
class HypotheticalIndexStore
{
public:
    void add(const StorageID & table_id, const IndexDescription & index);
    void remove(const StorageID & table_id, const String & index_name);
    void clear();

    /// Returns hypothetical indexes for a given table, or empty vector if none
    std::vector<IndexDescription> getForTable(const StorageID & table_id) const;

    /// All hypothetical indexes across all tables
    struct Entry
    {
        StorageID table_id;
        IndexDescription index;
    };
    std::vector<Entry> getAll() const;

    bool empty() const;

private:
    /// Key is (database, table) pair
    using Key = std::pair<String, String>;
    static Key makeKey(const StorageID & table_id);

    mutable std::mutex mutex;
    std::map<Key, std::vector<IndexDescription>> indexes;
};

}
