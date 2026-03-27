#pragma once

#include <Storages/IndicesDescription.h>
#include <Storages/StorageID.h>

#include <map>
#include <mutex>
#include <vector>

namespace DB
{

/// Session-scoped store for hypothetical (what-if) indexes.
/// These indexes exist only in memory and are never materialized to disk.
/// Used by EXPLAIN WHATIF to estimate the effect of adding a skip index.
class HypotheticalIndexStore
{
public:
    void add(const StorageID & table_id, const IndexDescription & index);
    void remove(const StorageID & table_id, const String & index_name);
    void clear();

    /// Returns hypothetical indexes for a given table, or empty vector if none.
    std::vector<IndexDescription> getForTable(const StorageID & table_id) const;

    /// Returns all hypothetical indexes across all tables.
    struct Entry
    {
        StorageID table_id;
        IndexDescription index;
    };
    std::vector<Entry> getAll() const;

    bool empty() const;

private:
    /// Key is (database, table) pair.
    using Key = std::pair<String, String>;
    static Key makeKey(const StorageID & table_id);

    mutable std::mutex mutex;
    std::map<Key, std::vector<IndexDescription>> indexes;
};

}
