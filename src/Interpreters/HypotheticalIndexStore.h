#pragma once

#include <Storages/IndicesDescription.h>
#include <Interpreters/StorageID.h>

#include <mutex>
#include <vector>

namespace DB
{

/// Session-scoped in-memory store for hypothetical indexes, used by EXPLAIN WHATIF
class HypotheticalIndexStore
{
public:
    /// Adds an index. Returns true if it was added, false if `if_not_exists` is true
    /// and an index with the same name already exists on this table.
    /// Throws if `if_not_exists` is false and the index already exists.
    bool add(const StorageID & table_id, const IndexDescription & index, bool if_not_exists);

    /// Removes an index. Returns true if it was removed, false if it didn't exist
    /// and `if_exists` is true. Throws if `if_exists` is false and the index didn't exist.
    bool remove(const StorageID & table_id, const String & index_name, bool if_exists);

    void clear();

    /// Returns hypothetical indexes for a given table, or empty vector if none.
    /// Matches by UUID when the table has one (so a drop-and-recreate with the
    /// same name produces no match); otherwise by (database, table) names.
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
    static bool sameTable(const StorageID & a, const StorageID & b);

    mutable std::mutex mutex;
    std::vector<Entry> entries;
};

}
