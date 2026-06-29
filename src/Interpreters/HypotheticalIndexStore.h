#pragma once

#include <Storages/IndicesDescription.h>
#include <Interpreters/StorageID.h>

#include <mutex>
#include <vector>

namespace DB
{

/// Session-scoped store for hypothetical indexes, used by EXPLAIN WHATIF
class HypotheticalIndexStore
{
public:
    bool add(const StorageID & table_id, const IndexDescription & index, bool if_not_exists);
    bool remove(const StorageID & table_id, const String & index_name, bool if_exists);

    void clear();

    std::vector<IndexDescription> getForTable(const StorageID & table_id) const;

    struct Entry
    {
        StorageID table_id;
        IndexDescription index;
    };
    std::vector<Entry> getAll() const;

private:
    static bool sameTable(const StorageID & a, const StorageID & b);

    mutable std::mutex mutex;
    std::vector<Entry> entries;
};

}
