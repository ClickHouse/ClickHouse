#pragma once

#include <Storages/IndicesDescription.h>
#include <Storages/ProjectionsDescription.h>
#include <Interpreters/StorageID.h>

#include <mutex>
#include <vector>

namespace DB
{

/// Session-scoped store for hypothetical indexes and projections, used by EXPLAIN WHATIF
class HypotheticalObjectStore
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
        const String & name() const { return index.name; }
    };
    std::vector<Entry> getAll() const;

    bool addProjection(const StorageID & table_id, const ProjectionDescription & projection, bool if_not_exists);
    bool removeProjection(const StorageID & table_id, const String & projection_name, bool if_exists);

    void clearProjections();

    std::vector<ProjectionDescription> getProjectionsForTable(const StorageID & table_id) const;

    struct ProjectionEntry
    {
        StorageID table_id;
        ProjectionDescription projection;
        const String & name() const { return projection.name; }
    };
    std::vector<ProjectionEntry> getAllProjections() const;

private:
    mutable std::mutex mutex;
    std::vector<Entry> entries;
    std::vector<ProjectionEntry> projection_entries;
};

}
