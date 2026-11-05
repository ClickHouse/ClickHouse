#pragma once

#include <Storages/IndicesDescription.h>
#include <Storages/ProjectionsDescription.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IASTHash.h>

#include <map>
#include <mutex>
#include <vector>

namespace DB
{

struct StorageInMemoryMetadata;

/// what a hypothetical definition binds to in the table: the type and default of each column it names,
/// and the table settings; only `CREATE` validates a definition, `EXPLAIN WHATIF` compares this
struct HypotheticalBinding
{
    /// a name that is not a column maps to an empty string
    std::map<String, String> columns;
    /// tree hashes of the bound defaults, because `columns` hides what `query_masking_rules` match
    std::map<String, IASTHash> defaults;
    /// the storage columns behind `columns`
    NameSet storage_columns;
    /// the ordered expansion of `*`
    Names wildcard;
    String settings;

    static HypotheticalBinding of(const IAST & definition, const StorageInMemoryMetadata & metadata);
    static HypotheticalBinding of(const ProjectionDescription & projection, const StorageInMemoryMetadata & metadata);

    /// empty when nothing changed, otherwise what changed; a change needs SELECT on the bound columns
    String changeTo(const HypotheticalBinding & current, const ContextPtr & context, const StorageID & table_id) const;
};

/// Session-scoped store for hypothetical indexes and projections, used by EXPLAIN WHATIF
class HypotheticalObjectStore
{
public:
    bool add(const StorageID & table_id, const IndexDescription & index, const HypotheticalBinding & binding, bool if_not_exists);
    bool remove(const StorageID & table_id, const String & index_name, bool if_exists);

    void clear();

    struct Entry
    {
        StorageID table_id;
        IndexDescription index;
        HypotheticalBinding binding;
        const String & name() const { return index.name; }
    };
    std::vector<Entry> getForTable(const StorageID & table_id) const;
    std::vector<Entry> getAll() const;

    bool addProjection(
        const StorageID & table_id, const ProjectionDescription & projection, const HypotheticalBinding & binding, bool if_not_exists);
    bool removeProjection(const StorageID & table_id, const String & projection_name, bool if_exists);

    void clearProjections();

    struct ProjectionEntry
    {
        StorageID table_id;
        ProjectionDescription projection;
        HypotheticalBinding binding;
        const String & name() const { return projection.name; }
    };
    std::vector<ProjectionEntry> getProjectionsForTable(const StorageID & table_id) const;
    std::vector<ProjectionEntry> getAllProjections() const;

private:
    mutable std::mutex mutex;
    std::vector<Entry> entries;
    std::vector<ProjectionEntry> projection_entries;
};

}
