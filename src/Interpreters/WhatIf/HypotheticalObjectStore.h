#pragma once

#include <Storages/IndicesDescription.h>
#include <Storages/ProjectionsDescription.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IASTHash.h>
#include <Storages/ColumnDefault.h>

#include <map>
#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

struct StorageInMemoryMetadata;

/// what a hypothetical definition binds to in the table: the columns it names and its analysis read, the ordered expansion
/// of each matcher, and the table settings; only `CREATE` validates a definition, `EXPLAIN WHATIF` compares this
struct HypotheticalBinding
{
    struct Column
    {
        String name_in_storage;
        String type;
        std::optional<ColumnDefaultKind> default_kind;
        IASTHash default_hash{};
        /// masked by `query_masking_rules`, so only for the reason
        String default_text;

        bool operator==(const Column & other) const
        {
            return type == other.type && default_kind == other.default_kind && default_hash == other.default_hash;
        }
    };

    std::map<String, Column> columns;
    std::vector<std::pair<ASTPtr, Names>> expansions;
    String settings;

    static HypotheticalBinding of(const IndexDescription & index, const StorageInMemoryMetadata & metadata);
    static HypotheticalBinding of(const ProjectionDescription & projection, const StorageInMemoryMetadata & metadata);

    /// empty when nothing changed, otherwise what changed; a change needs SELECT on the columns it names
    String changeIn(const StorageInMemoryMetadata & metadata, const ContextPtr & context, const StorageID & table_id) const;
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
        [[nodiscard]] const String & name() const { return index.name; }
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
        [[nodiscard]] const String & name() const { return projection.name; }
    };
    std::vector<ProjectionEntry> getProjectionsForTable(const StorageID & table_id) const;
    std::vector<ProjectionEntry> getAllProjections() const;

private:
    mutable std::mutex mutex;
    std::vector<Entry> entries;
    std::vector<ProjectionEntry> projection_entries;
};

}
