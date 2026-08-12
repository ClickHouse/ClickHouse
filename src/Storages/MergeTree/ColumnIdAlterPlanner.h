#pragma once

#include <Storages/MergeTree/ColumnIdMapping.h>
#include <Interpreters/Context_fwd.h>

#include <optional>
#include <vector>

namespace DB
{

class AlterCommands;
struct StorageInMemoryMetadata;
struct MergeTreeSettings;

/// How the column ID mapping should change for an ALTER, split across the two publish phases
/// (the crash-safety contract is in ColumnIdMapping.h): `new_mapping` goes out before the durable
/// metadata commit, then the two prune lists are applied after it.
struct ColumnIdAlterPlan
{
    /// The post-ALTER mapping in its phase-1 superset form: renamed columns hold both names,
    /// added columns have fresh IDs, dropped columns are still present. Set iff `column_ids_active`.
    std::optional<ColumnIdMapping> new_mapping;

    /// Old names of renamed columns; phase 2 prunes each with `finishRename`.
    std::vector<String> rename_old_names;

    /// Dropped columns' names; phase 2 removes each from the mapping.
    std::vector<String> drop_names;

    /// Whether this ALTER runs in column-ID mode: the table already has a mapping, or this ALTER
    /// activates one. Gates the metadata-only RENAME/DROP path and the mapping publish steps.
    bool column_ids_active = false;

    /// true iff post-ALTER settings persist serialization_info_version='with_column_ids'.
    bool persists_column_id_settings = false;
};

/// Compute how the column ID mapping should change for the given ALTER: first-time activation,
/// two-phase renames, adds/drops, and the flattened Nested guard. `current_mapping` is null when
/// the table has no mapping yet.
ColumnIdAlterPlan prepareColumnIdMappingForAlter(
    const AlterCommands & commands,
    const StorageInMemoryMetadata & old_metadata,
    const StorageInMemoryMetadata & new_metadata,
    const MergeTreeSettings & current_settings,
    const ColumnIdMappingPtr & current_mapping,
    ContextPtr context);

}
