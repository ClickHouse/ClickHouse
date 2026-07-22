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

/// How the column ID mapping should change for an ALTER, computed up-front and then
/// applied in two phases for crash safety (see the contract in ColumnIdMapping.h):
///   phase 1 (before the durable metadata commit): publish `new_mapping`, a SUPERSET
///           that still carries every renamed column's old name and every dropped
///           column, so a part surviving a crash mid-ALTER can still resolve its files;
///   phase 2 (after the commit succeeds): prune that superset using `rename_old_names`
///           and `drop_names`.
/// The add-before-commit / remove-after-commit asymmetry keeps the persisted mapping a
/// superset of what any surviving part needs at every crash point -- never missing an
/// entry. That is why the plan carries two prune lists instead of just a final mapping.
struct ColumnIdAlterPlan
{
    /// The post-ALTER mapping in its phase-1 SUPERSET form: renames have both the old and
    /// new logical names pointing at the ID (beginRename), added columns have fresh IDs,
    /// and dropped columns are still present. Published (disk then in-memory) before the
    /// metadata commit. Populated iff `column_ids_active`.
    std::optional<ColumnIdMapping> new_mapping;

    /// Old logical names of renamed columns, kept alive in `new_mapping` across the commit;
    /// finalizeColumnIdRenames prunes each (finishRename) afterwards. Retained through the
    /// commit so a crash before it leaves the old name -- which metadata still uses -- resolvable.
    std::vector<String> rename_old_names;

    /// Dropped columns' logical names, likewise kept in `new_mapping` until after the commit;
    /// finalizeColumnIdDrops removes them (two-phase drop for crash safety).
    std::vector<String> drop_names;

    /// Whether this ALTER runs in column-ID mode: true if the table already has a mapping or
    /// this ALTER activates one. Gates the metadata-only RENAME/DROP path (no data rewrite)
    /// and whether the mapping-publish / finalize steps run at all.
    bool column_ids_active = false;

    /// true iff post-ALTER settings persist serialization_info_version='with_column_ids'.
    bool persists_column_id_settings = false;
};

/// Table settings as they will be after this ALTER's `MODIFY SETTING` clauses
/// apply, so ALTERs mixing settings and column commands are evaluated against
/// the post-change state.
MergeTreeSettings settingsAfterAlter(
    const MergeTreeSettings & current_settings, const StorageInMemoryMetadata & new_metadata, ContextPtr context);

/// Compute how the column ID mapping should change for the given ALTER.
/// Handles first-time activation, two-phase renames, column adds/drops, and
/// the flattened Nested guard. `current_mapping` is null when the table has no
/// mapping yet.
ColumnIdAlterPlan prepareColumnIdMappingForAlter(
    const AlterCommands & commands,
    const StorageInMemoryMetadata & old_metadata,
    const StorageInMemoryMetadata & new_metadata,
    const MergeTreeSettings & current_settings,
    const ColumnIdMappingPtr & current_mapping,
    ContextPtr context);

}
