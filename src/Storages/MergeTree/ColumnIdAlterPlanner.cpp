#include <Storages/MergeTree/ColumnIdAlterPlanner.h>

#include <Storages/AlterCommands.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/NestedUtils.h>
#include <Parsers/ASTSetQuery.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>

#include <fmt/core.h>
#include <fmt/ranges.h>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool share_nested_offsets;
    extern const MergeTreeSettingsMergeTreeSerializationInfoVersion serialization_info_version;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Table settings as they will be after this ALTER's `MODIFY SETTING` clauses
/// apply, so ALTERs mixing settings and column commands are evaluated against
/// the post-change state.
MergeTreeSettings settingsAfterAlter(
    const MergeTreeSettings & current_settings, const StorageInMemoryMetadata & new_metadata, ContextPtr context)
{
    MergeTreeSettings settings(current_settings);
    if (new_metadata.settings_changes)
        settings.applyChanges(
            new_metadata.settings_changes->as<const ASTSetQuery &>().changes,
            context,
            /*is_loading_from_existing_metadata=*/true);
    return settings;
}

namespace
{

/// The table setting that opts a table into column IDs.
bool columnIdSettingsEnabled(const MergeTreeSettings & settings)
{
    return settings[MergeTreeSetting::serialization_info_version] == MergeTreeSerializationInfoVersion::WITH_COLUMN_IDS;
}

/// First-time activation happens when the (post-ALTER) settings opt in and
/// the ALTER contains a command the mapping exists to serve.
bool alterActivatesColumnIds(const MergeTreeSettings & settings_after_alter, const AlterCommands & commands)
{
    bool has_compatible_alter = std::any_of(commands.begin(), commands.end(), [](const auto & c)
    {
        return c.type == AlterCommand::RENAME_COLUMN
            || c.type == AlterCommand::DROP_COLUMN
            || c.type == AlterCommand::ADD_COLUMN;
    });

    return columnIdSettingsEnabled(settings_after_alter) && has_compatible_alter;
}

/// Rename / drop chains where the rename target is the source of another
/// rename or a drop in the same ALTER (e.g. `RENAME b TO tmp, RENAME c TO b`
/// or `DROP COLUMN b, RENAME COLUMN c TO b`).  Two-phase rename keeps the
/// old name in the mapping during phase 1, so the second command would trip
/// the duplicate-logical-name check in `beginRename` and throw
/// `LOGICAL_ERROR`.  Reject these patterns explicitly so the user gets a
/// clear message; the workaround is to split into two ALTERs.
void rejectUnsafeRenameChains(const AlterCommands & commands)
{
    std::set<String> sources_freed_in_this_alter;
    for (const auto & c : commands)
    {
        if (c.ignore)
            continue;
        if (c.type == AlterCommand::RENAME_COLUMN || c.type == AlterCommand::DROP_COLUMN)
            sources_freed_in_this_alter.insert(c.column_name);
    }
    for (const auto & c : commands)
    {
        if (c.ignore || c.type != AlterCommand::RENAME_COLUMN)
            continue;
        if (sources_freed_in_this_alter.contains(c.rename_to))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "ALTER on column-IDs table: cannot rename '{}' to '{}' in the "
                "same ALTER that frees '{}' (via another RENAME or DROP).  "
                "Two-phase rename cannot represent this safely; split into "
                "two ALTER statements.",
                c.column_name, c.rename_to, c.rename_to);
    }
}

/// Legality of a single RENAME COLUMN on the metadata-only path.  For
/// flattened Nested siblings, the shared offset stream name is derived from
/// the Nested prefix of the physical name, so a cross-parent rename is safe
/// only when that prefix stays coherent for the whole group.
void validateTwoPhaseRename(
    const AlterCommand & command, const AlterCommands & commands, const ColumnIdMapping & mapping, bool nested_offsets_shared)
{
    auto physical = mapping.getColumnId(command.column_name);
    auto [phys_parent, phys_child] = Nested::splitName(physical);
    auto [old_parent, old_child] = Nested::splitName(command.column_name);
    auto [new_parent, new_child] = Nested::splitName(command.rename_to);
    bool is_nested = !old_child.empty();
    bool changes_prefix = (old_parent != new_parent);
    bool physical_has_dot = !phys_child.empty();

    /// Plain-counter column ID (no dot, e.g. "5"): the offset stream name is
    /// derived from the logical Nested parent, so a cross-parent rename would
    /// change it while existing parts still carry the old stream name.
    if (nested_offsets_shared && is_nested && changes_prefix && !physical_has_dot)
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Renaming flattened Nested column '{}' across parent "
            "boundaries ('{}' -> '{}') is not supported when the "
            "column has a plain-counter column ID '{}'. "
            "As a workaround, add a new column with the desired "
            "name and copy data via ALTER TABLE ... UPDATE",
            command.column_name,
            old_parent,
            new_parent,
            physical);
    }

    /// Partial cross-parent Nested move is unsafe: the writer
    /// folds the offset stream by the physical Nested prefix
    /// (either the compound counter "5" in "5.x"/"5.y" or the
    /// identity prefix "n" in "n.x"/"n.y"), so leaving any
    /// sibling behind under the old logical parent makes two
    /// logical parents read/write through one offsets stream.
    /// Require all siblings sharing this physical prefix to be
    /// renamed to the same new parent in the same ALTER.
    if (nested_offsets_shared && is_nested && changes_prefix && physical_has_dot)
    {
        const String old_logical_prefix = old_parent + ".";
        const String new_logical_prefix = new_parent + ".";
        for (const auto & [other_logical, other_physical] : mapping.getLogicalToId())
        {
            if (other_logical == command.column_name)
                continue;
            if (!other_logical.starts_with(old_logical_prefix))
                continue;
            auto [other_phys_parent, other_phys_child] = Nested::splitName(other_physical);
            if (other_phys_parent != phys_parent)
                continue;
            bool other_renamed = false;
            for (const auto & other_cmd : commands)
            {
                if (other_cmd.ignore || other_cmd.type != AlterCommand::RENAME_COLUMN)
                    continue;
                if (other_cmd.column_name == other_logical
                    && other_cmd.rename_to.starts_with(new_logical_prefix))
                {
                    other_renamed = true;
                    break;
                }
            }
            if (!other_renamed)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Cross-parent Nested rename of '{}' to '{}' requires "
                    "sibling column '{}' (sharing physical prefix '{}') "
                    "to also be renamed to a child of '{}' in the same "
                    "ALTER. Partial cross-parent moves are unsafe "
                    "because the shared offset stream cannot be split.",
                    command.column_name, command.rename_to,
                    other_logical, phys_parent, new_parent);
        }
    }
}

/// DROP (or RENAME) of a column plus ADD COLUMN of the same name in one ALTER
/// cannot be made crash-safe as metadata-only: after a crash in between, the
/// re-added name would resolve to the freed column's bytes (see ColumnIdMapping.h).
void rejectDropOrRenameThenReAdd(const AlterCommands & commands, const std::set<String> & new_col_names)
{
    std::set<String> readded_names;

    /// Whole-column DROPs only: `CLEAR COLUMN` and partition-scoped
    /// `DROP COLUMN ... IN PARTITION` don't remove the column from metadata.
    std::set<String> explicitly_dropped;
    for (const auto & command : commands)
    {
        if (command.ignore)
            continue;
        if (command.type == AlterCommand::DROP_COLUMN && !command.clear && !command.partition)
            explicitly_dropped.insert(command.column_name);
    }
    for (const auto & dropped_name : explicitly_dropped)
    {
        if (new_col_names.contains(dropped_name))
        {
            readded_names.insert(dropped_name);
        }
        else
        {
            String prefix = dropped_name + ".";
            for (const auto & new_name : new_col_names)
            {
                if (new_name.starts_with(prefix))
                    readded_names.insert(new_name);
            }
        }
    }

    /// The RENAME side: `AlterCommands::validate` accepts
    /// `RENAME COLUMN b TO old_b, ADD COLUMN b ...` because validation is
    /// order-aware (after the rename, `b` is free), but once
    /// `finalizeColumnIdRenames` removes the old `b -> b` entry, the mapping
    /// has no entry for the new `b` and reads fall back to physical name `b`,
    /// which is the renamed column's bytes.
    std::set<String> rename_freed_sources;
    for (const auto & command : commands)
    {
        if (command.ignore || command.type != AlterCommand::RENAME_COLUMN)
            continue;
        rename_freed_sources.insert(command.column_name);
    }
    for (const auto & command : commands)
    {
        if (command.ignore || command.type != AlterCommand::ADD_COLUMN)
            continue;
        if (rename_freed_sources.contains(command.column_name))
        {
            readded_names.insert(command.column_name);
        }
        else
        {
            String prefix = command.column_name + ".";
            for (const auto & new_name : new_col_names)
            {
                if (new_name.starts_with(prefix) && rename_freed_sources.contains(new_name))
                    readded_names.insert(new_name);
            }
        }
    }

    if (!readded_names.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "ALTER on column-IDs table: DROP/RENAME of '{}' followed by ADD COLUMN "
            "of the same name in a single ALTER cannot be made crash-safe.  Split "
            "into two separate ALTER statements (the destructive op first, then "
            "the ADD); each is fully durable.",
            fmt::join(readded_names, "', '"));
}

/// Allocates column IDs for the columns this ALTER adds.  Flattened Nested
/// siblings share a compound ID prefix and incremental child additions reuse
/// it, so the shared offset stream (e.g. "5.size0") stays coherent across
/// old and new parts.
void allocateNewColumnIds(
    const StorageInMemoryMetadata & new_metadata, const std::set<String> & old_col_names, ColumnIdMapping & mapping)
{
    std::map<String, std::vector<std::pair<String, String>>> nested_groups;
    std::map<String, String> incremental_child_prefix; // logical -> phys parent
    std::vector<String> plain_new_columns;

    for (const auto & col : new_metadata.getColumns().getAllPhysical())
    {
        if (old_col_names.contains(col.name) || mapping.hasLogicalName(col.name))
            continue;

        auto [parent, child] = Nested::splitName(col.name);
        if (!child.empty())
        {
            String existing_prefix;
            for (const auto & other : new_metadata.getColumns().getAllPhysical())
            {
                if (other.name == col.name)
                    continue;
                auto [other_parent, other_child] = Nested::splitName(other.name);
                if (other_child.empty() || other_parent != parent)
                    continue;
                if (!old_col_names.contains(other.name))
                    continue;
                if (!mapping.hasLogicalName(other.name))
                    continue;
                const String & sibling_id = mapping.getColumnId(other.name);
                auto [sib_phys_parent, sib_phys_child] = Nested::splitName(sibling_id);
                if (!sib_phys_child.empty())
                {
                    existing_prefix = sib_phys_parent;
                    break;
                }
            }
            if (!existing_prefix.empty())
            {
                incremental_child_prefix[col.name] = existing_prefix;
                continue;
            }

            bool has_new_sibling = false;
            for (const auto & other : new_metadata.getColumns().getAllPhysical())
            {
                if (other.name == col.name)
                    continue;
                auto [other_parent, other_child] = Nested::splitName(other.name);
                if (!other_child.empty() && other_parent == parent
                    && !old_col_names.contains(other.name))
                {
                    has_new_sibling = true;
                    break;
                }
            }
            if (has_new_sibling)
            {
                nested_groups[parent].emplace_back(col.name, child);
                continue;
            }
        }
        plain_new_columns.push_back(col.name);
    }

    /// Allocate compound column IDs for flattened Nested groups (initial
    /// `ADD COLUMN n Nested(...)`).
    for (const auto & [parent, siblings] : nested_groups)
    {
        auto base = mapping.allocateColumnId();
        for (const auto & [logical_name, child_name] : siblings)
            mapping.addColumn(logical_name, base + "." + child_name);
    }

    /// Incremental child additions inherit the existing siblings' physical
    /// prefix so the shared offset stream stays coherent.
    for (const auto & [logical_name, phys_parent] : incremental_child_prefix)
    {
        auto [_, child_name] = Nested::splitName(logical_name);
        mapping.addColumn(logical_name, phys_parent + "." + child_name);
    }

    /// Allocate plain counter-based column IDs for non-Nested columns.
    for (const auto & col_name : plain_new_columns)
    {
        auto new_physical = mapping.allocateColumnId();
        mapping.addColumn(col_name, new_physical);
    }
}

}

ColumnIdAlterPlan prepareColumnIdMappingForAlter(
    const AlterCommands & commands,
    const StorageInMemoryMetadata & old_metadata,
    const StorageInMemoryMetadata & new_metadata,
    const MergeTreeSettings & current_settings,
    const ColumnIdMappingPtr & current_mapping,
    ContextPtr context)
{
    ColumnIdAlterPlan plan;

    /// Evaluate activation against the post-MODIFY-SETTING state so that a
    /// single ALTER mixing `MODIFY SETTING ... , RENAME COLUMN ...` activates
    /// column IDs and takes the metadata-only path in one shot.
    MergeTreeSettings effective_new_settings = settingsAfterAlter(current_settings, new_metadata, context);

    plan.persists_column_id_settings = effective_new_settings[MergeTreeSetting::serialization_info_version]
        == MergeTreeSerializationInfoVersion::WITH_COLUMN_IDS;

    bool should_activate = current_mapping == nullptr && alterActivatesColumnIds(effective_new_settings, commands);
    plan.column_ids_active = (current_mapping != nullptr) || should_activate;

    if (!plan.column_ids_active)
        return plan;

    ColumnIdMapping local_mapping;
    if (should_activate)
        local_mapping = ColumnIdMapping::createIdentity(old_metadata.getColumns().getAllPhysical());
    else if (current_mapping)
        local_mapping = *current_mapping;

    rejectUnsafeRenameChains(commands);

    /// When `share_nested_offsets` is off, dotted columns are independent and
    /// don't share an offsets stream, so the flattened-Nested rename
    /// restrictions don't apply.  `AlterCommands::validate` already disables
    /// the cross-parent Nested rule in that mode; mirror that here.
    bool nested_offsets_shared = effective_new_settings[MergeTreeSetting::share_nested_offsets];

    /// Two-phase rename: `beginRename` keeps both old and new logical names
    /// in the mapping so the persisted state is crash-safe.  After metadata
    /// commit, `finishRename` removes the old entry.
    for (const auto & command : commands)
    {
        if (command.ignore || command.type != AlterCommand::RENAME_COLUMN)
            continue;
        if (!local_mapping.hasLogicalName(command.column_name))
            continue;

        validateTwoPhaseRename(command, commands, local_mapping, nested_offsets_shared);
        local_mapping.beginRename(command.column_name, command.rename_to);
        plan.rename_old_names.push_back(command.column_name);
    }

    /// Use the actual metadata diff to handle flattened Nested correctly:
    /// `commands.apply` expands `ADD COLUMN n Nested(...)` into `n.x`, `n.y`, etc.
    std::set<String> old_col_names;
    for (const auto & col : old_metadata.getColumns().getAllPhysical())
        old_col_names.insert(col.name);
    std::set<String> new_col_names;
    for (const auto & col : new_metadata.getColumns().getAllPhysical())
        new_col_names.insert(col.name);

    rejectDropOrRenameThenReAdd(commands, new_col_names);

    allocateNewColumnIds(new_metadata, old_col_names, local_mapping);

    /// Two-phase drop for crash safety: keep dropped columns in the mapping until
    /// after the metadata commit; `finalizeColumnIdDrops` removes them post-commit
    /// (see the two-phase / reconcile contract in ColumnIdMapping.h).
    std::set<String> rename_old_set(plan.rename_old_names.begin(), plan.rename_old_names.end());
    for (const auto & col : old_metadata.getColumns().getAllPhysical())
    {
        if (!new_col_names.contains(col.name) && local_mapping.hasLogicalName(col.name)
            && !rename_old_set.contains(col.name))
            plan.drop_names.push_back(col.name);
    }

    plan.new_mapping.emplace(std::move(local_mapping));
    return plan;
}

}
