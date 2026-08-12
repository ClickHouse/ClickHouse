#include <Storages/MergeTree/ColumnIdAlterPlanner.h>

#include <Storages/AlterCommands.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/NestedUtils.h>
#include <Parsers/ASTSetQuery.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

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

namespace
{

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

/// A command names a Nested column by its parent ("n"), which no flattened column list holds --
/// only its children ("n.x"). Those children, among `physical_names`.
Names nestedChildrenIn(const String & parent_name, const std::set<String> & physical_names)
{
    Names children;
    const String prefix = parent_name + ".";
    for (const auto & physical_name : physical_names)
    {
        if (physical_name.starts_with(prefix))
            children.push_back(physical_name);
    }
    return children;
}

/// What a command's subject names among `schema_names`: itself, or a Nested parent's children --
/// a parent is not itself a physical column.
Names physicalNamesOf(const String & column_name, const std::set<String> & schema_names)
{
    if (schema_names.contains(column_name))
        return {column_name};
    return nestedChildrenIn(column_name, schema_names);
}

/// Whether this ALTER also moves `column_name` under `new_parent`, which is what makes a
/// cross-parent Nested move whole rather than partial.
bool isRenamedUnderParent(const AlterCommands & commands, const String & column_name, const String & new_parent)
{
    const String new_logical_prefix = new_parent + ".";
    for (const auto & command : commands)
    {
        if (command.ignore || command.type != AlterCommand::RENAME_COLUMN)
            continue;
        if (command.column_name == column_name && command.rename_to.starts_with(new_logical_prefix))
            return true;
    }
    return false;
}

/// Legality of a single RENAME COLUMN on the metadata-only path.  For
/// flattened Nested siblings, the shared offset stream name is derived from
/// the Nested prefix of the column ID, so a cross-parent rename is safe
/// only when that prefix stays coherent for the whole group.
void validateTwoPhaseRename(
    const AlterCommand & command, const AlterCommands & commands, const ColumnIdMapping & mapping, bool nested_offsets_shared)
{
    auto column_id = mapping.getColumnId(command.column_name);
    auto [id_parent, id_child] = Nested::splitName(column_id);
    auto [old_parent, old_child] = Nested::splitName(command.column_name);
    auto [new_parent, new_child] = Nested::splitName(command.rename_to);

    bool is_nested = !old_child.empty();
    bool changes_prefix = (old_parent != new_parent);
    if (!nested_offsets_shared || !is_nested || !changes_prefix)
        return;

    /// Plain-counter column ID (no dot, e.g. "5"): the offset stream name is
    /// derived from the logical Nested parent, so a cross-parent rename would
    /// change it while existing parts still carry the old stream name.
    if (id_child.empty())
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
            column_id);
    }

    /// Partial cross-parent Nested move is unsafe: leaving a sibling under the old logical parent
    /// makes two logical parents read and write through one offsets stream. A sibling shares that
    /// stream either through the same column-ID prefix, or -- for a plain-counter sibling, which a
    /// mapping written before one-pass allocation can hold -- through the old logical parent.
    const String old_logical_prefix = old_parent + ".";
    for (const auto & [other_logical, other_column_id] : mapping.getLogicalToId())
    {
        if (other_logical == command.column_name || !other_logical.starts_with(old_logical_prefix))
            continue;

        auto [other_id_parent, other_id_child] = Nested::splitName(other_column_id);
        bool shares_offsets = other_id_child.empty() || other_id_parent == id_parent;
        if (!shares_offsets || isRenamedUnderParent(commands, other_logical, new_parent))
            continue;

        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Cross-parent Nested rename of '{}' to '{}' requires "
            "sibling column '{}' (column ID '{}', sharing the group's "
            "offset stream) to also be renamed to a child of '{}' in the "
            "same ALTER. Partial cross-parent moves are unsafe "
            "because the shared offset stream cannot be split.",
            command.column_name, command.rename_to,
            other_logical, other_column_id, new_parent);
    }
}

/// A name the table ALREADY has must not be claimed by this ALTER -- as an `ADD COLUMN` or as a
/// RENAME's target -- even when the same ALTER drops or renames the column holding it.
/// `AlterCommands::validate` accepts that, being order-aware, but the mapping cannot represent it:
/// phase 1 keeps the old name until the metadata commit, so `beginRename` sees the claimed name
/// twice, and once phase 2 prunes the entry the claim resolves to the old column's ID -- its bytes
/// (see ColumnIdMapping.h). So collect the claims and reject any that the old schema holds.
/// A claim is a column as it BECOMES, hence the new schema when expanding a Nested parent.
void rejectNameReuse(
    const AlterCommands & commands, const std::set<String> & old_col_names, const std::set<String> & new_col_names)
{
    std::set<String> claimed_names;

    for (const auto & command : commands)
    {
        if (command.ignore)
            continue;

        if (command.type == AlterCommand::ADD_COLUMN)
        {
            auto names = physicalNamesOf(command.column_name, new_col_names);
            claimed_names.insert(names.begin(), names.end());
        }
        else if (command.type == AlterCommand::RENAME_COLUMN)
        {
            /// Renaming a whole Nested struct is refused upstream, so a target needs no expansion.
            claimed_names.insert(command.rename_to);
        }
    }

    Names reused_names;
    for (const auto & claimed_name : claimed_names)
    {
        if (old_col_names.contains(claimed_name))
            reused_names.push_back(claimed_name);
    }

    if (!reused_names.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "ALTER on column-IDs table: '{}' is a column this table already has, and this ALTER "
            "claims the name again -- by ADD COLUMN, or as a RENAME target -- which cannot be made "
            "crash-safe even if the same ALTER frees it.  Split into two separate ALTER statements, "
            "the destructive one first; each is fully durable.",
            fmt::join(reused_names, "', '"));
}

/// Allocates column IDs for the columns this ALTER adds.  Flattened Nested
/// siblings share a compound ID prefix and incremental child additions reuse
/// it, so the shared offset stream (e.g. "5.size0") stays coherent across
/// old and new parts.
void allocateNewColumnIds(
    const StorageInMemoryMetadata & new_metadata,
    const std::set<String> & old_col_names,
    const std::set<String> & rename_targets,
    ColumnIdMapping & mapping)
{
    const auto physical_columns = new_metadata.getColumns().getAllPhysical();

    /// A rename target was added to the mapping by phase 1 of this same ALTER, so it is no more
    /// "new" than a name the old schema had. Asking the whole mapping instead would also match a
    /// name an EARLIER ALTER retained, and bind the new column to the retained entry's ID.
    auto is_new_column = [&](const String & name) { return !old_col_names.contains(name) && !rename_targets.contains(name); };

    /// A retained entry is residue from an earlier ALTER whose finalize did not persist: only a
    /// table load prunes it, so it outlives the ALTER that left it. Drop it, since `addColumn`
    /// rejects a duplicate logical name and the re-added column must not inherit the freed ID.
    for (const auto & column : physical_columns)
    {
        if (!is_new_column(column.name) || !mapping.hasLogicalName(column.name))
            continue;

        LOG_WARNING(
            getLogger("ColumnIdAlterPlanner"),
            "Column ID mapping has a retained entry for '{}' (column ID '{}') that the schema has re-added; "
            "it was left behind by an earlier ALTER whose finalize did not persist. Dropping the entry and "
            "allocating a fresh column ID.",
            column.name, mapping.getColumnId(column.name));

        mapping.removeColumn(column.name);
    }

    /// The compound id prefix a flattened Nested parent's children take ("5" for "5.x"), so the group
    /// shares one offsets stream. Empty means the group keeps the plain-counter convention instead:
    /// a plain id derives its offsets stream from the LOGICAL parent, so a dotted new child beside
    /// plain siblings would open a second stream inside one logical group. Resolved once per parent:
    /// inherited from an already-mapped sibling, or a fresh counter id when the group has none.
    std::map<String, String> id_parent_by_nested_parent;

    auto nested_id_parent = [&](const String & parent) -> const String &
    {
        auto [it, inserted] = id_parent_by_nested_parent.try_emplace(parent);
        if (!inserted)
            return it->second;

        bool has_mapped_sibling = false;
        for (const auto & column : physical_columns)
        {
            auto [other_parent, other_child] = Nested::splitName(column.name);
            if (other_child.empty() || other_parent != parent || is_new_column(column.name))
                continue;

            has_mapped_sibling = true;
            auto [id_parent, id_child] = Nested::splitName(mapping.getColumnIdOrDefault(column.name));
            if (!id_child.empty())
            {
                it->second = id_parent;
                return it->second;
            }
        }

        if (!has_mapped_sibling)
            it->second = mapping.allocateColumnId();
        return it->second;
    };

    for (const auto & column : physical_columns)
    {
        if (!is_new_column(column.name))
            continue;

        auto [parent, child] = Nested::splitName(column.name);
        const String id_parent = child.empty() ? "" : nested_id_parent(parent);

        /// The child half of the id is a counter, never the logical child name: a name-derived half
        /// is reconstructed identically when a DROP and a re-ADD of that name land in two separate
        /// ALTERs, which would hand the re-added column the dropped column's streams. The counter
        /// never recycles, so the compound id is unique by construction.
        if (id_parent.empty())
            mapping.addColumn(column.name, mapping.allocateColumnId());
        else
            mapping.addColumn(column.name, id_parent + "." + mapping.allocateColumnId());
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

    /// A non-null but INACTIVE mapping (e.g. a leftover `column_ids.json` from a
    /// failed activation) means parts still use logical filenames, so it must not
    /// short-circuit to the "already active" path -- that would make RENAME/DROP
    /// metadata-only and break reads.  Gate on activation, not on non-null.
    bool has_active_mapping = current_mapping != nullptr && current_mapping->isActive();
    bool should_activate = !has_active_mapping && alterActivatesColumnIds(effective_new_settings, commands);
    plan.column_ids_active = has_active_mapping || should_activate;

    if (!plan.column_ids_active)
        return plan;

    ColumnIdMapping local_mapping;
    if (should_activate)
        local_mapping = ColumnIdMapping::createIdentity(old_metadata.getColumns().getAllPhysical());
    else
        local_mapping = *current_mapping;

    /// Use the actual metadata diff to handle flattened Nested correctly:
    /// `commands.apply` expands `ADD COLUMN n Nested(...)` into `n.x`, `n.y`, etc.
    std::set<String> old_col_names;
    for (const auto & col : old_metadata.getColumns().getAllPhysical())
        old_col_names.insert(col.name);
    std::set<String> new_col_names;
    for (const auto & col : new_metadata.getColumns().getAllPhysical())
        new_col_names.insert(col.name);

    /// Ahead of phase 1, which is where a name claimed twice would otherwise surface as
    /// `beginRename`'s duplicate-logical-name LOGICAL_ERROR.
    rejectNameReuse(commands, old_col_names, new_col_names);

    /// When `share_nested_offsets` is off, dotted columns are independent and
    /// don't share an offsets stream, so the flattened-Nested rename
    /// restrictions don't apply.  `AlterCommands::validate` already disables
    /// the cross-parent Nested rule in that mode; mirror that here.
    bool nested_offsets_shared = effective_new_settings[MergeTreeSetting::share_nested_offsets];

    /// Two-phase rename: `beginRename` keeps both old and new logical names
    /// in the mapping so the persisted state is crash-safe.  After metadata
    /// commit, `finishRename` removes the old entry.
    std::set<String> rename_targets;
    for (const auto & command : commands)
    {
        if (command.ignore || command.type != AlterCommand::RENAME_COLUMN)
            continue;
        if (!local_mapping.hasLogicalName(command.column_name))
            continue;

        validateTwoPhaseRename(command, commands, local_mapping, nested_offsets_shared);
        local_mapping.beginRename(command.column_name, command.rename_to);
        plan.rename_old_names.push_back(command.column_name);
        rename_targets.insert(command.rename_to);
    }

    allocateNewColumnIds(new_metadata, old_col_names, rename_targets, local_mapping);

    /// Two-phase drop for crash safety: keep dropped columns in the mapping until after the
    /// metadata commit, where phase 2 removes them (see the two-phase / reconcile contract
    /// in ColumnIdMapping.h).
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
