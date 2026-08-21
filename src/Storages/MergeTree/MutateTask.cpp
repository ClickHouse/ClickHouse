#include <Interpreters/TreeRewriter.h>
#include <Storages/MergeTree/MutateTask.h>

#include <Disks/SingleDiskVolume.h>
#include <IO/HashingWriteBuffer.h>
#include <Common/logger_useful.h>
#include <Common/escapeForFileName.h>
#include <Core/Settings.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/Statistics/Statistics.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/Squashing.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/createSubcolumnsExtractionActions.h>
#include <Processors/Transforms/TTLTransform.h>
#include <Processors/Transforms/TTLCalcTransform.h>
#include <Processors/Transforms/DistinctSortedTransform.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeProjectionPartsTask.h>
#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/TextIndexUtils.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeVariant.h>
#include <boost/algorithm/string/replace.hpp>
#include <Common/ProfileEventsScope.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Common/FailPoint.h>
#include <Storages/MergeTree/StatisticsSerialization.h>


namespace ProfileEvents
{
    extern const Event MutationTotalParts;
    extern const Event MutationUntouchedParts;
    extern const Event MutationCreatedEmptyParts;
    extern const Event MutationTotalMilliseconds;
    extern const Event MutationExecuteMilliseconds;
    extern const Event MutationAllPartColumns;
    extern const Event MutationSomePartColumns;
    extern const Event MutateTaskProjectionsCalculationMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric PartMutation;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 min_insert_block_size_bytes;
    extern const SettingsUInt64 min_insert_block_size_rows;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_remote_fs_zero_copy_replication;
    extern const MergeTreeSettingsBool always_use_copy_instead_of_hardlinks;
    extern const MergeTreeSettingsMilliseconds background_task_preferred_step_execution_time_ms;
    extern const MergeTreeSettingsBool exclude_deleted_rows_for_part_size_in_merge;
    extern const MergeTreeSettingsLightweightMutationProjectionMode lightweight_mutation_projection_mode;
    extern const MergeTreeSettingsBool materialize_ttl_recalculate_only;
    extern const MergeTreeSettingsFloat ratio_of_defaults_for_sparse_serialization;
    extern const MergeTreeSettingsBool ttl_only_drop_parts;
    extern const MergeTreeSettingsBool enable_index_granularity_compression;
    extern const MergeTreeSettingsBool columns_and_secondary_indices_sizes_lazy_calculation;
    extern const MergeTreeSettingsMergeTreeSerializationInfoVersion serialization_info_version;
    extern const MergeTreeSettingsMergeTreeStringSerializationVersion string_serialization_version;
    extern const MergeTreeSettingsMergeTreeNullableSerializationVersion nullable_serialization_version;
}

namespace FailPoints
{
    extern const char mt_mutate_task_pause_in_prepare[];
}

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

enum class ExecuteTTLType : uint8_t
{
    NONE = 0,
    NORMAL = 1,
    RECALCULATE= 2,
};

namespace MutationHelpers
{

static bool haveMutationsOfDynamicColumns(const MergeTreeData::DataPartPtr & data_part, const MutationCommands & commands)
{
    for (const auto & command : commands)
    {
        if (!command.column_name.empty())
        {
            auto column = data_part->tryGetColumn(command.column_name);
            if (column && column->type->hasDynamicSubcolumns())
                return true;
        }

        for (const auto & [column_name, _] : command.column_to_update_expression)
        {
            auto column = data_part->tryGetColumn(column_name);
            if (column && column->type->hasDynamicSubcolumns())
                return true;
        }
    }

    return false;
}

static UInt64 getExistingRowsCount(const Block & block)
{
    auto column = block.getByName(RowExistsColumn::name).column;
    const ColumnUInt8 * row_exists_col = typeid_cast<const ColumnUInt8 *>(column.get());

    if (!row_exists_col)
    {
        LOG_WARNING(&Poco::Logger::get("MutationHelpers::getExistingRowsCount"), "_row_exists column type is not UInt8");
        return block.rows();
    }

    UInt64 existing_count = 0;

    for (UInt8 row_exists : row_exists_col->getData())
        if (row_exists)
            existing_count++;

    return existing_count;
}

static NameSet getRemovedStatistics(const StorageMetadataPtr & metadata_snapshot, const MutationCommand & command)
{
    if (command.type != MutationCommand::Type::DROP_STATISTICS)
        return {};

    NameSet removed_stats;

    if (command.clear && command.statistics_columns.empty())
    {
        for (const auto & column_desc : metadata_snapshot->getColumns())
            removed_stats.insert(column_desc.name);
    }
    else
    {
        for (const auto & column_name : command.statistics_columns)
            removed_stats.insert(column_name);
    }

    return removed_stats;
}

/** Split mutation commands into two parts:
*   First part should be executed by mutations interpreter.
*   Other is just simple drop/renames, so they can be executed without interpreter.
*/
static void splitAndModifyMutationCommands(
    MergeTreeData::DataPartPtr part,
    StorageMetadataPtr metadata_snapshot,
    AlterConversionsPtr alter_conversions,
    const MutationCommands & commands,
    MutationCommands & for_interpreter,
    MutationCommands & for_file_renames,
    bool suitable_for_ttl_optimization,
    LoggerPtr log)
{
    auto part_columns = part->getColumnsDescription();
    const auto & table_columns = metadata_snapshot->getColumns();

    if (haveMutationsOfDynamicColumns(part, commands) || !isWidePart(part) || !isFullPartStorage(part->getDataPartStorage()))
    {
        NameSet mutated_columns;
        NameSet dropped_columns;
        NameSet ignored_columns;
        NameSet extra_columns_for_indices_and_projections;
        auto storage_columns = metadata_snapshot->getColumns().getAllPhysical().getNameSet();

        for (const auto & command : commands)
        {
            if (command.type == MutationCommand::Type::MATERIALIZE_COLUMN)
            {
                /// For ordinary column with default or materialized expression, MATERIALIZE COLUMN should not override past values
                /// So we only mutate column if `command.column_name` is a default/materialized column or if the part does not have physical column file
                auto column_ordinary = table_columns.getOrdinary().tryGetByName(command.column_name);
                if (!column_ordinary || !part->tryGetColumn(command.column_name) || !part->hasColumnFiles(*column_ordinary))
                {
                    for_interpreter.push_back(command);
                    mutated_columns.emplace(command.column_name);
                }

                /// Materialize column in case of complex data types like tuple can remove some nested columns
                /// Here we add it "for renames" because these set of commands also removes redundant files
                if (part_columns.has(command.column_name))
                    for_file_renames.push_back(command);
            }
            else if (command.type == MutationCommand::READ_COLUMN)
            {
                bool has_column = part_columns.has(command.column_name) || part_columns.hasNested(command.column_name);
                if (has_column || command.read_for_patch)
                {
                    for_interpreter.push_back(command);
                    mutated_columns.insert(command.column_name);
                }
            }
            else if (command.type == MutationCommand::Type::MATERIALIZE_INDEX
                || command.type == MutationCommand::Type::MATERIALIZE_STATISTICS
                || command.type == MutationCommand::Type::MATERIALIZE_PROJECTION
                || command.type == MutationCommand::Type::MATERIALIZE_TTL
                || command.type == MutationCommand::Type::REWRITE_PARTS
                || command.type == MutationCommand::Type::DELETE
                || command.type == MutationCommand::Type::UPDATE
                || command.type == MutationCommand::Type::APPLY_DELETED_MASK
                || command.type == MutationCommand::Type::APPLY_PATCHES)
            {
                for_interpreter.push_back(command);
                for (const auto & [column_name, expr] : command.column_to_update_expression)
                    mutated_columns.emplace(column_name);

                if (command.type == MutationCommand::Type::MATERIALIZE_TTL && suitable_for_ttl_optimization)
                {
                    for (const auto & col : part_columns)
                    {
                        if (!mutated_columns.contains(col.name))
                            ignored_columns.emplace(col.name);
                    }
                }
                if (command.type == MutationCommand::Type::MATERIALIZE_INDEX)
                {
                    const auto & all_indices = metadata_snapshot->getSecondaryIndices();
                    for (const auto & index : all_indices)
                    {
                        if (index.name == command.index_name)
                        {
                            auto required_columns = index.expression->getRequiredColumns();
                            for (const auto & column : required_columns)
                            {
                                auto column_in_storage = Nested::tryGetColumnNameInStorage(column, storage_columns);
                                if (column_in_storage && !part_columns.has(*column_in_storage))
                                    extra_columns_for_indices_and_projections.emplace(*column_in_storage);
                            }
                            break;
                        }
                    }
                }

                if (command.type == MutationCommand::Type::MATERIALIZE_PROJECTION)
                {
                    const auto & all_projections = metadata_snapshot->getProjections();
                    for (const auto & projection : all_projections)
                    {
                        if (projection.name == command.projection_name)
                        {
                            for (const auto & column : projection.required_columns)
                            {
                                if (projection.with_parent_part_offset && column == "_part_offset")
                                    continue;

                                auto column_in_storage = Nested::tryGetColumnNameInStorage(column, storage_columns);
                                if (column_in_storage && !part_columns.has(*column_in_storage))
                                    extra_columns_for_indices_and_projections.emplace(*column_in_storage);
                            }
                            break;
                        }
                    }
                }
            }
            else if (command.type == MutationCommand::Type::DROP_INDEX
                     || command.type == MutationCommand::Type::DROP_PROJECTION
                     || command.type == MutationCommand::Type::DROP_STATISTICS)
            {
                for_file_renames.push_back(command);
            }
            else if (bool has_column = part_columns.has(command.column_name), has_nested_column = part_columns.hasNested(command.column_name); has_column || has_nested_column)
            {
                if (command.type == MutationCommand::Type::DROP_COLUMN || command.type == MutationCommand::Type::RENAME_COLUMN)
                {
                    if (has_nested_column)
                    {
                        const auto & nested = part_columns.getNested(command.column_name);
                        assert(!nested.empty());
                        for (const auto & nested_column : nested)
                            mutated_columns.emplace(nested_column.name);
                    }
                    else
                        mutated_columns.emplace(command.column_name);

                    if (command.type == MutationCommand::Type::DROP_COLUMN)
                    {
                        /// We need to keep the clear command so we also clear dependent indices
                        if (command.clear)
                        {
                            for_interpreter.push_back(command);
                            for_file_renames.push_back(command); /// For packed parts
                        }
                        else
                            dropped_columns.emplace(command.column_name);
                    }
                }
            }
        }

        /// We don't add renames from commands, instead we take them from rename_map.
        /// It's important because required renames depend not only on part's data version (i.e. mutation version)
        /// but also on part's metadata version. Why we have such logic only for renames? Because all other types of alter
        /// can be deduced based on difference between part's schema and table schema.
        for (const auto & [rename_to, rename_from] : alter_conversions->getRenameMap())
        {
            if (part_columns.has(rename_from))
            {
                /// Actual rename
                for_interpreter.push_back(
                {
                    .type = MutationCommand::Type::READ_COLUMN,
                    .column_name = rename_to,
                });

                /// Not needed for compact parts (not executed), added here only to produce correct
                /// set of columns for new part and their serializations
                for_file_renames.push_back(
                {
                     .type = MutationCommand::Type::RENAME_COLUMN,
                     .column_name = rename_from,
                     .rename_to = rename_to
                });

                part_columns.rename(rename_from, rename_to);
            }
        }

        /// If it's compact part, then we don't need to actually remove files
        /// from disk we just don't read dropped columns
        for (const auto & column : part_columns)
        {
            if (!mutated_columns.contains(column.name))
            {
                if (!metadata_snapshot->getColumns().has(column.name) && !part->storage.getVirtualsPtr()->has(column.name) && !ignored_columns.contains(column.name))
                {
                    /// We cannot add the column because there's no such column in table.
                    /// It's okay if the column was dropped. It may also absent in dropped_columns
                    /// if the corresponding MUTATE_PART entry was not created yet or was created separately from current MUTATE_PART.
                    /// But we don't know for sure what happened.
                    auto part_metadata_version = part->getMetadataVersion();
                    auto table_metadata_version = metadata_snapshot->getMetadataVersion();

                    bool allow_equal_versions = part_metadata_version == table_metadata_version && part->old_part_with_no_metadata_version_on_disk;
                    if (part_metadata_version < table_metadata_version || allow_equal_versions)
                    {
                        LOG_WARNING(log, "Ignoring column {} from part {} with metadata version {} because there is no such column "
                                         "in table {} with metadata version {}. Assuming the column was dropped", column.name, part->name,
                                    part_metadata_version, part->storage.getStorageID().getNameForLogs(), table_metadata_version);
                        continue;
                    }

                    /// StorageMergeTree does not have metadata version
                    if (part->storage.supportsReplication())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} with metadata version {} contains column {} that is absent "
                                        "in table {} with metadata version {}",
                                        part->name, part_metadata_version, column.name,
                                        part->storage.getStorageID().getNameForLogs(), table_metadata_version);
                }

                for_interpreter.emplace_back(
                    MutationCommand{.type = MutationCommand::Type::READ_COLUMN, .column_name = column.name, .data_type = column.type});
            }
            else if (dropped_columns.contains(column.name))
            {
                /// Not needed for compact parts (not executed), added here only to produce correct
                /// set of columns for new part and their serializations
                for_file_renames.push_back(
                {
                     .type = MutationCommand::Type::DROP_COLUMN,
                     .column_name = column.name,
                });
            }
        }
        for (const auto & column_name : extra_columns_for_indices_and_projections)
        {
            if (mutated_columns.contains(column_name))
                continue;

            if (column_name == "_part_offset")
                continue;

            auto data_type = metadata_snapshot->getColumns().getColumn(
                GetColumnsOptions::AllPhysical,
                column_name).type;

            for_interpreter.push_back(
                MutationCommand
                {
                    .type = MutationCommand::Type::READ_COLUMN,
                    .column_name = column_name,
                    .data_type = std::move(data_type),
                }
            );
        }
    }
    else
    {
        for (const auto & command : commands)
        {
            if (command.type == MutationCommand::Type::MATERIALIZE_COLUMN)
            {
                /// For ordinary column with default or materialized expression, MATERIALIZE COLUMN should not override past values
                /// So we only mutate column if `command.column_name` is a default/materialized column or if the part does not have physical column file
                auto column_ordinary = table_columns.getOrdinary().tryGetByName(command.column_name);
                if (!column_ordinary || !part->tryGetColumn(command.column_name) || !part->hasColumnFiles(*column_ordinary))
                    for_interpreter.push_back(command);

                /// Materialize column in case of complex data types like tuple can remove some nested columns
                /// Here we add it "for renames" because these set of commands also removes redundant files
                if (part_columns.has(command.column_name))
                    for_file_renames.push_back(command);
            }
            else if (command.type == MutationCommand::Type::MATERIALIZE_INDEX
                || command.type == MutationCommand::Type::MATERIALIZE_STATISTICS
                || command.type == MutationCommand::Type::MATERIALIZE_PROJECTION
                || command.type == MutationCommand::Type::MATERIALIZE_TTL
                || command.type == MutationCommand::Type::REWRITE_PARTS
                || command.type == MutationCommand::Type::DELETE
                || command.type == MutationCommand::Type::APPLY_DELETED_MASK
                || command.type == MutationCommand::Type::APPLY_PATCHES)
            {
                for_interpreter.push_back(command);
            }
            else if (command.type == MutationCommand::Type::UPDATE)
            {
                for_interpreter.push_back(command);

                /// Update column can change the set of substreams for column if it
                /// changes serialization (for example from Sparse to not Sparse).
                /// We add it "for renames" because these set of commands also removes redundant files
                for_file_renames.push_back(command);
            }
            else if (command.type == MutationCommand::Type::DROP_INDEX
                     || command.type == MutationCommand::Type::DROP_PROJECTION
                     || command.type == MutationCommand::Type::DROP_STATISTICS)
            {
                for_file_renames.push_back(command);
            }
            else if (command.type == MutationCommand::Type::READ_COLUMN)
            {
                if (part_columns.has(command.column_name) || command.read_for_patch)
                {
                    for_interpreter.push_back(command);
                    for_file_renames.push_back(command);
                }
            }
            /// If we don't have this column in source part, we don't need to materialize it.
            else if (part_columns.has(command.column_name))
            {
                if (command.type == MutationCommand::Type::RENAME_COLUMN)
                    part_columns.rename(command.column_name, command.rename_to);

                for_file_renames.push_back(command);
            }
        }

        /// We don't add renames from commands, instead we take them from rename_map.
        /// It's important because required renames depend not only on part's data version (i.e. mutation version)
        /// but also on part's metadata version. Why we have such logic only for renames? Because all other types of alter
        /// can be deduced based on difference between part's schema and table schema.

        for (const auto & [rename_to, rename_from] : alter_conversions->getRenameMap())
        {
            for_file_renames.push_back({.type = MutationCommand::Type::RENAME_COLUMN, .column_name = rename_from, .rename_to = rename_to});
        }
    }
}

static void addRenamedColumnToColumnsSubstreams(
    ColumnsSubstreams & new_columns_substreams,
    const ColumnsSubstreams & old_columns_substreams,
    const String & new_name,
    const String & old_name,
    size_t old_position)
{
    new_columns_substreams.addColumn(new_name);
    const auto & old_substreams = old_columns_substreams.getColumnSubstreams(old_position);
    for (const auto & substream : old_substreams)
        new_columns_substreams.addSubstreamToLastColumn(ISerialization::getFileNameForRenamedColumnStream(old_name, new_name, substream));
}

static bool isDeletedMaskUpdated(const MutationCommand & command, const NameSet & storage_columns_set)
{
    if (storage_columns_set.contains(RowExistsColumn::name))
        return false;

    if (command.type == MutationCommand::READ_COLUMN)
        return command.read_for_patch && command.column_name == RowExistsColumn::name;

    if (command.type == MutationCommand::UPDATE)
    {
        return std::ranges::find_if(command.column_to_update_expression, [](const auto & pair)
        {
            return pair.first == RowExistsColumn::name;
        }) != command.column_to_update_expression.end();
    }

    return false;
}

/// Get the columns list of the resulting part in the same order as storage_columns.
static std::tuple<NamesAndTypesList, SerializationInfoByName, ColumnsSubstreams>
getColumnsForNewDataPart(
    MergeTreeData::DataPartPtr source_part,
    const Block & updated_header,
    NamesAndTypesList storage_columns,
    const SerializationInfoByName & serialization_infos,
    const MutationCommands & commands_for_interpreter,
    const MutationCommands & commands_for_removes)
{
    MutationCommands all_commands;
    all_commands.insert(all_commands.end(), commands_for_interpreter.begin(), commands_for_interpreter.end());
    all_commands.insert(all_commands.end(), commands_for_removes.begin(), commands_for_removes.end());

    NameSet removed_columns;
    NameToNameMap renamed_columns_to_from;
    NameToNameMap renamed_columns_from_to;
    ColumnsDescription part_columns(source_part->getColumns());
    NamesAndTypesList system_columns;

    bool deleted_mask_updated = false;
    bool affects_all_columns = false;
    bool supports_lightweight_deletes = source_part->supportLightweightDeleteMutate();

    NameSet storage_columns_set;
    for (const auto & [name, _] : storage_columns)
        storage_columns_set.insert(name);

    for (const auto & command : all_commands)
    {
        affects_all_columns |= command.affectsAllColumns();

        if (supports_lightweight_deletes)
            deleted_mask_updated |= isDeletedMaskUpdated(command, storage_columns_set);

        /// If we don't have this column in source part, than we don't need to materialize it
        if (!part_columns.has(command.column_name))
            continue;

        if (command.type == MutationCommand::DROP_COLUMN)
            removed_columns.insert(command.column_name);

        if (command.type == MutationCommand::RENAME_COLUMN)
        {
            renamed_columns_to_from.emplace(command.rename_to, command.column_name);
            renamed_columns_from_to.emplace(command.column_name, command.rename_to);
        }
    }

    auto persistent_virtuals = source_part->storage.getVirtualsPtr()->getNamesAndTypesList(VirtualsKind::Persistent);

    for (const auto & [name, type] : persistent_virtuals)
    {
        if (storage_columns_set.contains(name))
            continue;

        bool need_column = false;
        if (name == RowExistsColumn::name)
            need_column = deleted_mask_updated || (part_columns.has(name) && !affects_all_columns);
        else
            need_column = part_columns.has(name);

        if (need_column)
        {
            storage_columns.emplace_back(name, type);
            storage_columns_set.insert(name);
        }
    }

    SerializationInfo::Settings settings;
    /// If mutations doesn't affect all columns we must use serialization info settings from source part,
    /// because data files of some columns might be copied without actual serialization, so changes in serialization
    /// settings will not be applied for them (for example, new serialization versions for data types).
    if (!affects_all_columns)
    {
        settings = SerializationInfo::Settings
        {
            (*source_part->storage.getSettings())[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization],
            false,
            serialization_infos.getSettings().version,
            serialization_infos.getSettings().string_serialization_version,
            serialization_infos.getSettings().nullable_serialization_version,
        };
    }
    /// Otherwise use fresh settings from storage.
    else
    {
        settings = SerializationInfo::Settings
        {
            (*source_part->storage.getSettings())[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization],
            false,
            (*source_part->storage.getSettings())[MergeTreeSetting::serialization_info_version],
            (*source_part->storage.getSettings())[MergeTreeSetting::string_serialization_version],
            (*source_part->storage.getSettings())[MergeTreeSetting::nullable_serialization_version],
        };
    }

    SerializationInfoByName new_serialization_infos(settings);
    for (const auto & [name, old_info] : serialization_infos)
    {
        auto it = renamed_columns_from_to.find(name);
        auto new_name = it == renamed_columns_from_to.end() ? name : it->second;

        /// Column can be removed only in this data part by CLEAR COLUMN query.
        if (!storage_columns_set.contains(new_name) || removed_columns.contains(new_name))
            continue;

        /// In compact part we read all columns and all of them are in @updated_header.
        /// But in wide part we must keep serialization infos for columns that are not touched by mutation.
        if (!updated_header.has(new_name))
        {
            if (isWidePart(source_part))
                new_serialization_infos.emplace(new_name, old_info);
            continue;
        }

        auto old_type = part_columns.getPhysical(name).type;
        auto new_type = updated_header.getByName(new_name).type;

        if (settings.isAlwaysDefault() || !settings.canUseSparseSerialization(*new_type))
            continue;

        auto new_info = new_type->createSerializationInfo(settings);
        if (!old_info->structureEquals(*new_info))
        {
            new_serialization_infos.emplace(new_name, std::move(new_info));
            continue;
        }

        new_info = old_info->createWithType(*old_type, *new_type, settings);
        new_serialization_infos.emplace(new_name, std::move(new_info));
    }

    /// In compact parts we read all columns, because they all stored in a single file
    if (!isWidePart(source_part) || !isFullPartStorage(source_part->getDataPartStorage()))
        return {updated_header.getNamesAndTypesList(), new_serialization_infos, {}};

    const auto & source_columns = source_part->getColumns();
    std::unordered_map<String, DataTypePtr> source_columns_name_to_type;
    for (const auto & it : source_columns)
        source_columns_name_to_type[it.name] = it.type;

    const ColumnsSubstreams & source_columns_substreams = source_part->getColumnsSubstreams();
    bool fill_columns_substreams = !source_columns_substreams.empty();
    ColumnsSubstreams new_columns_substreams;

    for (auto it = storage_columns.begin(); it != storage_columns.end();)
    {
        if (updated_header.has(it->name))
        {
            auto updated_type = updated_header.getByName(it->name).type;
            if (updated_type != it->type)
                it->type = updated_type;

            if (fill_columns_substreams)
            {
                new_columns_substreams.addColumn(it->name);
                new_columns_substreams.addSubstreamToLastColumn("dummy");
            }

            ++it;
        }
        else
        {
            auto source_col = source_columns_name_to_type.find(it->name);
            if (source_col == source_columns_name_to_type.end())
            {
                /// Source part doesn't have column but some other column
                /// was renamed to it's name.
                auto renamed_it = renamed_columns_to_from.find(it->name);
                if (renamed_it != renamed_columns_to_from.end())
                {
                    source_col = source_columns_name_to_type.find(renamed_it->second);
                    if (source_col == source_columns_name_to_type.end())
                        it = storage_columns.erase(it);
                    else
                    {
                        /// Take a type from source part column.
                        /// It may differ from column type in storage.
                        it->type = source_col->second;

                        if (fill_columns_substreams)
                            addRenamedColumnToColumnsSubstreams(new_columns_substreams, source_columns_substreams, it->name, source_col->first, *source_part->getColumnPosition(source_col->first));

                        ++it;
                    }
                }
                else
                    it = storage_columns.erase(it);
            }
            else
            {
                /// Check that this column was renamed to some other name
                bool was_renamed = renamed_columns_from_to.contains(it->name);
                bool was_removed = removed_columns.contains(it->name);

                /// If we want to rename this column to some other name, than it
                /// should it's previous version should be dropped or removed
                if (renamed_columns_to_from.contains(it->name) && !was_renamed && !was_removed)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Incorrect mutation commands, trying to rename column {} to {}, "
                        "but part {} already has column {}",
                        renamed_columns_to_from[it->name], it->name, source_part->name, it->name);

                /// Column was renamed and no other column renamed to it's name
                /// or column is dropped.
                if (!renamed_columns_to_from.contains(it->name) && (was_renamed || was_removed))
                {
                    it = storage_columns.erase(it);
                }
                else
                {

                    if (was_removed)
                    { /// DROP COLUMN xxx, RENAME COLUMN yyy TO xxx
                        auto renamed_from = renamed_columns_to_from.at(it->name);
                        auto maybe_name_and_type = source_columns.tryGetByName(renamed_from);
                        if (!maybe_name_and_type)
                            throw Exception(
                                ErrorCodes::LOGICAL_ERROR,
                                "Got incorrect mutation commands, column {} was renamed from {}, but it doesn't exist in source columns {}",
                                it->name, renamed_from, source_columns.toString());

                        it->type = maybe_name_and_type->type;

                        if (fill_columns_substreams)
                            addRenamedColumnToColumnsSubstreams(new_columns_substreams, source_columns_substreams, it->name, renamed_from, *source_part->getColumnPosition(renamed_from));
                    }
                    else
                    {
                        /// Take a type from source part column.
                        /// It may differ from column type in storage.
                        it->type = source_col->second;
                        if (fill_columns_substreams)
                        {
                            new_columns_substreams.addColumn(it->name);
                            new_columns_substreams.addSubstreamsToLastColumn(source_columns_substreams.getColumnSubstreams(*source_part->getColumnPosition(it->name)));
                        }
                    }
                    ++it;
                }
            }
        }
    }

    return {storage_columns, new_serialization_infos, new_columns_substreams};
}


static ExecuteTTLType shouldExecuteTTL(const StorageMetadataPtr & metadata_snapshot, const ColumnDependencies & dependencies)
{
    if (!metadata_snapshot->hasAnyTTL())
        return ExecuteTTLType::NONE;

    bool has_ttl_expression = false;

    for (const auto & dependency : dependencies)
    {
        if (dependency.kind == ColumnDependency::TTL_EXPRESSION)
            has_ttl_expression = true;

        if (dependency.kind == ColumnDependency::TTL_TARGET)
            return ExecuteTTLType::NORMAL;
    }
    return has_ttl_expression ? ExecuteTTLType::RECALCULATE : ExecuteTTLType::NONE;
}

static ColumnsStatistics getStatisticsToRecalculate(const StorageMetadataPtr & metadata_snapshot, const NameSet & materialized_stats)
{
    ColumnsStatistics stats_to_recalc;
    const auto & stats_factory = MergeTreeStatisticsFactory::instance();
    const auto & columns = metadata_snapshot->getColumns();

    for (const auto & col_desc : columns)
    {
        if (!col_desc.statistics.empty() && materialized_stats.contains(col_desc.name))
            stats_to_recalc.emplace(col_desc.name, stats_factory.get(col_desc));
    }
    return stats_to_recalc;
}

static std::set<ProjectionDescriptionRawPtr> getProjectionsToRecalculate(
    const MergeTreeDataPartPtr & source_part,
    const StorageMetadataPtr & metadata_snapshot,
    const NameSet & materialized_projections)
{
    std::set<ProjectionDescriptionRawPtr> projections_to_recalc;
    bool is_full_part_storage = isFullPartStorage(source_part->getDataPartStorage());

    for (const auto & projection : metadata_snapshot->getProjections())
    {
        bool need_recalculate =
            materialized_projections.contains(projection.name)
            || (!is_full_part_storage
                && source_part->hasProjection(projection.name)
                && !source_part->hasBrokenProjection(projection.name));

        if (need_recalculate)
            projections_to_recalc.insert(&projection);
    }

    return projections_to_recalc;
}

static std::unordered_map<String, size_t> getStreamCounts(
    const MergeTreeDataPartPtr & data_part,
    const MergeTreeDataPartChecksums & source_part_checksums,
    const Names & column_names)
{
    std::unordered_map<String, size_t> stream_counts;

    for (const auto & column_name : column_names)
    {
        if (auto serialization = data_part->tryGetSerialization(column_name))
        {
            auto callback = [&](const ISerialization::SubstreamPath & substream_path)
            {
                auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(column_name, substream_path, ".bin", source_part_checksums, data_part->storage.getSettings());
                if (stream_name)
                    ++stream_counts[*stream_name];
            };

            serialization->enumerateStreams(callback);
        }
    }

    return stream_counts;
}

/// Files, that we don't need to remove and don't need to hardlink, for example columns.txt and checksums.txt.
/// Because we will generate new versions of them after we perform mutation.
static NameSet collectFilesToSkip(
    const MergeTreeDataPartPtr & source_part,
    const MergeTreeDataPartPtr & new_part,
    const Block & updated_header,
    const std::set<MergeTreeIndexPtr> & indices_to_recalc,
    const std::set<MergeTreeIndexPtr> & indices_to_drop,
    const String & mrk_extension,
    const std::vector<ProjectionDescriptionRawPtr> & projections_to_skip,
    const NameSet & updated_columns_in_patches)
{
    NameSet files_to_skip = source_part->getFileNamesWithoutChecksums();

    /// Do not hardlink this file because it's always rewritten at the end of mutation.
    files_to_skip.insert(IMergeTreeDataPart::SERIALIZATION_FILE_NAME);

    auto skip_index = [&files_to_skip, &mrk_extension](const MergeTreeIndexPtr & index)
    {
        auto index_substreams = index->getSubstreams();

        for (const auto & index_substream : index_substreams)
        {
            files_to_skip.insert(index->getFileName() + index_substream.suffix + index_substream.extension);
            files_to_skip.insert(index->getFileName() + index_substream.suffix + mrk_extension);
        }
    };

    for (const auto & index : indices_to_recalc)
        skip_index(index);
    for (const auto & index : indices_to_drop)
        skip_index(index);

    for (const auto & projection : projections_to_skip)
        files_to_skip.insert(projection->getDirectoryName());

    if (isWidePart(source_part))
    {
        auto new_stream_counts = getStreamCounts(new_part, source_part->checksums, new_part->getColumns().getNames());
        auto source_updated_stream_counts = getStreamCounts(source_part, source_part->checksums, updated_header.getNames());
        auto new_updated_stream_counts = getStreamCounts(new_part, source_part->checksums, updated_header.getNames());

        /// Columns updated in patches should be rewritten by mutation.
        for (const auto & stream_name : updated_columns_in_patches)
        {
            files_to_skip.insert(stream_name + ".bin");
            files_to_skip.insert(stream_name + mrk_extension);
        }

        /// Skip all modified files in new part.
        for (const auto & [stream_name, _] : new_updated_stream_counts)
        {
            files_to_skip.insert(stream_name + ".bin");
            files_to_skip.insert(stream_name + mrk_extension);
        }

        /// Skip files that we read from source part and do not write in new part.
        /// E.g. ALTER MODIFY from LowCardinality(String) to String.
        for (const auto & [stream_name, _] : source_updated_stream_counts)
        {
            /// If we read shared stream and do not write it
            /// (e.g. while ALTER MODIFY COLUMN from array of Nested type to String),
            /// we need to hardlink its files, because they will be lost otherwise.
            bool need_hardlink = new_updated_stream_counts[stream_name] == 0 && new_stream_counts[stream_name] != 0;

            if (!need_hardlink)
            {
                files_to_skip.insert(stream_name + ".bin");
                files_to_skip.insert(stream_name + mrk_extension);
            }
        }
    }

    return files_to_skip;
}

/// Apply commands to source_part i.e. remove and rename some columns in
/// source_part and return set of files, that have to be removed or renamed
/// from filesystem and in-memory checksums. Ordered result is important,
/// because we can apply renames that affects each other: x -> z, y -> x.
static NameToNameVector collectFilesForRenames(
    StorageMetadataPtr metadata_snapshot,
    MergeTreeData::DataPartPtr source_part,
    MergeTreeData::DataPartPtr new_part,
    const MutationCommands & commands_for_renames,
    const NameSet & updated_columns_in_patches,
    const String & mrk_extension)
{
    /// Collect counts for shared streams of different columns. As an example, Nested columns have shared stream with array sizes.
    auto stream_counts = getStreamCounts(source_part, source_part->checksums, source_part->getColumns().getNames());

    NameToNameVector rename_vector;
    NameSet collected_names;

    auto add_rename = [&rename_vector, &collected_names] (const std::string & file_rename_from, const std::string & file_rename_to)
    {
        if (collected_names.emplace(file_rename_from).second)
            rename_vector.emplace_back(file_rename_from, file_rename_to);
    };

    /// Remove old data
    for (const auto & command : commands_for_renames)
    {
        if (command.type == MutationCommand::Type::DROP_INDEX)
        {
            static const std::array<String, 2> extensions = {".idx2", ".idx"};
            static const std::array<String, 3> substreams = {"", ".dct", ".pst"};

            for (const auto & substream : substreams)
            {
                for (const auto & extension : extensions)
                {
                    const String index_filename = getIndexFileName(command.column_name, metadata_snapshot->escape_index_filenames);
                    const String filename = index_filename + substream + extension;
                    const String filename_mrk = index_filename + substream + mrk_extension;

                    if (source_part->checksums.has(filename))
                    {
                        add_rename(filename, "");
                        add_rename(filename_mrk, "");
                    }
                }
            }
        }
        else if (command.type == MutationCommand::Type::DROP_PROJECTION)
        {
            if (source_part->checksums.has(command.column_name + ".proj"))
                add_rename(command.column_name + ".proj", "");
        }
        else if (isWidePart(source_part))
        {
            if (command.type == MutationCommand::Type::DROP_COLUMN)
            {
                ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path)
                {
                    auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(command.column_name, substream_path, ".bin", source_part->checksums, source_part->storage.getSettings());

                    /// Delete files if they are no longer shared with another column.
                    if (stream_name && --stream_counts[*stream_name] == 0)
                    {
                        add_rename(*stream_name + ".bin", "");
                        add_rename(*stream_name + mrk_extension, "");
                    }
                };

                if (auto serialization = source_part->tryGetSerialization(command.column_name))
                    serialization->enumerateStreams(callback);
            }
            else if (command.type == MutationCommand::Type::RENAME_COLUMN)
            {
                /// Columns updated in patches should be rewritten by mutation.
                if (updated_columns_in_patches.contains(command.rename_to))
                    continue;

                String escaped_name_from = escapeForFileName(command.column_name);
                String escaped_name_to = escapeForFileName(command.rename_to);

                ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path)
                {
                    auto storage_settings = source_part->storage.getSettings();

                    String full_stream_from = ISerialization::getFileNameForStream(command.column_name, substream_path, ISerialization::StreamFileNameSettings(*storage_settings));
                    String full_stream_to = boost::replace_first_copy(full_stream_from, escaped_name_from, escaped_name_to);

                    auto stream_from = IMergeTreeDataPart::getStreamNameOrHash(full_stream_from, ".bin", source_part->checksums);
                    if (!stream_from)
                        return;

                    String stream_to = replaceFileNameToHashIfNeeded(full_stream_to, *storage_settings, &new_part->getDataPartStorage());

                    if (stream_from != stream_to)
                    {
                        add_rename(*stream_from + ".bin", stream_to + ".bin");
                        add_rename(*stream_from + mrk_extension, stream_to + mrk_extension);
                    }
                };

                if (auto serialization = source_part->tryGetSerialization(command.column_name))
                    serialization->enumerateStreams(callback);
            }
            else if (command.type == MutationCommand::Type::UPDATE || command.type == MutationCommand::Type::READ_COLUMN || command.type == MutationCommand::Type::MATERIALIZE_COLUMN)
            {
                /// Remove files for streams that exist in source_part,
                /// but were removed in new_part by MODIFY COLUMN or MATERIALIZE COLUMN from
                /// type with higher number of streams (e.g. LowCardinality -> String).
                auto old_streams = getStreamCounts(source_part, source_part->checksums, source_part->getColumns().getNames());
                auto new_streams = getStreamCounts(new_part, source_part->checksums, source_part->getColumns().getNames());

                for (const auto & [old_stream, _] : old_streams)
                {
                    if (!new_streams.contains(old_stream) && --stream_counts[old_stream] == 0)
                    {
                        add_rename(old_stream + ".bin", "");
                        add_rename(old_stream + mrk_extension, "");
                    }
                }
            }
        }
    }

    if (source_part->getSerializationInfos().needsPersistence() && !new_part->getSerializationInfos().needsPersistence())
        add_rename(IMergeTreeDataPart::SERIALIZATION_FILE_NAME, "");

    return rename_vector;
}

static void processStatisticsChanges(
    NameSet & files_to_skip,
    NameToNameVector & files_to_rename,
    ColumnsStatistics & all_statistics,
    const ColumnsStatistics & stats_to_recalc,
    const MutationCommands & commands_for_renames,
    const IMergeTreeDataPart & source_part,
    StorageMetadataPtr metadata_snapshot)
{
    auto storage_settings = source_part.storage.getSettings();
    String statistics_file_name(ColumnsStatistics::FILENAME);

    auto process_rename = [&](const String & from_name, const String & to_name)
    {
        auto it = all_statistics.find(from_name);
        if (it == all_statistics.end())
            return;

        if (!to_name.empty())
            all_statistics.emplace(to_name, it->second);

        all_statistics.erase(it);
    };

    for (const auto & command : commands_for_renames)
    {
        if (command.type == MutationCommand::Type::DROP_STATISTICS)
        {
            auto removed_stats = MutationHelpers::getRemovedStatistics(metadata_snapshot, command);

            for (const auto & stats_name : removed_stats)
                process_rename(stats_name, "");
        }
        else if (command.type == MutationCommand::Type::DROP_COLUMN)
        {
            process_rename(command.column_name, "");
        }
        else if (command.type == MutationCommand::Type::RENAME_COLUMN)
        {
            process_rename(command.column_name, command.rename_to);
        }
        else if (command.type == MutationCommand::Type::READ_COLUMN)
        {
            /// Some implicit statistics (like tdigest) may become
            /// incompatible with the new type. We need to remove them.
            const auto * column_desc = metadata_snapshot->getColumns().tryGet(command.column_name);
            if (!column_desc || column_desc->statistics.empty())
                process_rename(command.column_name, "");
        }
    }

    if (!stats_to_recalc.empty())
    {
        /// Create empty statistics for columns that are being recalculated.
        for (const auto & [stat_name, stat] : stats_to_recalc)
            all_statistics[stat_name] = stat->cloneEmpty();
    }

    /// Remove old statistics files.
    if (isFullPartStorage(source_part.getDataPartStorage()))
    {
        /// File with statistics is always written during mutation.bgf
        files_to_skip.emplace(statistics_file_name);
        const auto & source_checksums = source_part.checksums;

        if (all_statistics.empty() && source_checksums.files.contains(statistics_file_name))
        {
            files_to_rename.emplace_back(statistics_file_name, "");
        }

        /// Always write statistics in a new format.
        /// Remove old statistics files.
        for (const auto & [filename, _] : source_checksums.files)
        {
            if (filename.starts_with(STATS_FILE_PREFIX) && filename.ends_with(STATS_FILE_SUFFIX))
                files_to_rename.emplace_back(filename, "");
        }
    }
}

/// Initialize and write to disk new part fields like checksums, columns, etc.
void finalizeMutatedPart(
    const MergeTreeDataPartPtr & source_part,
    MergeTreeData::MutableDataPartPtr new_data_part,
    const IMergedBlockOutputStream::GatheredData & all_gathered_data,
    ExecuteTTLType execute_ttl_type,
    const CompressionCodecPtr & codec,
    ContextPtr context,
    StorageMetadataPtr metadata_snapshot,
    bool sync)
{
    std::vector<std::unique_ptr<WriteBufferFromFileBase>> written_files;

    if (new_data_part->uuid != UUIDHelpers::Nil)
    {
        auto out = new_data_part->getDataPartStorage().writeFile(IMergeTreeDataPart::UUID_FILE_NAME, 4096, context->getWriteSettings());
        HashingWriteBuffer out_hashing(*out);
        writeUUIDText(new_data_part->uuid, out_hashing);
        out_hashing.finalize();
        new_data_part->checksums.files[IMergeTreeDataPart::UUID_FILE_NAME].file_size = out_hashing.count();
        new_data_part->checksums.files[IMergeTreeDataPart::UUID_FILE_NAME].file_hash = out_hashing.getHash();
        written_files.push_back(std::move(out));
    }

    if (execute_ttl_type != ExecuteTTLType::NONE)
    {
        /// Write a file with ttl infos in json format.
        auto out_ttl = new_data_part->getDataPartStorage().writeFile("ttl.txt", 4096, context->getWriteSettings());
        HashingWriteBuffer out_hashing(*out_ttl);
        new_data_part->ttl_infos.write(out_hashing);
        out_hashing.finalize();
        new_data_part->checksums.files["ttl.txt"].file_size = out_hashing.count();
        new_data_part->checksums.files["ttl.txt"].file_hash = out_hashing.getHash();
        written_files.push_back(std::move(out_ttl));
    }

    const auto & serialization_infos = new_data_part->getSerializationInfos();
    if (serialization_infos.needsPersistence())
    {
        auto out_serialization = new_data_part->getDataPartStorage().writeFile(IMergeTreeDataPart::SERIALIZATION_FILE_NAME, 4096, context->getWriteSettings());
        HashingWriteBuffer out_hashing(*out_serialization);
        serialization_infos.writeJSON(out_hashing);
        out_hashing.finalize();
        new_data_part->checksums.files[IMergeTreeDataPart::SERIALIZATION_FILE_NAME].file_size = out_hashing.count();
        new_data_part->checksums.files[IMergeTreeDataPart::SERIALIZATION_FILE_NAME].file_hash = out_hashing.getHash();
        written_files.push_back(std::move(out_serialization));
    }

    const auto & statistics = all_gathered_data.statistics;
    new_data_part->setEstimates(statistics.getEstimates());

    if (!statistics.empty())
    {
        if (isFullPartStorage(new_data_part->getDataPartStorage()))
        {
            auto out = serializeStatisticsPacked(new_data_part->getDataPartStorage(), new_data_part->checksums, statistics, codec, context->getWriteSettings());
            written_files.push_back(std::move(out));
        }
        /// Write statistics as separate compressed files in packed parts to avoid double buffering.
        else
        {
            auto files = serializeStatisticsWide(new_data_part->getDataPartStorage(), new_data_part->checksums, statistics, codec, context->getWriteSettings());
            std::move(files.begin(), files.end(), std::back_inserter(written_files));
        }
    }

    {
        /// Write file with checksums.
        auto out_checksums = new_data_part->getDataPartStorage().writeFile("checksums.txt", 4096, context->getWriteSettings());
        new_data_part->checksums.write(*out_checksums);
        written_files.push_back(std::move(out_checksums));
    }

    {
        auto out_comp = new_data_part->getDataPartStorage().writeFile(IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME, 4096, context->getWriteSettings());
        DB::writeText(codec->getFullCodecDesc()->formatWithSecretsOneLine(), *out_comp);
        written_files.push_back(std::move(out_comp));
    }

    {
        auto out_metadata = new_data_part->getDataPartStorage().writeFile(IMergeTreeDataPart::METADATA_VERSION_FILE_NAME, 4096, context->getWriteSettings());
        DB::writeText(metadata_snapshot->getMetadataVersion(), *out_metadata);
        written_files.push_back(std::move(out_metadata));
    }

    {
        /// Write a file with a description of columns.
        auto out_columns = new_data_part->getDataPartStorage().writeFile("columns.txt", 4096, context->getWriteSettings());
        new_data_part->getColumns().writeText(*out_columns);
        written_files.push_back(std::move(out_columns));
    }

    if (!new_data_part->getColumnsSubstreams().empty())
    {
        /// Write a file with a description of columns substreams.
        auto out_columns_substreams = new_data_part->getDataPartStorage().writeFile(IMergeTreeDataPart::COLUMNS_SUBSTREAMS_FILE_NAME, 4096, context->getWriteSettings());
        new_data_part->getColumnsSubstreams().writeText(*out_columns_substreams);
        written_files.push_back(std::move(out_columns_substreams));
    }

    for (auto & file : written_files)
    {
        file->finalize();
        if (sync)
            file->sync();
    }
    /// Close files
    written_files.clear();

    new_data_part->rows_count = source_part->rows_count;
    new_data_part->index_granularity = source_part->index_granularity;
    new_data_part->minmax_idx = source_part->minmax_idx;
    new_data_part->modification_time = time(nullptr);

    if ((*new_data_part->storage.getSettings())[MergeTreeSetting::enable_index_granularity_compression])
    {
        if (auto new_index_granularity = new_data_part->index_granularity->optimize())
            new_data_part->index_granularity = std::move(new_index_granularity);
    }

    /// It's important to set index after index granularity.
    if (!new_data_part->storage.getPrimaryIndexCache())
        new_data_part->setIndex(*source_part->getIndex());

    /// Load rest projections which are hardlinked
    bool noop;
    new_data_part->loadProjections(false, false, noop, true /* if_not_loaded */);

    /// All information about sizes is stored in checksums.
    /// It doesn't make sense to touch filesystem for sizes.
    new_data_part->setBytesOnDisk(new_data_part->checksums.getTotalSizeOnDisk());
    new_data_part->setBytesUncompressedOnDisk(new_data_part->checksums.getTotalSizeUncompressedOnDisk());
    /// Also use information from checksums
    if (!(*new_data_part->storage.getSettings())[MergeTreeSetting::columns_and_secondary_indices_sizes_lazy_calculation])
        new_data_part->calculateColumnsAndSecondaryIndicesSizesOnDisk();

    new_data_part->default_codec = codec;
}

}

struct MutationContext
{
    MergeTreeData * data;
    MergeTreeDataMergerMutator * mutator;
    PartitionActionBlocker * merges_blocker;
    TableLockHolder * holder;
    MergeListEntry * mutate_entry;

    LoggerPtr log{getLogger("MutateTask")};

    FutureMergedMutatedPartPtr future_part;
    MergeTreeData::DataPartPtr source_part;
    StorageMetadataPtr metadata_snapshot;
    StorageSnapshotPtr storage_snapshot;
    DiskPtr disk;

    MutationCommandsConstPtr commands;
    time_t time_of_mutation;
    ContextPtr context;
    ReservationSharedPtr space_reservation;

    CompressionCodecPtr compression_codec;

    std::unique_ptr<CurrentMetrics::Increment> num_mutations;

    QueryPipelineBuilder mutating_pipeline_builder;
    QueryPipeline mutating_pipeline; // in
    std::unique_ptr<PullingPipelineExecutor> mutating_executor;
    ProgressCallback progress_callback;
    Block updated_header;

    std::unique_ptr<MutationsInterpreter> interpreter;
    UInt64 watch_prev_elapsed = 0;
    std::unique_ptr<MergeStageProgress> stage_progress;

    MutationCommands commands_for_part;
    MutationCommands for_interpreter;
    MutationCommands for_file_renames;

    NamesAndTypesList storage_columns;
    NameSet materialized_indices;
    NameSet materialized_projections;
    NameSet materialized_statistics;
    NameSet indices_to_drop_names;

    IMergedBlockOutputStream::GatheredData all_gathered_data;
    MergeTreeData::MutableDataPartPtr new_data_part;
    IMergedBlockOutputStreamPtr out;

    String mrk_extension;

    std::vector<ProjectionDescriptionRawPtr> projections_to_build;
    IMergeTreeDataPart::MinMaxIndexPtr minmax_idx;

    std::set<MergeTreeIndexPtr> indices_to_recalc;
    std::set<MergeTreeIndexPtr> text_indices_to_recalc;
    std::set<MergeTreeIndexPtr> indices_to_drop;
    ColumnsStatistics stats_to_recalc;
    std::set<ProjectionDescriptionRawPtr> projections_to_recalc;
    NameSet files_to_skip;
    NameToNameVector files_to_rename;

    bool need_sync;
    ExecuteTTLType execute_ttl_type{ExecuteTTLType::NONE};

    MergeTreeTransactionPtr txn;

    HardlinkedFiles hardlinked_files;

    bool need_prefix = true;

    scope_guard temporary_directory_lock;

    bool checkOperationIsNotCanceled() const
    {
        if (new_data_part ? merges_blocker->isCancelledForPartition(new_data_part->info.getPartitionId()) : merges_blocker->isCancelled()
            || (*mutate_entry)->is_cancelled)
        {
            throw Exception(ErrorCodes::ABORTED, "Cancelled mutating parts");
        }

        return true;
    }

    /// Whether we need to count lightweight delete rows in this mutation
    bool count_lightweight_deleted_rows;
    UInt64 execute_elapsed_ns = 0;
};

using MutationContextPtr = std::shared_ptr<MutationContext>;

// This class is responsible for:
// 1. get projection pipeline and a sink to write parts
// 2. build an executor that can write block to the input stream (actually we can write through it to generate as many parts as possible)
// 3. finalize the pipeline so that all parts are merged into one part

// In short it executed a mutation for the part an original part and for its every projection

/**
 *
 * An overview of how the process of mutation works for projections:
 *
 * The mutation for original parts is executed block by block,
 * but additionally we execute a SELECT query for each projection over a current block.
 * And we store results to a map : ProjectionName -> ArrayOfParts.
 *
 * Then, we have to merge all parts for each projection. But we will have constraint:
 * We cannot execute merge on more than 10 parts simulatiously.
 * So we build a tree of merges. At the beginning all the parts have level 0.
 * At each step we take not bigger than 10 parts from the same level
 * and merge it into a bigger part with incremented level.
 */
class PartMergerWriter
{
public:

    explicit PartMergerWriter(MutationContextPtr ctx_)
        : ctx(ctx_), projections(ctx->metadata_snapshot->projections)
    {
    }

    bool execute()
    {
        switch (state)
        {
            case State::NEED_PREPARE:
            {
                prepare();

                state = State::NEED_MUTATE_ORIGINAL_PART;
                return true;
            }
            case State::NEED_MUTATE_ORIGINAL_PART:
            {
                if (mutateOriginalPartAndPrepareProjections())
                    return true;

                state = State::NEED_EXECUTE_MERGE_SUBTASKS;
                return true;
            }
            case State::NEED_EXECUTE_MERGE_SUBTASKS:
            {
                if (iterateThroughAllMergeSubtasks())
                    return true;

                state = State::SUCCESS;
                return true;
            }
            case State::SUCCESS:
            {
                finalize();
                return false;
            }
        }
        return false;
    }

private:
    void prepare();
    bool mutateOriginalPartAndPrepareProjections();
    void createBuildTextIndexesTask();
    void writeTempProjectionPart(size_t projection_idx, Chunk chunk);
    void finalizeTempProjectionsAndIndexes();
    bool iterateThroughAllMergeSubtasks();
    void finalize();

    enum class State : uint8_t
    {
        NEED_PREPARE,
        NEED_MUTATE_ORIGINAL_PART,
        NEED_EXECUTE_MERGE_SUBTASKS,
        SUCCESS
    };

    State state{State::NEED_PREPARE};
    MutationContextPtr ctx;

    size_t projection_block_num = 0;
    std::vector<std::unique_ptr<MergeProjectionsIndexesTask>> merge_subtasks;

    using ProjectionNameToItsBlocks = std::map<String, MergeTreeData::MutableDataPartsVector>;
    ProjectionNameToItsBlocks projection_parts;

    std::vector<Squashing> projection_squashes;
    const ProjectionsDescription & projections;

    MutableDataPartStoragePtr temporary_text_index_storage;
    std::unique_ptr<BuildTextIndexTransform> build_text_index_transform;

    /// Existing rows count calculated during part writing.
    /// It is initialized in prepare(), calculated in mutateOriginalPartAndPrepareProjections()
    /// and set to new_data_part in finalize()
    size_t existing_rows_count;
};


void PartMergerWriter::prepare()
{
    const auto & settings = ctx->context->getSettingsRef();

    for (size_t i = 0, size = ctx->projections_to_build.size(); i < size; ++i)
    {
        // We split the materialization into multiple stages similar to the process of INSERT SELECT query.
        projection_squashes.emplace_back(std::make_shared<const Block>(ctx->updated_header), settings[Setting::min_insert_block_size_rows], settings[Setting::min_insert_block_size_bytes]);
    }

    if (!ctx->text_indices_to_recalc.empty())
    {
        createBuildTextIndexesTask();
    }

    existing_rows_count = 0;
}


bool PartMergerWriter::mutateOriginalPartAndPrepareProjections()
{
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    UInt64 step_time_ms = (*ctx->data->getSettings())[MergeTreeSetting::background_task_preferred_step_execution_time_ms].totalMilliseconds();

    do
    {
        Block cur_block;

        if (!ctx->checkOperationIsNotCanceled() || !ctx->mutating_executor->pull(cur_block))
        {
            finalizeTempProjectionsAndIndexes();
            return false;
        }

        ctx->out->write(cur_block);

        if (ctx->minmax_idx)
            ctx->minmax_idx->update(cur_block, MergeTreeData::getMinMaxColumnsNames(ctx->metadata_snapshot->getPartitionKey()));

        if (!ctx->all_gathered_data.statistics.empty())
            ctx->all_gathered_data.statistics.buildIfExists(cur_block);

        /// TODO: move this calculation to DELETE FROM mutation
        if (ctx->count_lightweight_deleted_rows)
            existing_rows_count += MutationHelpers::getExistingRowsCount(cur_block);

        UInt64 starting_offset = (*ctx->mutate_entry)->rows_written;
        for (size_t i = 0, size = ctx->projections_to_build.size(); i < size; ++i)
        {
            Chunk squashed_chunk;

            {
                ProfileEventTimeIncrement<Microseconds> projection_watch(ProfileEvents::MutateTaskProjectionsCalculationMicroseconds);
                Block block_to_squash = ctx->projections_to_build[i]->calculate(cur_block, starting_offset, ctx->context);

                /// Everything is deleted by lighweight delete
                if (block_to_squash.rows() == 0)
                    continue;

                projection_squashes[i].setHeader(block_to_squash.cloneEmpty());
                squashed_chunk = Squashing::squash(
                    projection_squashes[i].add({block_to_squash.getColumns(), block_to_squash.rows()}),
                    projection_squashes[i].getHeader());
            }

            if (squashed_chunk)
                writeTempProjectionPart(i, std::move(squashed_chunk));
        }

        if (build_text_index_transform)
            build_text_index_transform->aggregate(cur_block);

        (*ctx->mutate_entry)->rows_written += cur_block.rows();
        (*ctx->mutate_entry)->bytes_written_uncompressed += cur_block.bytes();
    } while (watch.elapsedMilliseconds() < step_time_ms);

    /// Need execute again
    return true;
}

void PartMergerWriter::createBuildTextIndexesTask()
{
    if (ctx->source_part->rows_count > std::numeric_limits<UInt32>::max())
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Cannot materialize text index in part {} with {} rows. Materialization of text index is not supported for parts with more than {} rows",
            ctx->source_part->name, ctx->source_part->rows_count, std::numeric_limits<UInt32>::max());
    }

    auto part_path = ctx->new_data_part->getDataPartStorage().getRelativePath();
    temporary_text_index_storage = createTemporaryTextIndexStorage(ctx->disk, part_path);
    std::vector<MergeTreeIndexPtr> text_indexes(ctx->text_indices_to_recalc.begin(), ctx->text_indices_to_recalc.end());

    build_text_index_transform = std::make_unique<BuildTextIndexTransform>(
        std::make_shared<Block>(ctx->updated_header),
        /*index_file_prefix=*/ "tmp",
        std::move(text_indexes),
        temporary_text_index_storage,
        ctx->out->getWriterSettings(),
        ctx->compression_codec,
        ctx->mrk_extension);
}

void PartMergerWriter::writeTempProjectionPart(size_t projection_idx, Chunk chunk)
{
    const auto & projection = *ctx->projections_to_build[projection_idx];
    const auto & projection_plan = projection_squashes[projection_idx];

    auto result = projection_plan.getHeader()->cloneWithColumns(chunk.detachColumns());

    auto tmp_part = MergeTreeDataWriter::writeTempProjectionPart(
        *ctx->data,
        ctx->log,
        result,
        projection,
        ctx->new_data_part.get(),
        ++projection_block_num);

    tmp_part->finalize();
    tmp_part->part->getDataPartStorage().commitTransaction();
    projection_parts[projection.name].emplace_back(std::move(tmp_part->part));
}

void PartMergerWriter::finalizeTempProjectionsAndIndexes()
{
    // Write the last block
    for (size_t i = 0, size = ctx->projections_to_build.size(); i < size; ++i)
    {
        auto squashed_chunk = Squashing::squash(
            projection_squashes[i].flush(),
            projection_squashes[i].getHeader());
        if (squashed_chunk)
            writeTempProjectionPart(i, std::move(squashed_chunk));
    }

    for (auto && [projection_name, temporary_projection_parts] : projection_parts)
    {
        auto merge_task = std::make_unique<MergeProjectionPartsTask>
        (
            projection_name,
            std::move(temporary_projection_parts),
            projections.get(projection_name),
            projection_block_num,
            ctx->context,
            ctx->holder,
            ctx->mutator,
            ctx->mutate_entry,
            ctx->time_of_mutation,
            ctx->new_data_part,
            ctx->space_reservation
        );

        merge_subtasks.push_back(std::move(merge_task));
    }

    if (build_text_index_transform)
    {
        build_text_index_transform->finalize();
        temporary_text_index_storage->commitTransaction();
        auto reader_settings = MergeTreeReaderSettings::createForMergeMutation(ctx->context->getReadSettings());
        const auto & indexes = build_text_index_transform->getIndexes();

        for (const auto & index : indexes)
        {
            auto segments = build_text_index_transform->getSegments(index->index.name, 0);

            auto merge_task = std::make_unique<MergeTextIndexesTask>(
                std::move(segments),
                ctx->new_data_part,
                index,
                /*merged_part_offsets=*/ nullptr,
                reader_settings,
                ctx->out->getWriterSettings());

            merge_subtasks.push_back(std::move(merge_task));
        }
    }
}

bool PartMergerWriter::iterateThroughAllMergeSubtasks()
{
    if (merge_subtasks.empty())
        return false;

    auto & task = merge_subtasks.back();
    if (task->executeStep())
        return true;

    task->addToChecksums(ctx->new_data_part->checksums);
    merge_subtasks.pop_back();
    return !merge_subtasks.empty();
}

void PartMergerWriter::finalize()
{
    if (ctx->count_lightweight_deleted_rows)
        ctx->new_data_part->existing_rows_count = existing_rows_count;

    if (temporary_text_index_storage)
        temporary_text_index_storage->removeRecursive();
}

class MutateAllPartColumnsTask : public IExecutableTask
{
public:

    explicit MutateAllPartColumnsTask(MutationContextPtr ctx_) : ctx(ctx_) {}

    void onCompleted() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    StorageID getStorageID() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    Priority getPriority() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    String getQueryId() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }

    bool executeStep() override
    {
        switch (state)
        {
            case State::NEED_PREPARE:
            {
                prepare();

                state = State::NEED_EXECUTE;
                return true;
            }
            case State::NEED_EXECUTE:
            {
                if (part_merger_writer_task->execute())
                    return true;

                state = State::NEED_FINALIZE;
                return true;
            }
            case State::NEED_FINALIZE:
            {
                finalize();

                state = State::SUCCESS;
                return true;
            }
            case State::SUCCESS:
            {
                return false;
            }
        }
        return false;
    }

    void cancel() noexcept override
    {
        if (ctx->out)
        {
            ctx->out->cancel();
        }
    }

private:

    void prepare()
    {
        ctx->new_data_part->getDataPartStorage().createDirectories();

        /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
        /// (which is locked in data.getTotalActiveSizeInBytes())
        /// (which is locked in shared mode when input streams are created) and when inserting new data
        /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
        /// deadlock is impossible.
        ctx->compression_codec
            = ctx->data->getCompressionCodecForPart(ctx->source_part->getBytesOnDisk(), ctx->source_part->ttl_infos, ctx->time_of_mutation);

        NameSet entries_to_hardlink;
        NameSet removed_indices;
        NameSet removed_projections;

        bool is_full_part_storage = isFullPartStorage(ctx->new_data_part->getDataPartStorage());

        for (const auto & command : ctx->for_file_renames)
        {
            if (command.type == MutationCommand::DROP_INDEX)
            {
                removed_indices.insert(command.column_name);
            }
            else if (command.type == MutationCommand::DROP_PROJECTION)
            {
                removed_projections.insert(command.column_name);
            }
        }

        bool is_full_wide_part = is_full_part_storage && isWidePart(ctx->new_data_part);
        const auto & indices = ctx->metadata_snapshot->getSecondaryIndices();

        MergeTreeIndices skip_indices;
        for (const auto & idx : indices)
        {
            if (removed_indices.contains(idx.name))
                continue;

            if (ctx->indices_to_drop_names.contains(idx.name))
                continue;

            /// For packed part we need to recalculate all indices because they are stored inside packed parts format
            /// For compact parts we need to recalculate indices because rewrite of compact part may produce a little bit different data part
            /// with different number of marks.
            bool need_recalculate = ctx->materialized_indices.contains(idx.name)
                || (!is_full_wide_part && ctx->source_part->hasSecondaryIndex(idx.name, ctx->metadata_snapshot));

            if (need_recalculate)
            {
                skip_indices.push_back(MergeTreeIndexFactory::instance().get(idx));
            }
            else
            {
                auto prefix = getIndexFileName(idx.name, idx.escape_filenames);
                auto it = ctx->source_part->checksums.files.upper_bound(prefix);
                while (it != ctx->source_part->checksums.files.end())
                {
                    if (!startsWith(it->first, prefix))
                        break;

                    entries_to_hardlink.insert(it->first);
                    ++it;
                }
            }
        }

        bool lightweight_delete_mode = ctx->updated_header.has(RowExistsColumn::name);
        bool lightweight_delete_drop = lightweight_delete_mode
            && (*ctx->data->getSettings())[MergeTreeSetting::lightweight_mutation_projection_mode] == LightweightMutationProjectionMode::DROP;

        const auto & projections = ctx->metadata_snapshot->getProjections();
        for (const auto & projection : projections)
        {
            if (removed_projections.contains(projection.name))
                continue;

            bool need_recalculate =
                (ctx->materialized_projections.contains(projection.name)
                || (!is_full_part_storage
                    && ctx->source_part->hasProjection(projection.name)
                    && !ctx->source_part->hasBrokenProjection(projection.name)))
                && !lightweight_delete_drop;

            if (need_recalculate)
            {
                ctx->projections_to_build.push_back(&projection);
            }
            else
            {
                if (!lightweight_delete_mode && ctx->source_part->checksums.has(projection.getDirectoryName()))
                    entries_to_hardlink.insert(projection.getDirectoryName());
            }
        }

        NameSet hardlinked_files;
        /// Create hardlinks for unchanged files
        for (auto it = ctx->source_part->getDataPartStorage().iterate(); it->isValid(); it->next())
        {
            if (!entries_to_hardlink.contains(it->name()))
            {
                /// Do nothing for skipped files
            }
            else if (it->isFile())
            {
                ctx->new_data_part->getDataPartStorage().createHardLinkFrom(
                    ctx->source_part->getDataPartStorage(), it->name(), it->name());
                hardlinked_files.insert(it->name());
            }
            else
            {
                // it's a projection part directory
                ctx->new_data_part->getDataPartStorage().createProjection(it->name());

                auto projection_data_part_storage_src = ctx->source_part->getDataPartStorage().getProjection(it->name());
                auto projection_data_part_storage_dst = ctx->new_data_part->getDataPartStorage().getProjection(it->name());

                for (auto p_it = projection_data_part_storage_src->iterate(); p_it->isValid(); p_it->next())
                {
                    projection_data_part_storage_dst->createHardLinkFrom(
                        *projection_data_part_storage_src, p_it->name(), p_it->name());

                    auto file_name_with_projection_prefix = fs::path(projection_data_part_storage_src->getPartDirectory()) / p_it->name();
                    hardlinked_files.insert(file_name_with_projection_prefix);
                }
            }
        }

        /// Tracking of hardlinked files required for zero-copy replication.
        /// We don't remove them when we delete last copy of source part because
        /// new part can use them.
        ctx->hardlinked_files.source_table_shared_id = ctx->source_part->storage.getTableSharedID();
        ctx->hardlinked_files.source_part_name = ctx->source_part->name;
        ctx->hardlinked_files.hardlinks_from_source_part = std::move(hardlinked_files);

        if (!ctx->mutating_pipeline_builder.initialized())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot mutate part columns with uninitialized mutations stream. It's a bug");

        auto builder = std::make_unique<QueryPipelineBuilder>(std::move(ctx->mutating_pipeline_builder));

        if (ctx->metadata_snapshot->hasPrimaryKey() || ctx->metadata_snapshot->hasSecondaryIndices())
        {
            auto indices_expression_dag = ctx->data->getPrimaryKeyAndSkipIndicesExpression(ctx->metadata_snapshot, skip_indices)->getActionsDAG().clone();
            auto extracting_subcolumns_dag = createSubcolumnsExtractionActions(builder->getHeader(), indices_expression_dag.getRequiredColumnsNames(), ctx->context);
            if (!extracting_subcolumns_dag.getNodes().empty())
                indices_expression_dag = ActionsDAG::merge(std::move(extracting_subcolumns_dag), std::move(indices_expression_dag));

            builder->addTransform(std::make_shared<ExpressionTransform>(
                builder->getSharedHeader(), std::make_shared<ExpressionActions>(std::move(indices_expression_dag))));

            builder->addTransform(std::make_shared<MaterializingTransform>(builder->getSharedHeader()));
        }

        PreparedSets::Subqueries subqueries;

        if (ctx->execute_ttl_type == ExecuteTTLType::NORMAL)
        {
            auto transform = std::make_shared<TTLTransform>(
                ctx->context,
                builder->getSharedHeader(),
                *ctx->data,
                ctx->metadata_snapshot,
                ctx->new_data_part,
                NamesAndTypesList{} /*expired_columns*/,
                ctx->time_of_mutation,
                true);
            subqueries = transform->getSubqueries();
            builder->addTransform(std::move(transform));
        }

        if (ctx->execute_ttl_type == ExecuteTTLType::RECALCULATE)
        {
            auto transform = std::make_shared<TTLCalcTransform>(ctx->context, builder->getSharedHeader(), *ctx->data, ctx->metadata_snapshot, ctx->new_data_part, ctx->time_of_mutation, true);
            subqueries = transform->getSubqueries();
            builder->addTransform(std::move(transform));
        }

        if (!subqueries.empty())
            builder = addCreatingSetsTransform(std::move(builder), std::move(subqueries), ctx->context);

        bool affects_all_columns = false;

        for (auto & command_for_interpreter : ctx->for_interpreter)
        {
            if (command_for_interpreter.affectsAllColumns())
            {
                affects_all_columns = true;
                break;
            }
        }

        MergeTreeIndexGranularityPtr index_granularity_ptr;
        /// Reuse source part granularity if mutation does not change number of rows
        if (!affects_all_columns && ctx->execute_ttl_type == ExecuteTTLType::NONE)
        {
            index_granularity_ptr = ctx->source_part->index_granularity;
        }
        else
        {
            index_granularity_ptr = createMergeTreeIndexGranularity(
                ctx->source_part->rows_count,
                ctx->source_part->getBytesUncompressedOnDisk(),
                *ctx->data->getSettings(),
                ctx->new_data_part->index_granularity_info,
                /*blocks_are_granules=*/ false);
        }

        ctx->minmax_idx = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();
        ctx->all_gathered_data.statistics = ColumnsStatistics(ctx->metadata_snapshot->getColumns());

        MutationHelpers::processStatisticsChanges(
            ctx->files_to_skip,
            ctx->files_to_rename,
            ctx->all_gathered_data.statistics,
            ctx->stats_to_recalc,
            ctx->for_file_renames,
            *ctx->source_part,
            ctx->metadata_snapshot);

        ctx->out = std::make_shared<MergedBlockOutputStream>(
            ctx->new_data_part,
            ctx->data->getSettings(),
            ctx->metadata_snapshot,
            ctx->new_data_part->getColumns(),
            skip_indices,
            ctx->compression_codec,
            std::move(index_granularity_ptr),
            ctx->txn ? ctx->txn->tid : Tx::PrehistoricTID,
            ctx->source_part->getBytesUncompressedOnDisk(),
            /*reset_columns=*/ true,
            /*blocks_are_granules_size=*/ false,
            ctx->context->getWriteSettings(),
            static_cast<WrittenOffsetSubstreams *>(nullptr));

        ctx->mutating_pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
        ctx->mutating_pipeline.setProgressCallback(ctx->progress_callback);
        /// Is calculated inside MergeProgressCallback.
        ctx->mutating_pipeline.disableProfileEventUpdate();
        ctx->mutating_executor = std::make_unique<PullingPipelineExecutor>(ctx->mutating_pipeline);

        part_merger_writer_task = std::make_unique<PartMergerWriter>(ctx);
    }

    void finalize()
    {
        bool noop;
        ctx->new_data_part->minmax_idx = std::move(ctx->minmax_idx);
        ctx->new_data_part->loadProjections(false, false, noop, true /* if_not_loaded */);
        ctx->mutating_executor.reset();
        ctx->mutating_pipeline.reset();

        auto out_mut = static_pointer_cast<MergedBlockOutputStream>(ctx->out);
        out_mut->finalizeIndexGranularity();
        out_mut->finalizePart(ctx->new_data_part, ctx->all_gathered_data, ctx->need_sync, nullptr);
        ctx->out.reset();
    }

    enum class State : uint8_t
    {
        NEED_PREPARE,
        NEED_EXECUTE,
        NEED_FINALIZE,

        SUCCESS
    };

    State state{State::NEED_PREPARE};

    MutationContextPtr ctx;

    std::unique_ptr<PartMergerWriter> part_merger_writer_task;
};


class MutateSomePartColumnsTask : public IExecutableTask
{
public:
    explicit MutateSomePartColumnsTask(MutationContextPtr ctx_) : ctx(ctx_) {}

    void onCompleted() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    StorageID getStorageID() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    Priority getPriority() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    String getQueryId() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }

    bool executeStep() override
    {
        switch (state)
        {
            case State::NEED_PREPARE:
            {
                prepare();
                state = State::NEED_EXECUTE;
                return true;
            }
            case State::NEED_EXECUTE:
            {
                if (part_merger_writer_task && part_merger_writer_task->execute())
                    return true;

                state = State::NEED_FINALIZE;
                return true;
            }
            case State::NEED_FINALIZE:
            {
                finalize();

                state = State::SUCCESS;
                return true;
            }
            case State::SUCCESS:
            {
                return false;
            }
        }
        return false;
    }

    void cancel() noexcept override
    {
        if (ctx->out)
        {
            ctx->out->cancel();
        }
    }

private:
    void prepare()
    {
        ctx->all_gathered_data.statistics = ctx->source_part->loadStatistics();

        MutationHelpers::processStatisticsChanges(
            ctx->files_to_skip,
            ctx->files_to_rename,
            ctx->all_gathered_data.statistics,
            ctx->stats_to_recalc,
            ctx->for_file_renames,
            *ctx->source_part,
            ctx->metadata_snapshot);

        if (ctx->execute_ttl_type != ExecuteTTLType::NONE)
            ctx->files_to_skip.insert("ttl.txt");

        ctx->new_data_part->getDataPartStorage().createDirectories();

        /// We should write version metadata on part creation to distinguish it from parts that were created without transaction.
        TransactionID tid = ctx->txn ? ctx->txn->tid : Tx::PrehistoricTID;
        /// NOTE do not pass context for writing to system.transactions_info_log,
        /// because part may have temporary name (with temporary block numbers). Will write it later.
        ctx->new_data_part->version.setCreationTID(tid, nullptr);
        ctx->new_data_part->storeVersionMetadata();

        auto settings = ctx->source_part->storage.getSettings();
        NameSet hardlinked_files;

        /// NOTE: Renames must be done in order
        for (const auto & [rename_from, rename_to] : ctx->files_to_rename)
        {
            if (rename_to.empty()) /// It's DROP COLUMN
            {
                /// pass
            }
            else
            {
                ctx->new_data_part->getDataPartStorage().createHardLinkFrom(
                    ctx->source_part->getDataPartStorage(), rename_from, rename_to);
                hardlinked_files.insert(rename_from);
            }
        }

        /// Create hardlinks for unchanged files
        for (auto it = ctx->source_part->getDataPartStorage().iterate(); it->isValid(); it->next())
        {
            if (ctx->files_to_skip.contains(it->name()))
                continue;

            String file_name = it->name();

            auto rename_it = std::find_if(ctx->files_to_rename.begin(), ctx->files_to_rename.end(), [&file_name](const auto & rename_pair)
            {
                return rename_pair.first == file_name;
            });

            if (rename_it != ctx->files_to_rename.end())
            {
                /// RENAMEs and DROPs already processed
                continue;
            }

            String destination = it->name();

            if (it->isFile())
            {
                if ((*settings)[MergeTreeSetting::always_use_copy_instead_of_hardlinks])
                {
                    ctx->new_data_part->getDataPartStorage().copyFileFrom(
                        ctx->source_part->getDataPartStorage(), it->name(), destination);
                }
                else
                {
                    ctx->new_data_part->getDataPartStorage().createHardLinkFrom(
                        ctx->source_part->getDataPartStorage(), it->name(), destination);

                    hardlinked_files.insert(it->name());
                }
            }
            else if (!endsWith(it->name(), ".tmp_proj")) // ignore projection tmp merge dir
            {
                // it's a projection part directory
                ctx->new_data_part->getDataPartStorage().createProjection(destination);

                auto projection_data_part_storage_src = ctx->source_part->getDataPartStorage().getProjection(destination);
                auto projection_data_part_storage_dst = ctx->new_data_part->getDataPartStorage().getProjection(destination);

                for (auto p_it = projection_data_part_storage_src->iterate(); p_it->isValid(); p_it->next())
                {
                    if ((*settings)[MergeTreeSetting::always_use_copy_instead_of_hardlinks])
                    {
                        projection_data_part_storage_dst->copyFileFrom(
                            *projection_data_part_storage_src, p_it->name(), p_it->name());
                    }
                    else
                    {
                        auto file_name_with_projection_prefix = fs::path(projection_data_part_storage_src->getPartDirectory()) / p_it->name();

                        projection_data_part_storage_dst->createHardLinkFrom(
                            *projection_data_part_storage_src, p_it->name(), p_it->name());

                        hardlinked_files.insert(file_name_with_projection_prefix);
                    }
                }
            }
        }

        /// Tracking of hardlinked files required for zero-copy replication.
        /// We don't remove them when we delete last copy of source part because
        /// new part can use them.
        ctx->hardlinked_files.source_table_shared_id = ctx->source_part->storage.getTableSharedID();
        ctx->hardlinked_files.source_part_name = ctx->source_part->name;
        ctx->hardlinked_files.hardlinks_from_source_part = std::move(hardlinked_files);
        (*ctx->mutate_entry)->columns_written = ctx->storage_columns.size() - ctx->updated_header.columns();

        ctx->new_data_part->checksums = ctx->source_part->checksums;
        /// We weed to remove checksums for dropped indices
        for (const auto & index : ctx->indices_to_drop)
        {
            auto index_substreams = index->getSubstreams();
            for (const auto & index_substream : index_substreams)
            {
                ctx->new_data_part->checksums.remove(index->getFileName() + index_substream.suffix + index_substream.extension);
                ctx->new_data_part->checksums.remove(index->getFileName() + index_substream.suffix + ctx->mrk_extension);
            }
        }

        ctx->compression_codec = ctx->source_part->default_codec;

        if (ctx->mutating_pipeline_builder.initialized())
        {
            auto builder = std::make_unique<QueryPipelineBuilder>(std::move(ctx->mutating_pipeline_builder));
            PreparedSets::Subqueries subqueries;

            if (ctx->execute_ttl_type == ExecuteTTLType::NORMAL)
            {
                auto transform = std::make_shared<TTLTransform>(
                    ctx->context,
                    builder->getSharedHeader(),
                    *ctx->data,
                    ctx->metadata_snapshot,
                    ctx->new_data_part,
                    NamesAndTypesList{} /*expired_columns*/,
                    ctx->time_of_mutation,
                    true);
                subqueries = transform->getSubqueries();
                builder->addTransform(std::move(transform));
            }

            if (ctx->execute_ttl_type == ExecuteTTLType::RECALCULATE)
            {
                auto transform = std::make_shared<TTLCalcTransform>(ctx->context, builder->getSharedHeader(), *ctx->data, ctx->metadata_snapshot, ctx->new_data_part, ctx->time_of_mutation, true);
                subqueries = transform->getSubqueries();
                builder->addTransform(std::move(transform));
            }

            if (!subqueries.empty())
                builder = addCreatingSetsTransform(std::move(builder), std::move(subqueries), ctx->context);

            ctx->out = std::make_shared<MergedColumnOnlyOutputStream>(
                ctx->new_data_part,
                ctx->data->getSettings(),
                ctx->metadata_snapshot,
                ctx->updated_header.getNamesAndTypesList(),
                std::vector<MergeTreeIndexPtr>(ctx->indices_to_recalc.begin(), ctx->indices_to_recalc.end()),
                ctx->compression_codec,
                ctx->source_part->index_granularity,
                ctx->source_part->getBytesUncompressedOnDisk(),
                static_cast<WrittenOffsetSubstreams *>(nullptr));

            ctx->mutating_pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
            ctx->mutating_pipeline.setProgressCallback(ctx->progress_callback);
            /// Is calculated inside MergeProgressCallback.
            ctx->mutating_pipeline.disableProfileEventUpdate();
            ctx->mutating_executor = std::make_unique<PullingPipelineExecutor>(ctx->mutating_pipeline);

            ctx->projections_to_build = std::vector<ProjectionDescriptionRawPtr>{ctx->projections_to_recalc.begin(), ctx->projections_to_recalc.end()};

            part_merger_writer_task = std::make_unique<PartMergerWriter>(ctx);
        }
    }

    void finalize()
    {
        if (ctx->mutating_executor)
        {
            ctx->mutating_executor.reset();
            ctx->mutating_pipeline.reset();

            auto out_mut = static_pointer_cast<MergedColumnOnlyOutputStream>(ctx->out);
            out_mut->finalizeIndexGranularity();
            auto changed_checksums = out_mut->fillChecksums(ctx->new_data_part, ctx->new_data_part->checksums);
            ctx->new_data_part->checksums.add(std::move(changed_checksums));

            auto new_columns_substreams = ctx->new_data_part->getColumnsSubstreams();
            if (!new_columns_substreams.empty())
            {
                auto changed_columns_substreams = out_mut->getColumnsSubstreams();
                new_columns_substreams = ColumnsSubstreams::merge(changed_columns_substreams, ctx->new_data_part->getColumnsSubstreams(), ctx->new_data_part->getColumns().getNames());
                ctx->new_data_part->setColumnsSubstreams(new_columns_substreams);
            }

            out_mut->finish(ctx->need_sync);
            ctx->out.reset();
        }

        for (const auto & [rename_from, rename_to] : ctx->files_to_rename)
        {
            if (rename_to.empty() && ctx->new_data_part->checksums.files.contains(rename_from))
            {
                ctx->new_data_part->checksums.files.erase(rename_from);
            }
            else if (ctx->new_data_part->checksums.files.contains(rename_from))
            {
                ctx->new_data_part->checksums.files[rename_to] = ctx->new_data_part->checksums.files[rename_from];
                ctx->new_data_part->checksums.files.erase(rename_from);
            }
        }

        MutationHelpers::finalizeMutatedPart(
            ctx->source_part,
            ctx->new_data_part,
            ctx->all_gathered_data,
            ctx->execute_ttl_type,
            ctx->compression_codec,
            ctx->context,
            ctx->metadata_snapshot,
            ctx->need_sync);
    }

    enum class State : uint8_t
    {
        NEED_PREPARE,
        NEED_EXECUTE,
        NEED_FINALIZE,

        SUCCESS
    };

    State state{State::NEED_PREPARE};
    MutationContextPtr ctx;
    MergedColumnOnlyOutputStreamPtr out;

    std::unique_ptr<PartMergerWriter> part_merger_writer_task{nullptr};
};

/*
 * Decorator that'll drop expired parts by replacing them with empty ones.
 * Main use case (only use case for now) is to decorate `MutateSomePartColumnsTask`,
 * which is used to recalculate TTL. If the part is expired, this class will replace it with
 * an empty one.
 *
 * Triggered when `ttl_only_drop_parts` is set and the only TTL is rows TTL.
 * */
class ExecutableTaskDropTTLExpiredPartsDecorator : public IExecutableTask
{
public:
    explicit ExecutableTaskDropTTLExpiredPartsDecorator(
        std::unique_ptr<IExecutableTask> executable_task_,
        MutationContextPtr ctx_
        )
        : executable_task(std::move(executable_task_)), ctx(ctx_) {}

    void onCompleted() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    StorageID getStorageID() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    Priority getPriority() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    String getQueryId() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }

    bool executeStep() override
    {
        switch (state)
        {
            case State::NEED_EXECUTE:
            {
                if (executable_task->executeStep())
                    return true;

                if (isRowsMaxTTLExpired())
                    replacePartWithEmpty();

                state = State::SUCCESS;
                return true;
            }
            case State::SUCCESS:
            {
                return false;
            }
        }
        return false;
    }

    void cancel() noexcept override
    {
        executable_task->cancel();
    }

private:
    enum class State
    {
        NEED_EXECUTE,

        SUCCESS
    };

    State state{State::NEED_EXECUTE};

    std::unique_ptr<IExecutableTask> executable_task;
    MutationContextPtr ctx;

    bool isRowsMaxTTLExpired() const
    {
        const auto ttl = ctx->new_data_part->ttl_infos.table_ttl;
        return ttl.max && ttl.max <= ctx->time_of_mutation;
    }

    void replacePartWithEmpty()
    {
        MergeTreePartInfo part_info = ctx->new_data_part->info;
        part_info.level += 1;

        MergeTreePartition partition = ctx->new_data_part->partition;
        std::string part_name = ctx->new_data_part->getNewName(part_info);

        auto [mutable_empty_part, _] = ctx->data->createEmptyPart(part_info, partition, part_name, ctx->txn);
        ctx->new_data_part = std::move(mutable_empty_part);
    }
};

MutateTask::MutateTask(
    FutureMergedMutatedPartPtr future_part_,
    StorageMetadataPtr metadata_snapshot_,
    MutationCommandsConstPtr commands_,
    MergeListEntry * mutate_entry_,
    time_t time_of_mutation_,
    ContextPtr context_,
    ReservationSharedPtr space_reservation_,
    TableLockHolder & table_lock_holder_,
    const MergeTreeTransactionPtr & txn,
    MergeTreeData & data_,
    MergeTreeDataMergerMutator & mutator_,
    PartitionActionBlocker & merges_blocker_,
    bool need_prefix_)
    : ctx(std::make_shared<MutationContext>())
{
    ctx->data = &data_;
    ctx->mutator = &mutator_;
    ctx->merges_blocker = &merges_blocker_;
    ctx->holder = &table_lock_holder_;
    ctx->mutate_entry = mutate_entry_;
    ctx->commands = commands_;
    ctx->context = context_;
    ctx->time_of_mutation = time_of_mutation_;
    ctx->future_part = future_part_;
    ctx->metadata_snapshot = metadata_snapshot_;
    ctx->storage_snapshot = ctx->data->getStorageSnapshotWithoutData(ctx->metadata_snapshot, context_);
    ctx->space_reservation = space_reservation_;
    ctx->storage_columns = metadata_snapshot_->getColumns().getAllPhysical();
    ctx->txn = txn;
    ctx->source_part = ctx->future_part->parts[0];
    ctx->need_prefix = need_prefix_;
}


bool MutateTask::execute()
{
    Stopwatch watch;
    SCOPE_EXIT({ ctx->execute_elapsed_ns += watch.elapsedNanoseconds(); });

    switch (state)
    {
        case State::NEED_PREPARE:
        {
            if (!prepare())
                return false;

            state = State::NEED_EXECUTE;
            return true;
        }
        case State::NEED_EXECUTE:
        {
            ctx->checkOperationIsNotCanceled();

            if (task->executeStep())
                return true;

            // The `new_data_part` is a shared pointer and must be moved to allow
            // part deletion in case it is needed in `MutateFromLogEntryTask::finalize`.
            //
            // `tryRemovePartImmediately` requires `std::shared_ptr::unique() == true`
            // to delete the part timely. When there are multiple shared pointers,
            // only the part state is changed to `Deleting`.
            //
            // Fetching a byte-identical part (in case of checksum mismatches) will fail with
            // `Part ... should be deleted after previous attempt before fetch`.
            promise.set_value(std::exchange(ctx->new_data_part, nullptr));
            return false;
        }
    }
    return false;
}

void MutateTask::cancel() noexcept
{
    if (task)
        task->cancel();

    if (ctx->new_data_part)
        ctx->new_data_part->removeIfNeeded();
}

void MutateTask::updateProfileEvents() const
{
    UInt64 total_elapsed_ms = (*ctx->mutate_entry)->watch.elapsedMilliseconds();
    UInt64 execute_elapsed_ms = ctx->execute_elapsed_ns / 1000000UL;

    ProfileEvents::increment(ProfileEvents::MutationTotalMilliseconds, total_elapsed_ms);
    ProfileEvents::increment(ProfileEvents::MutationExecuteMilliseconds, execute_elapsed_ms);
}

static bool canSkipConversionToNullable(const MergeTreeDataPartPtr & part, const StorageMetadataPtr & metadata_snapshot, const MutationCommand & command)
{
    if (command.type != MutationCommand::READ_COLUMN)
        return false;

    auto part_column = part->tryGetColumn(command.column_name);
    if (!part_column)
        return false;

    /// For ALTER MODIFY COLUMN from 'Type' to 'Nullable(Type)' we can skip mutation and
    /// apply only metadata conversion. But it doesn't work for custom serialization.
    const auto * to_nullable = typeid_cast<const DataTypeNullable *>(command.data_type.get());
    if (!to_nullable)
        return false;

    if (!part_column->type->equals(*to_nullable->getNestedType()))
        return false;

    auto serialization = part->getSerialization(command.column_name);
    if (serialization->getKindStack() != ISerialization::KindStack{ISerialization::Kind::DEFAULT})
        return false;

    /// We need to rewrite statistics because they have different serialization with nullable type.
    const auto * column_desc = metadata_snapshot->getColumns().tryGet(command.column_name);
    if (!column_desc->statistics.empty())
        return false;

    return true;
}

static bool canSkipConversionToVariant(const MergeTreeDataPartPtr & part, const MutationCommand & command)
{
    if (command.type != MutationCommand::READ_COLUMN)
        return false;

    auto part_column = part->tryGetColumn(command.column_name);
    if (!part_column)
        return false;

    /// For ALTER MODIFY COLUMN with Variant extension (like 'Variant(T1, T2)' to 'Variant(T1, T2, T3, ...)')
    /// we can skip mutation and apply only metadata conversion.
    return isVariantExtension(part_column->type, command.data_type);
}

static bool canSkipMutationCommandForPart(const MergeTreeDataPartPtr & part, const StorageMetadataPtr & metadata_snapshot, const MutationCommand & command, const ContextPtr & context)
{
    if (command.partition)
    {
        auto command_partition_id = part->storage.getPartitionIDFromQuery(command.partition, context);
        if (part->info.getPartitionId() != command_partition_id)
            return true;
    }

    /// APPLY PATCHES command is handled separately.
    if (command.type == MutationCommand::APPLY_PATCHES)
        return true;

    if (command.type == MutationCommand::APPLY_DELETED_MASK && !part->hasLightweightDelete())
        return true;

    if (canSkipConversionToNullable(part, metadata_snapshot, command))
        return true;

    if (canSkipConversionToVariant(part, command))
        return true;

    return false;
}

namespace
{
/// Calculate the set of indices which should be recalculated and dropped during mutation.
/// Also wraps the input stream into additional expression stream
void updateIndicesToRecalculateAndDrop(std::shared_ptr<MutationContext> & ctx)
{
    const MergeTreeDataPartPtr & source_part = ctx->source_part;
    QueryPipelineBuilder & builder = ctx->mutating_pipeline_builder;
    const StorageMetadataPtr & metadata_snapshot = ctx->metadata_snapshot;

    /// Checks if columns used in skipping indexes modified.
    const auto & index_factory = MergeTreeIndexFactory::instance();
    ASTPtr indices_recalc_expr_list = make_intrusive<ASTExpressionList>();
    const auto & indices = metadata_snapshot->getSecondaryIndices();
    bool is_full_part_storage = isFullPartStorage(source_part->getDataPartStorage());

    for (const auto & index : indices)
    {
        if (ctx->indices_to_drop_names.contains(index.name))
        {
            ctx->indices_to_drop.insert(index_factory.get(index));
            continue;
        }

        bool need_recalculate = ctx->materialized_indices.contains(index.name)
            || (!is_full_part_storage && source_part->hasSecondaryIndex(index.name, ctx->metadata_snapshot));

        if (need_recalculate)
        {
            bool inserted;
            auto index_ptr = index_factory.get(index);

            if (dynamic_cast<const MergeTreeIndexText *>(index_ptr.get()))
                inserted = ctx->text_indices_to_recalc.insert(index_ptr).second;
            else
                inserted = ctx->indices_to_recalc.insert(index_ptr).second;

            if (inserted)
            {
                ASTPtr expr_list = index.expression_list_ast->clone();
                for (const auto & expr : expr_list->children)
                    indices_recalc_expr_list->children.push_back(expr->clone());
            }
        }
    }

    if ((!ctx->indices_to_recalc.empty() || !ctx->text_indices_to_recalc.empty()) && builder.initialized())
    {
        auto indices_recalc_syntax
            = TreeRewriter(ctx->context).analyze(indices_recalc_expr_list, builder.getHeader().getNamesAndTypesList());
        auto indices_recalc_expr = ExpressionAnalyzer(indices_recalc_expr_list, indices_recalc_syntax, ctx->context).getActions(false);

        /// We can update only one column, but some skip idx expression may depend on several
        /// columns (c1 + c2 * c3). It works because this stream was created with help of
        /// MutationsInterpreter which knows about skip indices and stream 'in' already has
        /// all required columns.
        /// TODO move this logic to single place.
        builder.addTransform(std::make_shared<ExpressionTransform>(builder.getSharedHeader(), indices_recalc_expr));
        builder.addTransform(std::make_shared<MaterializingTransform>(builder.getSharedHeader()));
    }
}
}

bool MutateTask::prepare()
{
    FailPointInjection::pauseFailPoint(FailPoints::mt_mutate_task_pause_in_prepare);

    ProfileEvents::increment(ProfileEvents::MutationTotalParts);
    ctx->checkOperationIsNotCanceled();

    if (ctx->future_part->parts.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to mutate {} parts, not one. "
            "This is a bug.", ctx->future_part->parts.size());

    ctx->num_mutations = std::make_unique<CurrentMetrics::Increment>(CurrentMetrics::PartMutation);

    auto max_partition_blocks = std::make_shared<PartitionIdToMaxBlock>();
    max_partition_blocks->emplace(ctx->future_part->part_info.getPartitionId(), ctx->future_part->part_info.getMutationVersion());

    MergeTreeData::IMutationsSnapshot::Params params
    {
        .metadata_version = ctx->metadata_snapshot->getMetadataVersion(),
        .min_part_metadata_version = ctx->source_part->getMetadataVersion(),
        .min_part_data_versions = nullptr,
        .max_mutation_versions = std::move(max_partition_blocks),
        .need_data_mutations = false,
        .need_alter_mutations = true,
        .need_patch_parts = true,
    };

    auto mutations_snapshot = ctx->data->getMutationsSnapshot(params);
    auto alter_conversions = MergeTreeData::getAlterConversionsForPart(ctx->source_part, mutations_snapshot, ctx->context);
    auto context_for_reading = Context::createCopy(ctx->context);

    /// Allow mutations to work when force_index_by_date or force_primary_key is on.
    context_for_reading->setSetting("force_index_by_date", false);
    context_for_reading->setSetting("force_primary_key", false);
    context_for_reading->setSetting("apply_mutations_on_fly", false);
    /// Skip using large sets in KeyCondition
    context_for_reading->setSetting("use_index_for_in_with_subqueries_max_values", 100000);
    context_for_reading->setSetting("use_concurrency_control", false);
    /// disable parallel replicas for mutations
    context_for_reading->setSetting("enable_parallel_replicas", false);

    for (const auto & command : *ctx->commands)
    {
        if (!canSkipMutationCommandForPart(ctx->source_part, ctx->metadata_snapshot, command, context_for_reading))
            ctx->commands_for_part.emplace_back(command);
    }

    auto updated_columns_in_patches = alter_conversions->getColumnsUpdatedInPatches();

    for (const auto & name : updated_columns_in_patches)
    {
        GetColumnsOptions options = GetColumnsOptions::AllPhysical;
        auto column = ctx->storage_snapshot->tryGetColumn(options.withVirtuals(VirtualsKind::Persistent), name);

        /// Skip updated column if it was dropped from the table.
        if (!column)
            continue;

        ctx->commands_for_part.push_back(MutationCommand
        {
            .type = MutationCommand::READ_COLUMN,
            .column_name = name,
            .data_type = column->type,
            .read_for_patch = true,
        });
    }

    auto is_storage_touched = isStorageTouchedByMutations(
        ctx->source_part,
        mutations_snapshot,
        ctx->metadata_snapshot,
        ctx->commands_for_part,
        context_for_reading,
        [&my_ctx = *ctx](const Progress &) { my_ctx.checkOperationIsNotCanceled(); }
    );

    if (!is_storage_touched.any_rows_affected)
    {
        NameSet files_to_copy_instead_of_hardlinks;
        auto settings_ptr = ctx->data->getSettings();
        /// In zero-copy replication checksums file path in s3 (blob path) is used for zero copy locks in ZooKeeper. If we will hardlink checksums file, we will have the same blob path
        /// and two different parts (source and new mutated part) will use the same locks in ZooKeeper. To avoid this we copy checksums.txt to generate new blob path.
        /// Example:
        ///     part: all_0_0_0/checksums.txt -> /s3/blobs/shjfgsaasdasdasdasdasdas
        ///     locks path in zk: /zero_copy/tbl_id/s3_blobs_shjfgsaasdasdasdasdasdas/replica_name
        ///                                         ^ part name don't participate in lock path
        /// In case of full hardlink we will have:
        ///     part: all_0_0_0_1/checksums.txt -> /s3/blobs/shjfgsaasdasdasdasdasdas
        ///     locks path in zk: /zero_copy/tbl_id/s3_blobs_shjfgsaasdasdasdasdasdas/replica_name
        /// So we need to copy to have a new name
        bool copy_checksumns = ctx->data->supportsReplication() && (*settings_ptr)[MergeTreeSetting::allow_remote_fs_zero_copy_replication] && ctx->source_part->isStoredOnRemoteDiskWithZeroCopySupport();
        if (copy_checksumns)
            files_to_copy_instead_of_hardlinks.insert(IMergeTreeDataPart::FILE_FOR_REFERENCES_CHECK);

        LOG_TRACE(ctx->log, "Part {} doesn't change up to mutation version {}", ctx->source_part->name, ctx->future_part->part_info.mutation);
        std::string prefix;
        if (ctx->need_prefix)
            prefix = "tmp_clone_";

        IDataPartStorage::ClonePartParams clone_params
        {
            .txn = ctx->txn, .hardlinked_files = &ctx->hardlinked_files,
            .copy_instead_of_hardlink = (*settings_ptr)[MergeTreeSetting::always_use_copy_instead_of_hardlinks],
            .files_to_copy_instead_of_hardlinks = std::move(files_to_copy_instead_of_hardlinks),
            .keep_metadata_version = true,
        };

        MergeTreeData::MutableDataPartPtr part;
        scope_guard lock;

        {
            std::tie(part, lock) = ctx->data->cloneAndLoadDataPart(
                ctx->source_part, prefix, ctx->future_part->part_info, ctx->metadata_snapshot, clone_params, ctx->context->getReadSettings(), ctx->context->getWriteSettings(), true/*must_on_same_disk*/);
            part->getDataPartStorage().beginTransaction();
            ctx->temporary_directory_lock = std::move(lock);
        }

        ProfileEvents::increment(ProfileEvents::MutationUntouchedParts);
        promise.set_value(std::move(part));
        return false;
    }

    if (is_storage_touched.all_rows_affected)
    {
        bool has_only_delete_commands = std::ranges::all_of(ctx->commands_for_part, [](const auto & command)
        {
            return command.type == MutationCommand::DELETE;
        });

        if (has_only_delete_commands)
        {
            LOG_TRACE(ctx->log,
                "Part {} is fully deleted, creating empty part with mutation version {}",
                ctx->source_part->name, ctx->future_part->part_info.mutation);

            auto [empty_part, _] = ctx->data->createEmptyPart(
                ctx->future_part->part_info,
                ctx->source_part->partition,
                ctx->future_part->name,
                ctx->txn);

            ProfileEvents::increment(ProfileEvents::MutationCreatedEmptyParts);
            promise.set_value(std::move(empty_part));
            return false;
        }
    }

    LOG_TRACE(ctx->log, "Mutating part {} to mutation version {}", ctx->source_part->name, ctx->future_part->part_info.mutation);

    /// We must read with one thread because it guarantees that output stream will be sorted.
    /// Disable all settings that can enable reading with several streams.
    /// NOTE: isStorageTouchedByMutations() above is done without this settings because it
    /// should be ok to calculate count() with multiple streams.
    context_for_reading->setSetting("max_streams_to_max_threads_ratio", 1);
    context_for_reading->setSetting("max_threads", 1);
    context_for_reading->setSetting("allow_asynchronous_read_from_io_pool_for_merge_tree", false);
    context_for_reading->setSetting("max_streams_for_merge_tree_reading", Field(0));
    context_for_reading->setSetting("read_from_filesystem_cache_if_exists_otherwise_bypass_cache", 1);
    context_for_reading->setSetting("read_from_distributed_cache_if_exists_otherwise_bypass_cache", 1);

    bool suitable_for_ttl_optimization = ctx->metadata_snapshot->hasOnlyRowsTTL() && (*ctx->data->getSettings())[MergeTreeSetting::ttl_only_drop_parts];
    MutationHelpers::splitAndModifyMutationCommands(
        ctx->source_part,
        ctx->metadata_snapshot,
        alter_conversions,
        ctx->commands_for_part,
        ctx->for_interpreter,
        ctx->for_file_renames,
        suitable_for_ttl_optimization,
        ctx->log);

    ctx->stage_progress = std::make_unique<MergeStageProgress>(1.0);

    bool lightweight_delete_mode = false;

    if (!ctx->for_interpreter.empty())
    {
        /// Always disable filtering in mutations: we want to read and write all rows because for updates we rewrite only some of the
        /// columns and preserve the columns that are not affected, but after the update all columns must have the same number of row
        MutationsInterpreter::Settings settings(true);
        settings.apply_deleted_mask = false;

        ctx->interpreter = std::make_unique<MutationsInterpreter>(
            *ctx->data, ctx->source_part, alter_conversions,
            ctx->metadata_snapshot, ctx->for_interpreter,
            ctx->metadata_snapshot->getColumns().getNamesOfPhysical(), context_for_reading, settings);

        ctx->materialized_indices = ctx->interpreter->grabMaterializedIndices();
        ctx->indices_to_drop_names = ctx->interpreter->grabDroppedIndices();
        ctx->materialized_statistics = ctx->interpreter->grabMaterializedStatistics();
        ctx->materialized_projections = ctx->interpreter->grabMaterializedProjections();
        ctx->mutating_pipeline_builder = ctx->interpreter->execute();
        ctx->updated_header = ctx->interpreter->getUpdatedHeader();
        ctx->progress_callback = MergeProgressCallback(
            (*ctx->mutate_entry)->ptr(),
            ctx->watch_prev_elapsed,
            *ctx->stage_progress,
            [&my_ctx = *ctx]() { my_ctx.checkOperationIsNotCanceled(); });

        lightweight_delete_mode = ctx->updated_header.has(RowExistsColumn::name);
        /// If under the condition of lightweight delete mode with rebuild option, add projections again here as we can only know
        /// the condition as early as from here.
        if (lightweight_delete_mode
            && (*ctx->data->getSettings())[MergeTreeSetting::lightweight_mutation_projection_mode] == LightweightMutationProjectionMode::REBUILD)
        {
            for (const auto & projection : ctx->metadata_snapshot->getProjections())
            {
                if (!ctx->source_part->hasProjection(projection.name))
                    continue;

                ctx->materialized_projections.insert(projection.name);
            }
        }
    }

    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + ctx->future_part->name, ctx->space_reservation->getDisk(), 0);
    ctx->disk = single_disk_volume->getDisk();

    std::string prefix;
    if (ctx->need_prefix)
        prefix = TEMP_DIRECTORY_PREFIX;

    String tmp_part_dir_name = prefix + ctx->future_part->name;
    ctx->temporary_directory_lock = ctx->data->getTemporaryPartDirectoryHolder(tmp_part_dir_name);

    auto builder = ctx->data->getDataPartBuilder(ctx->future_part->name, single_disk_volume, tmp_part_dir_name, getReadSettings());
    builder.withPartFormat(ctx->future_part->part_format);
    builder.withPartInfo(ctx->future_part->part_info);

    ctx->new_data_part = std::move(builder).build();
    ctx->new_data_part->getDataPartStorage().beginTransaction();

    ctx->new_data_part->uuid = ctx->future_part->uuid;
    ctx->new_data_part->is_temp = true;
    ctx->new_data_part->ttl_infos = ctx->source_part->ttl_infos;

    /// It shouldn't be changed by mutation.
    ctx->new_data_part->index_granularity_info = ctx->source_part->index_granularity_info;

    auto [new_columns, new_infos, new_columns_substreams] = MutationHelpers::getColumnsForNewDataPart(
        ctx->source_part, ctx->updated_header, ctx->storage_columns,
        ctx->source_part->getSerializationInfos(), ctx->for_interpreter, ctx->for_file_renames);

    ctx->new_data_part->setColumns(new_columns, new_infos, ctx->metadata_snapshot->getMetadataVersion());
    if (!new_columns_substreams.empty())
        ctx->new_data_part->setColumnsSubstreams(new_columns_substreams);
    ctx->new_data_part->partition.assign(ctx->source_part->partition);

    /// Don't change granularity type while mutating subset of columns
    ctx->mrk_extension = ctx->source_part->index_granularity_info.mark_type.getFileExtension();

    const auto data_settings = ctx->data->getSettings();
    ctx->need_sync = data_settings->needSyncPart(ctx->source_part->rows_count, ctx->source_part->getBytesOnDisk());
    ctx->execute_ttl_type = ExecuteTTLType::NONE;

    if (ctx->mutating_pipeline_builder.initialized())
        ctx->execute_ttl_type = MutationHelpers::shouldExecuteTTL(ctx->metadata_snapshot, ctx->interpreter->getColumnDependencies());

    if ((*ctx->data->getSettings())[MergeTreeSetting::exclude_deleted_rows_for_part_size_in_merge] && lightweight_delete_mode)
    {
        /// This mutation contains lightweight delete and we need to count the deleted rows,
        /// Reset existing_rows_count of new data part to 0 and it will be updated while writing _row_exists column
        ctx->count_lightweight_deleted_rows = true;
    }
    else
    {
        ctx->count_lightweight_deleted_rows = false;

        /// No need to count deleted rows, copy existing_rows_count from source part
        ctx->new_data_part->existing_rows_count = ctx->source_part->existing_rows_count.value_or(ctx->source_part->rows_count);
    }

    /// All columns from part are changed and may be some more that were missing before in part
    /// TODO We can materialize compact part without copying data
    /// Also currently mutations of types with dynamic subcolumns in Wide part are possible only by
    /// rewriting the whole part.
    if (MutationHelpers::haveMutationsOfDynamicColumns(ctx->source_part, ctx->commands_for_part)
        || !isWidePart(ctx->source_part)
        || !isFullPartStorage(ctx->source_part->getDataPartStorage())
        || (ctx->interpreter && ctx->interpreter->isAffectingAllColumns()))
    {
        /// In case of replicated merge tree with zero copy replication
        /// Here Clickhouse claims that this new part can be deleted in temporary state without unlocking the blobs
        /// The blobs have to be removed along with the part, this temporary part owns them and does not share them yet.
        ctx->new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::REMOVE_BLOBS;

        bool drop_expired_parts = suitable_for_ttl_optimization && !(*ctx->data->getSettings())[MergeTreeSetting::materialize_ttl_recalculate_only];
        if (drop_expired_parts)
            task = std::make_unique<ExecutableTaskDropTTLExpiredPartsDecorator>(std::make_unique<MutateAllPartColumnsTask>(ctx), ctx);
        else
            task = std::make_unique<MutateAllPartColumnsTask>(ctx);

        ProfileEvents::increment(ProfileEvents::MutationAllPartColumns);
    }
    else /// TODO: check that we modify only non-key columns in this case.
    {
        updateIndicesToRecalculateAndDrop(ctx);

        auto lightweight_mutation_projection_mode = (*ctx->data->getSettings())[MergeTreeSetting::lightweight_mutation_projection_mode];
        bool lightweight_delete_drops_projections =
            lightweight_mutation_projection_mode == LightweightMutationProjectionMode::DROP
            || lightweight_mutation_projection_mode == LightweightMutationProjectionMode::THROW;

        std::vector<ProjectionDescriptionRawPtr> projections_to_skip;

        bool should_create_projections = !(lightweight_delete_mode && lightweight_delete_drops_projections);
        /// Under lightweight delete mode, if option is drop, projections_to_recalc should be empty.
        if (should_create_projections)
        {
            ctx->projections_to_recalc = MutationHelpers::getProjectionsToRecalculate(
                ctx->source_part,
                ctx->metadata_snapshot,
                ctx->materialized_projections);

            projections_to_skip.assign(ctx->projections_to_recalc.begin(), ctx->projections_to_recalc.end());
        }
        else
        {
            for (const auto & projection : ctx->metadata_snapshot->getProjections())
                projections_to_skip.emplace_back(&projection);
        }

        ctx->stats_to_recalc = MutationHelpers::getStatisticsToRecalculate(ctx->metadata_snapshot, ctx->materialized_statistics);

        auto all_indices_to_recalc = ctx->indices_to_recalc;
        all_indices_to_recalc.insert(ctx->text_indices_to_recalc.begin(), ctx->text_indices_to_recalc.end());

        ctx->files_to_skip = MutationHelpers::collectFilesToSkip(
            ctx->source_part,
            ctx->new_data_part,
            ctx->updated_header,
            all_indices_to_recalc,
            ctx->indices_to_drop,
            ctx->mrk_extension,
            projections_to_skip,
            updated_columns_in_patches);

        ctx->files_to_rename = MutationHelpers::collectFilesForRenames(
            ctx->metadata_snapshot,
            ctx->source_part,
            ctx->new_data_part,
            ctx->for_file_renames,
            updated_columns_in_patches,
            ctx->mrk_extension);

        /// In case of replicated merge tree with zero copy replication
        /// Here Clickhouse has to follow the common procedure when deleting new part in temporary state
        /// Some of the files within the blobs are shared with source part, some belongs only to the part
        /// Keeper has to be asked with unlock request to release the references to the blobs
        ctx->new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::ASK_KEEPER;

        bool drop_expired_parts = suitable_for_ttl_optimization && !(*ctx->data->getSettings())[MergeTreeSetting::materialize_ttl_recalculate_only];
        if (drop_expired_parts)
            task = std::make_unique<ExecutableTaskDropTTLExpiredPartsDecorator>(std::make_unique<MutateSomePartColumnsTask>(ctx), ctx);
        else
            task = std::make_unique<MutateSomePartColumnsTask>(ctx);

        ProfileEvents::increment(ProfileEvents::MutationSomePartColumns);
    }

    LOG_TRACE(ctx->log, "Mutate task prepared");

    return true;
}

const HardlinkedFiles & MutateTask::getHardlinkedFiles() const
{
    return ctx->hardlinked_files;
}

}
