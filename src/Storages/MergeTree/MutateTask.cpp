#include <Storages/MergeTree/MutateTask.h>

#include <DataTypes/ObjectUtils.h>
#include <Disks/SingleDiskVolume.h>
#include <IO/HashingWriteBuffer.h>
#include "Common/Logger.h"
#include <Common/CurrentMetrics.h>
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
#include <Storages/MergeTree/MergeTreeIndexGin.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeVariant.h>
#include <boost/algorithm/string/replace.hpp>
#include <Common/ProfileEventsScope.h>
#include "Analyzer/IQueryTreeNode.h"
#include "Analyzer/QueryTreeBuilder.h"
#include "Analyzer/Utils.h"
#include "Columns/ColumnsCommon.h"
#include "Core/Names.h"
#include "Core/SettingsEnums.h"
#include "DataTypes/IDataType.h"
#include "QueryPipeline/QueryPipelineBuilder.h"
#include "Storages/ColumnsDescription.h"
#include "Storages/MergeTree/MergeTreeDataPartChecksum.h"
#include "Storages/MergeTree/MergeTreeDataPartTTLInfo.h"
#include "Storages/ProjectionsDescription.h"
#include "base/defines.h"
#include <Core/ColumnsWithTypeAndName.h>


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
    extern const MergeTreeSettingsUInt64 max_file_name_length;
    extern const MergeTreeSettingsFloat ratio_of_defaults_for_sparse_serialization;
    extern const MergeTreeSettingsBool replace_long_file_name_to_hash;
    extern const MergeTreeSettingsBool ttl_only_drop_parts;
    extern const MergeTreeSettingsBool enable_index_granularity_compression;
    extern const MergeTreeSettingsBool columns_and_secondary_indices_sizes_lazy_calculation;
}

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_STATISTICS;
    extern const int BAD_ARGUMENTS;
}

enum class DependencyType
{
    SKIP_INDEX,
    PROJECTION,
    TTL,
    STATISTICS,
};

struct MutatedData
{
    using LWDProjectionMode = LightweightMutationProjectionMode;
    LWDProjectionMode lwd_projection_mode = LWDProjectionMode::THROW;

    explicit MutatedData(MergeTreeSettingsPtr settings)
        : lwd_projection_mode((*settings)[MergeTreeSetting::lightweight_mutation_projection_mode])
    {
    }

    MutatedData() = default;

    NameSet readonly_columns;
    NameSet updated_columns;
    NameSet updated_by_ttl_columns;

    bool affects_all_columns = false;
    bool has_lightweight_delete = false;

    enum class Action
    {
        Skip,
        Drop,
        Rebuild,
    };

    Action getDependencyAction(const Names & required_columns, DependencyType type) const
    {
        if (type == DependencyType::PROJECTION && has_lightweight_delete)
        {
            switch (lwd_projection_mode)
            {
                case LWDProjectionMode::DROP: return Action::Drop;
                case LWDProjectionMode::REBUILD: return Action::Rebuild;
                default: return Action::Skip;
            }
        }

        return isAnyColumnMutated(required_columns) ? Action::Rebuild : Action::Skip;
    }

    bool needRebuildDependency(const Names & required_columns, DependencyType type) const
    {
        return getDependencyAction(required_columns, type) == Action::Rebuild;
    }

    bool needDropDependency(const Names & required_columns, DependencyType type) const
    {
        return getDependencyAction(required_columns, type) == Action::Drop;
    }

    bool isColumnMutated(const String & column_name) const
    {
        return affects_all_columns
            || updated_columns.contains(column_name)
            || updated_by_ttl_columns.contains(column_name);
    }

    bool isAnyColumnMutated(const Names & columns) const
    {
        return affects_all_columns
            || std::ranges::any_of(columns, [&](const auto & column) { return updated_columns.contains(column); })
            || std::ranges::any_of(columns, [&](const auto & column) { return updated_by_ttl_columns.contains(column); });
    }

    bool isAnyColumnMutated() const { return affects_all_columns || !updated_columns.empty() || !updated_by_ttl_columns.empty(); }
};

struct FilesToRename
{
    FilesToRename() = default;
    explicit FilesToRename(NameSet source_files_) : source_files(std::move(source_files_)) {}

    void add(const String & rename_from, const String & rename_to)
    {
        if (rename_to.empty() && !source_files.contains(rename_from))
            return;

        if (renamed_files.emplace(rename_from).second)
            files_to_rename.emplace_back(rename_from, rename_to);
    }

    const NameToNameVector & getFilesToRename() const { return files_to_rename; }

private:
    NameSet source_files;
    NameSet renamed_files;
    NameToNameVector files_to_rename;
};

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

    MergeTreeData::MutableDataPartPtr new_data_part;
    IMergedBlockOutputStreamPtr out;

    String mrk_extension;

    MutatedData mutated_data;

    NameSet projections_to_drop;
    std::set<ProjectionDescriptionRawPtr> projections_to_build;

    NameSet indices_to_drop;
    std::set<MergeTreeIndexPtr> indices_to_build;

    NameSet statistics_to_drop;
    std::set<ColumnStatisticsPartPtr> statistics_to_build;

    ColumnsDescription new_part_columns;
    SerializationInfoByName new_serialization_infos;

    NameSet ttls_to_calculate;
    NameSet ttls_to_materialize;

    FilesToRename files_to_rename;

    enum class Mode
    {
        AllColumns,
        SomeColumns,
    };

    Mode execution_mode = Mode::AllColumns;

    IMergeTreeDataPart::MinMaxIndexPtr minmax_idx;

    MergeTreeData::DataPart::Checksums existing_indices_stats_checksums;
    NameSet files_to_skip;
    // NameToNameVector files_to_rename;

    bool need_sync;
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
    }

    return false;
}

static UInt64 getExistingRowsCount(const Block & block)
{
    auto column = block.getByName(RowExistsColumn::name).column;
    const auto * row_exists_col = typeid_cast<const ColumnUInt8 *>(column.get());

    if (!row_exists_col)
    {
        LOG_WARNING(&Poco::Logger::get("MutationHelpers::getExistingRowsCount"), "_row_exists column type is not UInt8");
        return block.rows();
    }

    return countBytesInFilter(row_exists_col->getData());
}

static void addUsedIdentifiers(const ASTPtr & ast, const ContextPtr & context, NameSet & identifiers)
{
    auto query_tree = buildQueryTree(ast, context);
    auto ast_identifiers = collectIdentifiersFullNames(query_tree);
    std::move(ast_identifiers.begin(), ast_identifiers.end(), std::inserter(identifiers, identifiers.end()));
}

static void analyzeProjectionCommands(MutationContext & ctx)
{
    auto add_projection_to_build = [&](const ProjectionDescription & projection)
    {
        ctx.projections_to_build.insert(&projection);

        for (const auto & column : projection.required_columns)
            ctx.mutated_data.readonly_columns.insert(column);
    };

    auto add_projection_to_drop = [&](const MutationCommand & command)
    {
        ctx.projections_to_drop.insert(command.projection_name);
        ctx.files_to_rename.add(command.projection_name + ".proj", "");
    };

    NameSet modified_columns;
    const auto & projections = ctx.metadata_snapshot->getProjections();

    for (const auto & command : ctx.commands_for_part)
    {
        if (command.type == MutationCommand::DROP_PROJECTION)
        {
            if (ctx.source_part->hasProjection(command.projection_name))
                add_projection_to_drop(command);
        }
        else if (command.type == MutationCommand::MATERIALIZE_PROJECTION)
        {
            if (!ctx.source_part->hasProjection(command.projection_name))
                add_projection_to_build(projections.get(command.projection_name));
        }
        else if (command.type == MutationCommand::READ_COLUMN)
        {
            auto column_in_part = ctx.source_part->tryGetColumn(command.column_name);
            if (column_in_part && command.data_type && !column_in_part->type->equals(*command.data_type))
                modified_columns.insert(command.column_name);
        }
    }

    for (const auto & projection : projections)
    {
        if (ctx.projections_to_drop.contains(projection.name) || !ctx.source_part->hasProjection(projection.name))
            continue;

         /// Always rebuild broken projections.
        if (ctx.source_part->hasBrokenProjection(projection.name))
        {
            LOG_DEBUG(ctx.log, "Will rebuild broken projection {}", projection.name);
            add_projection_to_build(projection);
            continue;
        }

        /// Check if the type of this column is changed and there are projections that
        /// have this column in the primary key. We should rebuild such projections.
        const auto & pk_columns = projection.metadata->getPrimaryKeyColumns();
        bool modified_pk_column = std::ranges::any_of(pk_columns, [&](const auto & pk_column) { return modified_columns.contains(pk_column); });

        if (modified_pk_column)
        {
            add_projection_to_build(projection);
            continue;
        }

        auto action = ctx.mutated_data.getDependencyAction(projection.required_columns, DependencyType::PROJECTION);

        if (action == MutatedData::Action::Rebuild)
        {
            add_projection_to_build(projection);
        }
        else if (action == MutatedData::Action::Drop)
        {
            add_projection_to_drop({.type = MutationCommand::Type::DROP_PROJECTION, .projection_name = projection.name});
        }
    }
}

static void addIndexFilesToRename(const String & index_name, MutationContext & ctx)
{
    static const std::array<String, 2> suffixes = {".idx2", ".idx"};
    static const std::array<String, 4> gin_suffixes = {".gin_dict", ".gin_post", ".gin_seg", ".gin_sid"}; /// .gin_* means generalized inverted index (aka. full-text-index)

    for (const auto & suffix : suffixes)
    {
        const String filename = INDEX_FILE_PREFIX + index_name + suffix;
        const String filename_mrk = INDEX_FILE_PREFIX + index_name + ctx.mrk_extension;

        ctx.files_to_rename.add(filename, "");
        ctx.files_to_rename.add(filename_mrk, "");
    }

    for (const auto & gin_suffix : gin_suffixes)
    {
        const String filename = INDEX_FILE_PREFIX + index_name + gin_suffix;
        ctx.files_to_rename.add(filename, "");
    }
}

static void analyzeSkipIndicesCommands(MutationContext & ctx)
{
    auto add_index_to_build = [&](const IndexDescription & index)
    {
        auto index_ptr = MergeTreeIndexFactory::instance().get(index);
        ctx.indices_to_build.insert(index_ptr);

        auto required_columns = index.expression->getRequiredColumns();
        for (const auto & column : required_columns)
            ctx.mutated_data.readonly_columns.insert(column);
    };

    auto add_index_to_drop = [&](const String & index_name)
    {
        ctx.indices_to_drop.insert(index_name);
        addIndexFilesToRename(index_name, ctx);
    };

    const auto & indices = ctx.metadata_snapshot->getSecondaryIndices();
    std::unordered_map<std::string_view, const IndexDescription *> index_by_name;

    for (const auto & index : indices)
    {
        index_by_name[index.name] = &index;
    }

    for (const auto & command : ctx.commands_for_part)
    {
        if (command.type == MutationCommand::DROP_INDEX)
        {
            if (ctx.source_part->hasSecondaryIndex(command.index_name))
                add_index_to_drop(command.index_name);
        }
        else if (command.type == MutationCommand::MATERIALIZE_INDEX)
        {
            if (!ctx.source_part->hasSecondaryIndex(command.index_name))
            {
                const auto it = index_by_name.find(command.index_name);
                if (it == index_by_name.end())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown index: {}", command.index_name);

                add_index_to_build(*it->second);
            }
        }
    }

    for (const auto & index : indices)
    {
        if (ctx.indices_to_drop.contains(index.name) || !ctx.source_part->hasSecondaryIndex(index.name))
            continue;

        auto required_columns = index.expression->getRequiredColumns();
        if (ctx.mutated_data.needRebuildDependency(required_columns, DependencyType::SKIP_INDEX))
            add_index_to_build(index);
    }
}

static void analyzeStatisticsCommands(MutationContext & ctx)
{
    auto add_statistic_to_build = [&](const ColumnDescription & column)
    {
        auto stat_ptr = MergeTreeStatisticsFactory::instance().get(column);
        ctx.statistics_to_build.insert(stat_ptr);
        ctx.mutated_data.readonly_columns.insert(column.name);
    };

    auto add_statistic_to_drop = [&](const String & column_name)
    {
        ctx.statistics_to_drop.insert(column_name);
        ctx.files_to_rename.add(STATS_FILE_PREFIX + column_name + STATS_FILE_SUFFIX, "");
    };

    const auto & table_columns = ctx.metadata_snapshot->getColumns();

    for (const auto & command : ctx.commands_for_part)
    {
        if (command.type == MutationCommand::DROP_STATISTICS)
        {
            for (const auto & column_name : command.statistics_columns)
                add_statistic_to_drop(column_name);
        }
        else if (command.type == MutationCommand::MATERIALIZE_STATISTICS)
        {
            for (const auto & column_name : command.statistics_columns)
            {
                auto column_desc = table_columns.tryGetColumnDescription(GetColumnsOptions::AllPhysical, column_name);
                if (!column_desc || column_desc->statistics.empty())
                    throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Unknown statistics column: {}", column_name);

                add_statistic_to_build(*column_desc);
            }
        }
    }

    for (const auto & column : table_columns)
    {
        if (ctx.statistics_to_drop.contains(column.name) || column.statistics.empty())
            continue;

        if (ctx.mutated_data.needRebuildDependency({column.name}, DependencyType::STATISTICS))
            add_statistic_to_build(column);
    }
}

static void analyzeTTLCommands(MutationContext & ctx)
{
    bool drop_only = (*ctx.data->getSettings())[MergeTreeSetting::ttl_only_drop_parts];
    bool recalculate_only = (*ctx.data->getSettings())[MergeTreeSetting::materialize_ttl_recalculate_only];

    auto column_ttls = ctx.metadata_snapshot->getColumnTTLs();
    auto table_ttls = ctx.metadata_snapshot->getTableTTLs().getAllDescriptions();

    if (column_ttls.empty() && table_ttls.empty())
        return;

    auto add_required_columns = [&](const TTLDescription & ttl)
    {
        for (const auto & column : ttl.expression_columns)
            ctx.mutated_data.readonly_columns.insert(column.name);

        for (const auto & column : ttl.where_expression_columns)
            ctx.mutated_data.readonly_columns.insert(column.name);
    };

    auto analyze_ttl = [&](const TTLDescription & ttl, const String & ttl_name, bool is_column_ttl)
    {
        if (recalculate_only || drop_only || ttl.mode == TTLMode::RECOMPRESS || ttl.mode == TTLMode::MOVE)
        {
            add_required_columns(ttl);
            ctx.ttls_to_calculate.insert(ttl_name);
        }
        else
        {
            if (is_column_ttl)
            {
                ctx.mutated_data.updated_by_ttl_columns.insert(ttl_name);
            }
            else
            {
                ctx.mutated_data.affects_all_columns = true;
                auto all_columns = ctx.metadata_snapshot->getColumns().getAllPhysical();

                for (const auto & column : all_columns)
                    ctx.mutated_data.updated_by_ttl_columns.insert(column.name);
            }

            add_required_columns(ttl);
            ctx.ttls_to_materialize.insert(ttl_name);
        }
    };

    auto it = std::ranges::find_if(ctx.commands_for_part, [](const auto & command)
    {
        return command.type == MutationCommand::MATERIALIZE_TTL;
    });

    if (it != ctx.commands_for_part.end())
    {
        ctx.for_interpreter.push_back(*it);

        for (const auto & ttl : table_ttls)
            analyze_ttl(ttl, ttl.result_column, false);

        for (const auto & [column_name, ttl] : column_ttls)
            analyze_ttl(ttl, column_name, true);

        return;
    }

    bool finished = false;
    while (!finished)
    {
        finished = true;

        for (const auto & [column_name, ttl] : column_ttls)
        {
            auto required_columns = ttl.expression_columns.getNames();
            if (ctx.mutated_data.needRebuildDependency(required_columns, DependencyType::TTL))
            {
                if (!ctx.mutated_data.isColumnMutated(column_name))
                    finished = false;

                analyze_ttl(ttl, column_name, true);
            }
        }
    }

    for (const auto & ttl : table_ttls)
    {
        auto required_columns = ttl.expression_columns.getNames();
        if (ctx.mutated_data.needRebuildDependency(required_columns, DependencyType::TTL))
            analyze_ttl(ttl, ttl.result_column, false);
    }
}

static void addColumn(MutationContext & ctx, const String & column_name, const DataTypePtr & type)
{
    if (ctx.new_part_columns.has(column_name))
        return;

    ctx.new_part_columns.add(ColumnDescription(column_name, type));

    if (!type->supportsSparseSerialization())
        return;

    SerializationInfo::Settings settings
    {
        .ratio_of_defaults_for_sparse = (*ctx.data->getSettings())[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization],
        .choose_kind = false
    };

    if (settings.isAlwaysDefault())
        return;

    auto new_info = type->createSerializationInfo(settings);
    ctx.new_serialization_infos.emplace(column_name, std::move(new_info));
}

static void dropColumn(MutationContext & ctx, const String & column_name)
{
    ctx.new_part_columns.remove(column_name);
    ctx.new_serialization_infos.erase(column_name);
}

MutatedData analyzeDataCommands(MutationContext & ctx)
{
    const auto & table_columns = ctx.metadata_snapshot->getColumns();

    for (const auto & command : ctx.commands_for_part)
    {
        if (command.type == MutationCommand::MATERIALIZE_COLUMN)
        {
            /// For ordinary column with default or materialized expression, MATERIALIZE COLUMN should not override past values
            /// So we only mutate column if `command.column_name` is a default/materialized column or if the part does not have physical column file
            auto column_ordinary = table_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::Ordinary, command.column_name);

            if (!column_ordinary || !ctx.source_part->tryGetColumn(command.column_name) || !ctx.source_part->hasColumnFiles(*column_ordinary))
            {
                ctx.for_interpreter.push_back(command);
                ctx.mutated_data.updated_columns.insert(command.column_name);
            }
        }
        else if (command.type == MutationCommand::DELETE || command.type == MutationCommand::APPLY_DELETED_MASK)
        {
            ctx.for_interpreter.push_back(command);
            ctx.mutated_data.affects_all_columns = true;
        }
        else if (command.type == MutationCommand::UPDATE)
        {
            ctx.for_interpreter.push_back(command);
            addUsedIdentifiers(command.predicate, ctx.context, ctx.mutated_data.readonly_columns);

            for (const auto & [column_name, ast] : command.column_to_update_expression)
            {
                if (column_name == RowExistsColumn::name)
                    ctx.mutated_data.has_lightweight_delete = true;

                ctx.mutated_data.updated_columns.insert(column_name);
                addUsedIdentifiers(ast, ctx.context, ctx.mutated_data.readonly_columns);
            }
        }
    }

    auto options = GetColumnsOptions(GetColumnsOptions::All).withVirtuals(VirtualsKind::Persistent);

    for (const auto & column : ctx.mutated_data.updated_columns)
    {
        auto type = ctx.storage_snapshot->getColumn(options, column).type;
        addColumn(ctx, column, type);
    }

    if (ctx.mutated_data.affects_all_columns && !ctx.mutated_data.has_lightweight_delete && ctx.new_part_columns.has(RowExistsColumn::name))
    {
        dropColumn(ctx, RowExistsColumn::name);
    }

    return ctx.mutated_data;
}

void addColumnsRequiredForDependencies(MutationContext & ctx)
{
    auto options = GetColumnsOptions(GetColumnsOptions::All).withSubcolumns().withVirtuals();
    const auto & updated_columns = ctx.mutated_data.updated_columns;
    const auto & updated_by_ttl_columns = ctx.mutated_data.updated_by_ttl_columns;

    for (const auto & column : ctx.mutated_data.updated_by_ttl_columns)
    {
        if (ctx.storage_snapshot->tryGetColumn(options, column) && !updated_columns.contains(column))
            ctx.for_interpreter.push_back({.type = MutationCommand::Type::READ_COLUMN, .column_name = column, .readonly = false});
    }

    for (const auto & column_name : ctx.mutated_data.readonly_columns)
    {
        if (ctx.storage_snapshot->tryGetColumn(options, column_name) && !updated_columns.contains(column_name) && !updated_by_ttl_columns.contains(column_name))
            ctx.for_interpreter.push_back({.type = MutationCommand::Type::READ_COLUMN, .column_name = column_name, .readonly = true});
    }
}

static void modifyColumn(MutationContext & ctx, const String & column_name, const DataTypePtr & new_type)
{
    auto old_type = ctx.new_part_columns.getPhysical(column_name).type;
    ctx.new_part_columns.modify(column_name, [&](auto & column) { column.type = new_type; });

    auto it = ctx.new_serialization_infos.find(column_name);
    if (it == ctx.new_serialization_infos.end())
        return;

    SerializationInfo::Settings settings
    {
        .ratio_of_defaults_for_sparse = (*ctx.data->getSettings())[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization],
        .choose_kind = false
    };

    auto old_info = it->second;
    ctx.new_serialization_infos.erase(it);

    if (!new_type->supportsSparseSerialization() || settings.isAlwaysDefault())
        return;

    auto new_info = new_type->createSerializationInfo(settings);

    if (!old_info->structureEquals(*new_info))
    {
        ctx.new_serialization_infos.emplace(column_name, std::move(new_info));
    }
    else
    {
        new_info = old_info->createWithType(*old_type, *new_type, settings);
        ctx.new_serialization_infos.emplace(column_name, std::move(new_info));
    }
}

static void renameColumn(MutationContext & ctx, const String & rename_from, const String & rename_to)
{
    ctx.new_part_columns.rename(rename_from, rename_to);

    auto it = ctx.new_serialization_infos.find(rename_from);
    if (it == ctx.new_serialization_infos.end())
        return;

    auto new_info = it->second;
    ctx.new_serialization_infos.erase(it);
    ctx.new_serialization_infos.emplace(rename_to, std::move(new_info));
}

void analyzeMetadataCommandsForSomeColumnsMode(MutationContext & ctx, const AlterConversionsPtr & alter_conversions)
{
    for (const auto & command : ctx.commands_for_part)
    {
        /// If we don't have this column in source part, we don't need to materialize it.
        if (!ctx.new_part_columns.has(command.column_name))
        {
            continue;
        }
        else if (command.type == MutationCommand::Type::READ_COLUMN)
        {
            ctx.for_interpreter.push_back(command);
            ctx.for_file_renames.push_back(command);

            if (command.data_type)
                modifyColumn(ctx, command.column_name, command.data_type);
        }
        else if (command.type == MutationCommand::Type::DROP_COLUMN)
        {
            ctx.for_file_renames.push_back(command);
            dropColumn(ctx, command.column_name);
        }
        // else if (command.type == MutationCommand::Type::RENAME_COLUMN)
        // {
        //     ctx.for_file_renames.push_back(command);
        //     renameColumn(ctx, command.column_name, command.rename_to);
        // }
    }

    /// We don't add renames from commands, instead we take them from rename_map.
    /// It's important because required renames depend not only on part's data version (i.e. mutation version)
    /// but also on part's metadata version. Why we have such logic only for renames? Because all other types of alter
    /// can be deduced based on difference between part's schema and table schema.
    for (const auto & [rename_to, rename_from] : alter_conversions->getRenameMap())
    {
        if (ctx.new_part_columns.has(rename_from))
            renameColumn(ctx, rename_from, rename_to);

        ctx.for_file_renames.push_back({.type = MutationCommand::Type::RENAME_COLUMN, .column_name = rename_from, .rename_to = rename_to});
    }
}

static void analyzeCommandsForSomeColumnsMode(MutationContext & ctx, const AlterConversionsPtr & alter_conversions)
{
    ctx.execution_mode = MutationContext::Mode::SomeColumns;
    ctx.mutated_data = MutatedData(ctx.data->getSettings());
    ctx.files_to_rename = FilesToRename(ctx.source_part->checksums.getAllFiles());
    ctx.new_part_columns = ctx.source_part->getColumnsDescription();
    ctx.new_serialization_infos = ctx.source_part->getSerializationInfos();

    analyzeDataCommands(ctx);
    analyzeTTLCommands(ctx);
    analyzeProjectionCommands(ctx);
    analyzeSkipIndicesCommands(ctx);
    analyzeStatisticsCommands(ctx);
    addColumnsRequiredForDependencies(ctx);
    analyzeMetadataCommandsForSomeColumnsMode(ctx, alter_conversions);

    if (ctx.mutated_data.affects_all_columns)
        ctx.execution_mode = MutationContext::Mode::AllColumns;
}

static void analyzeCommandsForAllColumnsMode(MutationContext & ctx, const AlterConversionsPtr & alter_conversions)
{
    ctx.execution_mode = MutationContext::Mode::AllColumns;
    ctx.mutated_data = MutatedData(ctx.data->getSettings());
    ctx.files_to_rename = FilesToRename(ctx.source_part->checksums.getAllFiles());
    ctx.new_part_columns = ctx.source_part->getColumnsDescription();
    ctx.new_serialization_infos = ctx.source_part->getSerializationInfos();

    analyzeDataCommands(ctx);
    analyzeTTLCommands(ctx);
    analyzeProjectionCommands(ctx);
    analyzeSkipIndicesCommands(ctx);
    analyzeStatisticsCommands(ctx);
    addColumnsRequiredForDependencies(ctx);

    NameSet dropped_columns;
    bool need_mutate_columns = ctx.mutated_data.isAnyColumnMutated();

    for (const auto & command : ctx.commands_for_part)
    {
        bool has_column = ctx.new_part_columns.has(command.column_name);
        bool has_nested_column = ctx.new_part_columns.hasNested(command.column_name);

        if (!has_column && !has_nested_column)
            continue;

        if (command.type == MutationCommand::Type::READ_COLUMN)
        {
            need_mutate_columns = true;
            ctx.for_interpreter.push_back(command);

            if (command.data_type)
                modifyColumn(ctx, command.column_name, command.data_type);
        }
        else if (command.type == MutationCommand::Type::DROP_COLUMN)
        {
            if (has_nested_column)
            {
                const auto & nested = ctx.new_part_columns.getNested(command.column_name);
                chassert(!nested.empty());

                for (const auto & nested_column : nested)
                    dropped_columns.emplace(nested_column.name);
            }
            else
            {
                dropped_columns.emplace(command.column_name);
            }

            need_mutate_columns = true;
            dropColumn(ctx, command.column_name);
        }
    }

    /// We don't add renames from commands, instead we take them from rename_map.
    /// It's important because required renames depend not only on part's data version (i.e. mutation version)
    /// but also on part's metadata version. Why we have such logic only for renames? Because all other types of alter
    /// can be deduced based on difference between part's schema and table schema.
    for (const auto & [rename_to, rename_from] : alter_conversions->getRenameMap())
    {
        if (ctx.new_part_columns.has(rename_from))
        {
            /// Actual rename
            ctx.for_interpreter.push_back(
            {
                .type = MutationCommand::Type::READ_COLUMN,
                .column_name = rename_to,
            });

            renameColumn(ctx, rename_from, rename_to);
            need_mutate_columns = true;
        }
    }

    UNUSED(need_mutate_columns);

    /// If it's compact part, then we don't need to actually remove files from disk we just don't read dropped columns
    for (const auto & column : ctx.new_part_columns)
    {
        const auto & part = ctx.source_part;
        const auto & metadata_snapshot = ctx.metadata_snapshot;

        if (!metadata_snapshot->getColumns().has(column.name) && !part->storage.getVirtualsPtr()->has(column.name))
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
                LOG_WARNING(ctx.log, "Ignoring column {} from part {} with metadata version {} because there is no such column "
                                    "in table {} with metadata version {}. Assuming the column was dropped", column.name, part->name,
                            part_metadata_version, part->storage.getStorageID().getNameForLogs(), table_metadata_version);
                continue;
            }

            /// StorageMergeTree does not have metadata version
            if (part->storage.supportsReplication())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} with metadata version {} contains column {} that is absent "
                                "in table {} with metadata version {}",
                                part->name, part_metadata_version, column.name,
                                part->storage.getStorageID().getNameForLogs(), table_metadata_version);
            }
        }

        ctx.for_interpreter.emplace_back(MutationCommand
        {
            .type = MutationCommand::Type::READ_COLUMN,
            .column_name = column.name,
            .data_type = column.type
        });
    }
}

static void analyzeCommands(MutationContext & ctx, const AlterConversionsPtr & alter_conversions)
{
    if (haveMutationsOfDynamicColumns(ctx.source_part, ctx.commands_for_part) || !isWidePart(ctx.source_part) || !isFullPartStorage(ctx.source_part->getDataPartStorage()))
    {
        analyzeCommandsForAllColumnsMode(ctx, alter_conversions);
    }
    else
    {
        analyzeCommandsForSomeColumnsMode(ctx, alter_conversions);
    }
}

static void addTransformToBuildIndices(QueryPipelineBuilder & builder, const std::set<MergeTreeIndexPtr> & indices_to_build, const ContextPtr & context)
{
    if (indices_to_build.empty() || !builder.initialized())
        return;

    ASTPtr indices_expr_list = std::make_shared<ASTExpressionList>();

    for (const auto & index : indices_to_build)
    {
        ASTPtr expr_list = index->index.expression_list_ast->clone();
        for (const auto & expr : expr_list->children)
            indices_expr_list->children.push_back(expr->clone());
    }

    auto indices_syntax = TreeRewriter(context).analyze(indices_expr_list, builder.getHeader().getNamesAndTypesList());
    auto indices_expr = ExpressionAnalyzer(
        indices_expr_list,
        indices_syntax, context).getActions(false);

    /// We can update only one column, but some skip idx expression may depend on several
    /// columns (c1 + c2 * c3). It works because this stream was created with help of
    /// MutationsInterpreter which knows about skip indices and stream 'in' already has
    /// all required columns.
    /// TODO move this logic to single place.
    builder.addTransform(std::make_shared<ExpressionTransform>(builder.getHeader(), indices_expr));
    builder.addTransform(std::make_shared<MaterializingTransform>(builder.getHeader()));
}

static void addTransformToBuildTTLs(QueryPipelineBuilderPtr & builder, const MutationContext & ctx)
{
    PreparedSets::Subqueries subqueries;

    if (!ctx.ttls_to_materialize.empty())
    {
        auto transform = std::make_shared<TTLTransform>(ctx.context, builder->getHeader(), *ctx.data, ctx.metadata_snapshot, ctx.new_data_part, ctx.time_of_mutation, true);
        subqueries = transform->getSubqueries();
        builder->addTransform(std::move(transform));
    }
    else if (!ctx.ttls_to_calculate.empty())
    {
        auto transform = std::make_shared<TTLCalcTransform>(ctx.context, builder->getHeader(), *ctx.data, ctx.metadata_snapshot, ctx.new_data_part, ctx.time_of_mutation, true);
        subqueries = transform->getSubqueries();
        builder->addTransform(std::move(transform));
    }

    if (!subqueries.empty())
        builder = addCreatingSetsTransform(std::move(builder), std::move(subqueries), ctx.context);
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
                auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(column_name, substream_path, source_part_checksums);
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
    const String & mrk_extension,
    const Names & projections_to_skip,
    const std::set<ColumnStatisticsPartPtr> & stats_to_recalc)
{
    NameSet files_to_skip = source_part->getFileNamesWithoutChecksums();

    /// Do not hardlink this file because it's always rewritten at the end of mutation.
    files_to_skip.insert(IMergeTreeDataPart::SERIALIZATION_FILE_NAME);

    for (const auto & index : indices_to_recalc)
    {
        /// Since MinMax index has .idx2 extension, we need to add correct extension.
        files_to_skip.insert(index->getFileName() + index->getSerializedFileExtension());
        files_to_skip.insert(index->getFileName() + mrk_extension);

        // Skip all full-text index files, for they will be rebuilt
        if (dynamic_cast<const MergeTreeIndexGin *>(index.get()))
        {
            auto index_filename = index->getFileName();
            files_to_skip.insert(index_filename + ".gin_dict");
            files_to_skip.insert(index_filename + ".gin_post");
            files_to_skip.insert(index_filename + ".gin_sed");
            files_to_skip.insert(index_filename + ".gin_sid");
        }
    }

    for (const auto & projection_dir : projections_to_skip)
        files_to_skip.insert(projection_dir);

    for (const auto & stat : stats_to_recalc)
        files_to_skip.insert(stat->getFileName() + STATS_FILE_SUFFIX);

    if (isWidePart(source_part))
    {
        auto new_stream_counts = getStreamCounts(new_part, source_part->checksums, new_part->getColumns().getNames());
        auto source_updated_stream_counts = getStreamCounts(source_part, source_part->checksums, updated_header.getNames());
        auto new_updated_stream_counts = getStreamCounts(new_part, source_part->checksums, updated_header.getNames());

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
static void collectFilesForRenames(MutationContext & ctx)
{
    /// Collect counts for shared streams of different columns. As an example, Nested columns have shared stream with array sizes.
    auto stream_counts = getStreamCounts(ctx.source_part, ctx.source_part->checksums, ctx.source_part->getColumns().getNames());

    /// Remove old data
    for (const auto & command : ctx.for_file_renames)
    {
        if (isWidePart(ctx.source_part))
        {
            if (command.type == MutationCommand::Type::DROP_COLUMN)
            {
                ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path)
                {
                    auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(command.column_name, substream_path, ctx.source_part->checksums);

                    /// Delete files if they are no longer shared with another column.
                    if (stream_name && --stream_counts[*stream_name] == 0)
                    {
                        ctx.files_to_rename.add(*stream_name + ".bin", "");
                        ctx.files_to_rename.add(*stream_name + ctx.mrk_extension, "");
                    }
                };

                if (auto serialization = ctx.source_part->tryGetSerialization(command.column_name))
                    serialization->enumerateStreams(callback);

                /// If we drop a column with statistics, we should also drop the stat file.
                ctx.files_to_rename.add(STATS_FILE_PREFIX + command.column_name + STATS_FILE_SUFFIX, "");
            }
            else if (command.type == MutationCommand::Type::RENAME_COLUMN)
            {
                String escaped_name_from = escapeForFileName(command.column_name);
                String escaped_name_to = escapeForFileName(command.rename_to);

                ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path)
                {
                    String full_stream_from = ISerialization::getFileNameForStream(command.column_name, substream_path);
                    String full_stream_to = boost::replace_first_copy(full_stream_from, escaped_name_from, escaped_name_to);

                    auto stream_from = IMergeTreeDataPart::getStreamNameOrHash(full_stream_from, ctx.source_part->checksums);
                    if (!stream_from)
                        return;

                    String stream_to;
                    auto storage_settings = ctx.source_part->storage.getSettings();

                    if ((*storage_settings)[MergeTreeSetting::replace_long_file_name_to_hash] && full_stream_to.size() > (*storage_settings)[MergeTreeSetting::max_file_name_length])
                        stream_to = sipHash128String(full_stream_to);
                    else
                        stream_to = full_stream_to;

                    if (stream_from != stream_to)
                    {
                        ctx.files_to_rename.add(*stream_from + ".bin", stream_to + ".bin");
                        ctx.files_to_rename.add(*stream_from + ctx.mrk_extension, stream_to + ctx.mrk_extension);
                    }
                };

                if (auto serialization = ctx.source_part->tryGetSerialization(command.column_name))
                    serialization->enumerateStreams(callback);

                /// if we rename a column with statistics, we should also rename the stat file.
                if (ctx.source_part->checksums.has(STATS_FILE_PREFIX + command.column_name + STATS_FILE_SUFFIX))
                    ctx.files_to_rename.add(STATS_FILE_PREFIX + command.column_name + STATS_FILE_SUFFIX, STATS_FILE_PREFIX + command.rename_to + STATS_FILE_SUFFIX);
            }
            else if (command.type == MutationCommand::Type::READ_COLUMN)
            {
                /// Remove files for streams that exist in source_part,
                /// but were removed in new_part by MODIFY COLUMN from
                /// type with higher number of streams (e.g. LowCardinality -> String).

                auto old_streams = getStreamCounts(ctx.source_part, ctx.source_part->checksums, ctx.source_part->getColumns().getNames());
                auto new_streams = getStreamCounts(ctx.new_data_part, ctx.source_part->checksums, ctx.source_part->getColumns().getNames());

                for (const auto & [old_stream, _] : old_streams)
                {
                    if (!new_streams.contains(old_stream) && --stream_counts[old_stream] == 0)
                    {
                        ctx.files_to_rename.add(old_stream + ".bin", "");
                        ctx.files_to_rename.add(old_stream + ctx.mrk_extension, "");
                    }
                }
            }
        }
    }

    if (!ctx.source_part->getSerializationInfos().empty() && ctx.new_data_part->getSerializationInfos().empty())
    {
        ctx.files_to_rename.add(IMergeTreeDataPart::SERIALIZATION_FILE_NAME, "");
    }
}


/// Initialize and write to disk new part fields like checksums, columns, etc.
void finalizeMutatedPart(
    const MergeTreeDataPartPtr & source_part,
    MergeTreeData::MutableDataPartPtr new_data_part,
    bool modified_ttl,
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

    if (modified_ttl)
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

    if (!new_data_part->getSerializationInfos().empty())
    {
        auto out_serialization = new_data_part->getDataPartStorage().writeFile(IMergeTreeDataPart::SERIALIZATION_FILE_NAME, 4096, context->getWriteSettings());
        HashingWriteBuffer out_hashing(*out_serialization);
        new_data_part->getSerializationInfos().writeJSON(out_hashing);
        out_hashing.finalize();
        new_data_part->checksums.files[IMergeTreeDataPart::SERIALIZATION_FILE_NAME].file_size = out_hashing.count();
        new_data_part->checksums.files[IMergeTreeDataPart::SERIALIZATION_FILE_NAME].file_hash = out_hashing.getHash();
        written_files.push_back(std::move(out_serialization));
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

                state = State::NEED_MERGE_PROJECTION_PARTS;
                return true;
            }
            case State::NEED_MERGE_PROJECTION_PARTS:
            {
                if (iterateThroughAllProjections())
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
    void writeTempProjectionPart(size_t projection_idx, Chunk chunk);
    void finalizeTempProjections();
    bool iterateThroughAllProjections();
    void constructTaskForProjectionPartsMerge();
    void finalize();

    enum class State : uint8_t
    {
        NEED_PREPARE,
        NEED_MUTATE_ORIGINAL_PART,
        NEED_MERGE_PROJECTION_PARTS,

        SUCCESS
    };

    State state{State::NEED_PREPARE};
    MutationContextPtr ctx;

    size_t block_num = 0;

    using ProjectionNameToItsBlocks = std::map<String, MergeTreeData::MutableDataPartsVector>;
    ProjectionNameToItsBlocks projection_parts;
    std::move_iterator<ProjectionNameToItsBlocks::iterator> projection_parts_iterator;

    std::vector<Squashing> projection_squashes;
    std::vector<ProjectionDescriptionRawPtr> projections_to_build;
    const ProjectionsDescription & projections;

    ExecutableTaskPtr merge_projection_parts_task_ptr;

    /// Existing rows count calculated during part writing.
    /// It is initialized in prepare(), calculated in mutateOriginalPartAndPrepareProjections()
    /// and set to new_data_part in finalize()
    size_t existing_rows_count;
};


void PartMergerWriter::prepare()
{
    const auto & settings = ctx->context->getSettingsRef();

    for (auto it = ctx->projections_to_build.begin(); it != ctx->projections_to_build.end(); ++it)
    {
        // We split the materialization into multiple stages similar to the process of INSERT SELECT query.
        projections_to_build.push_back(*it);
        projection_squashes.emplace_back(ctx->updated_header, settings[Setting::min_insert_block_size_rows], settings[Setting::min_insert_block_size_bytes]);
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
            finalizeTempProjections();
            return false;
        }

        if (ctx->minmax_idx)
            ctx->minmax_idx->update(cur_block, MergeTreeData::getMinMaxColumnsNames(ctx->metadata_snapshot->getPartitionKey()));

        ctx->out->write(cur_block);

        /// TODO: move this calculation to DELETE FROM mutation
        if (ctx->count_lightweight_deleted_rows)
            existing_rows_count += MutationHelpers::getExistingRowsCount(cur_block);

        for (size_t i = 0, size = ctx->projections_to_build.size(); i < size; ++i)
        {
            Chunk squashed_chunk;

            {
                ProfileEventTimeIncrement<Microseconds> projection_watch(ProfileEvents::MutateTaskProjectionsCalculationMicroseconds);
                Block block_to_squash = projections_to_build[i]->calculate(cur_block, ctx->context);

                projection_squashes[i].setHeader(block_to_squash.cloneEmpty());
                squashed_chunk = Squashing::squash(projection_squashes[i].add({block_to_squash.getColumns(), block_to_squash.rows()}));
            }

            if (squashed_chunk)
                writeTempProjectionPart(i, std::move(squashed_chunk));
        }

        (*ctx->mutate_entry)->rows_written += cur_block.rows();
        (*ctx->mutate_entry)->bytes_written_uncompressed += cur_block.bytes();
    } while (watch.elapsedMilliseconds() < step_time_ms);

    /// Need execute again
    return true;
}

void PartMergerWriter::writeTempProjectionPart(size_t projection_idx, Chunk chunk)
{
    const auto & projection = *projections_to_build[projection_idx];
    const auto & projection_plan = projection_squashes[projection_idx];

    auto result = projection_plan.getHeader().cloneWithColumns(chunk.detachColumns());

    auto tmp_part = MergeTreeDataWriter::writeTempProjectionPart(
        *ctx->data,
        ctx->log,
        result,
        projection,
        ctx->new_data_part.get(),
        ++block_num);

    tmp_part->finalize();
    tmp_part->part->getDataPartStorage().commitTransaction();
    projection_parts[projection.name].emplace_back(std::move(tmp_part->part));
}

void PartMergerWriter::finalizeTempProjections()
{
    // Write the last block
    for (size_t i = 0, size = ctx->projections_to_build.size(); i < size; ++i)
    {
        auto squashed_chunk = Squashing::squash(projection_squashes[i].flush());
        if (squashed_chunk)
            writeTempProjectionPart(i, std::move(squashed_chunk));
    }

    projection_parts_iterator = std::make_move_iterator(projection_parts.begin());

    /// Maybe there are no projections ?
    if (projection_parts_iterator != std::make_move_iterator(projection_parts.end()))
        constructTaskForProjectionPartsMerge();
}

void PartMergerWriter::constructTaskForProjectionPartsMerge()
{
    auto && [name, parts] = *projection_parts_iterator;
    const auto & projection = projections.get(name);

    merge_projection_parts_task_ptr = std::make_unique<MergeProjectionPartsTask>
    (
        name,
        std::move(parts),
        projection,
        block_num,
        ctx->context,
        ctx->holder,
        ctx->mutator,
        ctx->mutate_entry,
        ctx->time_of_mutation,
        ctx->new_data_part,
        ctx->space_reservation
    );
}


bool PartMergerWriter::iterateThroughAllProjections()
{
    /// In case if there are no projections we didn't construct a task
    if (!merge_projection_parts_task_ptr)
        return false;

    if (merge_projection_parts_task_ptr->executeStep())
        return true;

    ++projection_parts_iterator;

    if (projection_parts_iterator == std::make_move_iterator(projection_parts.end()))
        return false;

    constructTaskForProjectionPartsMerge();

    return true;
}

void PartMergerWriter::finalize()
{
    if (ctx->count_lightweight_deleted_rows)
        ctx->new_data_part->existing_rows_count = existing_rows_count;
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
        /// A stat file need to be renamed iff the column is renamed.
        NameToNameMap renamed_stats;

        for (const auto & command : ctx->for_file_renames)
        {
            if (command.type == MutationCommand::RENAME_COLUMN
                     && ctx->source_part->checksums.files.contains(STATS_FILE_PREFIX + command.column_name + STATS_FILE_SUFFIX))
                renamed_stats[STATS_FILE_PREFIX + command.column_name + STATS_FILE_SUFFIX] = STATS_FILE_PREFIX + command.rename_to + STATS_FILE_SUFFIX;
        }

        MergeTreeIndices skip_indices_to_build;
        NameSet skip_indices_to_build_names;

        for (const auto & idx : ctx->indices_to_build)
        {
            skip_indices_to_build.push_back(idx);
            skip_indices_to_build_names.insert(idx->index.name);
        }

        const auto & all_indices = ctx->metadata_snapshot->getSecondaryIndices();

        for (const auto & idx : all_indices)
        {
            if (ctx->indices_to_drop.contains(idx.name) || skip_indices_to_build_names.contains(idx.name))
                continue;

            auto prefix = fmt::format("{}{}.", INDEX_FILE_PREFIX, idx.name);
            auto it = ctx->source_part->checksums.files.upper_bound(prefix);
            while (it != ctx->source_part->checksums.files.end())
            {
                if (!startsWith(it->first, prefix))
                    break;

                entries_to_hardlink.insert(it->first);
                ctx->existing_indices_stats_checksums.addFile(it->first, it->second.file_size, it->second.file_hash);
                ++it;
            }
        }

        ColumnsStatistics stats_to_rewrite;
        const auto & columns = ctx->metadata_snapshot->getColumns();
        for (const auto & column : columns)
        {
            if (column.statistics.empty() || ctx->statistics_to_drop.contains(column.name))
                continue;

            /// We do not hard-link statistics which
            /// 1. In `DROP STATISTICS` statement. It is filtered by `removed_stats`
            /// 2. Not in column list anymore, including `DROP COLUMN`. It is not touched by this loop.
            String stat_file_name = STATS_FILE_PREFIX + column.name + STATS_FILE_SUFFIX;
            auto it = ctx->source_part->checksums.files.find(stat_file_name);
            if (it != ctx->source_part->checksums.files.end())
            {
                entries_to_hardlink.insert(it->first);
                ctx->existing_indices_stats_checksums.addFile(it->first, it->second.file_size, it->second.file_hash);
            }
        }

        NameSet hardlinked_files;
        /// Create hardlinks for unchanged files
        for (auto it = ctx->source_part->getDataPartStorage().iterate(); it->isValid(); it->next())
        {
            if (!entries_to_hardlink.contains(it->name()))
            {
                if (renamed_stats.contains(it->name()))
                {
                    ctx->new_data_part->getDataPartStorage().createHardLinkFrom(
                        ctx->source_part->getDataPartStorage(), it->name(), renamed_stats.at(it->name()));

                    hardlinked_files.insert(it->name());
                    /// Also we need to "rename" checksums to finalize correctly.
                    const auto & check_sum = ctx->source_part->checksums.files.at(it->name());
                    ctx->existing_indices_stats_checksums.addFile(renamed_stats.at(it->name()), check_sum.file_size, check_sum.file_hash);
                }
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
            auto indices_expression_dag = ctx->data->getPrimaryKeyAndSkipIndicesExpression(ctx->metadata_snapshot, skip_indices_to_build)->getActionsDAG().clone();
            auto extracting_subcolumns_dag = createSubcolumnsExtractionActions(builder->getHeader(), indices_expression_dag.getRequiredColumnsNames(), ctx->context);
            if (!extracting_subcolumns_dag.getNodes().empty())
                indices_expression_dag = ActionsDAG::merge(std::move(extracting_subcolumns_dag), std::move(indices_expression_dag));

            builder->addTransform(std::make_shared<ExpressionTransform>(
                builder->getHeader(), std::make_shared<ExpressionActions>(std::move(indices_expression_dag))));

            builder->addTransform(std::make_shared<MaterializingTransform>(builder->getHeader()));
        }

        MutationHelpers::addTransformToBuildTTLs(builder, *ctx);
        ctx->minmax_idx = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();

        MergeTreeIndexGranularityPtr index_granularity_ptr;

        /// Reuse source part granularity if mutation does not change number of rows
        if (!ctx->mutated_data.affects_all_columns)
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

        ctx->out = std::make_shared<MergedBlockOutputStream>(
            ctx->new_data_part,
            ctx->metadata_snapshot,
            ctx->new_data_part->getColumns(),
            skip_indices_to_build,
            stats_to_rewrite,
            ctx->compression_codec,
            std::move(index_granularity_ptr),
            ctx->txn ? ctx->txn->tid : Tx::PrehistoricTID,
            ctx->source_part->getBytesUncompressedOnDisk(),
            /*reset_columns=*/ true,
            /*blocks_are_granules_size=*/ false,
            ctx->context->getWriteSettings());

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

        static_pointer_cast<MergedBlockOutputStream>(ctx->out)->finalizePart(
            ctx->new_data_part, ctx->need_sync, nullptr, &ctx->existing_indices_stats_checksums);
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
        if (!ctx->ttls_to_materialize.empty() || !ctx->ttls_to_calculate.empty())
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
        const auto & files_to_rename_map = ctx->files_to_rename.getFilesToRename();

        /// NOTE: Renames must be done in order
        for (const auto & [rename_from, rename_to] : files_to_rename_map)
        {
            if (rename_to.empty()) /// It's DROP COLUMN
            {
                /// pass
            }
            else
            {
                ctx->new_data_part->getDataPartStorage().createHardLinkFrom(ctx->source_part->getDataPartStorage(), rename_from, rename_to);
                hardlinked_files.insert(rename_from);
            }
        }

        /// Create hardlinks for unchanged files
        for (auto it = ctx->source_part->getDataPartStorage().iterate(); it->isValid(); it->next())
        {
            if (ctx->files_to_skip.contains(it->name()))
                continue;

            String file_name = it->name();

            auto rename_it = std::find_if(files_to_rename_map.begin(), files_to_rename_map.end(), [&file_name](const auto & rename_pair)
            {
                return rename_pair.first == file_name;
            });

            if (rename_it != files_to_rename_map.end())
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

        ctx->compression_codec = ctx->source_part->default_codec;

        if (ctx->mutating_pipeline_builder.initialized())
        {
            auto builder = std::make_unique<QueryPipelineBuilder>(std::move(ctx->mutating_pipeline_builder));
            MutationHelpers::addTransformToBuildTTLs(builder, *ctx);

            ctx->out = std::make_shared<MergedColumnOnlyOutputStream>(
                ctx->new_data_part,
                ctx->metadata_snapshot,
                ctx->updated_header.getNamesAndTypesList(),
                std::vector<MergeTreeIndexPtr>(ctx->indices_to_build.begin(), ctx->indices_to_build.end()),
                ColumnsStatistics(ctx->statistics_to_build.begin(), ctx->statistics_to_build.end()),
                ctx->compression_codec,
                ctx->source_part->index_granularity,
                ctx->source_part->getBytesUncompressedOnDisk());

            ctx->mutating_pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
            ctx->mutating_pipeline.setProgressCallback(ctx->progress_callback);
            /// Is calculated inside MergeProgressCallback.
            ctx->mutating_pipeline.disableProfileEventUpdate();
            ctx->mutating_executor = std::make_unique<PullingPipelineExecutor>(ctx->mutating_pipeline);

            part_merger_writer_task = std::make_unique<PartMergerWriter>(ctx);
        }
    }

    void finalize()
    {
        if (ctx->mutating_executor)
        {
            ctx->mutating_executor.reset();
            ctx->mutating_pipeline.reset();

            auto merged_out = static_pointer_cast<MergedColumnOnlyOutputStream>(ctx->out);
            auto changed_checksums = merged_out->fillChecksums(ctx->new_data_part, ctx->new_data_part->checksums);
            ctx->new_data_part->checksums.add(std::move(changed_checksums));
            merged_out->finish(ctx->need_sync);
            ctx->out.reset();
        }

        const auto & files_to_rename_map = ctx->files_to_rename.getFilesToRename();

        for (const auto & [rename_from, rename_to] : files_to_rename_map)
        {
            if (rename_to.empty())
            {
                ctx->new_data_part->checksums.files.erase(rename_from);
            }
            else if (ctx->new_data_part->checksums.files.contains(rename_from))
            {
                ctx->new_data_part->checksums.files[rename_to] = ctx->new_data_part->checksums.files[rename_from];
                ctx->new_data_part->checksums.files.erase(rename_from);
            }
        }

        bool modified_ttl = !ctx->ttls_to_materialize.empty() || !ctx->ttls_to_calculate.empty();
        MutationHelpers::finalizeMutatedPart(ctx->source_part, ctx->new_data_part, modified_ttl, ctx->compression_codec, ctx->context, ctx->metadata_snapshot, ctx->need_sync);
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
    ctx->space_reservation = space_reservation_;
    ctx->storage_columns = metadata_snapshot_->getColumns().getAllPhysical();
    ctx->txn = txn;
    ctx->source_part = ctx->future_part->parts[0];
    ctx->need_prefix = need_prefix_;
    ctx->storage_snapshot = ctx->data->getStorageSnapshotWithoutData(ctx->metadata_snapshot, context_);
    extendObjectColumns(ctx->storage_columns, ctx->storage_snapshot->object_columns, /*with_subcolumns=*/ false);
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

            setDataPartPromise();
            return false;
        }
    }

    return false;
}

void MutateTask::setDataPartPromise()
{
    const auto ttl = ctx->new_data_part->ttl_infos.table_ttl;
    /// If part is expired according to TTL, create a new empty part.
    if (ttl.max && ttl.max <= ctx->time_of_mutation)
    {
        MergeTreePartInfo part_info = ctx->new_data_part->info;
        part_info.level += 1;

        MergeTreePartition partition = ctx->new_data_part->partition;
        std::string part_name = ctx->new_data_part->getNewName(part_info);

        auto [mutable_empty_part, _] = ctx->data->createEmptyPart(part_info, partition, part_name, ctx->txn);
        ctx->new_data_part = std::move(mutable_empty_part);
    }

    // The `new_data_part` is a shared pointer and must be moved to allow
    // part deletion in case it is needed in `MutateFromLogEntryTask::finalize`.
    //
    // `tryRemovePartImmediately` requires `std::shared_ptr::unique() == true`
    // to delete the part timely. When there are multiple shared pointers,
    // only the part state is changed to `Deleting`.
    //
    // Fetching a byte-identical part (in case of checksum mismatches) will fail with
    // `Part ... should be deleted after previous attempt before fetch`.
    promise.set_value(std::move(ctx->new_data_part));
}

void MutateTask::cancel() noexcept
{
    if (task)
        task->cancel();
}

void MutateTask::updateProfileEvents() const
{
    UInt64 total_elapsed_ms = (*ctx->mutate_entry)->watch.elapsedMilliseconds();
    UInt64 execute_elapsed_ms = ctx->execute_elapsed_ns / 1000000UL;

    ProfileEvents::increment(ProfileEvents::MutationTotalMilliseconds, total_elapsed_ms);
    ProfileEvents::increment(ProfileEvents::MutationExecuteMilliseconds, execute_elapsed_ms);
}

static bool canSkipConversionToNullable(const MergeTreeDataPartPtr & part, const MutationCommand & command)
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
    if (serialization->getKind() != ISerialization::Kind::DEFAULT)
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

static bool canSkipMutationCommandForPart(const MergeTreeDataPartPtr & part, const MutationCommand & command, const ContextPtr & context)
{
    if (command.partition)
    {
        auto command_partition_id = part->storage.getPartitionIDFromQuery(command.partition, context);
        if (part->info.getPartitionId() != command_partition_id)
            return true;
    }

    if (command.type == MutationCommand::APPLY_DELETED_MASK && !part->hasLightweightDelete())
        return true;

    if (canSkipConversionToNullable(part, command))
        return true;

    if (canSkipConversionToVariant(part, command))
        return true;

    return false;
}

bool MutateTask::prepare()
{
    ProfileEvents::increment(ProfileEvents::MutationTotalParts);
    ctx->checkOperationIsNotCanceled();

    if (ctx->future_part->parts.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to mutate {} parts, not one. "
            "This is a bug.", ctx->future_part->parts.size());

    ctx->num_mutations = std::make_unique<CurrentMetrics::Increment>(CurrentMetrics::PartMutation);

    MergeTreeData::IMutationsSnapshot::Params params
    {
        .metadata_version = ctx->metadata_snapshot->getMetadataVersion(),
        .min_part_metadata_version = ctx->source_part->getMetadataVersion(),
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
        if (!canSkipMutationCommandForPart(ctx->source_part, command, context_for_reading))
            ctx->commands_for_part.emplace_back(command);
    }

    auto is_storage_touched = isStorageTouchedByMutations(ctx->source_part, mutations_snapshot, ctx->metadata_snapshot, ctx->commands_for_part, context_for_reading);

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
                ctx->source_part, prefix, ctx->future_part->part_info, ctx->metadata_snapshot, clone_params, ctx->context->getReadSettings(), ctx->context->getWriteSettings(), /*must_on_same_disk=*/ true);

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

    MutationHelpers::analyzeCommands(*ctx, alter_conversions);
    ctx->stage_progress = std::make_unique<MergeStageProgress>(1.0);

    if (!ctx->for_interpreter.empty())
    {
        /// Always disable filtering in mutations: we want to read and write all rows because for updates we rewrite only some of the
        /// columns and preserve the columns that are not affected, but after the update all columns must have the same number of row
        MutationsInterpreter::Settings settings(true);
        settings.apply_deleted_mask = false;

        ctx->interpreter = std::make_unique<MutationsInterpreter>(
            *ctx->data,
            ctx->source_part,
            alter_conversions,
            ctx->metadata_snapshot,
            ctx->for_interpreter,
            ctx->metadata_snapshot->getColumns().getNamesOfPhysical(),
            context_for_reading,
            settings);

        ctx->mutating_pipeline_builder = ctx->interpreter->execute();
        ctx->updated_header = ctx->interpreter->getUpdatedHeader();

        if (ctx->execution_mode == MutationContext::Mode::AllColumns)
        {
            ctx->new_part_columns = {};
            ctx->new_serialization_infos = {};

            for (const auto & column : ctx->updated_header)
                MutationHelpers::addColumn(*ctx, column.name, column.type);
        }

        ctx->progress_callback = MergeProgressCallback(
            (*ctx->mutate_entry)->ptr(),
            ctx->watch_prev_elapsed,
            *ctx->stage_progress,
            [&my_ctx = *ctx]() { my_ctx.checkOperationIsNotCanceled(); });
    }

    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + ctx->future_part->name, ctx->space_reservation->getDisk(), 0);

    std::string prefix = ctx->need_prefix ? "tmp_mut_" : "";
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

    ctx->new_data_part->setColumns(
        ctx->new_part_columns.getAllPhysical(),
        ctx->new_serialization_infos,
        ctx->metadata_snapshot->getMetadataVersion());

    ctx->new_data_part->partition.assign(ctx->source_part->partition);

    /// Don't change granularity type while mutating subset of columns
    ctx->mrk_extension = ctx->source_part->index_granularity_info.mark_type.getFileExtension();

    const auto data_settings = ctx->data->getSettings();
    ctx->need_sync = data_settings->needSyncPart(ctx->source_part->rows_count, ctx->source_part->getBytesOnDisk());

    if ((*ctx->data->getSettings())[MergeTreeSetting::exclude_deleted_rows_for_part_size_in_merge])
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

    if (ctx->execution_mode == MutationContext::Mode::AllColumns)
    {
        /// In case of replicated merge tree with zero copy replication
        /// Here Clickhouse claims that this new part can be deleted in temporary state without unlocking the blobs
        /// The blobs have to be removed along with the part, this temporary part owns them and does not share them yet.
        ctx->new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::REMOVE_BLOBS;
        task = std::make_unique<MutateAllPartColumnsTask>(ctx);

        ProfileEvents::increment(ProfileEvents::MutationAllPartColumns);
    }
    else /// TODO: check that we modify only non-key columns in this case.
    {
        MutationHelpers::addTransformToBuildIndices(ctx->mutating_pipeline_builder, ctx->indices_to_build, ctx->context);
        Names projections_to_skip;

        for (const auto & projection : ctx->projections_to_drop)
            projections_to_skip.push_back(projection + ".proj");

        for (const auto & projection : ctx->projections_to_build)
            projections_to_skip.push_back(projection->getDirectoryName());

        ctx->files_to_skip = MutationHelpers::collectFilesToSkip(
            ctx->source_part,
            ctx->new_data_part,
            ctx->updated_header,
            ctx->indices_to_build,
            ctx->mrk_extension,
            projections_to_skip,
            ctx->statistics_to_build);

        MutationHelpers::collectFilesForRenames(*ctx);

        /// In case of replicated merge tree with zero copy replication
        /// Here Clickhouse has to follow the common procedure when deleting new part in temporary state
        /// Some of the files within the blobs are shared with source part, some belongs only to the part
        /// Keeper has to be asked with unlock request to release the references to the blobs
        ctx->new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::ASK_KEEPER;
        task = std::make_unique<MutateSomePartColumnsTask>(ctx);

        ProfileEvents::increment(ProfileEvents::MutationSomePartColumns);
    }

    return true;
}

const HardlinkedFiles & MutateTask::getHardlinkedFiles() const
{
    return ctx->hardlinked_files;
}

}
