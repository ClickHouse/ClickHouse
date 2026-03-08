#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/MutationsNonDeterministicHelpers.h>
#include <Interpreters/replaceSubcolumnsToGetSubcolumnFunctionInQuery.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Transforms/CheckSortedTransform.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <IO/WriteHelpers.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/PreparedSets.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Processors/Sources/ThrowingExceptionSource.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/createUniqueAliasesIfNecessary.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/CollectSets.h>
#include <Planner/Planner.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageDummy.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageMerge.h>

namespace ProfileEvents
{
    extern const Event MutationAffectedRowsUpperBound;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_nondeterministic_mutations;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsBool validate_mutation_query;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsAlterColumnSecondaryIndexMode alter_column_secondary_index_mode;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
    extern const MergeTreeSettingsBool materialize_ttl_recalculate_only;
    extern const MergeTreeSettingsBool ttl_only_drop_parts;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_MUTATION_COMMAND;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int CANNOT_UPDATE_COLUMN;
    extern const int UNEXPECTED_EXPRESSION;
    extern const int ILLEGAL_STATISTICS;
}

ASTPtr prepareQueryAffectedAST(const std::vector<MutationCommand> & commands, const StoragePtr & storage, ContextPtr context)
{
    /// Execute `SELECT count() FROM storage WHERE predicate1 OR predicate2 OR ...` query.
    /// The result can differ from the number of affected rows (e.g. if there is an UPDATE command that
    /// changes how many rows satisfy the predicates of the subsequent commands).
    /// But we can be sure that if count = 0, then no rows will be touched.

    auto select = make_intrusive<ASTSelectQuery>();

    select->setExpression(ASTSelectQuery::Expression::SELECT, make_intrusive<ASTExpressionList>());
    auto count_func = makeASTFunction("count");
    select->select()->children.push_back(count_func);

    ASTs conditions;
    for (const MutationCommand & command : commands)
    {
        if (ASTPtr condition = getPartitionAndPredicateExpressionForMutationCommand(command, storage, context))
            conditions.push_back(std::move(condition));
    }

    if (conditions.size() > 1)
    {
        auto coalesced_predicates = makeASTOperator("or");
        coalesced_predicates->arguments->children = std::move(conditions);
        select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(coalesced_predicates));
    }
    else if (conditions.size() == 1)
    {
        select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(conditions.front()));
    }

    return select;
}

namespace
{

QueryTreeNodePtr prepareQueryAffectedQueryTree(const std::vector<MutationCommand> & commands, const StoragePtr & storage, ContextPtr context)
{
    auto ast = prepareQueryAffectedAST(commands, storage, context);
    auto query_tree = buildQueryTree(ast, context);

    auto & query_node = query_tree->as<QueryNode &>();
    query_node.getJoinTree() = std::make_shared<TableNode>(storage, context);

    QueryTreePassManager query_tree_pass_manager(context);
    addQueryTreePasses(query_tree_pass_manager);
    query_tree_pass_manager.run(query_tree);

    return query_tree;
}

ColumnDependencies getAllColumnDependencies(
    const StorageMetadataPtr & metadata_snapshot,
    const NameSet & updated_columns,
    const StorageInMemoryMetadata::HasDependencyCallback & has_dependency)
{
    NameSet new_updated_columns = updated_columns;
    ColumnDependencies dependencies;

    while (!new_updated_columns.empty())
    {
        auto new_dependencies = metadata_snapshot->getColumnDependencies(new_updated_columns, true, has_dependency);
        new_updated_columns.clear();
        for (const auto & dependency : new_dependencies)
        {
            if (!dependencies.contains(dependency))
            {
                dependencies.insert(dependency);
                if (!dependency.isReadOnly())
                    new_updated_columns.insert(dependency.column_name);
            }
        }
    }

    return dependencies;
}

}


IsStorageTouched isStorageTouchedByMutations(
    MergeTreeData::DataPartPtr source_part,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot,
    const std::vector<MutationCommand> & commands,
    ContextPtr context,
    std::function<void(const Progress & value)> check_operation_is_not_cancelled)
{
    static constexpr IsStorageTouched no_rows = {.any_rows_affected = false, .all_rows_affected = false};
    static constexpr IsStorageTouched all_rows = {.any_rows_affected = true, .all_rows_affected = true};
    static constexpr IsStorageTouched some_rows = {.any_rows_affected = true, .all_rows_affected = false};

    if (commands.empty())
        return no_rows;

    auto storage_from_part = std::make_shared<StorageFromMergeTreeDataPart>(source_part, mutations_snapshot);
    bool all_commands_can_be_skipped = true;

    for (const auto & command : commands)
    {
        if (command.type == MutationCommand::APPLY_DELETED_MASK)
        {
            if (storage_from_part->hasLightweightDeletedMask())
            {
                /// The precise number of rows is unknown.
                ProfileEvents::increment(ProfileEvents::MutationAffectedRowsUpperBound, source_part->rows_count);
                return some_rows;
            }
        }
        else
        {
            if (!command.predicate) /// The command touches all rows.
            {
                ProfileEvents::increment(ProfileEvents::MutationAffectedRowsUpperBound, source_part->rows_count);
                return all_rows;
            }

            if (command.partition)
            {
                const String partition_id = storage_from_part->getPartitionIDFromQuery(command.partition, context);
                if (partition_id == source_part->info.getPartitionId())
                    all_commands_can_be_skipped = false;
            }
            else
            {
                all_commands_can_be_skipped = false;
            }
        }
    }

    if (all_commands_can_be_skipped)
        return no_rows;

    /// Always use the new analyzer path to match the rest of the mutation pipeline.
    auto select_query_tree = prepareQueryAffectedQueryTree(commands, storage_from_part, context);
    InterpreterSelectQueryAnalyzer interpreter(select_query_tree, context, SelectQueryOptions().ignoreLimits());
    BlockIO io = interpreter.execute();

    PullingAsyncPipelineExecutor executor(io.pipeline);
    io.pipeline.setConcurrencyControl(context->getSettingsRef()[Setting::use_concurrency_control]);
    /// It's actually not a progress callback, but a cancellation check.
    io.pipeline.setProgressCallback(check_operation_is_not_cancelled);

    Block block;
    while (block.rows() == 0 && executor.pull(block));

    if (!block.rows())
        return no_rows;
    if (block.rows() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "count() expression returned {} rows, not 1", block.rows());

    Block tmp_block;
    while (executor.pull(tmp_block));

    auto count = (*block.getByName("count()").column)[0].safeGet<UInt64>();
    ProfileEvents::increment(ProfileEvents::MutationAffectedRowsUpperBound, count);

    IsStorageTouched result;
    result.any_rows_affected = (count != 0);
    result.all_rows_affected = (count == source_part->rows_count);
    return result;
}

ASTPtr getPartitionAndPredicateExpressionForMutationCommand(
    const MutationCommand & command,
    const StoragePtr & storage,
    ContextPtr context
)
{
    ASTPtr partition_predicate_as_ast_func;
    if (command.partition)
    {
        String partition_id;

        auto storage_merge_tree = std::dynamic_pointer_cast<MergeTreeData>(storage);
        auto storage_from_merge_tree_data_part = std::dynamic_pointer_cast<StorageFromMergeTreeDataPart>(storage);
        if (storage_merge_tree)
            partition_id = storage_merge_tree->getPartitionIDFromQuery(command.partition, context);
        else if (storage_from_merge_tree_data_part)
            partition_id = storage_from_merge_tree_data_part->getPartitionIDFromQuery(command.partition, context);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ALTER UPDATE/DELETE ... IN PARTITION is not supported for non-MergeTree tables");

        partition_predicate_as_ast_func = makeASTOperator("equals",
                    make_intrusive<ASTIdentifier>("_partition_id"),
                    make_intrusive<ASTLiteral>(partition_id)
        );
    }

    if (command.predicate && command.partition)
        return makeASTOperator("and", command.predicate->clone(), std::move(partition_predicate_as_ast_func));
    return command.predicate ? command.predicate->clone() : partition_predicate_as_ast_func;
}

MutationsInterpreter::Source::Source(StoragePtr storage_) : storage(std::move(storage_))
{
}

MutationsInterpreter::Source::Source(
    MergeTreeData & storage_,
    MergeTreeData::DataPartPtr source_part_,
    AlterConversionsPtr alter_conversions_)
    : data(&storage_)
    , part(std::move(source_part_))
    , alter_conversions(std::move(alter_conversions_))
{
}

StorageSnapshotPtr MutationsInterpreter::Source::getStorageSnapshot(const StorageMetadataPtr & snapshot_, const ContextPtr & context_, bool with_data) const
{
    if (const auto * merge_tree = getMergeTreeData())
    {
        return with_data
            ? merge_tree->getStorageSnapshot(snapshot_, context_)
            : merge_tree->getStorageSnapshotWithoutData(snapshot_, context_);
    }

    chassert(storage);
    return with_data
        ? storage->getStorageSnapshot(snapshot_, context_)
        : storage->getStorageSnapshotWithoutData(snapshot_, context_);
}

StoragePtr MutationsInterpreter::Source::getStorage() const
{
    if (data)
        return data->shared_from_this();

    return storage;
}

const MergeTreeData * MutationsInterpreter::Source::getMergeTreeData() const
{
    if (data)
        return data;

    return dynamic_cast<const MergeTreeData *>(storage.get());
}

MergeTreeData::DataPartPtr MutationsInterpreter::Source::getMergeTreeDataPart() const
{
    return part;
}

bool MutationsInterpreter::Source::isMutatingDataPart() const
{
    return part != nullptr;
}

bool MutationsInterpreter::Source::supportsLightweightDelete() const
{
    if (part)
        return part->supportLightweightDeleteMutate();

    return storage->supportsLightweightDelete();
}

bool MutationsInterpreter::Source::materializeTTLRecalculateOnly() const
{
    return data && (*data->getSettings())[MergeTreeSetting::materialize_ttl_recalculate_only];
}

bool MutationsInterpreter::Source::hasSecondaryIndex(const String & name, StorageMetadataPtr metadata) const
{
    return part && part->hasSecondaryIndex(name, metadata);
}

bool MutationsInterpreter::Source::hasProjection(const String & name) const
{
    return part && part->hasProjection(name);
}

bool MutationsInterpreter::Source::hasBrokenProjection(const String & name) const
{
    return part && part->hasBrokenProjection(name);
}

bool MutationsInterpreter::Source::isCompactPart() const
{
    return part && part->getType() == MergeTreeDataPartType::Compact;
}

static Names getAvailableColumnsWithVirtuals(StorageMetadataPtr metadata_snapshot, const IStorage & storage)
{
    auto all_columns = metadata_snapshot->getColumns().getNamesOfPhysical();
    auto virtuals = storage.getVirtualsPtr();
    for (const auto & column : *virtuals)
        all_columns.push_back(column.name);
    return all_columns;
}

MutationsInterpreter::MutationsInterpreter(
    StoragePtr storage_,
    StorageMetadataPtr metadata_snapshot_,
    MutationCommands commands_,
    ContextPtr context_,
    Settings settings_)
    : MutationsInterpreter(
        Source(storage_),
        metadata_snapshot_, std::move(commands_),
        getAvailableColumnsWithVirtuals(metadata_snapshot_, *storage_),
        std::move(context_), std::move(settings_))
{
    if (settings.can_execute && !settings.return_mutated_rows && dynamic_cast<const MergeTreeData *>(source.getStorage().get()))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot execute mutation for {}. Mutation should be applied to every part separately.",
            source.getStorage()->getName());
    }

    prepare(!settings.can_execute);
}

MutationsInterpreter::MutationsInterpreter(
    MergeTreeData & storage_,
    MergeTreeData::DataPartPtr source_part_,
    AlterConversionsPtr alter_conversions_,
    StorageMetadataPtr metadata_snapshot_,
    MutationCommands commands_,
    Names available_columns_,
    ContextPtr context_,
    Settings settings_)
    : MutationsInterpreter(
        Source(storage_, source_part_, std::move(alter_conversions_)),
        std::move(metadata_snapshot_), std::move(commands_),
        std::move(available_columns_), std::move(context_), std::move(settings_))
{
    if (settings.max_threads != 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Mutation interpreter for single part must use 1 thread, got: {}", settings.max_threads);
    }

    const auto & part_columns = source_part_->getColumnsDescription();
    auto persistent_virtuals = storage_.getVirtualsPtr()->getNamesAndTypesList(VirtualsKind::Persistent);
    NameSet available_columns_set(available_columns.begin(), available_columns.end());

    for (const auto & column : persistent_virtuals)
    {
        if (part_columns.has(column.name) && !available_columns_set.contains(column.name))
            available_columns.push_back(column.name);
    }

    prepare(!settings.can_execute);
}

MutationsInterpreter::MutationsInterpreter(
    Source source_,
    StorageMetadataPtr metadata_snapshot_,
    MutationCommands commands_,
    Names available_columns_,
    ContextPtr context_,
    Settings settings_)
    : source(std::move(source_))
    , metadata_snapshot(metadata_snapshot_)
    , commands(std::move(commands_))
    , available_columns(std::move(available_columns_))
    , settings(std::move(settings_))
    , select_limits(SelectQueryOptions().analyze(!settings.can_execute).ignoreLimits())
    , logger(getLogger("MutationsInterpreter(" + source.getStorage()->getStorageID().getFullTableName() + ")"))
{
    auto mutable_context = Context::createCopy(context_);
    /// Allow mutations to work when force_index_by_date or force_primary_key is on.
    /// For regular mutations this is done in MutateTask::prepareMutationPartForExecution,
    /// but lightweight updates (updateLightweightImpl) bypass MutateTask.
    mutable_context->setSetting("force_primary_key", false);
    mutable_context->setSetting("force_index_by_date", false);
    context = std::move(mutable_context);
}

static NameSet getKeyColumns(const MutationsInterpreter::Source & source, const StorageMetadataPtr & metadata_snapshot)
{
    const MergeTreeData * merge_tree_data = source.getMergeTreeData();
    if (!merge_tree_data)
        return {};

    NameSet key_columns;

    for (const String & col : metadata_snapshot->getColumnsRequiredForPartitionKey())
        key_columns.insert(col);

    for (const String & col : metadata_snapshot->getColumnsRequiredForSortingKey())
        key_columns.insert(col);
    /// We don't process sample_by_ast separately because it must be among the primary key columns.

    if (!merge_tree_data->merging_params.sign_column.empty())
        key_columns.insert(merge_tree_data->merging_params.sign_column);

    if (!merge_tree_data->merging_params.version_column.empty())
        key_columns.insert(merge_tree_data->merging_params.version_column);

    return key_columns;
}

static void validateUpdateColumns(
    const MutationsInterpreter::Source & source,
    const StorageMetadataPtr & metadata_snapshot,
    const NameSet & updated_columns,
    const std::unordered_map<String, Names> & column_to_affected_materialized,
    const ContextPtr & context)
{
    auto storage_snapshot = source.getStorageSnapshot(metadata_snapshot, context, false);
    NameSet key_columns = getKeyColumns(source, metadata_snapshot);

    const auto & storage_columns = storage_snapshot->metadata->getColumns();
    const auto & virtual_columns = *storage_snapshot->virtual_columns;
    const auto & common_virtual_columns = IStorage::getCommonVirtuals();

    for (const auto & column_name : updated_columns)
    {
        if (key_columns.contains(column_name))
            throw Exception(ErrorCodes::CANNOT_UPDATE_COLUMN, "Cannot UPDATE key column {}", backQuote(column_name));

        if (storage_columns.tryGetColumn(GetColumnsOptions::Materialized, column_name))
            throw Exception(ErrorCodes::CANNOT_UPDATE_COLUMN, "Cannot UPDATE materialized column {}", backQuote(column_name));

        auto materialized_it = column_to_affected_materialized.find(column_name);
        if (materialized_it != column_to_affected_materialized.end())
        {
            for (const auto & materialized : materialized_it->second)
            {
                if (key_columns.contains(materialized))
                {
                    throw Exception(ErrorCodes::CANNOT_UPDATE_COLUMN,
                                    "Updated column {} affects MATERIALIZED column {}, which is a key column. "
                                    "Cannot UPDATE it", backQuote(column_name), backQuote(materialized));
                }
            }
        }

        auto ordinary_storage_column = storage_columns.tryGetColumn(GetColumnsOptions::Ordinary, column_name);
        if (!ordinary_storage_column)
        {
            /// Allow to override value of lightweight delete filter virtual column
            if (column_name == RowExistsColumn::name)
            {
                if (!source.supportsLightweightDelete())
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Lightweight delete is not supported for table");
            }
            else if (virtual_columns.tryGet(column_name) || common_virtual_columns.tryGet(column_name))
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Update is not supported for virtual column {} ", backQuote(column_name));
            }
            else
            {
                throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "There is no column {} in table", backQuote(column_name));
            }
        }
        else
        {
            /// Check if we have a subcolumn of this column as a key column.
            for (const auto & key_column : key_columns)
            {
                auto column = storage_columns.getColumnOrSubcolumn(GetColumnsOptions::All, key_column);
                if (column.isSubcolumn() && column_name == column.getNameInStorage())
                    throw Exception(ErrorCodes::CANNOT_UPDATE_COLUMN, "Cannot UPDATE column {} because its subcolumn {} is a key column", backQuote(column_name), backQuote(key_column));
            }
        }
    }
}

/// Returns ASTs of updated nested subcolumns, if all of subcolumns were updated.
/// They are used to validate sizes of nested arrays.
/// If some of subcolumns were updated and some weren't,
/// it makes sense to validate only updated columns with their old versions,
/// because their sizes couldn't change, since sizes of all nested subcolumns must be consistent.
static std::optional<std::vector<ASTPtr>> getExpressionsOfUpdatedNestedSubcolumns(
    const String & column_name,
    NameSet affected_materialized,
    const NamesAndTypesList & all_columns,
    const std::unordered_map<String, ASTPtr> & column_to_update_expression)
{
    std::vector<ASTPtr> res;
    auto source_name = Nested::splitName(column_name).first;

    /// Check this nested subcolumn
    for (const auto & column : all_columns)
    {
        auto split = Nested::splitName(column.name);
        if (isArray(column.type) && split.first == source_name && !split.second.empty())
        {
            // Materialized nested columns shall never be part of the update expression
            if (affected_materialized.contains(column.name))
                continue;

            auto it = column_to_update_expression.find(column.name);
            if (it == column_to_update_expression.end())
                return {};

            res.push_back(it->second);
        }
    }

    return res;
}

static bool extractRequiredNonTableColumnsFromStorage(
    const Names & columns_names,
    const StoragePtr & storage,
    const StorageSnapshotPtr & storage_snapshot,
    Names & extracted_column_names)
{
    if (std::dynamic_pointer_cast<StorageMerge>(storage))
        return false;

    if (std::dynamic_pointer_cast<StorageDistributed>(storage))
        return false;

    bool has_table_virtual_column = false;
    for (const auto & column_name : columns_names)
    {
        if (column_name == "_table" && storage->isVirtualColumn(column_name, storage_snapshot->metadata))
            has_table_virtual_column = true;
        else
            extracted_column_names.push_back(column_name);
    }

    return has_table_virtual_column;
}

void MutationsInterpreter::prepare(bool dry_run)
{
    if (is_prepared)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MutationsInterpreter is already prepared. It is a bug.");

    if (commands.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty mutation commands list");

    /// TODO Should we get columns, indices and projections from the part itself? Table metadata may be different
    const ColumnsDescription & columns_desc = metadata_snapshot->getColumns();
    const IndicesDescription & indices_desc = metadata_snapshot->getSecondaryIndices();
    const ProjectionsDescription & projections_desc = metadata_snapshot->getProjections();

    auto storage_snapshot = std::make_shared<StorageSnapshot>(*source.getStorage(), metadata_snapshot);
    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withVirtuals();

    auto all_columns = storage_snapshot->getColumnsByNames(options, available_columns);
    NameSet available_columns_set(available_columns.begin(), available_columns.end());

    NameSet updated_columns;
    bool materialize_ttl_recalculate_only = source.materializeTTLRecalculateOnly();
    bool has_lightweight_delete_materialization = false;
    bool has_rewrite_parts = false;

    for (const auto & command : commands)
    {
        if (command.type == MutationCommand::UPDATE || command.type == MutationCommand::DELETE)
            materialize_ttl_recalculate_only = false;

        if (command.type == MutationCommand::APPLY_DELETED_MASK)
            has_lightweight_delete_materialization = true;

        if (command.type == MutationCommand::REWRITE_PARTS)
            has_rewrite_parts = true;

        for (const auto & [name, _] : command.column_to_update_expression)
        {
            if (name == RowExistsColumn::name)
            {
                if (available_columns_set.emplace(name).second)
                    available_columns.push_back(name);
            }

            updated_columns.insert(name);
        }
    }

    /// We need to know which columns affect which MATERIALIZED columns, data skipping indices
    /// and projections to recalculate them if dependencies are updated.
    std::unordered_map<String, Names> column_to_affected_materialized;
    if (!updated_columns.empty())
    {
        for (const auto & column : columns_desc)
        {
            if (column.default_desc.kind == ColumnDefaultKind::Materialized && available_columns_set.contains(column.name))
            {
                auto query = column.default_desc.expression->clone();
                /// Replace all subcolumns to the getSubcolumn() to get only top level columns as required source columns.
                replaceSubcolumnsToGetSubcolumnFunctionInQuery(query, all_columns);
                auto syntax_result = TreeRewriter(context).analyze(query, all_columns);
                for (const auto & dependency : syntax_result->requiredSourceColumns())
                    if (updated_columns.contains(dependency))
                        column_to_affected_materialized[dependency].push_back(column.name);
            }
        }

        validateUpdateColumns(source, metadata_snapshot, updated_columns, column_to_affected_materialized, context);
    }

    StorageInMemoryMetadata::HasDependencyCallback has_dependency =
        [&](const String & name, ColumnDependency::Kind kind)
    {
        if (kind == ColumnDependency::PROJECTION)
            return source.hasProjection(name);

        if (kind == ColumnDependency::SKIP_INDEX)
            return source.hasSecondaryIndex(name, metadata_snapshot);

        return true;
    };

    if (settings.recalculate_dependencies_of_updated_columns)
        dependencies = getAllColumnDependencies(metadata_snapshot, updated_columns, has_dependency);

    bool need_rebuild_indexes = false;
    bool need_rebuild_indexes_for_update_delete = false;
    bool need_rebuild_projections = false;
    std::vector<String> read_columns;

    if (has_lightweight_delete_materialization || has_rewrite_parts)
    {
        auto & stage = stages.emplace_back(context);
        stage.affects_all_columns = true;

        need_rebuild_indexes = true;
        need_rebuild_projections = true;
    }

    if (settings.return_mutated_rows)
    {
        ASTs all_filters;
        all_filters.reserve(commands.size());

        for (const auto & command : commands)
        {
            using enum MutationCommand::Type;
            if (command.type != UPDATE && command.type != DELETE && command.type != READ_COLUMN)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Cannot apply command {} while returning mutated rows", command.type);
            }

            if (auto filter = getPartitionAndPredicateExpressionForMutationCommand(command))
                all_filters.push_back(std::move(filter));
        }

        ASTPtr filter;
        if (all_filters.size() > 1)
            filter = makeASTForLogicalOr(std::move(all_filters));
        else
            filter = std::move(all_filters.front());

        auto & stage = stages.emplace_back(context);
        stage.filters.push_back(std::move(filter));
    }

    const auto index_mode = source.getMergeTreeData()
        ? (*source.getMergeTreeData()->getSettings())[MergeTreeSetting::alter_column_secondary_index_mode]
        : AlterColumnSecondaryIndexMode::REBUILD;

    /// First, break a sequence of commands into stages.
    for (const auto & command : commands)
    {
        if (command.type == MutationCommand::DELETE)
        {
            mutation_kind.set(MutationKind::MUTATE_OTHER);
            addStageIfNeeded(command.mutation_version, true);
            stages.back().affects_all_columns = true;

            if (!settings.return_mutated_rows)
            {
                auto predicate = getPartitionAndPredicateExpressionForMutationCommand(command);
                predicate = makeASTFunction("isZeroOrNull", predicate);
                stages.back().filters.push_back(predicate);
            }

            /// ALTER DELETE can change the number of rows in the part, so we need to rebuild indexes and projection
            need_rebuild_projections = true;
            need_rebuild_indexes_for_update_delete = true;
        }
        else if (command.type == MutationCommand::UPDATE)
        {
            mutation_kind.set(MutationKind::MUTATE_OTHER);
            addStageIfNeeded(command.mutation_version, false);

            NameSet affected_materialized;

            for (const auto & [column_name, update_expr] : command.column_to_update_expression)
            {
                auto materialized_it = column_to_affected_materialized.find(column_name);
                if (materialized_it != column_to_affected_materialized.end())
                    for (const auto & mat_column : materialized_it->second)
                        affected_materialized.emplace(mat_column);
            }

            for (const auto & [column_name, update_expr] : command.column_to_update_expression)
            {
                /// When doing UPDATE column = expression WHERE condition
                /// we will replace column to the result of the following expression:
                ///
                /// CAST(if(condition, CAST(expression, type), column), type)
                ///
                /// Inner CAST is needed to make 'if' work when branches have no common type,
                /// example: type is UInt64, UPDATE x = -1 or UPDATE x = x - 1.
                ///
                /// Outer CAST is added just in case if we don't trust the returning type of 'if'.

                DataTypePtr type;
                if (auto physical_column = columns_desc.tryGetPhysical(column_name))
                {
                    type = physical_column->type;
                }
                else if (column_name == RowExistsColumn::name)
                {
                    type = RowExistsColumn::type;
                    deleted_mask_updated = true;
                }
                else
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown column {}", column_name);
                }

                auto type_literal = make_intrusive<ASTLiteral>(type->getName());
                ASTPtr condition = getPartitionAndPredicateExpressionForMutationCommand(command);

                /// And new check validateNestedArraySizes for Nested subcolumns
                if (isArray(type) && !Nested::splitName(column_name).second.empty())
                {
                    boost::intrusive_ptr<ASTFunction> function = nullptr;

                    auto nested_update_exprs = getExpressionsOfUpdatedNestedSubcolumns(column_name, affected_materialized, all_columns, command.column_to_update_expression);
                    if (!nested_update_exprs)
                    {
                        function = makeASTFunction("validateNestedArraySizes",
                            condition,
                            update_expr->clone(),
                            make_intrusive<ASTIdentifier>(column_name));
                        condition = makeASTOperator("and", condition, function);
                    }
                    else if (nested_update_exprs->size() > 1)
                    {
                        function = makeASTFunction("validateNestedArraySizes", condition);
                        for (const auto & it : *nested_update_exprs)
                            function->arguments->children.push_back(it->clone());
                        condition = makeASTOperator("and", condition, function);
                    }
                }

                auto updated_column = makeASTFunction("_CAST",
                    makeASTFunction("if",
                        condition,
                        makeASTFunction("_CAST",
                            update_expr->clone(),
                            type_literal),
                        make_intrusive<ASTIdentifier>(column_name)),
                    type_literal);

                stages.back().column_to_updated.emplace(column_name, updated_column);
            }

            if (!affected_materialized.empty())
            {
                stages.emplace_back(context);
                for (const auto & column : columns_desc)
                {
                    if (column.default_desc.kind == ColumnDefaultKind::Materialized)
                    {
                        auto type_literal = make_intrusive<ASTLiteral>(column.type->getName());

                        ASTPtr materialized_column = makeASTFunction("_CAST",
                            column.default_desc.expression->clone(),
                            type_literal);

                        /// We need to replace all subcolumns used in materialized expression to getSubcolumn() function,
                        /// because otherwise subcolumns are extracted before the source column is updated and we get
                        /// old subcolumns values.
                        replaceSubcolumnsToGetSubcolumnFunctionInQuery(materialized_column, all_columns);

                        stages.back().column_to_updated.emplace(
                            column.name,
                            materialized_column);
                    }
                }
            }

            /// If the part is compact and adaptive index granularity is enabled, modify data in one column via ALTER UPDATE can change
            /// the part granularity, so we need to rebuild indexes
            if (source.isCompactPart() && source.getMergeTreeData() && (*source.getMergeTreeData()->getSettings())[MergeTreeSetting::index_granularity_bytes] > 0)
                need_rebuild_indexes = true;
        }
        else if (command.type == MutationCommand::MATERIALIZE_COLUMN)
        {
            mutation_kind.set(MutationKind::MUTATE_OTHER);
            addStageIfNeeded(command.mutation_version, false);

            // Can't materialize a column in the sort key
            Names sort_columns = metadata_snapshot->getSortingKeyColumns();
            if (std::find(sort_columns.begin(), sort_columns.end(), command.column_name) != sort_columns.end())
            {
                throw Exception(ErrorCodes::CANNOT_UPDATE_COLUMN, "Refused to materialize column {} because it's in the sort key. Doing so could break the sort order", backQuote(command.column_name));
            }

            const auto & column = columns_desc.get(command.column_name);

            if (!column.default_desc.expression)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot materialize column `{}` because it doesn't have default expression", column.name);

            auto materialized_column = makeASTFunction(
                "_CAST", column.default_desc.expression->clone(), make_intrusive<ASTLiteral>(column.type->getName()));

            stages.back().column_to_updated.emplace(column.name, materialized_column);
        }
        else if (command.type == MutationCommand::MATERIALIZE_INDEX)
        {
            mutation_kind.set(MutationKind::MUTATE_INDEX_STATISTICS_PROJECTION);
            auto it = std::find_if(
                    std::cbegin(indices_desc), std::end(indices_desc),
                    [&](const IndexDescription & index)
                    {
                        return index.name == command.index_name;
                    });
            if (it == std::cend(indices_desc))
            {
                LOG_WARNING(logger, "Index {} does not exist, skipping materialization", command.index_name);
                continue;
            }

            if (!source.hasSecondaryIndex(it->name, metadata_snapshot))
            {
                auto query = (*it).expression_list_ast->clone();
                auto syntax_result = TreeRewriter(context).analyze(query, all_columns);
                const auto required_columns = syntax_result->requiredSourceColumns();
                for (const auto & column : required_columns)
                    dependencies.emplace(column, ColumnDependency::SKIP_INDEX);
                materialized_indices.emplace(command.index_name);
            }
        }
        else if (command.type == MutationCommand::MATERIALIZE_STATISTICS)
        {
            mutation_kind.set(MutationKind::MUTATE_INDEX_STATISTICS_PROJECTION);
            /// if we execute `ALTER TABLE ... MATERIALIZE STATISTICS ALL`, we materalize all the statistics in this table.
            if (command.statistics_columns.empty())
            {
                for (const auto & column_desc : columns_desc)
                {
                    if (!column_desc.statistics.empty())
                    {
                        dependencies.emplace(column_desc.name, ColumnDependency::STATISTICS);
                        materialized_statistics.emplace(column_desc.name);
                    }
                }
            }
            for (const auto & stat_column_name: command.statistics_columns)
            {
                if (!columns_desc.has(stat_column_name) || columns_desc.get(stat_column_name).statistics.empty())
                    throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Unknown statistics column: {}", stat_column_name);

                dependencies.emplace(stat_column_name, ColumnDependency::STATISTICS);
                materialized_statistics.emplace(stat_column_name);
            }
        }
        else if (command.type == MutationCommand::MATERIALIZE_PROJECTION)
        {
            mutation_kind.set(MutationKind::MUTATE_INDEX_STATISTICS_PROJECTION);
            if (!projections_desc.has(command.projection_name))
            {
                LOG_WARNING(logger, "Projection {} does not exist, skipping materialization", command.projection_name);
                continue;
            }
            const auto & projection = projections_desc.get(command.projection_name);
            if (!source.hasProjection(projection.name) || source.hasBrokenProjection(projection.name))
            {
                for (const auto & column : projection.required_columns)
                    dependencies.emplace(column, ColumnDependency::PROJECTION);
                materialized_projections.emplace(command.projection_name);
            }
        }
        else if (command.type == MutationCommand::DROP_INDEX)
        {
            mutation_kind.set(MutationKind::MUTATE_INDEX_STATISTICS_PROJECTION);
            materialized_indices.erase(command.index_name);
        }
        else if (command.type == MutationCommand::DROP_STATISTICS)
        {
            mutation_kind.set(MutationKind::MUTATE_INDEX_STATISTICS_PROJECTION);

            if (command.clear && command.statistics_columns.empty())
            {
                for (const auto & column_desc : columns_desc)
                {
                    if (!column_desc.statistics.empty())
                        materialized_statistics.erase(column_desc.name);
                }
            }
            else
            {
                for (const auto & stat_column_name: command.statistics_columns)
                    materialized_statistics.erase(stat_column_name);
            }
        }
        else if (command.type == MutationCommand::DROP_PROJECTION)
        {
            mutation_kind.set(MutationKind::MUTATE_INDEX_STATISTICS_PROJECTION);
            materialized_projections.erase(command.projection_name);
        }
        else if (command.type == MutationCommand::MATERIALIZE_TTL)
        {
            mutation_kind.set(MutationKind::MUTATE_OTHER);
            bool suitable_for_ttl_optimization = (*source.getMergeTreeData()->getSettings())[MergeTreeSetting::ttl_only_drop_parts]
                && metadata_snapshot->hasOnlyRowsTTL();

            if (materialize_ttl_recalculate_only || suitable_for_ttl_optimization)
            {
                // just recalculate ttl_infos without remove expired data
                auto all_columns_vec = all_columns.getNames();
                auto all_columns_set = NameSet(all_columns_vec.begin(), all_columns_vec.end());
                auto new_dependencies = metadata_snapshot->getColumnDependencies(all_columns_set, false, has_dependency);

                for (const auto & dependency : new_dependencies)
                {
                    if (dependency.kind == ColumnDependency::TTL_EXPRESSION)
                        dependencies.insert(dependency);
                }
            }
            else if (metadata_snapshot->hasRowsTTL()
                || metadata_snapshot->hasAnyRowsWhereTTL()
                || metadata_snapshot->hasAnyGroupByTTL())
            {
                for (const auto & column : all_columns)
                    dependencies.emplace(column.name, ColumnDependency::TTL_TARGET);
            }
            else
            {
                NameSet new_updated_columns;
                auto column_ttls = metadata_snapshot->getColumns().getColumnTTLs();
                for (const auto & elem : column_ttls)
                {
                    dependencies.emplace(elem.first, ColumnDependency::TTL_TARGET);
                    new_updated_columns.insert(elem.first);
                }

                auto all_columns_vec = all_columns.getNames();
                auto all_columns_set = NameSet(all_columns_vec.begin(), all_columns_vec.end());
                auto all_dependencies = getAllColumnDependencies(metadata_snapshot, all_columns_set, has_dependency);

                for (const auto & dependency : all_dependencies)
                {
                    if (dependency.kind == ColumnDependency::TTL_EXPRESSION)
                        dependencies.insert(dependency);
                }

                /// Recalc only skip indices and projections of columns which could be updated by TTL.
                auto new_dependencies = metadata_snapshot->getColumnDependencies(new_updated_columns, true, has_dependency);
                for (const auto & dependency : new_dependencies)
                {
                    if (dependency.kind == ColumnDependency::SKIP_INDEX
                        || dependency.kind == ColumnDependency::PROJECTION
                        || dependency.kind == ColumnDependency::STATISTICS)
                        dependencies.insert(dependency);
                }
            }

            if (dependencies.empty())
            {
                /// Very rare case. It can happen if we have only one MOVE TTL with constant expression.
                /// But we still have to read at least one column.
                dependencies.emplace(all_columns.front().name, ColumnDependency::TTL_EXPRESSION);
            }
        }
        else if (command.type == MutationCommand::READ_COLUMN)
        {
            mutation_kind.set(MutationKind::MUTATE_OTHER);
            read_columns.emplace_back(command.column_name);
            materialized_statistics.insert(command.column_name);

            if (const auto & merge_tree_data_part = source.getMergeTreeDataPart())
            {
                /// Check if the type of this column is changed and there are projections that have this column in the primary key or indices
                /// that depend on it. We should rebuild such projections and indices
                const auto & column = merge_tree_data_part->tryGetColumn(command.column_name);
                if (column && command.data_type && !column->type->equals(*command.data_type))
                {
                    for (const auto & projection : metadata_snapshot->getProjections())
                    {
                        const auto & pk_columns = projection.metadata->getPrimaryKeyColumns();
                        if (std::ranges::find(pk_columns, command.column_name) != pk_columns.end())
                        {
                            for (const auto & col : projection.required_columns)
                                dependencies.emplace(col, ColumnDependency::PROJECTION);
                            materialized_projections.insert(projection.name);
                        }
                    }

                    for (const auto & index : metadata_snapshot->getSecondaryIndices())
                    {
                        const auto & index_cols = index.expression->getRequiredColumns();
                        if (std::find(index_cols.begin(), index_cols.end(), command.column_name) != index_cols.end())
                        {
                            switch (index_mode)
                            {
                                case AlterColumnSecondaryIndexMode::THROW:
                                case AlterColumnSecondaryIndexMode::COMPATIBILITY:
                                    if (!index.isImplicitlyCreated())
                                    {
                                        /// The only way to reach this would be if the ALTER was created and then the table setting changed
                                        throw Exception(
                                            ErrorCodes::BAD_ARGUMENTS,
                                            "Cannot ALTER column `{}` because index `{}` depends on it", command.column_name, index.name);
                                    }
                                    /// For implicit indices we don't throw, we will rebuild them
                                    [[fallthrough]];
                                case AlterColumnSecondaryIndexMode::REBUILD:
                                {
                                    for (const auto & col : index_cols)
                                        dependencies.emplace(col, ColumnDependency::SKIP_INDEX);
                                    materialized_indices.insert(index.name);
                                    break;
                                }
                                case AlterColumnSecondaryIndexMode::DROP:
                                    dropped_indices.insert(index.name);
                            }
                        }
                    }
                }
            }
        }
        else if (command.type == MutationCommand::DROP_COLUMN && command.clear)
        {
            /// When clearing a column, we need to also clear any indices that depend on it
            for (const auto & index : metadata_snapshot->getSecondaryIndices())
            {
                const auto & index_cols = index.expression->getRequiredColumns();
                if (std::find(index_cols.begin(), index_cols.end(), command.column_name) != index_cols.end())
                    dropped_indices.insert(index.name);
            }
        }
        /// The following mutations handled separately:
        else if (command.type == MutationCommand::APPLY_DELETED_MASK
              || command.type == MutationCommand::APPLY_PATCHES
              || command.type == MutationCommand::REWRITE_PARTS)
        {
            continue;
        }
        else
        {
            throw Exception(
                ErrorCodes::UNKNOWN_MUTATION_COMMAND,
                "Unknown mutation command: {}",
                command.ast ? command.ast->formatForLogging() : fmt::to_string(command.type));
        }
    }

    if (!read_columns.empty())
    {
        stages.emplace_back(context);
        for (auto & column_name : read_columns)
            stages.back().column_to_updated.emplace(column_name, make_intrusive<ASTIdentifier>(column_name));
    }

    /// We care about affected indices and projections because we also need to rewrite them
    /// when one of index columns updated or filtered with delete.
    /// The same about columns, that are needed for calculation of TTL expressions.
    NameSet changed_columns;
    NameSet unchanged_columns;
    if (!dependencies.empty())
    {
        for (const auto & dependency : dependencies)
        {
            if (dependency.isReadOnly())
                unchanged_columns.insert(dependency.column_name);
            else
                changed_columns.insert(dependency.column_name);
        }

        if (!changed_columns.empty())
        {
            stages.emplace_back(context);
            for (const auto & column : changed_columns)
                stages.back().column_to_updated.emplace(column, make_intrusive<ASTIdentifier>(column));
        }

        if (!unchanged_columns.empty())
        {
            if (!stages.empty())
            {
                std::vector<Stage> stages_copy;
                /// Copy all filled stages except index calculation stage.
                /// We need to deep clone ASTs because prepareMutationStages may modify the ASTs in place
                /// (e.g., replacing scalar subqueries with default values during dry_run).
                for (const auto & stage : stages)
                {
                    stages_copy.emplace_back(context);
                    for (const auto & [name, ast] : stage.column_to_updated)
                        stages_copy.back().column_to_updated.emplace(name, ast->clone());
                    stages_copy.back().output_columns = stage.output_columns;
                    stages_copy.back().affects_all_columns = stage.affects_all_columns;
                    for (const auto & filter : stage.filters)
                        stages_copy.back().filters.push_back(filter->clone());
                }

                prepareMutationStages(stages_copy, true);

                QueryPlan plan;
                initQueryPlan(stages_copy.front(), plan);
                auto pipeline = addStreamsForLaterStages(stages_copy, plan);
                updated_header = std::make_unique<Block>(pipeline.getHeader());
            }

            /// Special step to recalculate affected indices, projections and TTL expressions.
            stages.emplace_back(context);
            stages.back().is_readonly = true;
            for (const auto & column : unchanged_columns)
                stages.back().column_to_updated.emplace(
                    column, make_intrusive<ASTIdentifier>(column));
        }
    }

    for (const auto & index : metadata_snapshot->getSecondaryIndices())
    {
        if (!source.hasSecondaryIndex(index.name, metadata_snapshot) || dropped_indices.contains(index.name))
            continue;

        if (need_rebuild_indexes_for_update_delete || need_rebuild_indexes)
        {
            if (index_mode == AlterColumnSecondaryIndexMode::DROP)
                dropped_indices.insert(index.name);
            else
                materialized_indices.insert(index.name);
            continue;
        }

        const auto & index_cols = index.expression->getRequiredColumns();
        bool changed = std::any_of(
            index_cols.begin(),
            index_cols.end(),
            [&](const auto & col) { return updated_columns.contains(col) || changed_columns.contains(col); });

        if (changed)
        {
            if (index_mode == AlterColumnSecondaryIndexMode::DROP)
                dropped_indices.insert(index.name);
            else
                materialized_indices.insert(index.name);
        }
    }

    for (const auto & projection : metadata_snapshot->getProjections())
    {
        if (!source.hasProjection(projection.name))
            continue;

        /// Always rebuild broken projections.
        if (source.hasBrokenProjection(projection.name))
        {
            LOG_DEBUG(logger, "Will rebuild broken projection {}", projection.name);
            materialized_projections.insert(projection.name);
            continue;
        }

        if (need_rebuild_projections)
        {
            materialized_projections.insert(projection.name);
            continue;
        }

        const auto & projection_cols = projection.required_columns;
        bool changed = std::any_of(
            projection_cols.begin(),
            projection_cols.end(),
            [&](const auto & col) { return updated_columns.contains(col) || changed_columns.contains(col); });

        if (changed)
            materialized_projections.insert(projection.name);
    }

    for (const auto & column : metadata_snapshot->getColumns())
    {
        if (column.statistics.empty())
            continue;

        if (updated_columns.contains(column.name) || changed_columns.contains(column.name))
            materialized_statistics.insert(column.name);
    }

    /// Stages might be empty when we materialize skip indices or projections which don't add any
    /// column dependencies.
    if (stages.empty())
        stages.emplace_back(context);

    is_prepared = true;
    prepareMutationStages(stages, dry_run);
}

void MutationsInterpreter::addStageIfNeeded(std::optional<UInt64> mutation_version, bool is_filter_stage)
{
    if (stages.empty() || !stages.back().column_to_updated.empty() || mutation_version != stages.back().mutation_version)
    {
        auto & stage = stages.emplace_back(context);
        stage.mutation_version = mutation_version;
    }

    /// First stage only supports filtering and can't update columns.
    if (stages.size() == 1 && !is_filter_stage)
    {
        auto & stage = stages.emplace_back(context);
        stage.mutation_version = mutation_version;
    }
}

void MutationsInterpreter::prepareMutationStages(std::vector<Stage> & prepared_stages, bool dry_run)
{
    auto storage_snapshot = source.getStorageSnapshot(metadata_snapshot, context, settings.can_execute);
    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withVirtuals();

    auto all_columns = storage_snapshot->getColumnsByNames(options, available_columns);

    bool has_filters = false;
    /// Next, for each stage calculate columns changed by this and previous stages.
    for (size_t i = 0; i < prepared_stages.size(); ++i)
    {
        if (settings.return_all_columns || prepared_stages[i].affects_all_columns)
        {
            for (const auto & column : all_columns)
            {
                if (column.name == RowExistsColumn::name && !deleted_mask_updated)
                    continue;

                prepared_stages[i].output_columns.insert(column.name);
            }

            has_filters = true;
            settings.apply_deleted_mask = true;
        }
        else
        {
            if (i > 0)
                prepared_stages[i].output_columns = prepared_stages[i - 1].output_columns;

            /// Make sure that all updated columns are included into output_columns set.
            /// This is important for a "hidden" column like _row_exists gets because it is a virtual column
            /// and so it is not in the list of AllPhysical columns.
            for (const auto & [column_name, _] : prepared_stages[i].column_to_updated)
            {
                /// If we rewrite the whole part in ALTER DELETE and mask is not updated
                /// do not write mask because it will be applied during execution of mutation.
                if (column_name == RowExistsColumn::name && has_filters && !deleted_mask_updated)
                    continue;

                prepared_stages[i].output_columns.insert(column_name);
            }
        }
    }

    auto storage_columns = metadata_snapshot->getColumns().getNamesOfPhysical();

    /// Add persistent virtual columns if the whole part is rewritten,
    /// because we should preserve them in parts after mutation.
    if (source.isMutatingDataPart() && prepared_stages.back().isAffectingAllColumns(storage_columns))
    {
        for (const auto & column_name : available_columns)
        {
            if (column_name == RowExistsColumn::name && has_filters && !deleted_mask_updated)
                continue;

            prepared_stages.back().output_columns.insert(column_name);
        }
    }

    /// Filter out ephemeral virtual columns from output_columns for each stage.
    /// Ephemeral virtual columns (like _distance, _sample_factor) are computed
    /// on-the-fly during reads and not stored in parts. Including them in output_columns
    /// would cause the mutation pipeline to request them from the source reader, but some
    /// can't be filled during mutation reads.
    /// The old ExpressionActionsChain naturally excluded unreferenced columns from the
    /// pipeline; with the new analyzer we need to exclude them explicitly.
    /// However, columns that are in column_to_updated are explicitly requested (e.g. via
    /// READ_COLUMN commands for lightweight updates that need _part, _part_offset, etc.)
    /// and must be preserved even if they are ephemeral virtual columns.
    /// When return_all_columns is true (e.g. for on-the-fly mutations in SELECT), ALL
    /// columns including ephemeral virtuals must be preserved because the caller explicitly
    /// needs them (e.g. _part_offset for applying patch parts).
    if (auto real_storage = (!settings.return_all_columns ? source.getStorage() : nullptr))
    {
        auto virtuals = real_storage->getVirtualsPtr();

        /// Collect columns explicitly requested via column_to_updated across ALL stages.
        /// These columns (e.g. _part, _part_offset from READ_COLUMN commands for lightweight
        /// updates) must be preserved even if they are ephemeral virtual columns, because
        /// output_columns propagates from earlier stages to later ones.
        NameSet explicitly_requested;
        for (const auto & stage : prepared_stages)
            for (const auto & [name, _] : stage.column_to_updated)
                explicitly_requested.insert(name);

        for (auto & stage : prepared_stages)
        {
            NameSet filtered;
            for (const auto & name : stage.output_columns)
            {
                if (explicitly_requested.contains(name))
                {
                    filtered.insert(name);
                    continue;
                }
                const auto * virt_desc = virtuals->tryGetDescription(name);
                if (!virt_desc || virt_desc->isPersistent())
                    filtered.insert(name);
            }
            stage.output_columns = std::move(filtered);
        }
    }

    /// Now, calculate action steps for each stage.
    /// Do it backwards to propagate information about columns required as input for a stage to the previous stage.
    for (int64_t i = prepared_stages.size() - 1; i >= 0; --i)
    {
        auto & stage = prepared_stages[i];

        /// Build AST expression list with all expressions needed in this stage.
        ASTPtr all_asts = make_intrusive<ASTExpressionList>();

        for (const auto & ast : stage.filters)
            all_asts->children.push_back(ast);

        for (const auto & kv : stage.column_to_updated)
            all_asts->children.push_back(kv.second);

        /// Add output columns (physical + persistent virtual) so they get INPUT nodes in
        /// the DAG. Ephemeral virtuals were already filtered from output_columns above.
        for (const auto & column : stage.output_columns)
            all_asts->children.push_back(make_intrusive<ASTIdentifier>(column));

        /// Build query tree from the combined AST and resolve it using the new analyzer.
        /// This resolves all identifiers, functions, and types in one pass.
        auto expression = buildQueryTree(all_asts, context);

        ColumnsDescription fake_column_descriptions{};
        for (const auto & column : all_columns)
            fake_column_descriptions.add(
                ColumnDescription(column.name, column.type),
                /*after_column=*/"", /*first=*/false, /*add_subcolumns=*/true);

        auto dummy_storage = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, fake_column_descriptions);

        /// Copy virtual columns from the real storage so the analyzer can resolve
        /// references to virtual columns like _part, _block_number, etc.
        auto real_storage = source.getStorage();
        if (real_storage)
            dummy_storage->setVirtuals(*real_storage->getVirtualsPtr());

        QueryTreeNodePtr fake_table_expression = std::make_shared<TableNode>(dummy_storage, context);

        /// When dry_run is true, we must not execute scalar subqueries to avoid deadlocks
        /// (e.g. ALTER referencing the same table in a scalar subquery).
        bool only_analyze = dry_run;
        QueryAnalyzer analyzer(only_analyze);
        analyzer.resolve(expression, fake_table_expression, context);
        /// Assign unique aliases (__table1, __table2, etc.) to all table expressions in
        /// the resolved tree. This is normally done by QueryAnalysisPass after resolve.
        /// Without this, table expressions in GLOBAL IN subqueries have no aliases, so
        /// GlobalPlannerContext::createColumnIdentifier generates non-unique identifiers
        /// (just the column name), causing "Column identifier is already registered" errors.
        createUniqueAliasesIfNecessary(expression, context);

        /// Give the fake table expression a unique alias so that column identifiers
        /// registered from it (e.g. "__mutation_source.key") don't collide with identifiers
        /// from table expressions inside subqueries (e.g. EXISTS or IN subqueries that
        /// reference tables with the same column names). Without this alias, identifiers
        /// from the fake table are just the bare column name (e.g. "key"), which can
        /// collide with identifiers from other tables that also lack aliases.
        /// Note: createUniqueAliasesIfNecessary does NOT visit fake_table_expression because
        /// it's the scope table, not part of the expression tree (ColumnNode stores its source
        /// as a weak pointer, not a tree child).
        fake_table_expression->setAlias("__mutation_source");

        GlobalPlannerContextPtr global_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, FiltersForTableExpressionMap{});
        auto mutable_context = Context::createCopy(context);
        auto planner_context = std::make_shared<PlannerContext>(std::move(mutable_context), global_planner_context, SelectQueryOptions{});

        /// Register only the columns from the fake table expression in the PlannerContext.
        /// We do NOT call collectSourceColumns (which traverses the entire expression tree
        /// including subqueries) because it registers columns from ALL table expressions in
        /// the tree, and GlobalPlannerContext::createColumnIdentifier throws on duplicate
        /// identifiers. Manually registering only the fake table's columns avoids this.
        {
            auto & table_expression_data = planner_context->getOrCreateTableExpressionData(fake_table_expression);
            for (const auto & column : all_columns)
            {
                auto column_identifier = global_planner_context->createColumnIdentifier(column, fake_table_expression);
                table_expression_data.addColumn(column, column_identifier);
            }
        }
        collectSets(expression, *planner_context);

        /// Get the input columns from all_columns (physical + all virtual columns).
        /// All virtual columns (including ephemeral ones like _part, _distance) are included
        /// as inputs so that expressions like UPDATE s = _part can reference them.
        ColumnsWithTypeAndName input_columns;
        for (const auto & column : all_columns)
            input_columns.emplace_back(column.type, column.name);

        size_t num_filters = stage.filters.size();
        size_t num_updates = stage.column_to_updated.size();

        /// Build a single DAG from the full resolved expression tree.
        /// This DAG contains all filter, update, and output column computations.
        /// Outputs are ordered: [filter_0, ..., filter_N-1, update_0, ..., update_M-1, output_col_0, ...].
        /// Note: We do NOT add aliases here. Aliases (renaming update expressions to target column names)
        /// are added only in the update step's DAG, to avoid name collisions with input columns
        /// that would cause expressions to be evaluated in the wrong step.
        auto full_dag = buildActionsDAGFromExpressionNode(expression, input_columns, planner_context, {}, false).first;

        /// Remember the original output names of update expressions (before aliasing).
        Names update_expression_names;
        for (size_t j = 0; j < num_updates; ++j)
            update_expression_names.push_back(full_dag.getOutputs().at(num_filters + j)->result_name);

        /// Add pass-through for output_columns that are inputs of the DAG but not
        /// already in the outputs. This ensures physical + persistent virtual columns
        /// survive through the filter/update steps. We don't add ALL inputs as pass-through
        /// because that would include ephemeral virtual columns (like _distance) that
        /// can't be read from parts during mutations.
        {
            NameSet output_names;
            for (const auto * output : full_dag.getOutputs())
                output_names.insert(output->result_name);
            for (const auto * input_node : full_dag.getInputs())
                if (!output_names.contains(input_node->result_name) && stage.output_columns.contains(input_node->result_name))
                    full_dag.getOutputs().push_back(input_node);
        }

        /// Now split the full DAG into separate steps for filters, updates, and projection.
        /// Each step's DAG must contain only the computations needed for that step,
        /// to prevent update expressions from being evaluated during filtering (which
        /// would overwrite column values before the actual update step runs).

        /// Build the filter step (if there are filters).
        if (num_filters > 0)
        {
            auto filter_dag = full_dag.clone();

            /// Combine multiple filter outputs into a single AND expression if needed.
            String filter_column_name;
            if (num_filters == 1)
            {
                filter_column_name = filter_dag.getOutputs().at(0)->result_name;
            }
            else
            {
                ActionsDAG::NodeRawConstPtrs filter_nodes;
                for (size_t j = 0; j < num_filters; ++j)
                    filter_nodes.push_back(filter_dag.getOutputs().at(j));

                auto and_function = FunctionFactory::instance().get("and", context);
                const auto & and_node = filter_dag.addFunction(and_function, filter_nodes, {});
                filter_dag.getOutputs().push_back(&and_node);
                filter_column_name = and_node.result_name;
            }

            /// Trim the filter DAG to only compute the filter expression plus
            /// pass-through of physical + persistent virtual columns. We use
            /// output_columns (not all_columns) because ephemeral virtual columns
            /// were filtered out and are not present in the DAG outputs.
            /// Columns not matching any INPUT node in the trimmed DAG will pass
            /// through the FilterStep automatically via updateHeader.
            NameSet filter_required_outputs;
            filter_required_outputs.insert(filter_column_name);
            for (const auto & column : stage.output_columns)
                filter_required_outputs.insert(column);
            filter_dag.removeUnusedActions(filter_required_outputs);

            stage.action_steps.push_back(Stage::ActionStep{.dag = std::move(filter_dag), .filter_column_name = std::move(filter_column_name)});
        }

        /// Build the update step (if there are column updates).
        if (num_updates > 0)
        {
            auto update_dag = full_dag.clone();

            /// Add aliases to rename update expression results to target column names.
            /// This is done only in the update step's DAG (not the full DAG) to avoid
            /// creating name collisions with input columns in other steps.
            {
                size_t update_idx = 0;
                for (const auto & kv : stage.column_to_updated)
                {
                    const auto & output_node = update_dag.findInOutputs(update_expression_names[update_idx]);
                    const auto & alias = update_dag.addAlias(output_node, kv.first);
                    update_dag.addOrReplaceInOutputs(alias);
                    ++update_idx;
                }
            }

            /// Trim the update DAG to only compute the update expressions plus
            /// pass-through of physical + persistent virtual columns (same
            /// reasoning as the filter step — use output_columns, not all_columns).
            NameSet update_required_outputs;
            for (const auto & kv : stage.column_to_updated)
                update_required_outputs.insert(kv.first);
            for (const auto & column : stage.output_columns)
                update_required_outputs.insert(column);
            update_dag.removeUnusedActions(update_required_outputs);

            /// Materialize constant output columns in the update DAG. The new analyzer's
            /// constant folding can produce ColumnConst outputs (e.g. UPDATE v = 3 WHERE 1).
            /// When multiple mutations are batched, columns with the same name but different
            /// constant values cause AMBIGUOUS_COLUMN_NAME errors during block structure checks.
            /// Wrapping them with materialize() converts them to regular columns.
            for (const auto & kv : stage.column_to_updated)
            {
                const auto & output_node = update_dag.findInOutputs(kv.first);
                if (output_node.column && isColumnConst(*output_node.column))
                    update_dag.addOrReplaceInOutputs(update_dag.materializeNode(output_node));
            }

            /// Use project_input=true so that only DAG outputs survive. This properly
            /// replaces updated columns: when an update expression doesn't reference the
            /// original column (e.g. UPDATE v = '100'), removeUnusedActions removes the
            /// INPUT node for that column. With project_input=false, the original column
            /// would pass through unconsumed alongside the ALIAS result, causing duplicates.
            /// With project_input=true, appendInputsForUnusedColumns (called later in
            /// addStreamsForLaterStages) adds INPUT nodes for all header columns, ensuring
            /// they are consumed. Only DAG output columns survive, correctly replacing
            /// updated columns.
            /// This also prevents the updated column from appearing in required_columns
            /// (computed from the DAG before appendInputsForUnusedColumns), so the reader
            /// doesn't request it from the part. This is important when the column type
            /// changed (e.g. MODIFY COLUMN after UPDATE): the reader would try to convert
            /// the old type to the new type via performRequiredConversions, which can fail
            /// for values that only the UPDATE expression would fix.
            stage.action_steps.push_back(Stage::ActionStep{.dag = std::move(update_dag), .filter_column_name = {}, .project_input = true});
        }

        /// Build the final projection step that keeps only output_columns.
        /// This step runs AFTER the update step, so updated columns already have their
        /// new values in the input. We just need to select the right columns, not recompute.
        /// The projection also removes filter columns and any other intermediate results.
        ///
        /// We only project output_columns (not column_to_updated keys) to match the old
        /// ExpressionActionsChain behavior. The output_columns set already includes
        /// column_to_updated keys (they are added in the output_columns computation loop
        /// above), except when explicitly excluded (e.g. _row_exists when has_filters is
        /// true and deleted_mask_updated is false — in that case, the ALTER DELETE physically
        /// removes rows, making _row_exists redundant in the output part).
        ///
        /// We use full_dag.getInputs() (not input_columns) because the expression
        /// resolution may have added extra INPUT nodes via addInputColumnIfNecessary
        /// (e.g., for ephemeral virtual columns like _part referenced in expressions).
        {
            NameSet final_output_names = stage.output_columns;

            ColumnsWithTypeAndName projection_columns;
            for (const auto * input_node : full_dag.getInputs())
                if (final_output_names.contains(input_node->result_name))
                    projection_columns.emplace_back(input_node->result_type, input_node->result_name);

            ActionsDAG projection_dag(projection_columns);
            /// When return_all_columns is true (on-the-fly mutations in SELECT), do NOT
            /// project away extra columns. The caller may need columns (e.g. _part_offset
            /// for patch merging) that are not in the mutation's output_columns or even
            /// in the DAG's inputs. With project_input=true, such columns would be dropped
            /// from the block. The old analyzer's ExpressionActionsChain had pass-through
            /// semantics for unmentioned columns; we replicate that by disabling projection.
            stage.action_steps.push_back(Stage::ActionStep{
                .dag = std::move(projection_dag),
                .filter_column_name = {},
                .project_input = !settings.return_all_columns});
        }

        /// Store prepared sets from the planner context.
        /// We use a shared_ptr with aliasing constructor to keep the PlannerContext alive
        /// while sharing the PreparedSets reference.
        stage.prepared_sets = PreparedSetsPtr(planner_context, &planner_context->getPreparedSets());

        /// Build subquery plans for IN clauses so that addCreatingSetsStep can wire them
        /// into the mutation pipeline. Without this, FutureSetFromSubquery objects created by
        /// collectSets would have no source QueryPlan, and build() would return null.
        ///
        /// Skip this when dry_run is true (validation mode): subquery plans are only needed
        /// for pipeline execution, and during dry_run the expression tree may contain
        /// EXISTS-turned-IN subqueries (QueryAnalyzer transforms non-correlated EXISTS to IN
        /// when only_analyze=true) whose nested table expressions can cause identifier
        /// collisions in the Planner.
        if (!dry_run && stage.prepared_sets)
        {
            auto subqueries = stage.prepared_sets->getSubqueries();
            auto subquery_options = SelectQueryOptions{}.subquery();
            subquery_options.ignore_limits = false;
            for (auto & subquery : subqueries)
            {
                if (subquery->get())
                    continue;

                auto query_tree = subquery->detachQueryTree();
                if (!query_tree)
                    continue;

                /// Ensure all table expressions inside the detached subquery tree have
                /// unique aliases. This is needed because the subquery tree was part of
                /// a larger expression tree where createUniqueAliasesIfNecessary was called,
                /// but building a standalone plan requires consistent aliases within
                /// the fresh GlobalPlannerContext.
                createUniqueAliasesIfNecessary(query_tree, context);

                Planner subquery_planner(
                    query_tree,
                    subquery_options,
                    std::make_shared<GlobalPlannerContext>(nullptr, nullptr, FiltersForTableExpressionMap{}));
                subquery_planner.buildQueryPlanIfNeeded();

                auto subquery_plan = std::move(subquery_planner).extractQueryPlan();
                subquery->setQueryPlan(std::make_unique<QueryPlan>(std::move(subquery_plan)));
            }
        }

        /// Compute required input columns for this stage by propagating requirements
        /// backwards through the action steps. Start from the last step's requirements,
        /// then for each preceding step: remove columns it produces (DAG outputs that
        /// are not pass-through INPUTs) and add columns it needs from its predecessor.
        /// This matches how ExpressionActionsChain::finalize computes source requirements:
        /// columns produced by intermediate steps (e.g. UPDATE v='100' creates v) are NOT
        /// requested from the source, preventing unnecessary reads that could trigger
        /// performRequiredConversions failures when column types have changed.
        {
            NameSet needed;
            /// Start with the last step's required columns.
            if (!stage.action_steps.empty())
            {
                const auto & last_step = stage.action_steps.back();
                for (const auto & name : last_step.dag.getRequiredColumnsNames())
                    needed.insert(name);
            }

            /// Propagate backwards through remaining steps.
            for (int64_t step_idx = static_cast<int64_t>(stage.action_steps.size()) - 2; step_idx >= 0; --step_idx)
            {
                const auto & step = stage.action_steps[step_idx];

                /// Determine which columns this step produces (non-INPUT outputs).
                NameSet produced;
                NameSet step_inputs;
                for (const auto * input : step.dag.getInputs())
                    step_inputs.insert(input->result_name);
                for (const auto & output_name : step.dag.getOutputs())
                    if (!step_inputs.contains(output_name->result_name))
                        produced.insert(output_name->result_name);

                /// Remove produced columns from needed set (they don't need to come from source).
                for (const auto & name : produced)
                    needed.erase(name);

                /// Add this step's own required inputs.
                for (const auto & name : step.dag.getRequiredColumnsNames())
                    needed.insert(name);
            }

            stage.required_columns = Names(needed.begin(), needed.end());

            /// When return_all_columns is true (e.g. Iceberg mutations, on-the-fly
            /// mutations in SELECT), the caller expects ALL columns including virtual
            /// columns in the output. The backwards propagation through action steps
            /// should include all output_columns, but the projection step with
            /// project_input=false doesn't create DAG inputs for pass-through
            /// columns. Ensure all output_columns are explicitly requested from
            /// the source so the storage produces data for them.
            if (settings.return_all_columns)
            {
                for (const auto & name : stage.output_columns)
                    needed.insert(name);
                stage.required_columns = Names(needed.begin(), needed.end());
            }

            /// When all columns are updated with constants and the WHERE clause doesn't
            /// reference any table columns, required_columns can end up empty. But the
            /// storage reader needs at least one column to know how many rows exist.
            if (stage.required_columns.empty() && !stage.output_columns.empty())
                stage.required_columns.push_back(*stage.output_columns.begin());
        }

        if (i)
        {
            /// Propagate information about columns needed as input.
            for (const auto & name : stage.required_columns)
                prepared_stages[i - 1].output_columns.insert(name);
        }
    }
}

std::optional<ActionsDAG> MutationsInterpreter::createFilterDAGForStage(const Stage & stage)
{
    ActionsDAG::NodeRawConstPtrs filter_nodes;
    for (const auto & action_step : stage.action_steps)
    {
        if (!action_step.filter_column_name.empty())
            filter_nodes.push_back(&action_step.dag.findInOutputs(action_step.filter_column_name));
    }

    if (filter_nodes.empty())
        return std::nullopt;

    return ActionsDAG::buildFilterActionsDAG(filter_nodes);
}

void MutationsInterpreter::Source::read(
    Stage & first_stage,
    QueryPlan & plan,
    const StorageMetadataPtr & snapshot_,
    const ContextPtr & context_,
    const Settings & mutation_settings) const
{
    auto required_columns = first_stage.required_columns;
    auto storage_snapshot = getStorageSnapshot(snapshot_, context_, mutation_settings.can_execute);

    if (!mutation_settings.can_execute)
    {
        auto header = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(required_columns));
        auto callback = []()
        {
            return DB::Exception(ErrorCodes::LOGICAL_ERROR, "Cannot execute a mutation because can_execute flag set to false");
        };

        Pipe pipe(std::make_shared<ThrowingExceptionSource>(header, callback));

        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        plan.addStep(std::move(read_from_pipe));
        return;
    }

    if (data)
    {
        createReadFromPartStep(
            MergeTreeSequentialSourceType::Mutation,
            plan,
            *data,
            storage_snapshot,
            RangesInDataPart(part),
            alter_conversions,
            nullptr,
            required_columns,
            nullptr,
            mutation_settings.apply_deleted_mask,
            createFilterDAGForStage(first_stage),
            false,
            false,
            context_,
            getLogger("MutationsInterpreter"));
    }
    else
    {
        auto select = make_intrusive<ASTSelectQuery>();
        std::shared_ptr<const ActionsDAG> filter_actions_dag;

        select->setExpression(ASTSelectQuery::Expression::SELECT, make_intrusive<ASTExpressionList>());
        for (const auto & column_name : first_stage.output_columns)
            select->select()->children.push_back(make_intrusive<ASTIdentifier>(column_name));

        /// Don't let select list be empty.
        if (select->select()->children.empty())
            select->select()->children.push_back(make_intrusive<ASTLiteral>(Field(0)));

        if (!first_stage.filters.empty())
        {
            ASTPtr where_expression;

            if (first_stage.filters.size() == 1)
            {
                where_expression = first_stage.filters[0];
            }
            else
            {
                auto coalesced_predicates = make_intrusive<ASTFunction>();
                coalesced_predicates->name = "and";
                coalesced_predicates->arguments = make_intrusive<ASTExpressionList>();
                coalesced_predicates->children.push_back(coalesced_predicates->arguments);
                coalesced_predicates->arguments->children = first_stage.filters;
                where_expression = std::move(coalesced_predicates);
            }

            select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_expression));

            if (auto filter = createFilterDAGForStage(first_stage))
                filter_actions_dag = std::make_shared<ActionsDAG>(std::move(*filter));
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to create filter DAG for stage with non-empty filters");
        }

        SelectQueryInfo query_info;
        query_info.query = std::move(select);
        query_info.filter_actions_dag = std::move(filter_actions_dag);
        query_info.prepared_sets = first_stage.prepared_sets;

        size_t max_block_size = context_->getSettingsRef()[Setting::max_block_size];
        Names extracted_column_names;
        const auto has_table_virtual_column
            = extractRequiredNonTableColumnsFromStorage(required_columns, storage, storage_snapshot, extracted_column_names);

        storage->read(plan, has_table_virtual_column ? extracted_column_names : required_columns, storage_snapshot,
            query_info, context_, QueryProcessingStage::FetchColumns, max_block_size, mutation_settings.max_threads);

        if (has_table_virtual_column && plan.isInitialized())
        {
            const auto & table_name = storage->getStorageID().getTableName();
            ColumnWithTypeAndName column;
            column.name = "_table";
            column.type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
            column.column = column.type->createColumnConst(0, Field(table_name));

            auto adding_column_dag = ActionsDAG::makeAddingColumnActions(std::move(column));
            auto expression_step = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(adding_column_dag));
            plan.addStep(std::move(expression_step));
        }

        if (!plan.isInitialized())
        {
            /// It may be possible when there is nothing to read from storage.
            auto header = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(required_columns));
            auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(header)));
            plan.addStep(std::move(read_from_pipe));
        }
    }
}

static void addCreatingSetsForPreparedSets(QueryPlan & plan, const PreparedSetsPtr & prepared_sets, ContextPtr context)
{
    if (!prepared_sets)
        return;

    auto subqueries = prepared_sets->getSubqueries();
    if (subqueries.empty())
        return;

    /// Build sets synchronously so that ReadFromMergeTree::initializePipeline
    /// (called later during buildQueryPipeline) can use them for index analysis.
    /// Without this, buildOrderedSetInplace would find a null source because
    /// addCreatingSetsStep below moves the source out of FutureSetFromSubquery.
    /// This mirrors the normal Planner behavior where sets are built inline
    /// during the first optimization pass (before addPlansForSets in the second pass).
    for (auto & subquery : subqueries)
    {
        if (subquery->get())
            continue;
        subquery->buildOrderedSetInplace(context);
    }

    addCreatingSetsStep(plan, std::move(subqueries), context);
}

void MutationsInterpreter::initQueryPlan(Stage & first_stage, QueryPlan & plan)
{
    // Mutations are not using concurrency control now. Queries, merges and mutations running together could lead to CPU overcommit.
    // TODO(serxa): Enable concurrency control for mutation queries and mutations. This should be done after CPU scheduler introduction.
    plan.setConcurrencyControl(false);

    source.read(first_stage, plan, metadata_snapshot, context, settings);
    addCreatingSetsForPreparedSets(plan, first_stage.prepared_sets, context);
}

QueryPipelineBuilder MutationsInterpreter::addStreamsForLaterStages(const std::vector<Stage> & prepared_stages, QueryPlan & plan) const
{
    for (const Stage & stage : prepared_stages)
    {
        /// Build sets before the action steps that use them.
        addCreatingSetsForPreparedSets(plan, stage.prepared_sets, context);

        for (const auto & action_step : stage.action_steps)
        {
            if (action_step.dag.hasArrayJoin())
                throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION, "arrayJoin is not allowed in mutations");

            auto dag = action_step.dag.clone();
            if (action_step.project_input)
                dag.appendInputsForUnusedColumns(*plan.getCurrentHeader());

            if (!action_step.filter_column_name.empty())
            {
                /// Execute DELETEs.
                plan.addStep(std::make_unique<FilterStep>(plan.getCurrentHeader(), std::move(dag), action_step.filter_column_name, false));
            }
            else
            {
                /// Execute UPDATE or final projection.
                plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(dag)));
            }
        }
    }

    QueryPlanOptimizationSettings do_not_optimize_plan_settings(context);
    do_not_optimize_plan_settings.optimize_plan = false;
    /// Disable PREWHERE optimization for mutation pipelines. The mutation pipeline builds
    /// its own filter/update/projection steps (FilterStep, ExpressionStep) on top of the
    /// source step's output header. The PREWHERE optimization moves the filter from the
    /// FilterStep into the source step (via updatePrewhereInfo), which changes the source
    /// step's output header to include prewhere columns. This causes a mismatch between
    /// the source's declared header and the chunks it produces, leading to "Invalid number
    /// of columns in chunk" errors. The filter is already applied as a separate pipeline
    /// step, so PREWHERE is not needed.
    do_not_optimize_plan_settings.optimize_prewhere = false;

    auto pipeline = std::move(*plan.buildQueryPipeline(do_not_optimize_plan_settings, BuildQueryPipelineSettings(context)));

    /// The ReadFromPart step's declared output header may not include columns that the
    /// actual MergeTreeSequentialSource adds internally (e.g. _row_exists when
    /// apply_deleted_mask is true). Since the action step DAGs are built using the plan
    /// header (which doesn't have these columns), no DAG input consumes them, and
    /// ExpressionStep/FilterStep's updateHeader passes them through as "remaining".
    /// We must trim the pipeline to only include the expected output columns.
    /// However, when return_all_columns is true (e.g. Iceberg mutations, on-the-fly
    /// mutations), the caller expects all columns including virtual columns like _path,
    /// _file, _row_number, etc. These are not in output_columns, so we skip trimming.
    if (!settings.return_all_columns)
    {
        const auto & last_stage = prepared_stages.back();
        NameSet expected_columns = last_stage.output_columns;

        const auto & header = pipeline.getHeader();
        bool needs_trim = false;
        for (size_t i = 0; i < header.columns(); ++i)
        {
            if (!expected_columns.contains(header.getByPosition(i).name))
            {
                needs_trim = true;
                break;
            }
        }

        if (needs_trim)
        {
            ColumnsWithTypeAndName trim_columns;
            for (const auto & col : header)
                if (expected_columns.contains(col.name))
                    trim_columns.push_back(col);

            ActionsDAG trim_dag(trim_columns);
            trim_dag.appendInputsForUnusedColumns(header);

            auto expression = std::make_shared<ExpressionActions>(std::move(trim_dag), ExpressionActionsSettings(context));
            pipeline.addSimpleTransform([&](const SharedHeader & h)
            {
                return std::make_shared<ExpressionTransform>(h, expression);
            });
        }
    }

    pipeline.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<MaterializingTransform>(header);
    });

    return pipeline;
}

void MutationsInterpreter::validate()
{
    /// For Replicated* storages mutations cannot employ non-deterministic functions
    /// because that produces inconsistencies between replicas
    if (startsWith(source.getStorage()->getName(), "Replicated") && !context->getSettingsRef()[Setting::allow_nondeterministic_mutations])
    {
        for (const auto & command : commands)
        {
            const auto nondeterministic_func_data = findFirstNonDeterministicFunction(command, context);
            if (nondeterministic_func_data.subquery)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "ALTER UPDATE/ALTER DELETE statement with subquery may be nondeterministic, "
                                                           "see allow_nondeterministic_mutations setting");

            if (nondeterministic_func_data.nondeterministic_function_name)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "The source storage is replicated so ALTER UPDATE/ALTER DELETE statements must use only deterministic functions. "
                    "Function '{}' is non-deterministic", *nondeterministic_func_data.nondeterministic_function_name);
        }
    }

    // Make sure the mutation query is valid.
    // Always use the new analyzer path to match prepareMutationStages which uses the
    // new analyzer directly.
    if (context->getSettingsRef()[Setting::validate_mutation_query])
        prepareQueryAffectedQueryTree(commands, source.getStorage(), context);

    QueryPlan plan;

    initQueryPlan(stages.front(), plan);
    auto pipeline = addStreamsForLaterStages(stages, plan);
}

QueryPipelineBuilder MutationsInterpreter::execute()
{
    if (!settings.can_execute)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot execute mutations interpreter because can_execute flag set to false");

    QueryPlan plan;
    initQueryPlan(stages.front(), plan);
    auto builder = addStreamsForLaterStages(stages, plan);

    /// Sometimes we update just part of columns (for example UPDATE mutation)
    /// in this case we don't read sorting key, so just we don't check anything.
    if (auto sort_desc = getStorageSortDescriptionIfPossible(builder.getHeader()))
    {
        builder.addSimpleTransform([&](const SharedHeader & header)
        {
            return std::make_shared<CheckSortedTransform>(header, *sort_desc);
        });
    }

    if (!updated_header)
        updated_header = std::make_unique<Block>(builder.getHeader());

    return builder;
}

std::vector<MutationActions> MutationsInterpreter::getMutationActions() const
{
    std::vector<MutationActions> result;
    for (const auto & stage : stages)
    {
        for (const auto & action_step : stage.action_steps)
        {
            result.push_back({action_step.dag.clone(), action_step.filter_column_name, action_step.project_input, stage.mutation_version});
        }
    }

    return result;
}

Block MutationsInterpreter::getUpdatedHeader() const
{
    // If it's an index/projection materialization, we don't write any data columns, thus empty header is used
    return mutation_kind.mutation_kind == MutationKind::MUTATE_INDEX_STATISTICS_PROJECTION ? Block{} : *updated_header;
}

const ColumnDependencies & MutationsInterpreter::getColumnDependencies() const
{
    return dependencies;
}

size_t MutationsInterpreter::evaluateCommandsSize(const MutationCommands & commands_, const StoragePtr & storage_, ContextPtr context_)
{
    return prepareQueryAffectedAST(commands_, storage_, context_)->size();
}

std::optional<SortDescription> MutationsInterpreter::getStorageSortDescriptionIfPossible(const Block & header) const
{
    Names sort_columns = metadata_snapshot->getSortingKeyColumns();
    std::vector<bool> reverse_flags = metadata_snapshot->getSortingKeyReverseFlags();
    SortDescription sort_description;
    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    for (size_t i = 0; i < sort_columns_size; ++i)
    {
        if (header.has(sort_columns[i]))
        {
            if (!reverse_flags.empty() && reverse_flags[i])
                sort_description.emplace_back(sort_columns[i], -1, 1);
            else
                sort_description.emplace_back(sort_columns[i], 1, 1);
        }
        else
        {
            return {};
        }
    }

    return sort_description;
}

ASTPtr MutationsInterpreter::getPartitionAndPredicateExpressionForMutationCommand(const MutationCommand & command) const
{
    return DB::getPartitionAndPredicateExpressionForMutationCommand(command, source.getStorage(), context);
}

bool MutationsInterpreter::Stage::isAffectingAllColumns(const Names & storage_columns) const
{
    if (affects_all_columns)
        return true;

    /// Is subset
    for (const auto & storage_column : storage_columns)
        if (!output_columns.contains(storage_column))
            return false;

    return true;
}

bool MutationsInterpreter::isAffectingAllColumns() const
{
    auto storage_columns = metadata_snapshot->getColumns().getNamesOfPhysical();
    if (stages.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mutation interpreter has no stages");

    /// Find first not readonly stage from the end.
    for (auto it = stages.rbegin(); it != stages.rend(); ++it)
    {
        if (!it->is_readonly)
            return it->isAffectingAllColumns(storage_columns);
    }

    return false;
}

void MutationsInterpreter::MutationKind::set(const MutationKindEnum & kind)
{
    mutation_kind = std::max(mutation_kind, kind);
}

}
