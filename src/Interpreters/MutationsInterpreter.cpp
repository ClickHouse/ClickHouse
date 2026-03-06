#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterSelectQuery.h>
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
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/createUniqueAliasesIfNecessary.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Planner/ActionsChain.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/Planner.h>
#include <Planner/PlannerContext.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/CollectSets.h>
#include <Planner/Utils.h>
#include <Interpreters/Context.h>
#include <Parsers/makeASTForLogicalFunction.h>
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
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool allow_nondeterministic_mutations;
    extern const SettingsUInt64 max_bytes_to_transfer;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 max_rows_to_transfer;
    extern const SettingsOverflowMode transfer_overflow_mode;
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
    const StorageMetadataPtr & metadata_snapshot,
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

    std::optional<InterpreterSelectQuery> interpreter_select_query;
    BlockIO io;

    if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        auto select_query_tree = prepareQueryAffectedQueryTree(commands, storage_from_part, context);
        InterpreterSelectQueryAnalyzer interpreter(select_query_tree, context, SelectQueryOptions().ignoreLimits());
        io = interpreter.execute();
    }
    else
    {
        ASTPtr select_query = prepareQueryAffectedAST(commands, storage_from_part, context);
        /// Interpreter must be alive, when we use result of execute() method.
        /// For some reason it may copy context and give it into ExpressionTransform
        /// after that we will use context from destroyed stack frame in our stream.
        interpreter_select_query.emplace(
            select_query, context, storage_from_part, metadata_snapshot, SelectQueryOptions().ignoreLimits());

        io = interpreter_select_query->execute();
    }

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
    auto new_context = Context::createCopy(context_);
    use_analyzer = new_context->getSettingsRef()[Setting::allow_experimental_analyzer];
    if (use_analyzer)
        LOG_TEST(logger, "Will use new analyzer to prepare mutation");
    context = std::move(new_context);
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
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown index: {}", command.index_name);

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

    /// Now, calculate `expressions_chain` for each stage except the first.
    /// Do it backwards to propagate information about columns required as input for a stage to the previous stage.
    for (int64_t i = prepared_stages.size() - 1; i >= 0; --i)
    {
        auto & stage = prepared_stages[i];

        ASTPtr all_asts = make_intrusive<ASTExpressionList>();

        for (const auto & ast : stage.filters)
            all_asts->children.push_back(ast);

        for (const auto & kv : stage.column_to_updated)
            all_asts->children.push_back(kv.second);

        /// Add all output columns to prevent ExpressionAnalyzer from deleting them from source columns.
        for (const auto & column : stage.output_columns)
            all_asts->children.push_back(make_intrusive<ASTIdentifier>(column));

        /// Executing scalar subquery on that stage can lead to deadlock
        /// e.g. ALTER referencing the same table in scalar subquery
        bool execute_scalar_subqueries = !dry_run;

        if (use_analyzer)
        {
            /// --- New analyzer path ---
            /// 1. Build query tree from AST expression list and resolve against storage.
            auto execution_context = Context::createCopy(context);
            auto expression = buildQueryTree(all_asts, execution_context);
            /// Use the real storage so that virtual columns (e.g. `_part`) are available,
            /// but pass an empty TableLockHolder to avoid calling lockForShare —
            /// the mutation background thread already holds a write lock on the storage.
            auto table_node = std::make_shared<TableNode>(
                source.getStorage(), TableLockHolder{}, storage_snapshot);

            bool ignore_in_subqueries = !context->getSettingsRef()[Setting::validate_mutation_query];
            QueryAnalyzer query_analyzer(/*only_analyze=*/!execute_scalar_subqueries, ignore_in_subqueries, /*use_storage_snapshot_without_data=*/dry_run);
            query_analyzer.resolve(expression, table_node, execution_context);
            createUniqueAliasesIfNecessary(expression, execution_context);

            /// 2. Set up PlannerContext, collect source columns and sets.
            auto global_planner_context = std::make_shared<GlobalPlannerContext>(
                nullptr, nullptr, FiltersForTableExpressionMap{});
            auto planner_context = std::make_shared<PlannerContext>(
                execution_context, global_planner_context, SelectQueryOptions{});

            collectSourceColumns(expression, planner_context, /*keep_alias_columns=*/true);
            collectSets(expression, *planner_context);

            /// 3. Build input columns from all available columns plus any
            /// virtual columns actually referenced by the expression
            /// (e.g. `_part`, `_partition_id`). The old analyzer path discovers
            /// these dynamically in TreeRewriterResult::collectUsedColumns.
            ColumnsWithTypeAndName input_columns;
            NameSet input_columns_set;
            for (const auto & col : all_columns)
            {
                input_columns.emplace_back(col.type, col.name);
                input_columns_set.insert(col.name);
            }
            /// collectSourceColumns recorded which columns the expression uses.
            /// Add any referenced virtual columns or subcolumns that are not
            /// already in the input.  The old analyzer path discovers these
            /// dynamically in TreeRewriterResult::collectUsedColumns.
            /// Note: the table may not be registered if the expression doesn't
            /// reference any columns (e.g. MATERIALIZE COLUMN with a constant default).
            const auto * table_expression_data = planner_context->getTableExpressionDataOrNull(table_node);
            if (table_expression_data)
            {
                for (const auto & selected_name : table_expression_data->getSelectedColumnsNames())
                {
                    if (input_columns_set.contains(selected_name))
                        continue;

                    /// Virtual column (e.g. `_part`, `_partition_id`).
                    if (auto virtual_column = storage_snapshot->virtual_columns->tryGet(selected_name))
                    {
                        input_columns.emplace_back(virtual_column->type, virtual_column->name);
                        input_columns_set.insert(selected_name);
                        continue;
                    }

                    /// Subcolumn (e.g. `json.a` for a JSON/Dynamic parent column).
                    /// The read infrastructure (MergeTreeSequentialSource, IMergeTreeReader)
                    /// already supports reading subcolumns transparently.
                    auto col_options = GetColumnsOptions(GetColumnsOptions::All).withSubcolumns();
                    if (auto column = storage_snapshot->tryGetColumn(col_options, selected_name))
                    {
                        input_columns.emplace_back(column->type, column->name);
                        input_columns_set.insert(selected_name);
                    }
                }
            }

            ColumnNodePtrWithHashSet empty_correlated_columns;

            /// Remember the boundary between filters and other expressions in all_asts.
            const size_t num_filters = stage.filters.size();

            stage.new_actions_chain = std::make_unique<ActionsChain>();
            auto & actions_chain = *stage.new_actions_chain;

            /// 4. Build filter step (combine all filter expressions with AND).
            if (!stage.filters.empty())
            {
                /// The resolved `expression` is a LIST node whose children
                /// correspond 1:1 to all_asts->children.
                /// First num_filters children are the filter expressions.
                QueryTreeNodePtr filter_node;
                if (num_filters == 1)
                {
                    filter_node = expression->getChildren()[0];
                }
                else
                {
                    /// Combine filters with AND in AST, then build a separate query tree for it.
                    auto combined_ast = makeASTForLogicalAnd(ASTs(stage.filters.begin(), stage.filters.end()));
                    auto combined_expr_list = make_intrusive<ASTExpressionList>();
                    combined_expr_list->children.push_back(combined_ast);
                    auto combined_tree = buildQueryTree(combined_expr_list, execution_context);
                    QueryAnalyzer combined_analyzer(/*only_analyze=*/!execute_scalar_subqueries, ignore_in_subqueries, /*use_storage_snapshot_without_data=*/dry_run);
                    combined_analyzer.resolve(combined_tree, table_node, execution_context);
                    collectSourceColumns(combined_tree, planner_context, true);
                    collectSets(combined_tree, *planner_context);
                    filter_node = combined_tree->getChildren()[0];
                }

                auto filter_actions = std::make_shared<ActionsAndProjectInputsFlag>();
                filter_actions->dag = ActionsDAG(input_columns);
                /// Use PlannerActionsVisitor directly instead of
                /// buildActionsDAGFromExpressionNode, because the latter
                /// replaces DAG outputs with only the expression results.
                /// We keep stage output columns + the filter expression as
                /// outputs, matching the old analyzer path (where the DAG
                /// outputs are output_columns + filter_col after finalize).
                /// This is important for on-fly mutation application where
                /// getReadTaskColumns uses getNames() to track which columns
                /// are already available from previous prewhere steps.
                PlannerActionsVisitor actions_visitor(planner_context, empty_correlated_columns, false);
                auto [expression_nodes, correlated_subtrees] = actions_visitor.visit(filter_actions->dag, filter_node);
                correlated_subtrees.assertEmpty("in mutation filter");
                chassert(expression_nodes.size() == 1);

                /// Build a map from the current outputs for O(1) lookup,
                /// then keep only columns in stage.output_columns + the filter.
                std::unordered_map<std::string_view, const ActionsDAG::Node *> output_map;
                for (const auto * node : filter_actions->dag.getOutputs())
                    output_map.emplace(node->result_name, node);

                auto & dag_outputs = filter_actions->dag.getOutputs();
                dag_outputs.clear();
                for (const auto & name : stage.output_columns)
                {
                    if (auto it = output_map.find(name); it != output_map.end())
                        dag_outputs.push_back(it->second);
                }
                dag_outputs.push_back(expression_nodes[0]);

                stage.filter_column_names.push_back(expression_nodes[0]->result_name);
                actions_chain.addStep(
                    std::make_unique<ActionsChainStep>(std::move(filter_actions)));
            }

            /// 5. Build update step.
            if (!stage.column_to_updated.empty())
            {
                auto available_columns_for_step = actions_chain.getStepsSize() > 0
                    ? actions_chain.getLastStepAvailableOutputColumns()
                    : input_columns;

                /// Build a combined expression list for all update expressions.
                auto update_expr_list = make_intrusive<ASTExpressionList>();
                for (const auto & kv : stage.column_to_updated)
                    update_expr_list->children.push_back(kv.second);

                auto update_tree = buildQueryTree(update_expr_list, execution_context);
                QueryAnalyzer update_analyzer(/*only_analyze=*/!execute_scalar_subqueries, ignore_in_subqueries, /*use_storage_snapshot_without_data=*/dry_run);
                update_analyzer.resolve(update_tree, table_node, execution_context);
                collectSourceColumns(update_tree, planner_context, true);
                collectSets(update_tree, *planner_context);

                auto update_actions = std::make_shared<ActionsAndProjectInputsFlag>();
                update_actions->dag = ActionsDAG(available_columns_for_step);
                PlannerActionsVisitor update_visitor(planner_context, empty_correlated_columns, false);
                auto [update_expression_nodes, update_correlated_subtrees] = update_visitor.visit(update_actions->dag, update_tree);
                update_correlated_subtrees.assertEmpty("in mutation update");

                /// Add aliases: expression result name -> target column name.
                size_t idx = 0;
                for (const auto & kv : stage.column_to_updated)
                {
                    const auto & dag_node = *update_expression_nodes[idx];
                    const auto & alias = update_actions->dag.addAlias(dag_node, kv.first);
                    update_actions->dag.addOrReplaceInOutputs(alias);
                    ++idx;
                }

                /// Keep only stage.output_columns + aliased update columns as
                /// outputs, matching the filter step pattern. See the comment
                /// in the filter step above for why this matters.
                std::unordered_map<std::string_view, const ActionsDAG::Node *> update_output_map;
                for (const auto * node : update_actions->dag.getOutputs())
                    update_output_map.emplace(node->result_name, node);

                auto & update_dag_outputs = update_actions->dag.getOutputs();
                update_dag_outputs.clear();
                for (const auto & name : stage.output_columns)
                {
                    if (auto it = update_output_map.find(name); it != update_output_map.end())
                        update_dag_outputs.push_back(it->second);
                }
                for (const auto & kv : stage.column_to_updated)
                {
                    if (auto it = update_output_map.find(kv.first); it != update_output_map.end())
                        update_dag_outputs.push_back(it->second);
                }

                actions_chain.addStep(
                    std::make_unique<ActionsChainStep>(std::move(update_actions)));
            }

            /// 6. Build initial step if chain is empty (needed for first stage).
            ///    Use only the columns from output_columns (matching the old
            ///    path which uses syntax_result->required_source_columns).
            ///    Using all input_columns would prevent finalize from pruning
            ///    the identity DAG and cause reading unnecessary columns.
            if (i == 0 && actions_chain.getStepsSize() == 0)
            {
                ColumnsWithTypeAndName initial_columns;
                for (const auto & col : input_columns)
                    if (stage.output_columns.contains(col.name))
                        initial_columns.push_back(col);

                auto initial_actions = std::make_shared<ActionsAndProjectInputsFlag>();
                initial_actions->dag = ActionsDAG(initial_columns);
                actions_chain.addStep(
                    std::make_unique<ActionsChainStep>(std::move(initial_actions)));
            }

            /// 7. Build projection step - keep only output_columns.
            {
                auto available_columns_for_proj = actions_chain.getStepsSize() > 0
                    ? actions_chain.getLastStepAvailableOutputColumns()
                    : input_columns;

                ActionsDAG proj_dag(available_columns_for_proj);
                ActionsDAG::NodeRawConstPtrs proj_outputs;
                for (const auto & name : stage.output_columns)
                    proj_outputs.push_back(&proj_dag.findInOutputs(name));
                proj_dag.getOutputs() = std::move(proj_outputs);

                auto proj_actions = std::make_shared<ActionsAndProjectInputsFlag>();
                proj_actions->dag = std::move(proj_dag);
                proj_actions->project_input = true;
                actions_chain.addStep(
                    std::make_unique<ActionsChainStep>(std::move(proj_actions)));
            }

            actions_chain.finalize();

            /// ActionsChain::finalize unconditionally sets project_input = true
            /// for every step. But for on-fly mutation application, input columns
            /// like _part_offset (added by addPatchPartsColumns to the first reader)
            /// must pass through non-projection steps.  The old analyzer path
            /// (ExpressionActionsChain::finalize) only sets project_input for
            /// non-first steps, so match that: clear project_input on all steps
            /// except the last (projection) one.
            for (size_t s = 0; s + 1 < actions_chain.getStepsSize(); ++s)
                actions_chain[s]->getActions()->project_input = false;

            /// 8. Store prepared sets (aliasing shared_ptr keeps planner_context alive).
            stage.new_prepared_sets = std::shared_ptr<PreparedSets>(
                planner_context, &planner_context->getPreparedSets());

            /// 9. Propagate required columns to previous stage.
            if (i > 0)
            {
                const auto & first_step = actions_chain.getSteps().front();
                for (const auto & col_name : first_step->getInputColumnNames())
                    prepared_stages[i - 1].output_columns.insert(col_name);
            }

        }
        else
        {
            /// --- Old analyzer path (unchanged) ---
            auto syntax_result = TreeRewriter(context).analyze(
                all_asts, all_columns, source.getStorage(), storage_snapshot,
                false, true, execute_scalar_subqueries);

            stage.analyzer = std::make_unique<ExpressionAnalyzer>(all_asts, syntax_result, context);

            ExpressionActionsChain & actions_chain = stage.expressions_chain;

            if (!stage.filters.empty())
            {
                auto ast = stage.filters.front();
                if (stage.filters.size() > 1)
                    ast = makeASTForLogicalAnd(std::move(stage.filters));

                if (!actions_chain.steps.empty())
                    actions_chain.addStep();

                stage.analyzer->appendExpression(actions_chain, ast, dry_run);
                stage.filter_column_names.push_back(ast->getColumnName());
            }

            if (!stage.column_to_updated.empty())
            {
                if (!actions_chain.steps.empty())
                    actions_chain.addStep();

                for (const auto & kv : stage.column_to_updated)
                    stage.analyzer->appendExpression(actions_chain, kv.second, dry_run);

                auto & actions = actions_chain.getLastStep().actions();

                for (const auto & kv : stage.column_to_updated)
                {
                    auto column_name = kv.second->getColumnName();
                    const auto & dag_node = actions->dag.findInOutputs(column_name);
                    const auto & alias = actions->dag.addAlias(dag_node, kv.first);
                    actions->dag.addOrReplaceInOutputs(alias);
                }
            }

            if (i == 0 && actions_chain.steps.empty())
                actions_chain.lastStep(syntax_result->required_source_columns);

            /// Remove all intermediate columns.
            actions_chain.addStep();
            actions_chain.getLastStep().required_output.clear();
            ActionsDAG::NodeRawConstPtrs new_index;
            for (const auto & name : stage.output_columns)
                actions_chain.getLastStep().addRequiredOutput(name);

            actions_chain.getLastActions();
            actions_chain.finalize();

            if (i)
            {
                /// Propagate information about columns needed as input.
                for (const auto & column : actions_chain.steps.front()->getRequiredColumns())
                    prepared_stages[i - 1].output_columns.insert(column.name);
            }
        }
    }
}

MutationsInterpreter::Stage::Stage(ContextPtr context_) : expressions_chain(context_) {}
MutationsInterpreter::Stage::~Stage() = default;
MutationsInterpreter::Stage::Stage(Stage &&) noexcept = default;
MutationsInterpreter::Stage & MutationsInterpreter::Stage::operator=(Stage &&) noexcept = default;

/// Build QueryPlans for subquery sets (IN subqueries) on the new analyzer path,
/// then add a DelayedCreatingSetsStep to the plan. This mirrors
/// addBuildSubqueriesForSetsStepIfNeeded from the Planner.
static void buildSubqueryPlansForSetsAndAdd(QueryPlan & query_plan, const PreparedSetsPtr & prepared_sets, ContextPtr context_)
{
    if (!prepared_sets)
        return;

    auto subqueries = prepared_sets->getSubqueries();
    if (subqueries.empty())
        return;

    for (auto & subquery : subqueries)
    {
        if (subquery->get())
            continue;

        auto query_tree = subquery->detachQueryTree();
        if (!query_tree)
            continue;

        auto subquery_options = SelectQueryOptions{}.subquery();
        subquery_options.ignore_limits = false;
        Planner subquery_planner(
            query_tree,
            subquery_options,
            std::make_shared<GlobalPlannerContext>(nullptr, nullptr, FiltersForTableExpressionMap{}));
        subquery_planner.buildQueryPlanIfNeeded();

        auto subquery_plan = std::move(subquery_planner).extractQueryPlan();
        for (const auto & ctx : subquery_plan.getInterpretersContexts())
            query_plan.addInterpreterContext(ctx);
        subquery->setQueryPlan(std::make_unique<QueryPlan>(std::move(subquery_plan)));
    }

    const auto & settings = context_->getSettingsRef();
    SizeLimits network_transfer_limits(
        settings[Setting::max_rows_to_transfer],
        settings[Setting::max_bytes_to_transfer],
        settings[Setting::transfer_overflow_mode]);
    auto prepared_sets_cache = context_->getPreparedSetsCache();

    auto step = std::make_unique<DelayedCreatingSetsStep>(
        query_plan.getCurrentHeader(),
        std::move(subqueries),
        network_transfer_limits,
        prepared_sets_cache);
    query_plan.addStep(std::move(step));
}

std::optional<ActionsDAG> MutationsInterpreter::createFilterDAGForStage(const Stage & stage)
{
    const auto & names = stage.filter_column_names;
    if (names.empty())
        return std::nullopt;

    ActionsDAG::NodeRawConstPtrs nodes(names.size());
    if (stage.analyzer)
    {
        /// Old path
        for (size_t i = 0; i < names.size(); ++i)
            nodes[i] = &stage.expressions_chain.steps[i]->actions()->dag.findInOutputs(names[i]);
    }
    else
    {
        /// New path
        const auto & chain_steps = stage.new_actions_chain->getSteps();
        for (size_t i = 0; i < names.size(); ++i)
            nodes[i] = &chain_steps[i]->getActions()->dag.findInOutputs(names[i]);
    }

    return ActionsDAG::buildFilterActionsDAG(nodes);
}

void MutationsInterpreter::Source::read(
    Stage & first_stage,
    QueryPlan & plan,
    const StorageMetadataPtr & snapshot_,
    const ContextPtr & context_,
    const Settings & mutation_settings) const
{
    Names required_columns;
    if (first_stage.analyzer)
        required_columns = first_stage.expressions_chain.steps.front()->getRequiredColumns().getNames();
    else
    {
        const auto & first_step = first_stage.new_actions_chain->getSteps().front();
        for (const auto & col_name : first_step->getInputColumnNames())
            required_columns.push_back(col_name);
    }

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

void MutationsInterpreter::initQueryPlan(Stage & first_stage, QueryPlan & plan)
{
    // Mutations are not using concurrency control now. Queries, merges and mutations running together could lead to CPU overcommit.
    // TODO(serxa): Enable concurrency control for mutation queries and mutations. This should be done after CPU scheduler introduction.
    plan.setConcurrencyControl(false);

    source.read(first_stage, plan, metadata_snapshot, context, settings);

    if (first_stage.analyzer)
        addDelayedCreatingSetsStep(plan, first_stage.analyzer->getPreparedSets(), context);
    else
        buildSubqueryPlansForSetsAndAdd(plan, first_stage.new_prepared_sets, context);
}

QueryPipelineBuilder MutationsInterpreter::addStreamsForLaterStages(const std::vector<Stage> & prepared_stages, QueryPlan & plan) const
{
    for (const Stage & stage : prepared_stages)
    {
        if (stage.analyzer)
        {
            /// Old path
            for (size_t i = 0; i < stage.expressions_chain.steps.size(); ++i)
            {
                const auto & step = stage.expressions_chain.steps[i];
                if (step->actions()->dag.hasArrayJoin())
                    throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION, "arrayJoin is not allowed in mutations");

                if (i < stage.filter_column_names.size())
                {
                    auto dag = step->actions()->dag.clone();
                    if (step->actions()->project_input)
                        dag.appendInputsForUnusedColumns(*plan.getCurrentHeader());
                    /// Execute DELETEs.
                    plan.addStep(std::make_unique<FilterStep>(plan.getCurrentHeader(), std::move(dag), stage.filter_column_names[i], false));
                }
                else
                {
                    auto dag = step->actions()->dag.clone();
                    if (step->actions()->project_input)
                        dag.appendInputsForUnusedColumns(*plan.getCurrentHeader());
                    /// Execute UPDATE or final projection.
                    plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(dag)));
                }
            }

            addDelayedCreatingSetsStep(plan, stage.analyzer->getPreparedSets(), context);
        }
        else
        {
            /// New path
            const auto & chain_steps = stage.new_actions_chain->getSteps();
            for (size_t i = 0; i < chain_steps.size(); ++i)
            {
                const auto & step = chain_steps[i];
                if (step->getActions()->dag.hasArrayJoin())
                    throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION, "arrayJoin is not allowed in mutations");

                auto dag = step->getActions()->dag.clone();
                if (step->getActions()->project_input)
                    dag.appendInputsForUnusedColumns(*plan.getCurrentHeader());

                if (i < stage.filter_column_names.size())
                    plan.addStep(std::make_unique<FilterStep>(
                        plan.getCurrentHeader(), std::move(dag),
                        stage.filter_column_names[i], false));
                else
                    plan.addStep(std::make_unique<ExpressionStep>(
                        plan.getCurrentHeader(), std::move(dag)));
            }

            buildSubqueryPlansForSetsAndAdd(plan, stage.new_prepared_sets, context);
        }
    }

    QueryPlanOptimizationSettings do_not_optimize_plan_settings(context);
    do_not_optimize_plan_settings.optimize_plan = false;

    auto pipeline = std::move(*plan.buildQueryPipeline(do_not_optimize_plan_settings, BuildQueryPipelineSettings(context)));

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

    // Make sure the mutation query is valid
    if (context->getSettingsRef()[Setting::validate_mutation_query])
    {
        if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
            prepareQueryAffectedQueryTree(commands, source.getStorage(), context);
        else
        {
            ASTPtr select_query = prepareQueryAffectedAST(commands, source.getStorage(), context);
            InterpreterSelectQuery(select_query, context, source.getStorage(), metadata_snapshot);
        }
    }

    /// When validate_mutation_query is disabled, IN subqueries may reference
    /// non-existent tables and were left unresolved.  Skip building the query
    /// plan — it would fail on the unresolved nodes.
    if (!context->getSettingsRef()[Setting::validate_mutation_query])
        return;

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
        if (stage.analyzer)
        {
            /// Old path
            for (size_t i = 0; i < stage.expressions_chain.steps.size(); ++i)
            {
                const auto & step = stage.expressions_chain.steps[i];
                bool project_input = step->actions()->project_input;
                if (i < stage.filter_column_names.size())
                    result.push_back({step->actions()->dag.clone(), stage.filter_column_names[i], project_input, stage.mutation_version});
                else
                    result.push_back({step->actions()->dag.clone(), "", project_input, stage.mutation_version});
            }
        }
        else
        {
            /// New path
            const auto & chain_steps = stage.new_actions_chain->getSteps();
            for (size_t i = 0; i < chain_steps.size(); ++i)
            {
                const auto & step = chain_steps[i];
                bool project_input = step->getActions()->project_input;
                if (i < stage.filter_column_names.size())
                    result.push_back({step->getActions()->dag.clone(), stage.filter_column_names[i], project_input, stage.mutation_version});
                else
                    result.push_back({step->getActions()->dag.clone(), "", project_input, stage.mutation_version});
            }
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

size_t MutationsInterpreter::evaluateCommandsSize()
{
    return prepareQueryAffectedAST(commands, source.getStorage(), context)->size();
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
