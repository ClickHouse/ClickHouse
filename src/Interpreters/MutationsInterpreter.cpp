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
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
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
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsBool validate_mutation_query;
}

namespace MergeTreeSetting
{
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


namespace
{

ASTPtr prepareQueryAffectedAST(const std::vector<MutationCommand> & commands, const StoragePtr & storage, ContextPtr context)
{
    /// Execute `SELECT count() FROM storage WHERE predicate1 OR predicate2 OR ...` query.
    /// The result can differ from the number of affected rows (e.g. if there is an UPDATE command that
    /// changes how many rows satisfy the predicates of the subsequent commands).
    /// But we can be sure that if count = 0, then no rows will be touched.

    auto select = std::make_shared<ASTSelectQuery>();

    select->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
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
                    std::make_shared<ASTIdentifier>("_partition_id"),
                    std::make_shared<ASTLiteral>(partition_id)
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

bool MutationsInterpreter::Source::hasSecondaryIndex(const String & name) const
{
    return part && part->hasSecondaryIndex(name);
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
    if (new_context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        new_context->setSetting("allow_experimental_analyzer", false);
        LOG_TEST(logger, "Will use old analyzer to prepare mutation");
    }
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
                auto [key_column_name, key_subcolumn_name] = Nested::splitName(key_column);
                if (key_column_name == column_name && ordinary_storage_column->type->hasSubcolumn(key_subcolumn_name))
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
            return source.hasSecondaryIndex(name);

        return true;
    };

    if (settings.recalculate_dependencies_of_updated_columns)
        dependencies = getAllColumnDependencies(metadata_snapshot, updated_columns, has_dependency);

    bool need_rebuild_indexes = false;
    bool need_rebuild_projections = false;
    std::vector<String> read_columns;

    if (has_lightweight_delete_materialization)
    {
        auto & stage = stages.emplace_back(context);
        stage.affects_all_columns = true;

        need_rebuild_indexes = true;
        need_rebuild_projections = true;
    }

    if (has_rewrite_parts)
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

            /// ALTER DELETE can changes number of rows in the part, so we need to rebuild indexes and projection
            need_rebuild_indexes = true;
            need_rebuild_projections = true;
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

                auto type_literal = std::make_shared<ASTLiteral>(type->getName());
                ASTPtr condition = getPartitionAndPredicateExpressionForMutationCommand(command);

                /// And new check validateNestedArraySizes for Nested subcolumns
                if (isArray(type) && !Nested::splitName(column_name).second.empty())
                {
                    std::shared_ptr<ASTFunction> function = nullptr;

                    auto nested_update_exprs = getExpressionsOfUpdatedNestedSubcolumns(column_name, affected_materialized, all_columns, command.column_to_update_expression);
                    if (!nested_update_exprs)
                    {
                        function = makeASTFunction("validateNestedArraySizes",
                            condition,
                            update_expr->clone(),
                            std::make_shared<ASTIdentifier>(column_name));
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
                        std::make_shared<ASTIdentifier>(column_name)),
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
                        auto type_literal = std::make_shared<ASTLiteral>(column.type->getName());

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
                "_CAST", column.default_desc.expression->clone(), std::make_shared<ASTLiteral>(column.type->getName()));

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

            if (!source.hasSecondaryIndex(it->name))
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

            /// Check if the type of this column is changed and there are projections that
            /// have this column in the primary key. We should rebuild such projections.
            if (const auto & merge_tree_data_part = source.getMergeTreeDataPart())
            {
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
                }
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
            throw Exception(ErrorCodes::UNKNOWN_MUTATION_COMMAND, "Unknown mutation command type: {}", DB::toString<int>(command.type));
        }
    }

    if (!read_columns.empty())
    {
        stages.emplace_back(context);
        for (auto & column_name : read_columns)
            stages.back().column_to_updated.emplace(column_name, std::make_shared<ASTIdentifier>(column_name));
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
                stages.back().column_to_updated.emplace(column, std::make_shared<ASTIdentifier>(column));
        }

        if (!unchanged_columns.empty())
        {
            if (!stages.empty())
            {
                std::vector<Stage> stages_copy;
                /// Copy all filled stages except index calculation stage.
                for (const auto & stage : stages)
                {
                    stages_copy.emplace_back(context);
                    stages_copy.back().column_to_updated = stage.column_to_updated;
                    stages_copy.back().output_columns = stage.output_columns;
                    stages_copy.back().filters = stage.filters;
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
                    column, std::make_shared<ASTIdentifier>(column));
        }
    }

    for (const auto & index : metadata_snapshot->getSecondaryIndices())
    {
        if (!source.hasSecondaryIndex(index.name))
            continue;

        if (need_rebuild_indexes)
        {
            materialized_indices.insert(index.name);
            continue;
        }

        const auto & index_cols = index.expression->getRequiredColumns();
        bool changed = std::any_of(
            index_cols.begin(),
            index_cols.end(),
            [&](const auto & col) { return updated_columns.contains(col) || changed_columns.contains(col); });

        if (changed)
            materialized_indices.insert(index.name);
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
    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withExtendedObjects().withVirtuals();

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

        ASTPtr all_asts = std::make_shared<ASTExpressionList>();

        for (const auto & ast : stage.filters)
            all_asts->children.push_back(ast);

        for (const auto & kv : stage.column_to_updated)
            all_asts->children.push_back(kv.second);

        /// Add all output columns to prevent ExpressionAnalyzer from deleting them from source columns.
        for (const auto & column : stage.output_columns)
            all_asts->children.push_back(std::make_shared<ASTIdentifier>(column));

        /// Executing scalar subquery on that stage can lead to deadlock
        /// e.g. ALTER referencing the same table in scalar subquery
        bool execute_scalar_subqueries = !dry_run;
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

void MutationsInterpreter::Source::read(
    Stage & first_stage,
    QueryPlan & plan,
    const StorageMetadataPtr & snapshot_,
    const ContextPtr & context_,
    const Settings & mutation_settings) const
{
    auto required_columns = first_stage.expressions_chain.steps.front()->getRequiredColumns().getNames();
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
        const auto & steps = first_stage.expressions_chain.steps;
        const auto & names = first_stage.filter_column_names;
        size_t num_filters = names.size();

        std::optional<ActionsDAG> filter;
        if (!first_stage.filter_column_names.empty())
        {
            ActionsDAG::NodeRawConstPtrs nodes(num_filters);
            for (size_t i = 0; i < num_filters; ++i)
                nodes[i] = &steps[i]->actions()->dag.findInOutputs(names[i]);

            filter = ActionsDAG::buildFilterActionsDAG(nodes);
        }

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
            std::move(filter),
            false,
            false,
            context_,
            getLogger("MutationsInterpreter"));
    }
    else
    {
        auto select = std::make_shared<ASTSelectQuery>();

        select->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
        for (const auto & column_name : first_stage.output_columns)
            select->select()->children.push_back(std::make_shared<ASTIdentifier>(column_name));

        /// Don't let select list be empty.
        if (select->select()->children.empty())
            select->select()->children.push_back(std::make_shared<ASTLiteral>(Field(0)));

        if (!first_stage.filters.empty())
        {
            ASTPtr where_expression;
            if (first_stage.filters.size() == 1)
                where_expression = first_stage.filters[0];
            else
            {
                auto coalesced_predicates = std::make_shared<ASTFunction>();
                coalesced_predicates->name = "and";
                coalesced_predicates->arguments = std::make_shared<ASTExpressionList>();
                coalesced_predicates->children.push_back(coalesced_predicates->arguments);
                coalesced_predicates->arguments->children = first_stage.filters;
                where_expression = std::move(coalesced_predicates);
            }
            select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_expression));
        }

        SelectQueryInfo query_info;
        query_info.query = std::move(select);

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
    addCreatingSetsStep(plan, first_stage.analyzer->getPreparedSets(), context);
}

QueryPipelineBuilder MutationsInterpreter::addStreamsForLaterStages(const std::vector<Stage> & prepared_stages, QueryPlan & plan) const
{
    for (const Stage & stage : prepared_stages)
    {
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

        addCreatingSetsStep(plan, stage.analyzer->getPreparedSets(), context);
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
