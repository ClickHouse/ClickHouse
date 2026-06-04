#include <Interpreters/convertFieldToType.h>
#include <Planner/PlannerJoinTree.h>

#include <Core/Settings.h>

#include <Core/ParallelReplicasMode.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/quoteString.h>
#include <Common/scope_guard_safe.h>

#include <Columns/ColumnAggregateFunction.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Functions/FunctionFactory.h>

#include <AggregateFunctions/AggregateFunctionCount.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>

#include <Storages/IStorage.h>
#include <Storages/IStorageCluster.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageMerge.h>
#include <Storages/StorageValues.h>
#include <Storages/StorageView.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/QueryTreeBuilder.h>

#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/ReadFromTableFunctionStep.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/getCustomKeyFilterForParallelReplicas.h>
#include <Interpreters/ClusterProxy/executeQuery.h>

#include <Planner/CollectColumnIdentifiers.h>
#include <Planner/Planner.h>
#include <Planner/PlannerJoins.h>
#include <Planner/PlannerJoinsLogical.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/Utils.h>
#include <Planner/CollectSets.h>
#include <Planner/CollectTableExpressionData.h>

#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Planner/findQueryForParallelReplicas.h>
#include <Interpreters/DirectJoinMergeTreeEntity.h>

#include <ranges>

namespace DB
{
namespace Setting
{
    extern const SettingsMap additional_table_filters;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool allow_experimental_query_deduplication;
    extern const SettingsBool async_socket_for_remote;
    extern const SettingsBool empty_result_for_aggregation_by_empty_set;
    extern const SettingsBool enable_unaligned_array_join;
    extern const SettingsBool join_use_nulls;
    extern const SettingsJoinAlgorithm join_algorithm;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 max_columns_to_read;
    extern const SettingsUInt64 max_distributed_connections;
    extern const SettingsUInt64 max_rows_in_set_to_optimize_join;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsFloat max_streams_to_max_threads_ratio;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 max_threads_min_free_memory_per_thread;
    extern const SettingsBool optimize_sorting_by_input_stream_properties;
    extern const SettingsBool optimize_trivial_count_query;
    extern const SettingsUInt64 parallel_replicas_count;
    extern const SettingsString parallel_replicas_custom_key;
    extern const SettingsParallelReplicasMode parallel_replicas_mode;
    extern const SettingsUInt64 parallel_replicas_custom_key_range_lower;
    extern const SettingsUInt64 parallel_replicas_custom_key_range_upper;
    extern const SettingsBool parallel_replicas_for_non_replicated_merge_tree;
    extern const SettingsUInt64 parallel_replicas_min_number_of_rows_per_replica;
    extern const SettingsUInt64 parallel_replica_offset;
    extern const SettingsBool optimize_move_to_prewhere;
    extern const SettingsBool optimize_move_to_prewhere_if_final;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsBoolAuto query_plan_join_swap_table;
    extern const SettingsUInt64 min_joined_block_size_rows;
    extern const SettingsUInt64 min_joined_block_size_bytes;
    extern const SettingsBool use_join_disjunctions_push_down;
    extern const SettingsBool query_plan_display_internal_aliases;
    extern const SettingsBool enable_lazy_columns_replication;
    extern const SettingsBool parallel_replicas_allow_materialized_views;
    extern const SettingsBool parallel_replicas_allow_view_over_mergetree;
    extern const SettingsBool serialize_query_plan;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ACCESS_DENIED;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int TOO_MANY_COLUMNS;
    extern const int UNSUPPORTED_METHOD;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Check if current user has privileges to SELECT columns from table
/// Throws an exception if access to any column from `column_names` is not granted
/// If `column_names` is empty, check access to any columns and return names of accessible columns
NameSet checkAccessRights(const TableNode & table_node, const Names & column_names, const ContextPtr & query_context)
{
    /// StorageDummy is created on preliminary stage, ignore access check for it.
    if (typeid_cast<const StorageDummy *>(table_node.getStorage().get()))
        return {};

    const auto & storage_id = table_node.getStorageID();
    const auto & storage_snapshot = table_node.getStorageSnapshot();

    if (column_names.empty())
    {
        NameSet accessible_columns;
        /** For a trivial queries like "SELECT count() FROM table", "SELECT 1 FROM table" access is granted if at least
          * one table column is accessible.
          */
        auto access = query_context->getAccess();
        for (const auto & column : storage_snapshot->metadata->getColumns())
        {
            if (access->isGranted(AccessType::SELECT, storage_id.database_name, storage_id.table_name, column.name))
                accessible_columns.insert(column.name);
        }

        if (accessible_columns.empty())
        {
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "{}: Not enough privileges. To execute this query, it's necessary to have the grant SELECT for at least one column on {}",
                query_context->getUserName(),
                storage_id.getFullTableName());
        }
        return accessible_columns;
    }

    // In case of cross-replication we don't know what database is used for the table.
    // `storage_id.hasDatabase()` can return false only on the initiator node.
    // Each shard will use the default database (in the case of cross-replication shards may have different defaults).
    if (storage_id.hasDatabase())
        query_context->checkAccess(AccessType::SELECT, storage_id, column_names);

    return {};
}

/// Check access rights for all tables referenced in a subquery
void checkAccessRightsForSubquery(const QueryTreeNodePtr & subquery_node, const ContextPtr & query_context)
{
    auto table_nodes = extractAllTableReferences(subquery_node);
    for (const auto & table_node_ptr : table_nodes)
    {
        const auto & table_node = table_node_ptr->as<TableNode &>();
        if (typeid_cast<const StorageDummy *>(table_node.getStorage().get()))
            continue;

        const auto & storage_id = table_node.getStorageID();
        if (storage_id.hasDatabase())
            query_context->checkAccess(AccessType::SELECT, storage_id);
    }
}

bool shouldIgnoreQuotaAndLimits(const TableNode & table_node)
{
    const auto & storage_id = table_node.getStorageID();
    if (!storage_id.hasDatabase())
        return false;
    if (storage_id.database_name == DatabaseCatalog::SYSTEM_DATABASE)
    {
        static const boost::container::flat_set<std::string_view> tables_ignoring_quota{"quotas", "quota_limits", "quota_usage", "quotas_usage", "one"};
        if (tables_ignoring_quota.contains(storage_id.table_name))
            return true;
    }
    return false;
}

NameAndTypePair chooseSmallestColumnToReadFromStorage(const StoragePtr & storage, const StorageSnapshotPtr & storage_snapshot, const NameSet & column_names_allowed_to_select)
{
    /** We need to read at least one column to find the number of rows.
      * We will find a column with minimum <compressed_size, type_size, uncompressed_size>.
      * Because it is the column that is cheapest to read.
      */
    class ColumnWithSize
    {
    public:
        ColumnWithSize(NameAndTypePair column_, ColumnSize column_size_)
            : column(std::move(column_))
            , compressed_size(column_size_.data_compressed)
            , uncompressed_size(column_size_.data_uncompressed)
            , type_size(column.type->haveMaximumSizeOfValue() ? column.type->getMaximumSizeOfValueInMemory() : 100)
        {
        }

        bool operator<(const ColumnWithSize & rhs) const
        {
            return std::tie(compressed_size, type_size, uncompressed_size)
                < std::tie(rhs.compressed_size, rhs.type_size, rhs.uncompressed_size);
        }

        NameAndTypePair column;
        size_t compressed_size = 0;
        size_t uncompressed_size = 0;
        size_t type_size = 0;
    };

    std::vector<ColumnWithSize> columns_with_sizes;

    auto column_sizes = storage->getColumnSizes();
    auto column_names_and_types = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns());

    if (!column_names_allowed_to_select.empty())
    {
        auto it = column_names_and_types.begin();
        while (it != column_names_and_types.end())
        {
            if (!column_names_allowed_to_select.contains(it->name))
                it = column_names_and_types.erase(it);
            else
                ++it;
        }
    }

    if (!column_sizes.empty())
    {
        for (auto & column_name_and_type : column_names_and_types)
        {
            auto it = column_sizes.find(column_name_and_type.name);
            if (it == column_sizes.end())
                continue;

            columns_with_sizes.emplace_back(column_name_and_type, it->second);
        }
    }

    NameAndTypePair result;

    if (!columns_with_sizes.empty())
        result = std::min_element(columns_with_sizes.begin(), columns_with_sizes.end())->column;
    else
        /// If we have no information about columns sizes, choose a column of minimum size of its data type
        result = ExpressionActions::getSmallestColumn(column_names_and_types);

    return result;
}

bool applyTrivialCountIfPossible(
    QueryPlan & query_plan,
    SelectQueryInfo & select_query_info,
    const TableNode * table_node,
    const TableFunctionNode * table_function_node,
    const QueryTreeNodePtr & query_tree,
    ContextMutablePtr & query_context,
    const Names & columns_names,
    const PlannerContext & planner_context)
{
    const auto & settings = query_context->getSettingsRef();
    if (!settings[Setting::optimize_trivial_count_query])
        return false;

    const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
    if (!storage->supportsTrivialCountOptimization(
            table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot(), query_context))
        return false;

    if (getEffectiveRowPolicyFilter(storage, query_context))
        return false;

    if (select_query_info.additional_filter_ast)
        return false;

    /** Transaction check here is necessary because
      * MergeTree maintains total count for all parts in Active state and it simply returns that number for trivial select count() from table query.
      * But if we have current transaction, then we should return number of rows in current snapshot (that may include parts in Outdated state),
      * so we have to use totalRowsByPartitionPredicate() instead of totalRows even for trivial query
      * See https://github.com/ClickHouse/ClickHouse/pull/24258/files#r828182031
      */
    if (query_context->getCurrentTransaction())
        return false;

    /// can't apply if FINAL
    if (table_node && table_node->getTableExpressionModifiers().has_value() &&
        (table_node->getTableExpressionModifiers()->hasFinal() || table_node->getTableExpressionModifiers()->hasSampleSizeRatio() ||
         table_node->getTableExpressionModifiers()->hasSampleOffsetRatio() || table_node->getTableExpressionModifiers()->hasStream()))
        return false;
    if (table_function_node && table_function_node->getTableExpressionModifiers().has_value()
        && (table_function_node->getTableExpressionModifiers()->hasFinal()
            || table_function_node->getTableExpressionModifiers()->hasSampleSizeRatio()
            || table_function_node->getTableExpressionModifiers()->hasSampleOffsetRatio()
            || table_function_node->getTableExpressionModifiers()->hasStream()))
        return false;

    // TODO: It's possible to optimize count() given only partition predicates
    auto & main_query_node = query_tree->as<QueryNode &>();
    if (main_query_node.hasGroupBy() || main_query_node.hasPrewhere() || main_query_node.hasWhere())
        return false;

    if (settings[Setting::allow_experimental_query_deduplication] || settings[Setting::empty_result_for_aggregation_by_empty_set])
        return false;

    QueryTreeNodes aggregates = collectAggregateFunctionNodes(query_tree);
    if (aggregates.size() != 1)
        return false;

    const auto & function_node = aggregates.front().get()->as<const FunctionNode &>();
    chassert(function_node.getAggregateFunction() != nullptr);
    const auto * count_func = typeid_cast<const AggregateFunctionCount *>(function_node.getAggregateFunction().get());
    if (!count_func)
        return false;

    /// Some storages can optimize trivial count in read() method instead of totalRows() because it still can
    /// require reading some data (but much faster than reading columns).
    /// Set a special flag in query info so the storage will see it and optimize count in read() method.
    select_query_info.optimize_trivial_count = true;

    /// Get number of rows
    std::optional<UInt64> num_rows = storage->totalRows(query_context);
    if (!num_rows)
        return false;

    if (settings[Setting::allow_experimental_parallel_reading_from_replicas] > 0 && settings[Setting::max_parallel_replicas] > 1)
    {
        /// Imagine the situation when we have a query with parallel replicas and
        /// this code executed on the remote server.
        /// If we will apply trivial count optimization, then each remote server will do the same
        /// and we will have N times more rows as the result on the initiator.
        /// TODO: This condition seems unneeded when we will make the parallel replicas with custom key
        /// to work on top of MergeTree instead of Distributed.
        if (settings[Setting::parallel_replicas_mode] == ParallelReplicasMode::CUSTOM_KEY_RANGE ||
            settings[Setting::parallel_replicas_mode] == ParallelReplicasMode::CUSTOM_KEY_SAMPLING ||
            settings[Setting::parallel_replicas_mode] == ParallelReplicasMode::SAMPLING_KEY)
            return false;

        /// The query could use trivial count if it didn't use parallel replicas, so let's disable it
        query_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
        LOG_TRACE(getLogger("Planner"), "Disabling parallel replicas to be able to use a trivial count optimization");

    }

    /// Set aggregation state
    const AggregateFunctionCount & agg_count = *count_func;
    std::vector<char> state(agg_count.sizeOfData());
    AggregateDataPtr place = state.data();
    agg_count.create(place);
    SCOPE_EXIT_MEMORY_SAFE(agg_count.destroy(place));
    AggregateFunctionCount::set(place, num_rows.value());

    auto column = ColumnAggregateFunction::create(function_node.getAggregateFunction());
    column->insertFrom(place);

    /// Use the aggregate function's action node identifier (e.g. `count()`) as the column
    /// name so the emitted block already matches the header the outer planner expects at
    /// `WithMergeableState`. This lets the caller skip both the rename step and the
    /// recursive `Planner` that was only used to derive the expected header.
    String trivial_count_column_name = calculateActionNodeName(aggregates.front(), planner_context);
    if (trivial_count_column_name.empty())
        trivial_count_column_name = columns_names.front();

    auto block_with_count = std::make_shared<const Block>(Block{
        {std::move(column),
         std::make_shared<DataTypeAggregateFunction>(function_node.getAggregateFunction(), agg_count.getArgumentTypes(), Array{}),
         trivial_count_column_name}});

    auto source = std::make_shared<SourceFromSingleChunk>(block_with_count);
    auto prepared_count = std::make_unique<ReadFromPreparedSource>(Pipe(std::move(source)));
    prepared_count->setStepDescription("Optimized trivial count");
    query_plan.addStep(std::move(prepared_count));

    return true;
}

void prepareBuildQueryPlanForTableExpression(const QueryTreeNodePtr & table_expression, const SelectQueryOptions & select_query_options, PlannerContextPtr & planner_context)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    auto & table_expression_data = planner_context->getTableExpressionDataOrThrow(table_expression);
    auto columns_names = table_expression_data.getColumnNames();

    auto * table_node = table_expression->as<TableNode>();
    auto * table_function_node = table_expression->as<TableFunctionNode>();
    auto * query_node = table_expression->as<QueryNode>();
    auto * union_node = table_expression->as<UnionNode>();

    /** The current user must have the SELECT privilege.
      * We do not check access rights for table functions because they have been already checked in ITableFunction::execute().
      */
    NameSet columns_names_allowed_to_select;
    if (table_node)
    {
        const auto & column_names_with_aliases = table_expression_data.getSelectedColumnsNames();
        columns_names_allowed_to_select = checkAccessRights(*table_node, column_names_with_aliases, query_context);
    }
    else if ((query_node || union_node) && select_query_options.check_subquery_table_access)
    {
        /// Check permissions for all tables referenced in the subquery.
        /// This is needed because in only_analyze mode, subqueries are not recursively planned,
        /// so their permission checks would otherwise be skipped.
        checkAccessRightsForSubquery(table_expression, query_context);
    }

    if (columns_names.empty())
    {
        NameAndTypePair additional_column_to_read;

        if (table_node || table_function_node)
        {
            const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
            const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();
            additional_column_to_read = chooseSmallestColumnToReadFromStorage(storage, storage_snapshot, columns_names_allowed_to_select);
        }
        else if (query_node || union_node)
        {
            const auto & projection_columns = query_node ? query_node->getProjectionColumns() : union_node->computeProjectionColumns();

            if (projection_columns.empty())
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Cannot read from subquery with empty projection");

            NamesAndTypesList projection_columns_list(projection_columns.begin(), projection_columns.end());
            /// Pass skip_subcolumns=false: subquery projection columns are full
            /// query-level outputs (e.g. tup.a from CountDistinctPass rewrite),
            /// not storage meta-subcolumns (.size0, .keys) that should be skipped.
            additional_column_to_read = ExpressionActions::getSmallestColumn(projection_columns_list, /*skip_subcolumns=*/ false);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected table, table function, query or union. Actual {}",
                            table_expression->formatASTForErrorMessage());
        }

        auto & global_planner_context = planner_context->getGlobalPlannerContext();
        if (!table_expression_data.hasColumn(additional_column_to_read.name))
        {
            const auto & column_identifier = global_planner_context->createColumnIdentifierOrGet(additional_column_to_read, table_expression);
            columns_names.push_back(additional_column_to_read.name);
            table_expression_data.addColumn(additional_column_to_read, column_identifier);
        }
    }

    /// Limitation on the number of columns to read
    if (settings[Setting::max_columns_to_read] && columns_names.size() > settings[Setting::max_columns_to_read])
        throw Exception(
            ErrorCodes::TOO_MANY_COLUMNS,
            "Limit for number of columns to read exceeded. Requested: {}, maximum: {}",
            columns_names.size(),
            settings[Setting::max_columns_to_read].value);
}

void updatePrewhereOutputsIfNeeded(SelectQueryInfo & table_expression_query_info,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot)
{
    if (!table_expression_query_info.prewhere_info)
        return;

    auto & prewhere_actions = table_expression_query_info.prewhere_info->prewhere_actions;

    NameSet required_columns;
    if (column_names.size() == 1)
        required_columns.insert(column_names[0]);

    auto & table_expression_modifiers = table_expression_query_info.table_expression_modifiers;
    if (table_expression_modifiers)
    {
        if (table_expression_modifiers->hasSampleSizeRatio()
            || table_expression_query_info.planner_context->getQueryContext()->getSettingsRef()[Setting::parallel_replicas_count] > 1)
        {
            /// We evaluate sampling for Merge lazily so we need to get all the columns
            if (storage_snapshot->storage.getName() == "Merge")
            {
                const auto columns = storage_snapshot->metadata->getColumns().getAll();
                for (const auto & column : columns)
                    required_columns.insert(column.name);
            }
            else
            {
                auto columns_required_for_sampling = storage_snapshot->metadata->getColumnsRequiredForSampling();
                required_columns.insert(columns_required_for_sampling.begin(), columns_required_for_sampling.end());
            }
        }

        if (table_expression_modifiers->hasFinal())
        {
            auto columns_required_for_final = storage_snapshot->metadata->getColumnsRequiredForFinal();
            required_columns.insert(columns_required_for_final.begin(), columns_required_for_final.end());
        }
    }

    std::unordered_set<const ActionsDAG::Node *> required_output_nodes;

    for (const auto * input : prewhere_actions.getInputs())
    {
        if (required_columns.contains(input->result_name))
            required_output_nodes.insert(input);
    }

    if (required_output_nodes.empty())
        return;

    auto & prewhere_outputs = prewhere_actions.getOutputs();
    for (const auto & output : prewhere_outputs)
    {
        auto required_output_node_it = required_output_nodes.find(output);
        if (required_output_node_it == required_output_nodes.end())
            continue;

        required_output_nodes.erase(required_output_node_it);
    }

    prewhere_outputs.insert(prewhere_outputs.end(), required_output_nodes.begin(), required_output_nodes.end());
}

std::optional<FilterDAGInfo> buildRowPolicyFilterIfNeeded(const StoragePtr & storage,
    SelectQueryInfo & table_expression_query_info,
    PlannerContextPtr & planner_context,
    std::set<std::string> & used_row_policies)
{
    const auto & query_context = planner_context->getQueryContext();

    auto row_policy_filter = getEffectiveRowPolicyFilter(storage, query_context);
    if (!row_policy_filter)
        return {};

    for (const auto & row_policy : row_policy_filter->policies)
    {
        auto name = row_policy->getFullName().toString();
        if (query_context->hasQueryContext())
            query_context->getQueryContext()->addUsedRowPolicy(name);
        used_row_policies.emplace(std::move(name));
    }

    return buildFilterInfo(row_policy_filter->expression, table_expression_query_info.table_expression, planner_context);
}

std::optional<FilterDAGInfo> buildCustomKeyFilterIfNeeded(const StoragePtr & storage,
    SelectQueryInfo & table_expression_query_info,
    PlannerContextPtr & planner_context)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    if (settings[Setting::parallel_replicas_count] <= 1 || settings[Setting::parallel_replicas_custom_key].value.empty())
        return {};

    auto custom_key_ast = parseCustomKeyForTable(settings[Setting::parallel_replicas_custom_key], *query_context);
    if (!custom_key_ast)
        throw DB::Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Parallel replicas processing with custom_key has been requested "
                "(setting 'max_parallel_replicas'), but the table does not have custom_key defined for it "
                " or it's invalid (setting 'parallel_replicas_custom_key')");

    LOG_TRACE(getLogger("Planner"), "Processing query on a replica using custom_key '{}'", settings[Setting::parallel_replicas_custom_key].value);

    auto parallel_replicas_custom_filter_ast = getCustomKeyFilterForParallelReplica(
        settings[Setting::parallel_replicas_count],
        settings[Setting::parallel_replica_offset],
        std::move(custom_key_ast),
        {settings[Setting::parallel_replicas_mode],
         settings[Setting::parallel_replicas_custom_key_range_lower],
         settings[Setting::parallel_replicas_custom_key_range_upper]},
        storage->getInMemoryMetadataPtr(query_context, false)->columns,
        query_context);

    return buildFilterInfo(parallel_replicas_custom_filter_ast, table_expression_query_info.table_expression, planner_context);
}

/// Parse `additional_table_filters` for this table expression and assign the AST into
/// `table_expression_query_info.additional_filter_ast`. This is the pure, side-effect-free
/// part of `buildAdditionalFiltersIfNeeded` — no planner-context mutation, no prewhere
/// touch — so it can be called early (before prewhere / row-policy / trivial-count /
/// trivial-limit decisions) and later consumers can simply read the parsed AST.
void parseAdditionalFilterAstIfNeeded(const StoragePtr & storage,
    const String & table_expression_alias,
    SelectQueryInfo & table_expression_query_info,
    const ContextPtr & query_context)
{
    const auto & settings = query_context->getSettingsRef();

    auto const & additional_filters = settings[Setting::additional_table_filters].value;
    if (additional_filters.empty())
        return;

    auto const & storage_id = storage->getStorageID();

    for (const auto & additional_filter : additional_filters)
    {
        const auto & tuple = additional_filter.safeGet<Tuple>();
        auto const & table = tuple.at(0).safeGet<String>();
        auto const & filter = tuple.at(1).safeGet<String>();

        if (table == table_expression_alias ||
            (table == storage_id.getTableName() && query_context->getCurrentDatabase() == storage_id.getDatabaseName()) ||
            (table == storage_id.getFullNameNotQuoted()))
        {
            ParserExpression parser;
            table_expression_query_info.additional_filter_ast = parseQuery(
                parser,
                filter.data(),
                filter.data() + filter.size(),
                "additional filter",
                settings[Setting::max_query_size],
                settings[Setting::max_parser_depth],
                settings[Setting::max_parser_backtracks]);
            return;
        }
    }
}

/// Apply filters from additional_table_filters setting. Expects
/// `parseAdditionalFilterAstIfNeeded` to have been called earlier so
/// `table_expression_query_info.additional_filter_ast` is populated.
std::optional<FilterDAGInfo> buildAdditionalFiltersIfNeeded(
    SelectQueryInfo & table_expression_query_info,
    const PrewhereInfoPtr & prewhere_info,
    PlannerContextPtr & planner_context)
{
    const auto & additional_filter_ast = table_expression_query_info.additional_filter_ast;
    if (!additional_filter_ast)
        return {};

    auto filter_info = buildFilterInfo(additional_filter_ast, table_expression_query_info.table_expression, planner_context);
    if (prewhere_info)
    {
        for (const auto * input : filter_info.actions.getInputs())
            prewhere_info->prewhere_actions.tryRestoreColumn(input->result_name);
    }
    return filter_info;
}

UInt64 mainQueryNodeBlockSizeByLimit(const SelectQueryInfo & select_query_info)
{
    // Since we support negative limit, query node field could potentially be Int64 implying negative value.
    // So, we have to handle to separately
    auto const & main_query_node = select_query_info.query_tree->as<QueryNode const &>();

    /// Constness of limit and offset is validated during query analysis stage
    UInt64 limit_length = 0;
    if (main_query_node.hasLimit())
    {
        const auto & field = main_query_node.getLimit()->as<ConstantNode &>().getValue();

        const bool is_uint64 = !convertFieldToType(field, DataTypeUInt64()).isNull();

        // Negative LIMIT, skip optimization
        if (!is_uint64)
            return 0;

        limit_length = field.safeGet<UInt64>();
    }

    UInt64 limit_offset = 0;
    if (main_query_node.hasOffset())
    {
        const auto & field = main_query_node.getOffset()->as<ConstantNode &>().getValue();
        const bool is_uint64 = !convertFieldToType(field, DataTypeUInt64()).isNull();

        // Negative OFFSET, skip optimization
        if (!is_uint64)
            return 0;

        limit_offset = field.safeGet<UInt64>();
    }

    /** If not specified DISTINCT, WHERE, GROUP BY, HAVING, ORDER BY, JOIN, LIMIT BY, LIMIT WITH TIES
      * but LIMIT is specified with UInt64 value, and limit + offset < max_block_size,
      * then as the block size we will use limit + offset (not to read more from the table than requested),
      * and also set the number of threads to 1.
      */
    if (main_query_node.hasLimit()
        && !main_query_node.isDistinct()
        && !main_query_node.isLimitWithTies()
        && !main_query_node.hasPrewhere()
        && !main_query_node.hasWhere()
        && select_query_info.filter_asts.empty()
        && !main_query_node.hasGroupBy()
        && !main_query_node.hasHaving()
        && !main_query_node.hasOrderBy()
        && !main_query_node.hasLimitBy()
        && !select_query_info.need_aggregate
        && !select_query_info.has_window
        && limit_length <= std::numeric_limits<UInt64>::max() - limit_offset)
        return limit_length + limit_offset;
    return 0;
}

std::unique_ptr<ExpressionStep> createComputeAliasColumnsStep(
    AliasColumnExpressions & alias_column_expressions, const SharedHeader & current_header)
{
    ActionsDAG merged_alias_columns_actions_dag(current_header->getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs action_dag_outputs = merged_alias_columns_actions_dag.getInputs();

    for (auto & alias_column_expression : alias_column_expressions)
    {
        auto & alias_column_actions_dag = alias_column_expression.second;
        const auto & current_outputs = alias_column_actions_dag.getOutputs();
        action_dag_outputs.insert(action_dag_outputs.end(), current_outputs.begin(), current_outputs.end());
        merged_alias_columns_actions_dag.mergeNodes(std::move(alias_column_actions_dag));
    }

    for (const auto * output_node : action_dag_outputs)
        merged_alias_columns_actions_dag.addOrReplaceInOutputs(*output_node);
    merged_alias_columns_actions_dag.removeUnusedActions(false);

    auto alias_column_step = std::make_unique<ExpressionStep>(current_header, std::move(merged_alias_columns_actions_dag));
    alias_column_step->setStepDescription("Compute alias columns");
    return alias_column_step;
}

JoinTreeQueryPlan buildQueryPlanForTableExpression(QueryTreeNodePtr table_expression,
    const QueryTreeNodePtr & parent_join_tree,
    const SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContextPtr & planner_context,
    bool is_single_table_expression,
    bool wrap_read_columns_in_subquery)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    auto & table_expression_data = planner_context->getTableExpressionDataOrThrow(table_expression);

    QueryProcessingStage::Enum till_stage = QueryProcessingStage::Enum::FetchColumns;

    if (wrap_read_columns_in_subquery)
    {
        auto columns = table_expression_data.getColumns();
        table_expression = buildSubqueryToReadColumnsFromTableExpression(columns, table_expression, query_context);
    }

    auto * table_node = table_expression->as<TableNode>();
    auto * table_function_node = table_expression->as<TableFunctionNode>();
    auto * query_node = table_expression->as<QueryNode>();
    auto * union_node = table_expression->as<UnionNode>();

    /// Hoisted to function scope so the rename block below can skip the recursive
    /// `Planner` when trivial count produced a header that already matches the
    /// expected one.
    bool is_trivial_count_applied = false;

    QueryPlan query_plan;
    std::unordered_map<const QueryNode *, const QueryPlan::Node *> query_node_to_plan_step_mapping;
    std::set<std::string> used_row_policies;
    UsefulSets useful_sets;

    if (table_node || table_function_node)
    {
        const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
        const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();

        auto table_expression_query_info = select_query_info;
        table_expression_query_info.table_expression = table_expression;
        if (const auto & filter_actions = table_expression_data.getFilterActions())
            table_expression_query_info.filter_actions_dag = std::make_shared<const ActionsDAG>(filter_actions->clone());

        /// Parse additional_table_filters early so that later decisions (trivial-count,
        /// trivial-limit) can see `additional_filter_ast` before the actual filter DAG
        /// is built further down
        ///
        /// Skip under `only_analyze`, since we may not have the database in case of Distributed.
        if (!select_query_options.only_analyze)
            parseAdditionalFilterAstIfNeeded(
                storage, table_expression->getOriginalAlias(), table_expression_query_info, query_context);

        const size_t memory_limited_max_threads = getMaxThreadsForAvailableMemory(
            settings[Setting::max_threads], settings[Setting::max_threads_min_free_memory_per_thread]);
        size_t max_streams = memory_limited_max_threads;
        size_t max_threads_execute_query = memory_limited_max_threads;

        /**
         * To simultaneously query more remote servers when async_socket_for_remote is off
         * instead of max_threads, max_distributed_connections is used:
         * since threads there mostly spend time waiting for data from remote servers,
         * we can increase the degree of parallelism to avoid sequential querying of remote servers.
         *
         * DANGER: that can lead to insane number of threads working if there are a lot of stream and prefer_localhost_replica is used.
         *
         * That is not needed when async_socket_for_remote is on, because in that case
         * threads are not blocked waiting for data from remote servers.
         *
         */
        bool is_sync_remote = table_expression_data.isRemote() && !settings[Setting::async_socket_for_remote];
        if (is_sync_remote)
        {
            max_streams = settings[Setting::max_distributed_connections];
            max_threads_execute_query = settings[Setting::max_distributed_connections];
        }

        UInt64 max_block_size = settings[Setting::max_block_size];
        UInt64 max_block_size_limited = 0;
        if (is_single_table_expression && !select_query_options.only_analyze)
        {
            /** If not specified DISTINCT, WHERE, GROUP BY, HAVING, ORDER BY, JOIN, LIMIT BY, LIMIT WITH TIES
              * but LIMIT is specified, and limit + offset < max_block_size,
              * then as the block size we will use limit + offset (not to read more from the table than requested),
              * and also set the number of threads to 1.
              */
            /// Use the same effective-filter checks as the row-policy / additional-filter
            /// planning further down: the trivial-LIMIT optimization must be disabled
            /// whenever those filters actually apply, so the flags must agree.
            bool has_additional_filters = !!table_expression_query_info.additional_filter_ast
                || !!getEffectiveRowPolicyFilter(storage, query_context);
            if (!has_additional_filters)
                max_block_size_limited = mainQueryNodeBlockSizeByLimit(select_query_info);
            if (max_block_size_limited)
            {
                if (max_block_size_limited < max_block_size)
                {
                    max_block_size = std::max<UInt64>(1, max_block_size_limited);
                    max_streams = 1;
                    max_threads_execute_query = 1;
                }

                if (select_query_info.local_storage_limits.local_limits.size_limits.max_rows != 0)
                {
                    if (max_block_size_limited < select_query_info.local_storage_limits.local_limits.size_limits.max_rows)
                        table_expression_query_info.trivial_limit = max_block_size_limited;
                    /// Ask to read just enough rows to make the max_rows limit effective (so it has a chance to be triggered).
                    else if (select_query_info.local_storage_limits.local_limits.size_limits.max_rows < std::numeric_limits<UInt64>::max())
                        table_expression_query_info.trivial_limit = 1 + select_query_info.local_storage_limits.local_limits.size_limits.max_rows;
                }
                else
                {
                    table_expression_query_info.trivial_limit = max_block_size_limited;
                }
            }

            if (!max_block_size)
                throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND,
                    "Setting 'max_block_size' cannot be zero");
        }

        /// If necessary, we request more sources than the number of threads - to distribute the work evenly over the threads
        if (max_streams > 1 && !is_sync_remote)
        {
            if (auto streams_with_ratio = static_cast<double>(max_streams) * static_cast<double>(settings[Setting::max_streams_to_max_threads_ratio]);
                canConvertTo<size_t>(streams_with_ratio))
                max_streams = static_cast<size_t>(streams_with_ratio);
            else
                throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND,
                    "Exceeded limit for `max_streams` with `max_streams_to_max_threads_ratio`. "
                    "Make sure that `max_streams * max_streams_to_max_threads_ratio` is in some reasonable boundaries, current value: {}",
                    streams_with_ratio);
        }

        if (max_streams == 0)
            max_streams = 1;

        if (table_node)
            table_expression_query_info.table_expression_modifiers = table_node->getTableExpressionModifiers();
        else
            table_expression_query_info.table_expression_modifiers = table_function_node->getTableExpressionModifiers();

        bool need_rewrite_query_with_final = storage->needRewriteQueryWithFinal(table_expression_data.getColumnNames());
        if (need_rewrite_query_with_final)
        {
            if (table_expression_query_info.table_expression_modifiers)
            {
                const auto & table_expression_modifiers = table_expression_query_info.table_expression_modifiers;
                auto sample_size_ratio = table_expression_modifiers->getSampleSizeRatio();
                auto sample_offset_ratio = table_expression_modifiers->getSampleOffsetRatio();

                table_expression_query_info.table_expression_modifiers = TableExpressionModifiers(true /*has_final*/,
                    sample_size_ratio,
                    sample_offset_ratio);
            }
            else
            {
                table_expression_query_info.table_expression_modifiers = TableExpressionModifiers(true /*has_final*/,
                    {} /*sample_size_ratio*/,
                    {} /*sample_offset_ratio*/);
            }
        }

        /// Apply trivial_count optimization if possible
        is_trivial_count_applied = !select_query_options.only_analyze && !select_query_options.build_logical_plan && is_single_table_expression
            && (table_node || table_function_node) && select_query_info.has_aggregates
            && applyTrivialCountIfPossible(
                query_plan,
                table_expression_query_info,
                table_node,
                table_function_node,
                select_query_info.query_tree,
                planner_context->getMutableQueryContext(),
                table_expression_data.getColumnNames(),
                *planner_context);

        if (is_trivial_count_applied)
        {
            till_stage = QueryProcessingStage::WithMergeableState;
        }
        else
        {
            if (!select_query_options.only_analyze)
            {
                auto & row_level_filter = table_expression_query_info.row_level_filter;
                auto & prewhere_info = table_expression_query_info.prewhere_info;
                const auto & prewhere_actions = table_expression_data.getPrewhereFilterActions();
                const auto & columns_names = table_expression_data.getColumnNames();

                std::vector<std::pair<FilterDAGInfo, DescriptionHolderPtr>> where_filters;

                if (prewhere_actions && select_query_options.build_logical_plan)
                {
                    /// Collect columns needed by row policy and additional filters
                    NameSet columns_needed_by_other_filters;

                    /// Pre-build additional table filter to know what columns it needs
                    auto additional_filters_info_temp = buildAdditionalFiltersIfNeeded(
                        table_expression_query_info, prewhere_info, planner_context);
                    if (additional_filters_info_temp)
                    {
                        for (const auto * input : additional_filters_info_temp->actions.getInputs())
                            columns_needed_by_other_filters.insert(input->result_name);
                    }

                    /// Clone prewhere actions and add required columns to outputs
                    auto prewhere_actions_clone = prewhere_actions->clone();
                    const auto prewhere_column_name = prewhere_actions_clone.getOutputs().at(0)->result_name;

                    /// Add columns needed by other filters to prewhere outputs
                    auto & prewhere_outputs = prewhere_actions_clone.getOutputs();

                    /// Build set of existing outputs for fast lookup
                    std::unordered_set<const ActionsDAG::Node *> existing_outputs(
                        prewhere_outputs.begin(), prewhere_outputs.end());

                    /// Iterate inputs in deterministic order and add missing nodes
                    for (const auto * input : prewhere_actions_clone.getInputs())
                    {
                        if (columns_needed_by_other_filters.contains(input->result_name)
                            && !existing_outputs.contains(input))
                        {
                            prewhere_outputs.push_back(input);
                        }
                    }

                    /// Check if prewhere filter column should be removed
                    const bool keep_for_query = std::ranges::contains(columns_names, prewhere_column_name);
                    const bool keep_for_filters = columns_needed_by_other_filters.contains(prewhere_column_name);
                    const bool remove_prewhere_column = !keep_for_query && !keep_for_filters;

                    where_filters.emplace_back(
                        FilterDAGInfo{
                            std::move(prewhere_actions_clone),
                            prewhere_column_name,
                            remove_prewhere_column},
                        makeDescription("Prewhere"));
                }
                else if (prewhere_actions)
                {
                    prewhere_info = std::make_shared<PrewhereInfo>();
                    prewhere_info->prewhere_actions = prewhere_actions->clone();
                    prewhere_info->prewhere_column_name = prewhere_actions->getOutputs().at(0)->result_name;
                    /// Do not remove prewhere column if it is needed later
                    bool keep_prewhere_column = std::ranges::contains(columns_names, prewhere_info->prewhere_column_name);
                    prewhere_info->remove_prewhere_column = !keep_prewhere_column;
                    prewhere_info->need_filter = true;
                }

                updatePrewhereOutputsIfNeeded(table_expression_query_info, table_expression_data.getColumnNames(), storage_snapshot);

                auto row_policy_filter_info
                    = buildRowPolicyFilterIfNeeded(storage, table_expression_query_info, planner_context, used_row_policies);
                if (row_policy_filter_info)
                {
                    table_expression_data.setRowLevelFilterActions(row_policy_filter_info->actions.clone());
                    /// TODO: Never put row-level security filter in WHERE clause for storages that do not support PREWHERE to avoid merging of filters.
                    if (storage->supportsPrewhere())
                        row_level_filter = std::make_shared<FilterDAGInfo>(std::move(*row_policy_filter_info));
                    else
                        where_filters.emplace_back(std::move(*row_policy_filter_info), makeDescription("Row-level security filter"));
                }

                if (query_context->canUseParallelReplicasCustomKey())
                {
                    if (settings[Setting::parallel_replicas_count] > 1)
                    {
                        if (auto parallel_replicas_custom_key_filter_info= buildCustomKeyFilterIfNeeded(storage, table_expression_query_info, planner_context))
                            where_filters.emplace_back(std::move(*parallel_replicas_custom_key_filter_info), makeDescription("Parallel replicas custom key filter"));
                    }
                    else if (auto * distributed = typeid_cast<StorageDistributed *>(storage.get());
                             distributed && query_context->canUseParallelReplicasCustomKeyForCluster(*distributed->getCluster()))
                    {
                        planner_context->getMutableQueryContext()->setSetting("distributed_group_by_no_merge", 2);
                        /// We disable prefer_localhost_replica because if one of the replicas is local it will create a single local plan
                        /// instead of executing the query with multiple replicas
                        /// We can enable this setting again for custom key parallel replicas when we can generate a plan that will use both a
                        /// local plan and remote replicas
                        planner_context->getMutableQueryContext()->setSetting("prefer_localhost_replica", Field{0});
                    }
                }

                if (auto additional_filters_info = buildAdditionalFiltersIfNeeded(table_expression_query_info, prewhere_info, planner_context))
                {
                    appendSetsFromActionsDAG(additional_filters_info->actions, useful_sets);
                    where_filters.emplace_back(std::move(*additional_filters_info), makeDescription("additional filter"));
                }

                if (!select_query_options.build_logical_plan)
                    till_stage = storage->getQueryProcessingStage(
                        query_context, select_query_options.to_stage, storage_snapshot, table_expression_query_info);

                if (select_query_options.build_logical_plan)
                {
                    auto sample_block = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(columns_names));

                    if (table_node)
                    {
                        String table_name;
                        if (!table_node->getTemporaryTableName().empty())
                            table_name = table_node->getTemporaryTableName();
                        else
                            table_name = table_node->getStorageID().getFullTableName();

                        auto reading_from_table = std::make_unique<ReadFromTableStep>(
                            sample_block,
                            table_name,
                            table_expression_query_info.table_expression_modifiers.value_or(TableExpressionModifiers{}));

                        query_plan.addStep(std::move(reading_from_table));
                    }
                    else if (table_function_node)
                    {
                        auto table_function_ast = table_function_node->toAST();
                        table_function_ast->setAlias({});

                        WriteBufferFromOwnString out;
                        IAST::FormatSettings format_settings(
                            /*one_line=*/true,
                            IdentifierQuotingRule::WhenNecessary,
                            IdentifierQuotingStyle::Backticks,
                            /*show_secrets_=*/false);

                        table_function_ast->format(out, format_settings);

                        auto table_function_serialized_ast = std::move(out.str());

                        auto reading_from_table_function = std::make_unique<ReadFromTableFunctionStep>(
                            sample_block,
                            std::move(table_function_serialized_ast),
                            table_expression_query_info.table_expression_modifiers.value_or(TableExpressionModifiers{}));

                        query_plan.addStep(std::move(reading_from_table_function));
                    }
                }
                else
                {
                    /// It is just a safety check needed until we have a proper sending plan to replicas.
                    /// If we have a non-trivial storage like View it might create its own Planner inside read(), run findTableForParallelReplicas()
                    /// and find some other table that might be used for reading with parallel replicas. It will lead to errors.
                    const bool no_tables_or_another_table_chosen_for_reading_with_parallel_replicas_mode
                        = query_context->canUseParallelReplicasOnFollower()
                        && table_node != planner_context->getGlobalPlannerContext()->parallel_replicas_table;
                    if (no_tables_or_another_table_chosen_for_reading_with_parallel_replicas_mode)
                    {
                        bool disable_parallel_replicas_for_storage = true;
                        ContextPtr updated_context = query_context;
                        if (const UnionNode * table_union = planner_context->getGlobalPlannerContext()->parallel_replicas_table_union)
                        {
                            SelectQueryOptions options;
                            for (const auto & child : table_union->getQueries().getNodes())
                            {
                                if (table_node == findTableForParallelReplicas(child, options))
                                {
                                    disable_parallel_replicas_for_storage = false;
                                    break;
                                }
                            }
                        }

                        if (disable_parallel_replicas_for_storage)
                        {
                            auto mutable_context = Context::createCopy(query_context);
                            mutable_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
                            updated_context = mutable_context;
                        }

                        storage->read(
                            query_plan,
                            columns_names,
                            storage_snapshot,
                            table_expression_query_info,
                            std::move(updated_context),
                            till_stage,
                            max_block_size,
                            max_streams);
                    }
                    else
                    {
                        storage->read(
                            query_plan,
                            columns_names,
                            storage_snapshot,
                            table_expression_query_info,
                            query_context,
                            till_stage,
                            max_block_size,
                            max_streams);
                    }
                }

                auto parallel_replicas_enabled_for_storage
                    = [](const StoragePtr & current_storage, const ContextPtr & context, const Settings & query_settings)
                {
                    const auto * table_ptr = current_storage.get();

                    if (query_settings[Setting::parallel_replicas_allow_view_over_mergetree])
                    {
                        const auto * view = typeid_cast<const StorageView *>(current_storage.get());
                        if (view)
                        {
                            auto underlying_storage = view->getUnderlyingMergeTreeStorageForParallelReplicas(context);
                            if (!underlying_storage)
                                return false;

                            table_ptr = underlying_storage.get();
                        }
                    }

                    const auto * mv = typeid_cast<const StorageMaterializedView *>(current_storage.get());
                    if (mv)
                    {
                        if (!query_settings[Setting::parallel_replicas_allow_materialized_views])
                            return false;

                        // address refreshable MVs separately, currently leads to logical error
                        if (mv->isRefreshable())
                            return false;

                        table_ptr = mv->getTargetTable().get();
                    }

                    if (!table_ptr->isMergeTree())
                        return false;

                    if (!table_ptr->supportsReplication() && !query_settings[Setting::parallel_replicas_for_non_replicated_merge_tree])
                        return false;

                    return true;
                };

                /// query_plan can be empty if there is nothing to read
                if (query_plan.isInitialized() && !select_query_options.build_logical_plan
                    && parallel_replicas_enabled_for_storage(storage, query_context, settings))
                {
                    /// we need to decide if parallel replicas is supported for join tree while visiting left table expression
                    /// therefore, here both join sides are analysed
                    auto allow_parallel_replicas_for_join_tree
                        = [&parallel_replicas_enabled_for_storage](const QueryTreeNodePtr & join_tree_node, const ContextPtr & context, const Settings & query_settings)
                    {
                        if (join_tree_node->as<CrossJoinNode>())
                            return false;

                        const JoinNode * join_node = join_tree_node->as<JoinNode>();
                        if (!join_node)
                            return true;

                        const auto & left_table_expr = join_node->getLeftTableExpression();
                        const auto * left_table = typeid_cast<const TableNode *>(left_table_expr.get());
                        if (left_table && left_table->getStorage()->isView())
                            return false;

                        const auto join_kind = join_node->getKind();
                        const auto join_strictness = join_node->getStrictness();
                        if ((join_kind == JoinKind::Inner && join_strictness == JoinStrictness::All) || join_kind == JoinKind::Left)
                        {
                            // check that left table expression can be used for parallel replicas
                            if (left_table)
                                return parallel_replicas_enabled_for_storage(left_table->getStorage(), context, query_settings);

                            const auto * left_table_function = left_table_expr->as<TableFunctionNode>();
                            if (left_table_function)
                                return parallel_replicas_enabled_for_storage(left_table_function->getStorage(), context, query_settings);

                            // check if left one is not subquery
                            return left_table_expr->getNodeType() != QueryTreeNodeType::QUERY
                                && left_table_expr->getNodeType() != QueryTreeNodeType::UNION
                                && left_table_expr->getNodeType() != QueryTreeNodeType::JOIN
                                && left_table_expr->getNodeType() != QueryTreeNodeType::ARRAY_JOIN
                                && left_table_expr->getNodeType() != QueryTreeNodeType::CROSS_JOIN;
                        }

                        if (join_kind == JoinKind::Right)
                        {
                            // parallel replicas is allowed only simple RIGHT JOINs i.e. t1 RIGHT JOIN t2
                            if (left_table_expr->getNodeType() != QueryTreeNodeType::TABLE
                                && left_table_expr->getNodeType() != QueryTreeNodeType::TABLE_FUNCTION)
                                return false;

                            const auto & right_table_expr = join_node->getRightTableExpression();
                            const auto * right_table = right_table_expr->as<TableNode>();
                            const auto * right_table_function = right_table_expr->as<TableFunctionNode>();
                            if (!right_table && !right_table_function)
                                return false;

                            const auto right_storage = right_table ? right_table->getStorage() : right_table_function->getStorage();
                            if (parallel_replicas_enabled_for_storage(right_storage, context, query_settings))
                            {
                                const auto * left_table_function = left_table_expr->as<TableFunctionNode>();
                                const auto left_storage = (left_table ? left_table->getStorage() : left_table_function->getStorage());
                                if (!parallel_replicas_enabled_for_storage(left_storage, context, query_settings))
                                    // TODO: support parallel replicas for (non_mt_table RIGHT JOIN mt_table) later
                                    return false;

                                return true;
                            }
                        }

                        return false;
                    };

                    if (query_context->canUseParallelReplicasCustomKey() && query_context->getClientInfo().distributed_depth == 0)
                    {
                        if (auto cluster = query_context->getClusterForParallelReplicas();
                            query_context->canUseParallelReplicasCustomKeyForCluster(*cluster))
                        {
                            planner_context->getMutableQueryContext()->setSetting("prefer_localhost_replica", Field{0});
                            auto modified_query_info = select_query_info;
                            modified_query_info.cluster = std::move(cluster);
                            till_stage = QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;
                            QueryPlan query_plan_parallel_replicas;
                            ClusterProxy::executeQueryWithParallelReplicasCustomKey(
                                query_plan_parallel_replicas,
                                storage->getStorageID(),
                                modified_query_info,
                                storage->getInMemoryMetadataPtr(query_context, false)->getColumns(),
                                storage_snapshot,
                                till_stage,
                                table_expression_query_info.query_tree,
                                query_context);
                            query_plan = std::move(query_plan_parallel_replicas);
                        }
                    }
                    else if (
                        ClusterProxy::canUseParallelReplicasOnInitiator(query_context)
                        && allow_parallel_replicas_for_join_tree(parent_join_tree, query_context, settings))
                    {
                        // (1) find read step
                        QueryPlan::Node * node = query_plan.getRootNode();
                        ReadFromMergeTree * reading = nullptr;
                        while (node)
                        {
                            reading = typeid_cast<ReadFromMergeTree *>(node->step.get());
                            if (reading)
                                break;

                            /// Empty table or all data pruned — nothing to read, skip parallel replicas.
                            if (typeid_cast<ReadNothingStep *>(node->step.get()))
                                break;

                            QueryPlan::Node * prev_node = node;
                            if (!node->children.empty())
                            {
                                node = node->children.at(0);
                            }
                            else
                            {
                                throw Exception(
                                    ErrorCodes::LOGICAL_ERROR,
                                    "Step is expected to be ReadFromMergeTree but it's {}",
                                    prev_node->step->getName());
                            }
                        }

                        // (2) if it's ReadFromMergeTree - run index analysis and check number of rows to read
                        if (reading && settings[Setting::parallel_replicas_min_number_of_rows_per_replica] > 0)
                        {
                            auto result_ptr = reading->selectRangesToRead();
                            UInt64 rows_to_read = result_ptr->selected_rows;

                            if (table_expression_query_info.trivial_limit > 0 && table_expression_query_info.trivial_limit < rows_to_read)
                                rows_to_read = table_expression_query_info.trivial_limit;

                            if (max_block_size_limited && (max_block_size_limited < rows_to_read))
                                rows_to_read = max_block_size_limited;

                            const size_t number_of_replicas_to_use
                                = rows_to_read / settings[Setting::parallel_replicas_min_number_of_rows_per_replica];
                            LOG_TRACE(
                                getLogger("Planner"),
                                "Estimated {} rows to read. It is enough work for {} parallel replicas",
                                rows_to_read,
                                number_of_replicas_to_use);

                            if (number_of_replicas_to_use <= 1)
                            {
                                planner_context->getMutableQueryContext()->setSetting(
                                    "allow_experimental_parallel_reading_from_replicas", Field(0));
                                planner_context->getMutableQueryContext()->setSetting("max_parallel_replicas", UInt64{1});
                                LOG_DEBUG(getLogger("Planner"), "Disabling parallel replicas because there aren't enough rows to read");
                            }
                            else if (number_of_replicas_to_use < settings[Setting::max_parallel_replicas])
                            {
                                planner_context->getMutableQueryContext()->setSetting("max_parallel_replicas", number_of_replicas_to_use);
                                LOG_DEBUG(getLogger("Planner"), "Reducing the number of replicas to use to {}", number_of_replicas_to_use);
                            }
                        }

                        // (3) if parallel replicas still enabled - replace reading step
                        if (reading && planner_context->getQueryContext()->canUseParallelReplicasOnInitiator())
                        {
                            till_stage = QueryProcessingStage::WithMergeableState;
                            QueryPlan query_plan_parallel_replicas;
                            QueryPlanStepPtr reading_step = std::move(node->step);
                            ClusterProxy::executeQueryWithParallelReplicas(
                                query_plan_parallel_replicas,
                                storage->getStorageID(),
                                till_stage,
                                table_expression_query_info.query_tree,
                                table_expression_query_info.planner_context,
                                query_context,
                                table_expression_query_info.storage_limits,
                                std::move(reading_step));
                            query_plan = std::move(query_plan_parallel_replicas);
                        }
                        else
                        {
                            QueryPlan query_plan_no_parallel_replicas;
                            storage->read(
                                query_plan_no_parallel_replicas,
                                columns_names,
                                storage_snapshot,
                                table_expression_query_info,
                                query_context,
                                till_stage,
                                max_block_size,
                                max_streams);
                            query_plan = std::move(query_plan_no_parallel_replicas);
                        }
                    }
                }

                auto & alias_column_expressions = table_expression_data.getAliasColumnExpressions();
                if (!alias_column_expressions.empty() && query_plan.isInitialized() && till_stage == QueryProcessingStage::FetchColumns)
                {
                    auto alias_column_step = createComputeAliasColumnsStep(alias_column_expressions, query_plan.getCurrentHeader());
                    query_plan.addStep(std::move(alias_column_step));
                }

                for (auto && [filter_info, description] : where_filters)
                {
                    if (query_plan.isInitialized() &&
                        till_stage == QueryProcessingStage::FetchColumns)
                    {
                        auto filter_step = std::make_unique<FilterStep>(query_plan.getCurrentHeader(),
                            std::move(filter_info.actions),
                            filter_info.column_name,
                            filter_info.do_remove_column);
                        description->setStepDescription(*filter_step);
                        query_plan.addStep(std::move(filter_step));
                    }
                }

                if (query_context->hasQueryContext() && !select_query_options.is_internal)
                {
                    auto local_storage_id = storage->getStorageID();
                    query_context->getQueryContext()->addQueryAccessInfo(
                        backQuoteIfNeed(local_storage_id.getDatabaseName()),
                        local_storage_id.getFullTableName(),
                        columns_names);
                }
            }

            if (query_plan.isInitialized())
            {
                /** Specify the number of threads only if it wasn't specified in storage.
                  *
                  * But in case of remote query and prefer_localhost_replica=1 (default)
                  * The inner local query (that is done in the same process, without
                  * network interaction), it will setMaxThreads earlier and distributed
                  * query will not update it.
                  */
                if (!query_plan.getMaxThreads() || is_sync_remote)
                    query_plan.setMaxThreads(max_threads_execute_query);

                query_plan.setConcurrencyControl(settings[Setting::use_concurrency_control]);
            }
            else
            {
                /// Create step which reads from empty source if storage has no data.
                const auto & column_names = table_expression_data.getColumnNames();
                auto source_header = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names));
                auto read_nothing = std::make_unique<ReadNothingStep>(source_header);
                read_nothing->setStepDescription("Read from NullSource");
                query_plan.addStep(std::move(read_nothing));
                query_plan.setMaxThreads(max_threads_execute_query);

                auto & alias_column_expressions = table_expression_data.getAliasColumnExpressions();
                if (!alias_column_expressions.empty())
                {
                    auto alias_column_step = createComputeAliasColumnsStep(alias_column_expressions, query_plan.getCurrentHeader());
                    query_plan.addStep(std::move(alias_column_step));
                }
            }
        }
    }
    else if (query_node || union_node)
    {
        if (select_query_options.only_analyze)
        {
            auto projection_columns = query_node ? query_node->getProjectionColumns() : union_node->computeProjectionColumns();
            Block source_header;
            for (auto & projection_column : projection_columns)
                source_header.insert(ColumnWithTypeAndName(projection_column.type, projection_column.name));

            auto read_nothing = std::make_unique<ReadNothingStep>(std::make_shared<const Block>(source_header));
            read_nothing->setStepDescription("Read from NullSource");
            query_plan.addStep(std::move(read_nothing));
        }
        else
        {
            std::shared_ptr<GlobalPlannerContext> subquery_planner_context;
            if (wrap_read_columns_in_subquery)
                subquery_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{});
            else
                subquery_planner_context = planner_context->getGlobalPlannerContext();

            auto subquery_options = select_query_options.subquery();
            Planner subquery_planner(table_expression, subquery_options, subquery_planner_context);
            /// Propagate storage limits to subquery
            subquery_planner.addStorageLimits(*select_query_info.storage_limits);
            subquery_planner.buildQueryPlanIfNeeded();
            const auto & mapping = subquery_planner.getQueryNodeToPlanStepMapping();
            query_node_to_plan_step_mapping.insert(mapping.begin(), mapping.end());
            query_plan = std::move(subquery_planner).extractQueryPlan();
        }

        auto & alias_column_expressions = table_expression_data.getAliasColumnExpressions();
        if (!alias_column_expressions.empty() && query_plan.isInitialized() && till_stage == QueryProcessingStage::FetchColumns)
        {
            auto alias_column_step = createComputeAliasColumnsStep(alias_column_expressions, query_plan.getCurrentHeader());
            query_plan.addStep(std::move(alias_column_step));
        }
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected table, table function, query or union. Actual {}",
                        table_expression->formatASTForErrorMessage());
    }

    if (till_stage == QueryProcessingStage::FetchColumns)
    {
        ActionsDAG rename_actions_dag(query_plan.getCurrentHeader()->getColumnsWithTypeAndName());
        ActionsDAG::NodeRawConstPtrs updated_actions_dag_outputs;

        for (auto & output_node : rename_actions_dag.getOutputs())
        {
            if (select_query_options.ignore_rename_columns)
            {
                /// In case of plan serialization, only storage source column names are required.
                /// Still, Interpreter up to FetchColumns is created for this (to support distributed over distributed).
                /// Apparently, FetchColumns returns not the source columns, but identifiers (with prefix e.g. __table1.)
                /// So, here (under the special option) we rename back. Hopefully this will be removed someday.
                const auto * column_name = table_expression_data.getColumnNameOrNull(output_node->result_name);
                if (!column_name)
                    updated_actions_dag_outputs.push_back(output_node);
                else
                    updated_actions_dag_outputs.push_back(&rename_actions_dag.addAlias(*output_node, *column_name));
            }
            else
            {
                const auto * column_identifier = table_expression_data.getColumnIdentifierOrNull(output_node->result_name);
                if (!column_identifier)
                {
                    /// This is needed only for distributed over distributed case with plan serialization as well.
                    /// StorageDistributed::read apparently returns column identifiers instead of column names for
                    /// to_stage == QueryProcessingStage::FetchColumns (unlike other storages, which do not aware about identifiers).
                    /// So, we do not rename but just pass names as is.
                    ///
                    /// Overall, IStorage::read    -> FetchColumns returns normal column names (except Distributed, which is inconsistent)
                    /// Interpreter::getQueryPlan  -> FetchColumns returns identifiers (why?) and this the reason for the bug ^ in Distributed
                    /// Hopefully there is no other case when we read from Distributed up to FetchColumns.
                    if (table_node && table_node->getStorage()->isRemote() && select_query_options.to_stage == QueryProcessingStage::FetchColumns)
                        updated_actions_dag_outputs.push_back(output_node);
                }
                else
                    updated_actions_dag_outputs.push_back(&rename_actions_dag.addAlias(*output_node, *column_identifier));
            }
        }

        rename_actions_dag.getOutputs() = std::move(updated_actions_dag_outputs);

        auto rename_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(rename_actions_dag));
        rename_step->setStepDescription(select_query_options.ignore_rename_columns
            ? "Change column identifiers to column names"
            : "Change column names to column identifiers");

        query_plan.addStep(std::move(rename_step));
    }
    else if (!is_trivial_count_applied)
    {
        /// We need to know the header that the outer planner expects at `till_stage` so we
        /// can insert a rename if the local plan emits different column names. The cheap
        /// way to compute it is to run the outer query through the planner under
        /// `only_analyze` (it skips the actual storage read) and read back its header.
        ///
        /// Trivial count already emits the column with the aggregate's action-node name
        /// (see `applyTrivialCountIfPossible`), so the structure matches the expected
        /// header by construction and we can skip the recursive planner entirely.
        SelectQueryOptions analyze_query_options = SelectQueryOptions(till_stage).analyze();
        Planner planner(select_query_info.query_tree,
            analyze_query_options,
            select_query_info.planner_context);
        planner.buildQueryPlanIfNeeded();

        auto expected_header = planner.getQueryPlan().getCurrentHeader();

        if (!blocksHaveEqualStructure(*query_plan.getCurrentHeader(), *expected_header))
        {
            auto expected_block = *expected_header;
            materializeBlockInplace(expected_block);

            auto rename_actions_dag = ActionsDAG::makeConvertingActions(
                query_plan.getCurrentHeader()->getColumnsWithTypeAndName(),
                expected_block.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Position,
                planner_context->getQueryContext(),
                true /*ignore_constant_values*/,
                false /*add_cast_columns*/,
                nullptr /*new_names*/);
            auto rename_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(rename_actions_dag));
            if (table_expression_data.isRemote())
                rename_step->setStepDescription("Change remote column names to local column names");
            else
                rename_step->setStepDescription("Change column names");
            query_plan.addStep(std::move(rename_step));
        }
    }

    return JoinTreeQueryPlan{
        .query_plan = std::move(query_plan),
        .stage = till_stage,
        .used_row_policies = std::move(used_row_policies),
        .useful_sets = std::move(useful_sets),
        .query_node_to_plan_step_mapping = std::move(query_node_to_plan_step_mapping),
    };
}


JoinTreeQueryPlan joinPlansWithStep(
    QueryPlanStepPtr join_step,
    JoinTreeQueryPlan left_join_tree_query_plan,
    JoinTreeQueryPlan right_join_tree_query_plan)
{
    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(left_join_tree_query_plan.query_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(right_join_tree_query_plan.query_plan)));

    QueryPlan result_plan;
    result_plan.unitePlans(std::move(join_step), {std::move(plans)});

    /// Collect all required row_policies and actions sets from left and right join tree query plans

    auto result_used_row_policies = std::move(left_join_tree_query_plan.used_row_policies);
    for (const auto & right_join_tree_query_plan_row_policy : right_join_tree_query_plan.used_row_policies)
        result_used_row_policies.insert(right_join_tree_query_plan_row_policy);

    auto result_useful_sets = std::move(left_join_tree_query_plan.useful_sets);
    for (const auto & useful_set : right_join_tree_query_plan.useful_sets)
        result_useful_sets.insert(useful_set);

    auto result_mapping = std::move(left_join_tree_query_plan.query_node_to_plan_step_mapping);
    const auto & r_mapping = right_join_tree_query_plan.query_node_to_plan_step_mapping;
    result_mapping.insert(r_mapping.begin(), r_mapping.end());

    return JoinTreeQueryPlan{
        .query_plan = std::move(result_plan),
        .stage = QueryProcessingStage::FetchColumns,
        .used_row_policies = std::move(result_used_row_policies),
        .useful_sets = std::move(result_useful_sets),
        .query_node_to_plan_step_mapping = std::move(result_mapping),
    };
}

JoinTreeQueryPlan buildQueryPlanForCrossJoinNode(
    const QueryTreeNodePtr & join_table_expression,
    std::vector<JoinTreeQueryPlan> plans,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context)
{
    auto & cross_join_node = join_table_expression->as<CrossJoinNode &>();
    for (const auto & plan : plans)
    {
        if (plan.stage != QueryProcessingStage::FetchColumns)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "JOIN {} table expression expected to process query to fetch columns stage. Actual {}",
                cross_join_node.formatASTForErrorMessage(),
                QueryProcessingStage::toString(plan.stage));
    }

    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    const auto & table_expressions = cross_join_node.getTableExpressions();
    bool display_internal_aliases = settings[Setting::query_plan_display_internal_aliases];

    auto left_join_tree_query_plan = std::move(plans[0]);
    auto left_table_label = getQueryDisplayLabel(table_expressions.at(0), display_internal_aliases);

    for (size_t i = 1; i < plans.size(); ++i)
    {
        auto right_join_tree_query_plan = std::move(plans[i]);

        const auto & left_header = left_join_tree_query_plan.query_plan.getCurrentHeader();
        const auto & right_header = right_join_tree_query_plan.query_plan.getCurrentHeader();
        JoinExpressionActions join_expression_actions(*left_header, *right_header);
        auto join_step_logical = std::make_unique<JoinStepLogical>(
            left_header,
            right_header,
            JoinOperator{JoinKind::Cross},
            std::move(join_expression_actions),
            outer_scope_columns,
            std::unordered_map<String, const ActionsDAG::Node *>{},
            settings[Setting::join_use_nulls],
            JoinSettings(settings),
            SortingStep::Settings(settings));

        auto right_table_label = getQueryDisplayLabel(table_expressions.at(i), display_internal_aliases);
        join_step_logical->setInputLabels(std::move(left_table_label), std::move(right_table_label));
        left_table_label = join_step_logical->getReadableRelationName();

        appendSetsFromActionsDAG(join_step_logical->getActionsDAG(), left_join_tree_query_plan.useful_sets);
        left_join_tree_query_plan = joinPlansWithStep(
            std::move(join_step_logical),
            std::move(left_join_tree_query_plan),
            std::move(right_join_tree_query_plan));
    }

    return left_join_tree_query_plan;
}

void tryMakeDirectJoinWithMergeTree(const JoinOperator & join_operator,
    QueryPlan & right_query_plan,
    PreparedJoinStorage & prepared_join,
    PlannerContextPtr & planner_context)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    /// In chooseJoinAlgorithm, direct has the highest priority (automatically used with dictionary or storage join).
    /// Use direct join with MergeTree only if 'direct' is explicitly specified as the single option.
    if (settings[Setting::join_algorithm].value != std::vector{JoinAlgorithm::DIRECT})
        return;

    bool allow_strictness = join_operator.strictness == JoinStrictness::All ||
        join_operator.strictness == JoinStrictness::Semi ||
        join_operator.strictness == JoinStrictness::Anti;

    if (!allow_strictness || !isInnerOrLeft(join_operator.kind))
        return;
    if (!join_operator.residual_filter.empty() || join_operator.expression.size() != 1)
        return;
    auto [predicate_type, lhs, rhs] = join_operator.expression[0].asBinaryPredicate();
    if (predicate_type != JoinConditionOperator::Equals)
        return;

    /// Check that right plan is ReadFromMergeTree with ExpressionStep/FilterStep on the top
    auto * root_node = right_query_plan.getRootNode();
    if (!root_node || !root_node->step)
        return;
    const auto * expr_step = root_node->step.get();
    if (!typeid_cast<const ExpressionStep *>(expr_step) && !typeid_cast<const FilterStep *>(expr_step))
        return;
    if (root_node->children.size() != 1 || !root_node->children.front())
        return;

    const auto * children_step = root_node->children.front()->step.get();
    bool is_allowed_storage = typeid_cast<const ReadFromMergeTree *>(children_step)
                           || typeid_cast<const ReadNothingStep *>(children_step)
                           || typeid_cast<const ReadFromPreparedSource *>(children_step);
    if (!is_allowed_storage)
        return;

    if (lhs.fromRight() && rhs.fromLeft())
        std::swap(lhs, rhs);
    else if (!lhs.fromLeft() || !rhs.fromRight())
        return;

    auto lookup_plan = right_query_plan.clone();
    auto & lookup_read_step = lookup_plan.getRootNode()->children.front()->step;
    if (auto * lookup_reading_step = typeid_cast<ReadFromMergeTree *>(lookup_read_step.get()))
    {
        /// We need to analyze index again with new condition
        lookup_reading_step->setAnalyzedResult(nullptr);
        /// Hand-constructed filter dag has same hash key each time, so disable cache
        lookup_reading_step->disableQueryConditionCache();
        /// initializePipeline is done multiple times concurrently, so not to remove parts snapshot
        lookup_reading_step->disableMergeTreePartsSnapshotRemoval();
    }

    for (const auto & column_name : lookup_plan.getCurrentHeader()->getNames())
        prepared_join.column_mapping[column_name] = column_name;

    auto filter_dag = JoinExpressionActions::getSubDAG(rhs);

    prepared_join.storage_key_value = std::make_unique<DirectJoinMergeTreeEntity>(std::move(lookup_plan), std::move(filter_dag), query_context);
    bool use_nulls = settings[Setting::join_use_nulls] && isLeftOrFull(join_operator.kind);
    auto join_lookup_step = std::make_unique<JoinStepLogicalLookup>(std::move(right_query_plan), std::move(prepared_join), use_nulls);

    right_query_plan = {};
    right_query_plan.addStep(std::move(join_lookup_step));
}

std::optional<Names> tryExtractLookupJoinRightKeys(const JoinOperator & join_operator)
{
    bool allowed_inner = isInner(join_operator.kind) && (join_operator.strictness == JoinStrictness::All
        || join_operator.strictness == JoinStrictness::Any);
    bool allowed_left = isLeft(join_operator.kind) && (join_operator.strictness == JoinStrictness::Any
        || join_operator.strictness == JoinStrictness::All
        || join_operator.strictness == JoinStrictness::Semi
        || join_operator.strictness == JoinStrictness::Anti);
    if (!allowed_inner && !allowed_left)
        return {};

    if (!join_operator.residual_filter.empty() || join_operator.expression.empty())
        return {};

    Names right_key_names;
    right_key_names.reserve(join_operator.expression.size());
    for (const auto & predicate : join_operator.expression)
    {
        auto [predicate_type, lhs, rhs] = predicate.asBinaryPredicate();
        if (predicate_type != JoinConditionOperator::Equals)
            return {};

        if (lhs.fromRight() && rhs.fromLeft())
            std::swap(lhs, rhs);
        else if (!lhs.fromLeft() || !rhs.fromRight())
            return {};

        /// The lookup-index join (`MergeTreeTableJoinEntity`) hashes the left probe key
        /// columns against a cache built from the stored (right) key columns using
        /// `HashMethodHashed`. Identical values hash equally only when the column types are
        /// identical: e.g. a `Nullable(UInt64)` or `LowCardinality(UInt64)` left key hashes
        /// differently from a `UInt64` stored key (the null map / dictionary indexes are
        /// hashed too), and a different underlying type (`Int32` vs `UInt64`) hashes
        /// differently as well. In all such cases the regular join path inserts a cast to a
        /// common supertype, which this fast path bypasses. Decline the lookup join when the
        /// left and right key types are not exactly equal, so it falls back to `HashJoin`.
        if (!lhs.getType()->equals(*rhs.getType()))
            return {};

        right_key_names.push_back(rhs.getColumnName());
    }

    return right_key_names;
}

JoinTreeQueryPlan buildQueryPlanForJoinNode(
    const QueryTreeNodePtr & join_table_expression,
    JoinTreeQueryPlan left_join_tree_query_plan,
    JoinTreeQueryPlan right_join_tree_query_plan,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context)
{
    auto & join_node = join_table_expression->as<JoinNode &>();
    if (left_join_tree_query_plan.stage != QueryProcessingStage::FetchColumns)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "JOIN {} left table expression expected to process query to fetch columns stage. Actual {}",
            join_node.formatASTForErrorMessage(),
            QueryProcessingStage::toString(left_join_tree_query_plan.stage));

    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    auto join_step_logical = buildJoinStepLogical(
        left_join_tree_query_plan.query_plan.getCurrentHeader(),
        right_join_tree_query_plan.query_plan.getCurrentHeader(),
        outer_scope_columns,
        join_node,
        planner_context);

    /// `JoinStepLogicalLookup` is not serializable, so it cannot be used when the query plan
    /// is going to be serialized: either for execution with parallel replicas on the initiator,
    /// or when `serialize_query_plan` is enabled (e.g. for distributed queries).
    bool allow_lookup_join = !query_context->canUseParallelReplicasOnInitiator()
        && !settings[Setting::serialize_query_plan];
    PreparedJoinStorage prepared_join;
    bool allow_storage_join = right_join_tree_query_plan.used_row_policies.empty()
        && right_join_tree_query_plan.stage == QueryProcessingStage::FetchColumns
        && right_join_tree_query_plan.useful_sets.empty();
    if (allow_storage_join)
        prepared_join = tryGetStorageInTableJoin(join_node.getRightTableExpression(), planner_context);
    /// Skip the eager lookup index lookup when `join_algorithm` does not permit `direct` join.
    /// Without this gate we would pay the cost of building the lookup cache during planning and then
    /// still fall back to `HashJoin` (or another algorithm) in `chooseJoinAlgorithm`, leaving the cache unused.
    const auto & join_algorithms = settings[Setting::join_algorithm].value;
    bool direct_join_enabled = TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::DIRECT)
        || TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::DEFAULT);
    if (!prepared_join && allow_storage_join && allow_lookup_join && direct_join_enabled)
    {
        if (auto right_key_names = tryExtractLookupJoinRightKeys(join_step_logical->getJoinOperator()))
            prepared_join = tryGetLookupJoinStorage(*right_key_names, join_node.getRightTableExpression(), planner_context);
    }
    if (prepared_join)
    {
        bool use_nulls = settings[Setting::join_use_nulls] && isLeftOrFull(join_node.getKind());
        auto join_lookup_step = std::make_unique<JoinStepLogicalLookup>(std::move(right_join_tree_query_plan.query_plan), std::move(prepared_join), use_nulls);
        right_join_tree_query_plan.query_plan = {};
        right_join_tree_query_plan.query_plan.addStep(std::move(join_lookup_step));
    }
    else
    {
        tryMakeDirectJoinWithMergeTree(join_step_logical->getJoinOperator(), right_join_tree_query_plan.query_plan, prepared_join, planner_context);
    }

    appendSetsFromActionsDAG(join_step_logical->getActionsDAG(), left_join_tree_query_plan.useful_sets);
    return joinPlansWithStep(
        std::move(join_step_logical),
        std::move(left_join_tree_query_plan),
        std::move(right_join_tree_query_plan));
}

JoinTreeQueryPlan buildQueryPlanForArrayJoinNode(const QueryTreeNodePtr & array_join_table_expression,
    JoinTreeQueryPlan join_tree_query_plan,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context)
{
    auto & array_join_node = array_join_table_expression->as<ArrayJoinNode &>();
    if (join_tree_query_plan.stage != QueryProcessingStage::FetchColumns)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "ARRAY JOIN {} table expression expected to process query to fetch columns stage. Actual {}",
            array_join_node.formatASTForErrorMessage(),
            QueryProcessingStage::toString(join_tree_query_plan.stage));

    auto plan = std::move(join_tree_query_plan.query_plan);
    auto plan_output_columns = plan.getCurrentHeader()->getColumnsWithTypeAndName();

    ActionsDAG array_join_action_dag(plan_output_columns);
    ColumnNodePtrWithHashSet empty_correlated_columns_set;
    PlannerActionsVisitor actions_visitor(planner_context, empty_correlated_columns_set);
    std::unordered_set<std::string> array_join_expressions_output_nodes;

    Names array_join_column_names;
    array_join_column_names.reserve(array_join_node.getJoinExpressions().getNodes().size());
    for (auto & array_join_expression : array_join_node.getJoinExpressions().getNodes())
    {
        const auto & array_join_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(array_join_expression);
        array_join_column_names.push_back(array_join_column_identifier);

        auto & array_join_expression_column = array_join_expression->as<ColumnNode &>();
        auto [expression_dag_index_nodes, correlated_subtrees] = actions_visitor.visit(array_join_action_dag, array_join_expression_column.getExpressionOrThrow());
        correlated_subtrees.assertEmpty("in ARRAY JOIN");

        for (auto & expression_dag_index_node : expression_dag_index_nodes)
        {
            const auto * array_join_column_node = &array_join_action_dag.addAlias(*expression_dag_index_node, array_join_column_identifier);
            array_join_action_dag.getOutputs().push_back(array_join_column_node);
            array_join_expressions_output_nodes.insert(array_join_column_node->result_name);
        }
    }

    array_join_action_dag.appendInputsForUnusedColumns(*plan.getCurrentHeader());

    auto array_join_actions = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(array_join_action_dag));
    array_join_actions->setStepDescription("ARRAY JOIN actions");
    appendSetsFromActionsDAG(array_join_actions->getExpression(), join_tree_query_plan.useful_sets);
    plan.addStep(std::move(array_join_actions));

    ActionsDAG drop_unused_columns_before_array_join_actions_dag(plan.getCurrentHeader()->getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs drop_unused_columns_before_array_join_actions_dag_updated_outputs;
    std::unordered_set<std::string_view> drop_unused_columns_before_array_join_actions_dag_updated_outputs_names;

    auto & drop_unused_columns_before_array_join_actions_dag_outputs = drop_unused_columns_before_array_join_actions_dag.getOutputs();
    size_t drop_unused_columns_before_array_join_actions_dag_outputs_size = drop_unused_columns_before_array_join_actions_dag_outputs.size();

    for (size_t i = 0; i < drop_unused_columns_before_array_join_actions_dag_outputs_size; ++i)
    {
        const auto & output = drop_unused_columns_before_array_join_actions_dag_outputs[i];

        if (drop_unused_columns_before_array_join_actions_dag_updated_outputs_names.contains(output->result_name))
            continue;

        if (!array_join_expressions_output_nodes.contains(output->result_name) &&
            !outer_scope_columns.contains(output->result_name))
            continue;

        drop_unused_columns_before_array_join_actions_dag_updated_outputs.push_back(output);
        drop_unused_columns_before_array_join_actions_dag_updated_outputs_names.insert(output->result_name);
    }

    drop_unused_columns_before_array_join_actions_dag_outputs = std::move(drop_unused_columns_before_array_join_actions_dag_updated_outputs);

    auto drop_unused_columns_before_array_join_transform_step = std::make_unique<ExpressionStep>(plan.getCurrentHeader(),
        std::move(drop_unused_columns_before_array_join_actions_dag));
    drop_unused_columns_before_array_join_transform_step->setStepDescription("DROP unused columns before ARRAY JOIN");
    plan.addStep(std::move(drop_unused_columns_before_array_join_transform_step));

    const auto & settings = planner_context->getQueryContext()->getSettingsRef();
    auto array_join_step = std::make_unique<ArrayJoinStep>(
        plan.getCurrentHeader(),
        ArrayJoin{std::move(array_join_column_names), array_join_node.isLeft()},
        settings[Setting::enable_unaligned_array_join],
        settings[Setting::max_block_size],
        settings[Setting::enable_lazy_columns_replication]
        );

    array_join_step->setStepDescription("ARRAY JOIN");
    plan.addStep(std::move(array_join_step));

    return JoinTreeQueryPlan{
        .query_plan = std::move(plan),
        .stage = QueryProcessingStage::FetchColumns,
        .used_row_policies = std::move(join_tree_query_plan.used_row_policies),
        .useful_sets = std::move(join_tree_query_plan.useful_sets),
        .query_node_to_plan_step_mapping = std::move(join_tree_query_plan.query_node_to_plan_step_mapping),
    };
}

}

JoinTreeQueryPlan buildJoinTreeQueryPlan(const QueryTreeNodePtr & query_node,
    const SelectQueryInfo & select_query_info,
    SelectQueryOptions & select_query_options,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context)
{
    const QueryTreeNodePtr & join_tree_node = query_node->as<QueryNode &>().getJoinTree();
    auto table_expressions_stack = buildTableExpressionsStack(join_tree_node);
    size_t table_expressions_stack_size = table_expressions_stack.size();
    bool is_single_table_expression = table_expressions_stack_size == 1;

    std::vector<ColumnIdentifierSet> table_expressions_outer_scope_columns(table_expressions_stack_size);
    ColumnIdentifierSet current_outer_scope_columns = outer_scope_columns;

    if (is_single_table_expression)
    {
        auto * table_node = table_expressions_stack[0]->as<TableNode>();
        if (table_node && shouldIgnoreQuotaAndLimits(*table_node))
        {
            select_query_options.ignore_quota = true;
            select_query_options.ignore_limits = true;
        }
    }

    size_t joins_count = 0;
    bool is_full_join = false;
    bool is_global_join = false;
    bool is_right_join_with_remote_table = false;
    int first_join_pos = -1;
    int last_right_join_pos = -1;
    bool is_cross_join = false;
    /// For each table, table function, query, union table expressions prepare before query plan build
    for (size_t i = 0; i < table_expressions_stack_size; ++i)
    {
        const auto & table_expression = table_expressions_stack[i];
        auto table_expression_type = table_expression->getNodeType();
        if (table_expression_type == QueryTreeNodeType::ARRAY_JOIN)
            continue;

        if (table_expression_type == QueryTreeNodeType::CROSS_JOIN)
        {
            joins_count += table_expression->as<const CrossJoinNode &>().getTableExpressions().size() - 1;
            is_cross_join = true;
            continue;
        }

        if (table_expression_type == QueryTreeNodeType::JOIN)
        {
            ++joins_count;
            const auto & join_node = table_expression->as<const JoinNode &>();
            const auto join_kind = join_node.getKind();

            if (join_kind == JoinKind::Full)
                is_full_join = true;

            if (join_node.getLocality() == JoinLocality::Global)
                is_global_join = true;

            // save join positions for later check
            if (first_join_pos < 0 && (join_kind == JoinKind::Left || join_kind == JoinKind::Inner || join_kind == JoinKind::Right))
                first_join_pos = static_cast<int>(i);
            if (join_kind == JoinKind::Right)
                last_right_join_pos = static_cast<int>(i);

            /// For RIGHT JOIN with a distributed table on the right side, disable parallel replicas.
            /// The distributed table on the right side would be wrapped into a subquery,
            /// causing parallel replicas to incorrectly choose the left table for parallel reading.
            /// Each replica would then independently read the full distributed table, resulting in duplicate data.
            if (join_kind == JoinKind::Right)
            {
                const auto & right_expression_data = planner_context->getTableExpressionDataOrThrow(join_node.getRightTableExpression());
                is_right_join_with_remote_table = right_expression_data.isRemote();
            }

            continue;
        }

        prepareBuildQueryPlanForTableExpression(table_expression, select_query_options, planner_context);
    }

    auto should_disable_parallel_replicas = [&]() -> bool
    {
        /// n-way join like LEFT/INNER/RIGHT ... RIGHT ...
        /// if last RIGHT join position is after LEFT/INNER/RIGHT(another) join then the left side of the RIGHT join can't be parallelized
        if (first_join_pos >= 0 && last_right_join_pos >= 0 && first_join_pos < last_right_join_pos)
            return true;

        /// for n-way join with FULL JOIN or GLOBAL JOINS or CROSS JOIN
        if (joins_count > 1 && (is_full_join || is_global_join || is_cross_join))
            return true;

        /// For RIGHT JOIN with distributed table on the right side
        if (is_right_join_with_remote_table)
            return true;

        return false;
    };

    if (should_disable_parallel_replicas())
        planner_context->getMutableQueryContext()->setSetting("enable_parallel_replicas", Field{0});


    // in case of n-way JOINs the table expression stack contains several join nodes
    // so, we need to find right parent node for a table expression to pass into buildQueryPlanForTableExpression()
    QueryTreeNodePtr parent_join_tree = join_tree_node;
    for (const auto & node : table_expressions_stack)
    {
        if (node->getNodeType() == QueryTreeNodeType::JOIN ||
            node->getNodeType() == QueryTreeNodeType::CROSS_JOIN ||
            node->getNodeType() == QueryTreeNodeType::ARRAY_JOIN)
        {
            parent_join_tree = node;
            break;
        }
    }

    /** If left most table expression query plan is planned to stage that is not equal to fetch columns,
      * then left most table expression is responsible for providing valid JOIN TREE part of final query plan.
      *
      * Examples: Distributed, Merge storages, Parallel Replicas
      */
    auto left_table_expression = table_expressions_stack.front();

    /** If the leftmost table uses IStorageCluster (e.g., s3Cluster, hdfsCluster)
      * and there are multiple tables (indicating a JOIN), we must wrap it in a subquery.
      * This prevents IStorageCluster from receiving the full JOIN query, which it cannot handle.
      *
      * IStorageCluster is a simple storage that just forwards queries to remote nodes.
      * Unlike StorageDistributed, it cannot decompose and handle JOINs across multiple tables,
      * because remote nodes don't have access to other tables in the JOIN.
      *
      * StorageDistributed has sophisticated query planning logic to handle JOINs and should
      * NOT be wrapped (wrapping breaks tests like 03577_server_constant_folding).
      */
    bool should_wrap_left_table = false;
    bool has_multiple_tables = table_expressions_stack.size() > 1;

    if (has_multiple_tables)
    {
        // Get the actual storage to check its type
        auto * table_node = left_table_expression->as<TableNode>();
        auto * table_function_node = left_table_expression->as<TableFunctionNode>();

        if (table_node || table_function_node)
        {
            const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
            // Only wrap if it's specifically IStorageCluster, not StorageDistributed or other remote storages
            should_wrap_left_table = (dynamic_cast<const IStorageCluster *>(storage.get()) != nullptr);
        }
    }

    auto left_table_expression_query_plan = buildQueryPlanForTableExpression(
        left_table_expression,
        parent_join_tree,
        select_query_info,
        select_query_options,
        planner_context,
        is_single_table_expression,
        should_wrap_left_table /*wrap_read_columns_in_subquery*/);
    if (left_table_expression_query_plan.stage != QueryProcessingStage::FetchColumns)
        return left_table_expression_query_plan;

    for (Int64 i = static_cast<Int64>(table_expressions_stack_size) - 1; i >= 0; --i)
    {
        table_expressions_outer_scope_columns[i] = current_outer_scope_columns;
        auto & table_expression = table_expressions_stack[i];
        auto table_expression_type = table_expression->getNodeType();

        if (table_expression_type == QueryTreeNodeType::JOIN)
            collectTopLevelColumnIdentifiers(table_expression, planner_context, current_outer_scope_columns);
        else if (table_expression_type == QueryTreeNodeType::CROSS_JOIN)
            collectTopLevelColumnIdentifiers(table_expression, planner_context, current_outer_scope_columns);
        else if (table_expression_type == QueryTreeNodeType::ARRAY_JOIN)
            collectTopLevelColumnIdentifiers(table_expression, planner_context, current_outer_scope_columns);
    }

    std::vector<JoinTreeQueryPlan> query_plans_stack;

    for (size_t i = 0; i < table_expressions_stack_size; ++i)
    {
        const auto & table_expression = table_expressions_stack[i];
        auto table_expression_node_type = table_expression->getNodeType();

        if (table_expression_node_type == QueryTreeNodeType::ARRAY_JOIN)
        {
            if (query_plans_stack.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expected at least 1 query plan on stack before ARRAY JOIN processing. Actual {}",
                    query_plans_stack.size());

            auto query_plan = std::move(query_plans_stack.back());
            query_plans_stack.back() = buildQueryPlanForArrayJoinNode(table_expression,
                std::move(query_plan),
                table_expressions_outer_scope_columns[i],
                planner_context);
        }
        else if (table_expression_node_type == QueryTreeNodeType::JOIN)
        {
            if (query_plans_stack.size() < 2)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expected at least 2 query plans on stack before JOIN processing. Actual {}",
                    query_plans_stack.size());

            auto right_query_plan = std::move(query_plans_stack.back());
            query_plans_stack.pop_back();

            auto left_query_plan = std::move(query_plans_stack.back());
            query_plans_stack.pop_back();

            query_plans_stack.push_back(buildQueryPlanForJoinNode(
                table_expression,
                std::move(left_query_plan),
                std::move(right_query_plan),
                table_expressions_outer_scope_columns[i],
                planner_context));
        }
        else if (table_expression_node_type == QueryTreeNodeType::CROSS_JOIN)
        {
            auto & cross_join_node = table_expression->as<CrossJoinNode &>();
            size_t num_tables = cross_join_node.getTableExpressions().size();
            if (query_plans_stack.size() < num_tables)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expected at least {} query plans on stack before CROSS JOIN processing. Actual {}",
                    num_tables,
                    query_plans_stack.size());

            std::vector<JoinTreeQueryPlan> plans;
            for (size_t pos = query_plans_stack.size() - num_tables; pos <  query_plans_stack.size(); ++pos)
                plans.emplace_back(std::move(query_plans_stack[pos]));

            query_plans_stack.resize(query_plans_stack.size() - num_tables);

            query_plans_stack.push_back(buildQueryPlanForCrossJoinNode(
                table_expression,
                std::move(plans),
                table_expressions_outer_scope_columns[i],
                planner_context));
        }
        else
        {
            if (table_expression == left_table_expression)
            {
                query_plans_stack.push_back(std::move(left_table_expression_query_plan)); /// NOLINT
                left_table_expression = {};
                continue;
            }

            // find parent join node
            parent_join_tree.reset();
            for (size_t j = i + 1; j < table_expressions_stack.size(); ++j)
            {
                const auto & node = table_expressions_stack[j];
                if (node->getNodeType() == QueryTreeNodeType::JOIN || node->getNodeType() == QueryTreeNodeType::ARRAY_JOIN
                    || node->getNodeType() == QueryTreeNodeType::CROSS_JOIN)
                {
                    parent_join_tree = node;
                    break;
                }
            }

            /** If table expression is remote and it is not left most table expression, we wrap read columns from such
              * table expression in subquery.
              */
            bool is_remote = planner_context->getTableExpressionDataOrThrow(table_expression).isRemote();
            query_plans_stack.push_back(buildQueryPlanForTableExpression(
                table_expression,
                parent_join_tree,
                select_query_info,
                select_query_options,
                planner_context,
                is_single_table_expression,
                is_remote /*wrap_read_columns_in_subquery*/));
        }
    }

    if (query_plans_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Expected 1 query plan for JOIN TREE. Actual {}",
            query_plans_stack.size());

    return std::move(query_plans_stack.back());
}

}
