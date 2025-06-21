#include <Planner/PlannerJoinTree.h>

#include <Core/Settings.h>

#include <Core/ParallelReplicasMode.h>
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
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageDistributed.h>

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
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <Storages/StorageDummy.h>

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
    extern const SettingsBool query_plan_use_new_logical_join_step;
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
    extern const SettingsUInt64 min_joined_block_size_bytes;
}

namespace ErrorCodes
{
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
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
    const Names & columns_names)
{
    const auto & settings = query_context->getSettingsRef();
    if (!settings[Setting::optimize_trivial_count_query])
        return false;

    const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
    if (!storage->supportsTrivialCountOptimization(
            table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot(), query_context))
        return false;

    auto storage_id = storage->getStorageID();
    auto row_policy_filter = query_context->getRowPolicyFilter(storage_id.getDatabaseName(),
        storage_id.getTableName(),
        RowPolicyFilterType::SELECT_FILTER);
    if (row_policy_filter)
        return {};

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
         table_node->getTableExpressionModifiers()->hasSampleOffsetRatio()))
        return false;
    if (table_function_node && table_function_node->getTableExpressionModifiers().has_value()
        && (table_function_node->getTableExpressionModifiers()->hasFinal()
            || table_function_node->getTableExpressionModifiers()->hasSampleSizeRatio()
            || table_function_node->getTableExpressionModifiers()->hasSampleOffsetRatio()))
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

    /// get count() argument type
    DataTypes argument_types;
    argument_types.reserve(columns_names.size());
    {
        const Block source_header = table_node ? table_node->getStorageSnapshot()->getSampleBlockForColumns(columns_names)
                                               : table_function_node->getStorageSnapshot()->getSampleBlockForColumns(columns_names);
        for (const auto & column_name : columns_names)
            argument_types.push_back(source_header.getByName(column_name).type);
    }

    Block block_with_count{
        {std::move(column),
         std::make_shared<DataTypeAggregateFunction>(function_node.getAggregateFunction(), argument_types, Array{}),
         columns_names.front()}};

    auto source = std::make_shared<SourceFromSingleChunk>(block_with_count);
    auto prepared_count = std::make_unique<ReadFromPreparedSource>(Pipe(std::move(source)));
    prepared_count->setStepDescription("Optimized trivial count");
    query_plan.addStep(std::move(prepared_count));

    return true;
}

void prepareBuildQueryPlanForTableExpression(const QueryTreeNodePtr & table_expression, PlannerContextPtr & planner_context)
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
            NamesAndTypesList projection_columns_list(projection_columns.begin(), projection_columns.end());
            additional_column_to_read = ExpressionActions::getSmallestColumn(projection_columns_list);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected table, table function, query or union. Actual {}",
                            table_expression->formatASTForErrorMessage());
        }

        auto & global_planner_context = planner_context->getGlobalPlannerContext();
        const auto & column_identifier = global_planner_context->createColumnIdentifier(additional_column_to_read, table_expression);
        columns_names.push_back(additional_column_to_read.name);
        table_expression_data.addColumn(additional_column_to_read, column_identifier);
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
    auto storage_id = storage->getStorageID();
    const auto & query_context = planner_context->getQueryContext();

    auto row_policy_filter = query_context->getRowPolicyFilter(storage_id.getDatabaseName(), storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
    if (!row_policy_filter || row_policy_filter->empty())
        return {};

    for (const auto & row_policy : row_policy_filter->policies)
    {
        auto name = row_policy->getFullName().toString();
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
        storage->getInMemoryMetadataPtr()->columns,
        query_context);

    return buildFilterInfo(parallel_replicas_custom_filter_ast, table_expression_query_info.table_expression, planner_context);
}

/// Apply filters from additional_table_filters setting
std::optional<FilterDAGInfo> buildAdditionalFiltersIfNeeded(const StoragePtr & storage,
    const String & table_expression_alias,
    SelectQueryInfo & table_expression_query_info,
    PlannerContextPtr & planner_context)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    auto const & additional_filters = settings[Setting::additional_table_filters].value;
    if (additional_filters.empty())
        return {};

    auto const & storage_id = storage->getStorageID();

    ASTPtr additional_filter_ast;
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
            additional_filter_ast = parseQuery(
                parser,
                filter.data(),
                filter.data() + filter.size(),
                "additional filter",
                settings[Setting::max_query_size],
                settings[Setting::max_parser_depth],
                settings[Setting::max_parser_backtracks]);
            break;
        }
    }

    if (!additional_filter_ast)
        return {};

    table_expression_query_info.additional_filter_ast = additional_filter_ast;
    return buildFilterInfo(additional_filter_ast, table_expression_query_info.table_expression, planner_context);
}

UInt64 mainQueryNodeBlockSizeByLimit(const SelectQueryInfo & select_query_info)
{
    auto const & main_query_node = select_query_info.query_tree->as<QueryNode const &>();

    /// Constness of limit and offset is validated during query analysis stage
    size_t limit_length = 0;
    if (main_query_node.hasLimit())
        limit_length = main_query_node.getLimit()->as<ConstantNode &>().getValue().safeGet<UInt64>();

    size_t limit_offset = 0;
    if (main_query_node.hasOffset())
        limit_offset = main_query_node.getOffset()->as<ConstantNode &>().getValue().safeGet<UInt64>();

    /** If not specified DISTINCT, WHERE, GROUP BY, HAVING, ORDER BY, JOIN, LIMIT BY, LIMIT WITH TIES
      * but LIMIT is specified, and limit + offset < max_block_size,
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
    std::unordered_map<std::string, ActionsDAG> & alias_column_expressions, const Header & current_header)
{
    ActionsDAG merged_alias_columns_actions_dag(current_header.getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs action_dag_outputs = merged_alias_columns_actions_dag.getInputs();

    for (auto & [column_name, alias_column_actions_dag] : alias_column_expressions)
    {
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

    QueryProcessingStage::Enum from_stage = QueryProcessingStage::Enum::FetchColumns;

    if (wrap_read_columns_in_subquery)
    {
        auto columns = table_expression_data.getColumns();
        table_expression = buildSubqueryToReadColumnsFromTableExpression(columns, table_expression, query_context);
    }

    auto * table_node = table_expression->as<TableNode>();
    auto * table_function_node = table_expression->as<TableFunctionNode>();
    auto * query_node = table_expression->as<QueryNode>();
    auto * union_node = table_expression->as<UnionNode>();

    QueryPlan query_plan;
    std::unordered_map<const QueryNode *, const QueryPlan::Node *> query_node_to_plan_step_mapping;
    std::set<std::string> used_row_policies;

    if (table_node || table_function_node)
    {
        const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
        const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();

        auto table_expression_query_info = select_query_info;
        table_expression_query_info.table_expression = table_expression;
        if (const auto & filter_actions = table_expression_data.getFilterActions())
            table_expression_query_info.filter_actions_dag = std::make_shared<const ActionsDAG>(filter_actions->clone());

        size_t max_streams = settings[Setting::max_threads];
        size_t max_threads_execute_query = settings[Setting::max_threads];

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
        if (is_single_table_expression)
        {
            /** If not specified DISTINCT, WHERE, GROUP BY, HAVING, ORDER BY, JOIN, LIMIT BY, LIMIT WITH TIES
              * but LIMIT is specified, and limit + offset < max_block_size,
              * then as the block size we will use limit + offset (not to read more from the table than requested),
              * and also set the number of threads to 1.
              */
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
            if (auto streams_with_ratio = max_streams * settings[Setting::max_streams_to_max_threads_ratio];
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
        bool is_trivial_count_applied = !select_query_options.only_analyze && !select_query_options.build_logical_plan && is_single_table_expression
            && (table_node || table_function_node) && select_query_info.has_aggregates && settings[Setting::additional_table_filters].value.empty()
            && applyTrivialCountIfPossible(
                query_plan,
                table_expression_query_info,
                table_node,
                table_function_node,
                select_query_info.query_tree,
                planner_context->getMutableQueryContext(),
                table_expression_data.getColumnNames());

        if (is_trivial_count_applied)
        {
            from_stage = QueryProcessingStage::WithMergeableState;
        }
        else
        {
            if (!select_query_options.only_analyze)
            {
                auto & prewhere_info = table_expression_query_info.prewhere_info;
                const auto & prewhere_actions = table_expression_data.getPrewhereFilterActions();
                const auto & columns_names = table_expression_data.getColumnNames();

                std::vector<std::pair<FilterDAGInfo, std::string>> where_filters;

                if (prewhere_actions && select_query_options.build_logical_plan)
                {
                    where_filters.emplace_back(
                        FilterDAGInfo{
                            prewhere_actions->clone(),
                            prewhere_actions->getOutputs().at(0)->result_name,
                            true},
                        "Prewhere");
                }
                else if (prewhere_actions)
                {
                    prewhere_info = std::make_shared<PrewhereInfo>();
                    prewhere_info->prewhere_actions = prewhere_actions->clone();
                    prewhere_info->prewhere_column_name = prewhere_actions->getOutputs().at(0)->result_name;
                    /// Do not remove prewhere column if it is the only column to read
                    bool keep_prewhere_column = columns_names.size() == 1 && columns_names.at(0) == prewhere_info->prewhere_column_name;
                    prewhere_info->remove_prewhere_column = !keep_prewhere_column;
                    prewhere_info->need_filter = true;
                }

                updatePrewhereOutputsIfNeeded(table_expression_query_info, table_expression_data.getColumnNames(), storage_snapshot);

                const auto add_filter = [&](FilterDAGInfo & filter_info, std::string description)
                {
                    bool is_final = table_expression_query_info.table_expression_modifiers
                        && table_expression_query_info.table_expression_modifiers->hasFinal();
                    bool optimize_move_to_prewhere
                        = settings[Setting::optimize_move_to_prewhere] && (!is_final || settings[Setting::optimize_move_to_prewhere_if_final]);

                    auto supported_prewhere_columns = storage->supportedPrewhereColumns();
                    if (!select_query_options.build_logical_plan && storage->canMoveConditionsToPrewhere() && optimize_move_to_prewhere
                        && (!supported_prewhere_columns || supported_prewhere_columns->contains(filter_info.column_name)))
                    {
                        if (!prewhere_info)
                        {
                            prewhere_info = std::make_shared<PrewhereInfo>();
                            prewhere_info->prewhere_actions = std::move(filter_info.actions);
                            prewhere_info->prewhere_column_name = filter_info.column_name;
                            prewhere_info->remove_prewhere_column = filter_info.do_remove_column;
                            prewhere_info->need_filter = true;
                        }
                        else if (!prewhere_info->row_level_filter)
                        {
                            prewhere_info->row_level_filter = std::move(filter_info.actions);
                            prewhere_info->row_level_column_name = filter_info.column_name;
                            prewhere_info->need_filter = true;
                        }
                        else
                        {
                            where_filters.emplace_back(std::move(filter_info), std::move(description));
                        }

                    }
                    else
                    {
                        where_filters.emplace_back(std::move(filter_info), std::move(description));
                    }
                };

                auto row_policy_filter_info
                    = buildRowPolicyFilterIfNeeded(storage, table_expression_query_info, planner_context, used_row_policies);
                if (row_policy_filter_info)
                {
                    table_expression_data.setRowLevelFilterActions(row_policy_filter_info->actions.clone());
                    add_filter(*row_policy_filter_info, "Row-level security filter");
                }

                if (query_context->canUseParallelReplicasCustomKey())
                {
                    if (settings[Setting::parallel_replicas_count] > 1)
                    {
                        if (auto parallel_replicas_custom_key_filter_info= buildCustomKeyFilterIfNeeded(storage, table_expression_query_info, planner_context))
                            add_filter(*parallel_replicas_custom_key_filter_info, "Parallel replicas custom key filter");
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

                const auto & table_expression_alias = table_expression->getOriginalAlias();
                if (auto additional_filters_info = buildAdditionalFiltersIfNeeded(storage, table_expression_alias, table_expression_query_info, planner_context))
                    add_filter(*additional_filters_info, "additional filter");

                if (!select_query_options.build_logical_plan)
                    from_stage = storage->getQueryProcessingStage(
                        query_context, select_query_options.to_stage, storage_snapshot, table_expression_query_info);

                if (select_query_options.build_logical_plan)
                {
                    auto sample_block = storage_snapshot->getSampleBlockForColumns(columns_names);

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
                            /*hilite=*/false,
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
                        auto mutable_context = Context::createCopy(query_context);
                        mutable_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
                        storage->read(
                            query_plan,
                            columns_names,
                            storage_snapshot,
                            table_expression_query_info,
                            std::move(mutable_context),
                            from_stage,
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
                            from_stage,
                            max_block_size,
                            max_streams);
                    }
                }

                auto parallel_replicas_enabled_for_storage = [](const StoragePtr & table, const Settings & query_settings)
                {
                    if (!table->isMergeTree())
                        return false;

                    if (!table->supportsReplication() && !query_settings[Setting::parallel_replicas_for_non_replicated_merge_tree])
                        return false;

                    return true;
                };

                /// query_plan can be empty if there is nothing to read
                if (query_plan.isInitialized() && !select_query_options.build_logical_plan && parallel_replicas_enabled_for_storage(storage, settings))
                {
                    auto allow_parallel_replicas_for_table_expression = [](const QueryTreeNodePtr & join_tree_node)
                    {
                        if (join_tree_node->as<CrossJoinNode>())
                            return false;

                        const JoinNode * join_node = join_tree_node->as<JoinNode>();
                        if (!join_node)
                            return true;

                        const auto join_kind = join_node->getKind();
                        const auto join_strictness = join_node->getStrictness();
                        if (join_kind == JoinKind::Left || join_kind == JoinKind::Right
                            || (join_kind == JoinKind::Inner && join_strictness == JoinStrictness::All))
                            return true;

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
                            from_stage = QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;
                            QueryPlan query_plan_parallel_replicas;
                            ClusterProxy::executeQueryWithParallelReplicasCustomKey(
                                query_plan_parallel_replicas,
                                storage->getStorageID(),
                                modified_query_info,
                                storage->getInMemoryMetadataPtr()->getColumns(),
                                storage_snapshot,
                                from_stage,
                                table_expression_query_info.query_tree,
                                query_context);
                            query_plan = std::move(query_plan_parallel_replicas);
                        }
                    }
                    else if (
                        ClusterProxy::canUseParallelReplicasOnInitiator(query_context)
                        && allow_parallel_replicas_for_table_expression(parent_join_tree))
                    {
                        // (1) find read step
                        QueryPlan::Node * node = query_plan.getRootNode();
                        ReadFromMergeTree * reading = nullptr;
                        while (node)
                        {
                            reading = typeid_cast<ReadFromMergeTree *>(node->step.get());
                            if (reading)
                                break;

                            QueryPlan::Node * prev_node = node;
                            if (!node->children.empty())
                            {
                                chassert(node->children.size() == 1);
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

                        chassert(reading);

                        // (2) if it's ReadFromMergeTree - run index analysis and check number of rows to read
                        if (settings[Setting::parallel_replicas_min_number_of_rows_per_replica] > 0)
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
                        if (planner_context->getQueryContext()->canUseParallelReplicasOnInitiator())
                        {
                            from_stage = QueryProcessingStage::WithMergeableState;
                            QueryPlan query_plan_parallel_replicas;
                            QueryPlanStepPtr reading_step = std::move(node->step);
                            ClusterProxy::executeQueryWithParallelReplicas(
                                query_plan_parallel_replicas,
                                storage->getStorageID(),
                                from_stage,
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
                                from_stage,
                                max_block_size,
                                max_streams);
                            query_plan = std::move(query_plan_no_parallel_replicas);
                        }
                    }
                }

                auto & alias_column_expressions = table_expression_data.getAliasColumnExpressions();
                if (!alias_column_expressions.empty() && query_plan.isInitialized() && from_stage == QueryProcessingStage::FetchColumns)
                {
                    auto alias_column_step = createComputeAliasColumnsStep(alias_column_expressions, query_plan.getCurrentHeader());
                    query_plan.addStep(std::move(alias_column_step));
                }

                for (auto && [filter_info, description] : where_filters)
                {
                    if (query_plan.isInitialized() &&
                        from_stage == QueryProcessingStage::FetchColumns)
                    {
                        auto filter_step = std::make_unique<FilterStep>(query_plan.getCurrentHeader(),
                            std::move(filter_info.actions),
                            filter_info.column_name,
                            filter_info.do_remove_column);
                        filter_step->setStepDescription(description);
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
                auto source_header = storage_snapshot->getSampleBlockForColumns(column_names);
                Pipe pipe(std::make_shared<NullSource>(source_header));
                auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
                read_from_pipe->setStepDescription("Read from NullSource");
                query_plan.addStep(std::move(read_from_pipe));
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

            Pipe pipe(std::make_shared<NullSource>(source_header));
            auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
            read_from_pipe->setStepDescription("Read from NullSource");
            query_plan.addStep(std::move(read_from_pipe));
        }
        else
        {
            std::shared_ptr<GlobalPlannerContext> subquery_planner_context;
            if (wrap_read_columns_in_subquery)
                subquery_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, FiltersForTableExpressionMap{});
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
        if (!alias_column_expressions.empty() && query_plan.isInitialized() && from_stage == QueryProcessingStage::FetchColumns)
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

    if (from_stage == QueryProcessingStage::FetchColumns)
    {
        ActionsDAG rename_actions_dag(query_plan.getCurrentHeader().getColumnsWithTypeAndName());
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
    else
    {
        SelectQueryOptions analyze_query_options = SelectQueryOptions(from_stage).analyze();
        Planner planner(select_query_info.query_tree,
            analyze_query_options,
            select_query_info.planner_context);
        planner.buildQueryPlanIfNeeded();

        auto expected_header = planner.getQueryPlan().getCurrentHeader();

        if (!blocksHaveEqualStructure(query_plan.getCurrentHeader(), expected_header))
        {
            materializeBlockInplace(expected_header);

            auto rename_actions_dag = ActionsDAG::makeConvertingActions(
                query_plan.getCurrentHeader().getColumnsWithTypeAndName(),
                expected_header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Position,
                true /*ignore_constant_values*/);
            auto rename_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(rename_actions_dag));
            std::string step_description = table_expression_data.isRemote() ? "Change remote column names to local column names" : "Change column names";
            rename_step->setStepDescription(std::move(step_description));
            query_plan.addStep(std::move(rename_step));
        }
    }

    return JoinTreeQueryPlan{
        .query_plan = std::move(query_plan),
        .from_stage = from_stage,
        .used_row_policies = std::move(used_row_policies),
        .query_node_to_plan_step_mapping = std::move(query_node_to_plan_step_mapping),
    };
}

void joinCastPlanColumnsToNullable(QueryPlan & plan_to_add_cast, PlannerContextPtr & planner_context, const FunctionOverloadResolverPtr & to_nullable_function)
{
    ActionsDAG cast_actions_dag(plan_to_add_cast.getCurrentHeader().getColumnsWithTypeAndName());

    for (auto & output_node : cast_actions_dag.getOutputs())
    {
        if (planner_context->getGlobalPlannerContext()->hasColumnIdentifier(output_node->result_name))
        {
            DataTypePtr type_to_check = output_node->result_type;
            if (const auto * type_to_check_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type_to_check.get()))
                type_to_check = type_to_check_low_cardinality->getDictionaryType();

            if (type_to_check->canBeInsideNullable())
                output_node = &cast_actions_dag.addFunction(to_nullable_function, {output_node}, output_node->result_name);
        }
    }

    cast_actions_dag.appendInputsForUnusedColumns(plan_to_add_cast.getCurrentHeader());
    auto cast_join_columns_step = std::make_unique<ExpressionStep>(plan_to_add_cast.getCurrentHeader(), std::move(cast_actions_dag));
    cast_join_columns_step->setStepDescription("Cast JOIN columns to Nullable");
    plan_to_add_cast.addStep(std::move(cast_join_columns_step));
}

void joinCastPlanColumnsToNullable(QueryPlan & left_plan, QueryPlan & right_plan, PlannerContextPtr & planner_context, JoinKind join_kind)
{
    const auto & query_context = planner_context->getQueryContext();
    auto to_nullable_function = FunctionFactory::instance().get("toNullable", query_context);
    if (isFull(join_kind))
    {
        joinCastPlanColumnsToNullable(left_plan, planner_context, to_nullable_function);
        joinCastPlanColumnsToNullable(right_plan, planner_context, to_nullable_function);
    }
    else if (isLeft(join_kind))
    {
        joinCastPlanColumnsToNullable(right_plan, planner_context, to_nullable_function);
    }
    else if (isRight(join_kind))
    {
        joinCastPlanColumnsToNullable(left_plan, planner_context, to_nullable_function);
    }
}

std::optional<ActionsDAG> createStepToDropColumns(
    const Block & header,
    const ColumnIdentifierSet & outer_scope_columns,
    const PlannerContextPtr & planner_context)
{
    ActionsDAG drop_unused_columns_after_join_actions_dag(header.getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs drop_unused_columns_after_join_actions_dag_updated_outputs;
    std::unordered_set<std::string_view> drop_unused_columns_after_join_actions_dag_updated_outputs_names;
    std::optional<size_t> first_skipped_column_node_index;

    auto & drop_unused_columns_after_join_actions_dag_outputs = drop_unused_columns_after_join_actions_dag.getOutputs();
    size_t drop_unused_columns_after_join_actions_dag_outputs_size = drop_unused_columns_after_join_actions_dag_outputs.size();

    const auto & global_planner_context = planner_context->getGlobalPlannerContext();

    for (size_t i = 0; i < drop_unused_columns_after_join_actions_dag_outputs_size; ++i)
    {
        const auto & output = drop_unused_columns_after_join_actions_dag_outputs[i];

        if (drop_unused_columns_after_join_actions_dag_updated_outputs_names.contains(output->result_name)
            || !global_planner_context->hasColumnIdentifier(output->result_name))
            continue;

        if (!outer_scope_columns.contains(output->result_name))
        {
            if (!first_skipped_column_node_index)
                first_skipped_column_node_index = i;
            continue;
        }

        drop_unused_columns_after_join_actions_dag_updated_outputs.push_back(output);
        drop_unused_columns_after_join_actions_dag_updated_outputs_names.insert(output->result_name);
    }

    if (!first_skipped_column_node_index)
        return {};

    /** It is expected that JOIN TREE query plan will contain at least 1 column, even if there are no columns in outer scope.
      *
      * Example: SELECT count() FROM test_table_1 AS t1, test_table_2 AS t2;
      */
    if (drop_unused_columns_after_join_actions_dag_updated_outputs.empty() && first_skipped_column_node_index)
        drop_unused_columns_after_join_actions_dag_updated_outputs.push_back(drop_unused_columns_after_join_actions_dag_outputs[*first_skipped_column_node_index]);

    drop_unused_columns_after_join_actions_dag_outputs = std::move(drop_unused_columns_after_join_actions_dag_updated_outputs);

    return drop_unused_columns_after_join_actions_dag;
}

std::tuple<QueryPlan, JoinPtr> buildJoinQueryPlan(
    QueryPlan left_plan,
    QueryPlan right_plan,
    std::shared_ptr<TableJoin> & table_join,
    JoinClausesAndActions & join_clauses_and_actions,
    UsefulSets & left_useful_sets,
    const QueryTreeNodePtr & right_table_expression,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context,
    const SelectQueryInfo & select_query_info)
{
    const Block & left_header = left_plan.getCurrentHeader();
    const Block & right_header = right_plan.getCurrentHeader();

    auto columns_from_left_table = left_header.getNamesAndTypesList();
    auto columns_from_right_table = right_header.getNamesAndTypesList();

    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    JoinKind join_kind = table_join->kind();
    JoinStrictness join_strictness = table_join->strictness();

    table_join->setInputColumns(columns_from_left_table, columns_from_right_table);

    for (auto & column_from_joined_table : columns_from_left_table)
    {
        /// Add columns to output only if they are presented in outer scope, otherwise they can be dropped
        if (planner_context->getGlobalPlannerContext()->hasColumnIdentifier(column_from_joined_table.name) &&
            outer_scope_columns.contains(column_from_joined_table.name))
            table_join->setUsedColumn(column_from_joined_table, JoinTableSide::Left);
    }

    for (auto & column_from_joined_table : columns_from_right_table)
    {
        /// Add columns to output only if they are presented in outer scope, otherwise they can be dropped
        if (planner_context->getGlobalPlannerContext()->hasColumnIdentifier(column_from_joined_table.name) &&
            outer_scope_columns.contains(column_from_joined_table.name))
            table_join->setUsedColumn(column_from_joined_table, JoinTableSide::Right);
    }

    if (table_join->getOutputColumns(JoinTableSide::Left).empty() && table_join->getOutputColumns(JoinTableSide::Right).empty())
    {
        /// We should add all duplicated columns, because join algorithm add either all column with specified name or none
        auto set_used_column_with_duplicates = [&](const NamesAndTypesList & columns, JoinTableSide join_table_side)
        {
            const auto & column_name = columns.front().name;
            for (const auto & column : columns)
                if (column.name == column_name)
                    table_join->setUsedColumn(column, join_table_side);
        };

        if (!columns_from_left_table.empty())
            set_used_column_with_duplicates(columns_from_left_table, JoinTableSide::Left);
        else if (!columns_from_right_table.empty())
            set_used_column_with_duplicates(columns_from_right_table, JoinTableSide::Right);
    }

    trySetStorageInTableJoin(right_table_expression, table_join);
    auto prepared_join_storage = tryGetStorageInTableJoin(right_table_expression, planner_context);
    auto hash_table_stat_cache_key = preCalculateCacheKey(right_table_expression, select_query_info);
    JoinAlgorithmParams params(*planner_context->getQueryContext());
    params.hash_table_key_hash = calculateCacheKey(table_join, hash_table_stat_cache_key);

    auto join_algorithm = chooseJoinAlgorithm(
        table_join, prepared_join_storage, left_header, right_header, params);
    auto result_plan = QueryPlan();

    bool is_filled_join = join_algorithm->isFilled();
    if (is_filled_join)
    {
        auto filled_join_step
            = std::make_unique<FilledJoinStep>(left_plan.getCurrentHeader(), join_algorithm, settings[Setting::max_block_size]);

        filled_join_step->setStepDescription("Filled JOIN");
        left_plan.addStep(std::move(filled_join_step));

        result_plan = std::move(left_plan);
    }
    else
    {
        auto add_sorting = [&] (QueryPlan & plan, const Names & key_names, JoinTableSide join_table_side)
        {
            SortDescription sort_description;
            sort_description.reserve(key_names.size());
            for (const auto & key_name : key_names)
                sort_description.emplace_back(key_name);

            SortingStep::Settings sort_settings(query_context->getSettingsRef());

            auto sorting_step = std::make_unique<SortingStep>(
                plan.getCurrentHeader(), std::move(sort_description), 0 /*limit*/, sort_settings, true /*is_sorting_for_merge_join*/);
            sorting_step->setStepDescription(fmt::format("Sort {} before JOIN", join_table_side));
            plan.addStep(std::move(sorting_step));
        };

        auto crosswise_connection = CreateSetAndFilterOnTheFlyStep::createCrossConnection();
        auto add_create_set = [&settings, crosswise_connection](QueryPlan & plan, const Names & key_names, JoinTableSide join_table_side)
        {
            auto creating_set_step = std::make_unique<CreateSetAndFilterOnTheFlyStep>(
                plan.getCurrentHeader(), key_names, settings[Setting::max_rows_in_set_to_optimize_join], crosswise_connection, join_table_side);
            creating_set_step->setStepDescription(fmt::format("Create set and filter {} joined stream", join_table_side));

            auto * step_raw_ptr = creating_set_step.get();
            plan.addStep(std::move(creating_set_step));
            return step_raw_ptr;
        };

        if (join_algorithm->pipelineType() == JoinPipelineType::YShaped && join_kind != JoinKind::Paste)
        {
            const auto & join_clause = table_join->getOnlyClause();

            bool join_type_allows_filtering = (join_strictness == JoinStrictness::All || join_strictness == JoinStrictness::Any)
                                            && (isInner(join_kind) || isLeft(join_kind) || isRight(join_kind));


            auto has_non_const = [](const Block & block, const auto & keys)
            {
                for (const auto & key : keys)
                {
                    const auto & column = block.getByName(key).column;
                    if (column && !isColumnConst(*column))
                        return true;
                }
                return false;
            };

            /// This optimization relies on the sorting that should buffer data from both streams before emitting any rows.
            /// Sorting on a stream with const keys can start returning rows immediately and pipeline may stuck.
            /// Note: it's also doesn't work with the read-in-order optimization.
            /// No checks here because read in order is not applied if we have `CreateSetAndFilterOnTheFlyStep` in the pipeline between the reading and sorting steps.
            bool has_non_const_keys = has_non_const(left_plan.getCurrentHeader(), join_clause.key_names_left)
                && has_non_const(right_plan.getCurrentHeader(), join_clause.key_names_right);

            if (settings[Setting::max_rows_in_set_to_optimize_join] > 0 && join_type_allows_filtering && has_non_const_keys)
            {
                auto * left_set = add_create_set(left_plan, join_clause.key_names_left, JoinTableSide::Left);
                auto * right_set = add_create_set(right_plan, join_clause.key_names_right, JoinTableSide::Right);

                if (isInnerOrLeft(join_kind))
                    right_set->setFiltering(left_set->getSet());

                if (isInnerOrRight(join_kind))
                    left_set->setFiltering(right_set->getSet());
            }

            add_sorting(left_plan, join_clause.key_names_left, JoinTableSide::Left);
            add_sorting(right_plan, join_clause.key_names_right, JoinTableSide::Right);
        }

        auto join_pipeline_type = join_algorithm->pipelineType();

        ColumnIdentifierSet required_columns_after_join = outer_scope_columns;

        if (join_clauses_and_actions.residual_join_expressions_actions)
        {
            for (const auto * input : join_clauses_and_actions.residual_join_expressions_actions->getInputs())
                required_columns_after_join.insert(input->result_name);
        }

        if (required_columns_after_join.empty())
        {
            if (left_header.columns() > 1)
                required_columns_after_join.insert(left_header.getByPosition(0).name);
            else if (right_header.columns() > 1)
                required_columns_after_join.insert(right_header.getByPosition(0).name);
        }

        auto join_step = std::make_unique<JoinStep>(
            left_plan.getCurrentHeader(),
            right_plan.getCurrentHeader(),
            join_algorithm,
            settings[Setting::max_block_size],
            settings[Setting::min_joined_block_size_bytes],
            settings[Setting::max_threads],
            required_columns_after_join,
            false /*optimize_read_in_order*/,
            true /*optimize_skip_unused_shards*/);

        auto setting_swap = settings[Setting::query_plan_join_swap_table];
        join_step->swap_join_tables = setting_swap.is_auto ? std::nullopt : std::make_optional(setting_swap.base);

        join_step->setStepDescription(fmt::format("JOIN {}", join_pipeline_type));

        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::make_unique<QueryPlan>(std::move(left_plan)));
        plans.emplace_back(std::make_unique<QueryPlan>(std::move(right_plan)));

        result_plan.unitePlans(std::move(join_step), {std::move(plans)});
    }

    /// If residuals were not moved to JOIN algorithm,
    /// we need to process add then as WHERE condition after JOIN
    if (join_clauses_and_actions.residual_join_expressions_actions)
    {
        auto outputs = join_clauses_and_actions.residual_join_expressions_actions->getOutputs();
        if (outputs.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected 1 output column in JOIN actions, got {}",
                join_clauses_and_actions.residual_join_expressions_actions->dumpDAG());

        join_clauses_and_actions.residual_join_expressions_actions->appendInputsForUnusedColumns(result_plan.getCurrentHeader());
        for (const auto * input_node : join_clauses_and_actions.residual_join_expressions_actions->getInputs())
            join_clauses_and_actions.residual_join_expressions_actions->addOrReplaceInOutputs(*input_node);

        appendSetsFromActionsDAG(*join_clauses_and_actions.residual_join_expressions_actions, left_useful_sets);
        auto filter_step = std::make_unique<FilterStep>(result_plan.getCurrentHeader(),
            std::move(*join_clauses_and_actions.residual_join_expressions_actions),
            outputs[0]->result_name,
            /* remove_column = */ false); /// Unused columns will be removed by next step
        filter_step->setStepDescription("Residual JOIN filter");
        result_plan.addStep(std::move(filter_step));

        join_clauses_and_actions.residual_join_expressions_actions.reset();
    }

    const auto & header_after_join = result_plan.getCurrentHeader();
    if (header_after_join.columns() > outer_scope_columns.size())
    {
        auto drop_unused_columns_after_join_actions_dag = createStepToDropColumns(header_after_join, outer_scope_columns, planner_context);
        if (drop_unused_columns_after_join_actions_dag)
        {
            auto drop_unused_columns_after_join_transform_step = std::make_unique<ExpressionStep>(result_plan.getCurrentHeader(), std::move(*drop_unused_columns_after_join_actions_dag));
            drop_unused_columns_after_join_transform_step->setStepDescription("Drop unused columns after JOIN");
            result_plan.addStep(std::move(drop_unused_columns_after_join_transform_step));
        }
    }

    return {std::move(result_plan), std::move(join_algorithm)};
}

JoinTreeQueryPlan buildQueryPlanForCrossJoinNode(
    const QueryTreeNodePtr & join_table_expression,
    std::vector<JoinTreeQueryPlan> plans,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context,
    const SelectQueryInfo & select_query_info)
{
    auto & cross_join_node = join_table_expression->as<CrossJoinNode &>();
    for (const auto & plan : plans)
    {
        if (plan.from_stage != QueryProcessingStage::FetchColumns)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "JOIN {} table expression expected to process query to fetch columns stage. Actual {}",
                cross_join_node.formatASTForErrorMessage(),
                QueryProcessingStage::toString(plan.from_stage));
    }

    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    auto left_join_tree_query_plan = std::move(plans[0]);
    for (size_t i = 1; i < plans.size(); ++i)
    {
        auto right_join_tree_query_plan = std::move(plans[i]);

        auto left_plan = std::move(left_join_tree_query_plan.query_plan);
        auto right_plan = std::move(right_join_tree_query_plan.query_plan);

        JoinClausesAndActions join_clauses_and_actions;
        auto join_type = cross_join_node.getJoinTypes()[i - 1];

        auto table_join = std::make_shared<TableJoin>(settings, query_context->getGlobalTemporaryVolume(), query_context->getTempDataOnDisk());
        table_join->getTableJoin().kind = JoinKind::Cross;
        table_join->getTableJoin().locality = join_type.locality;

        auto [result_plan, join_algorithm] = buildJoinQueryPlan(
            std::move(left_plan),
            std::move(right_plan),
            table_join,
            join_clauses_and_actions,
            left_join_tree_query_plan.useful_sets,
            cross_join_node.getTableExpressions()[i],
            outer_scope_columns,
            planner_context,
            select_query_info);

        for (const auto & right_join_tree_query_plan_row_policy : right_join_tree_query_plan.used_row_policies)
            left_join_tree_query_plan.used_row_policies.insert(right_join_tree_query_plan_row_policy);

        /// Collect all required actions sets in `left_join_tree_query_plan.useful_sets`
        if (!join_algorithm->isFilled())
            for (const auto & useful_set : right_join_tree_query_plan.useful_sets)
                left_join_tree_query_plan.useful_sets.insert(useful_set);

        auto mapping = std::move(left_join_tree_query_plan.query_node_to_plan_step_mapping);
        auto & r_mapping = right_join_tree_query_plan.query_node_to_plan_step_mapping;
        mapping.insert(r_mapping.begin(), r_mapping.end());

        left_join_tree_query_plan =  JoinTreeQueryPlan{
            .query_plan = std::move(result_plan),
            .from_stage = QueryProcessingStage::FetchColumns,
            .used_row_policies = std::move(left_join_tree_query_plan.used_row_policies),
            .useful_sets = std::move(left_join_tree_query_plan.useful_sets),
            .query_node_to_plan_step_mapping = std::move(mapping),
        };
    }

    return left_join_tree_query_plan;
}

JoinTreeQueryPlan buildQueryPlanForJoinNodeLegacy(
    const QueryTreeNodePtr & join_table_expression,
    JoinTreeQueryPlan left_join_tree_query_plan,
    JoinTreeQueryPlan right_join_tree_query_plan,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context,
    const SelectQueryInfo & select_query_info)
{
    auto & join_node = join_table_expression->as<JoinNode &>();

    auto left_plan = std::move(left_join_tree_query_plan.query_plan);
    auto left_plan_output_columns = left_plan.getCurrentHeader().getColumnsWithTypeAndName();
    if (right_join_tree_query_plan.from_stage != QueryProcessingStage::FetchColumns)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "JOIN {} right table expression expected to process query to fetch columns stage. Actual {}",
            join_node.formatASTForErrorMessage(),
            QueryProcessingStage::toString(right_join_tree_query_plan.from_stage));

    auto right_plan = std::move(right_join_tree_query_plan.query_plan);
    auto right_plan_output_columns = right_plan.getCurrentHeader().getColumnsWithTypeAndName();

    JoinClausesAndActions join_clauses_and_actions;
    JoinKind join_kind = join_node.getKind();
    JoinStrictness join_strictness = join_node.getStrictness();

    std::optional<bool> join_constant;

    if (join_strictness == JoinStrictness::All || join_strictness == JoinStrictness::Semi  || join_strictness == JoinStrictness::Anti)
        join_constant = tryExtractConstantFromJoinNode(join_table_expression);


    bool can_move_out_residuals = false;

    if (!join_constant && join_node.isOnJoinExpression())
    {
        join_clauses_and_actions = buildJoinClausesAndActions(left_plan_output_columns,
            right_plan_output_columns,
            join_table_expression,
            planner_context);

        const auto & left_pre_filters = join_clauses_and_actions.join_clauses[0].getLeftFilterConditionNodes();
        const auto & right_pre_filters = join_clauses_and_actions.join_clauses[0].getRightFilterConditionNodes();
        auto check_pre_filter = [](JoinTableSide side, const auto & pre_filters)
        {
            if (!pre_filters.empty() && pre_filters.size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected only one {} pre-filter condition node. Actual [{}]",
                    side, fmt::join(pre_filters | std::views::transform([](const auto & node) { return node->result_name; }), ", "));
        };
        check_pre_filter(JoinTableSide::Left, left_pre_filters);
        check_pre_filter(JoinTableSide::Right, right_pre_filters);

        can_move_out_residuals = join_clauses_and_actions.join_clauses.size() == 1
            && join_strictness == JoinStrictness::All
            && (join_kind == JoinKind::Inner || join_kind == JoinKind::Cross || join_kind == JoinKind::Comma)
            && (right_pre_filters.empty() || FilterStep::canUseType(right_pre_filters[0]->result_type))
            && (left_pre_filters.empty() || FilterStep::canUseType(left_pre_filters[0]->result_type));

        auto add_pre_filter = [can_move_out_residuals](ActionsDAG & join_expressions_actions, QueryPlan & plan, UsefulSets & useful_sets, const auto & pre_filters)
        {
            join_expressions_actions.appendInputsForUnusedColumns(plan.getCurrentHeader());
            appendSetsFromActionsDAG(join_expressions_actions, useful_sets);

            QueryPlanStepPtr join_expressions_actions_step;
            if (can_move_out_residuals && !pre_filters.empty())
                join_expressions_actions_step = std::make_unique<FilterStep>(plan.getCurrentHeader(), std::move(join_expressions_actions), pre_filters[0]->result_name, false);
            else
                join_expressions_actions_step = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(join_expressions_actions));

            join_expressions_actions_step->setStepDescription("JOIN actions");
            plan.addStep(std::move(join_expressions_actions_step));
        };
        add_pre_filter(join_clauses_and_actions.left_join_expressions_actions, left_plan, left_join_tree_query_plan.useful_sets, left_pre_filters);
        add_pre_filter(join_clauses_and_actions.right_join_expressions_actions, right_plan, right_join_tree_query_plan.useful_sets, right_pre_filters);
    }

    std::unordered_map<ColumnIdentifier, DataTypePtr> left_plan_column_name_to_cast_type;
    std::unordered_map<ColumnIdentifier, DataTypePtr> right_plan_column_name_to_cast_type;

    if (join_node.isUsingJoinExpression())
    {
        auto & join_node_using_columns_list = join_node.getJoinExpression()->as<ListNode &>();
        for (auto & join_node_using_node : join_node_using_columns_list.getNodes())
        {
            auto & join_node_using_column_node = join_node_using_node->as<ColumnNode &>();
            auto & inner_columns_list = join_node_using_column_node.getExpressionOrThrow()->as<ListNode &>();

            auto & left_inner_column_node = inner_columns_list.getNodes().at(0);
            auto * left_inner_column = left_inner_column_node->as<ColumnNode>();
            if (!left_inner_column)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "JOIN USING clause expected column identifier. Actual {}",
                    left_inner_column_node->formatASTForErrorMessage());

            auto & right_inner_column_node = inner_columns_list.getNodes().at(1);
            auto * right_inner_column = right_inner_column_node->as<ColumnNode>();
            if (!right_inner_column)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "JOIN USING clause expected column identifier. Actual {}",
                    right_inner_column_node->formatASTForErrorMessage());

            const auto & join_node_using_column_node_type = join_node_using_column_node.getColumnType();
            if (!left_inner_column->getColumnType()->equals(*join_node_using_column_node_type))
            {
                const auto & left_inner_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(left_inner_column_node);
                left_plan_column_name_to_cast_type.emplace(left_inner_column_identifier, join_node_using_column_node_type);
            }

            if (!right_inner_column->getColumnType()->equals(*join_node_using_column_node_type))
            {
                const auto & right_inner_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(right_inner_column_node);
                right_plan_column_name_to_cast_type.emplace(right_inner_column_identifier, join_node_using_column_node_type);
            }
        }
    }

    auto join_cast_plan_output_nodes = [&](QueryPlan & plan_to_add_cast, std::unordered_map<std::string, DataTypePtr> & plan_column_name_to_cast_type)
    {
        ActionsDAG cast_actions_dag(plan_to_add_cast.getCurrentHeader().getColumnsWithTypeAndName());

        for (auto & output_node : cast_actions_dag.getOutputs())
        {
            auto it = plan_column_name_to_cast_type.find(output_node->result_name);
            if (it == plan_column_name_to_cast_type.end())
                continue;

            const auto & cast_type = it->second;
            output_node = &cast_actions_dag.addCast(*output_node, cast_type, output_node->result_name);
        }

        cast_actions_dag.appendInputsForUnusedColumns(plan_to_add_cast.getCurrentHeader());
        auto cast_join_columns_step
            = std::make_unique<ExpressionStep>(plan_to_add_cast.getCurrentHeader(), std::move(cast_actions_dag));
        cast_join_columns_step->setStepDescription("Cast JOIN USING columns");
        plan_to_add_cast.addStep(std::move(cast_join_columns_step));
    };

    if (!left_plan_column_name_to_cast_type.empty())
        join_cast_plan_output_nodes(left_plan, left_plan_column_name_to_cast_type);

    if (!right_plan_column_name_to_cast_type.empty())
        join_cast_plan_output_nodes(right_plan, right_plan_column_name_to_cast_type);

    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    if (settings[Setting::join_use_nulls])
        joinCastPlanColumnsToNullable(left_plan, right_plan, planner_context, join_kind);

    auto table_join = std::make_shared<TableJoin>(settings, query_context->getGlobalTemporaryVolume(), query_context->getTempDataOnDisk());
    table_join->getTableJoin() = join_node.toASTTableJoin()->as<ASTTableJoin &>();

    if (join_constant)
    {
        /** If there is JOIN with always true constant, we transform it to cross.
          * If there is JOIN with always false constant, we do not process JOIN keys.
          * It is expected by join algorithm to handle such case.
          *
          * Example: SELECT * FROM test_table AS t1 INNER JOIN test_table AS t2 ON 1;
          */
        if (*join_constant)
            join_kind = JoinKind::Cross;
    }

    if (join_kind == JoinKind::Comma)
        join_kind = JoinKind::Cross;

    table_join->getTableJoin().kind = join_kind;

    table_join->setIsJoinWithConstant(join_constant != std::nullopt);

    if (join_node.isOnJoinExpression())
    {
        const auto & join_clauses = join_clauses_and_actions.join_clauses;
        bool is_asof = table_join->strictness() == JoinStrictness::Asof;

        if (join_clauses.size() != 1 && is_asof)
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "ASOF join doesn't support JOIN ON expression {}",
                join_node.formatASTForErrorMessage());
        }

        auto & table_join_clauses = table_join->getClauses();

        for (const auto & join_clause : join_clauses)
        {
            table_join_clauses.emplace_back();

            const auto & join_clause_left_key_nodes = join_clause.getLeftKeyNodes();
            const auto & join_clause_right_key_nodes = join_clause.getRightKeyNodes();

            size_t join_clause_key_nodes_size = join_clause_left_key_nodes.size();
            chassert(join_clause_key_nodes_size == join_clause_right_key_nodes.size());

            if (join_clause_key_nodes_size == 0 && !can_move_out_residuals)
                throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Cannot determine join keys in {}", join_node.formatASTForErrorMessage());

            /// If there are no keys, but only conditions that cannot be used as keys, then it is a cross join.
            /// Example: SELECT * FROM t1 JOIN t2 ON t1.x > t2.y
            /// Same as: SELECT * FROM t1 CROSS JOIN t2 WHERE t1.x > t2.y
            if (join_clause_key_nodes_size == 0 && can_move_out_residuals)
            {
                table_join->getTableJoin().kind = JoinKind::Cross;
                table_join->setIsJoinWithConstant(true);
                table_join_clauses.pop_back();
                continue;
            }

            auto & table_join_clause = table_join_clauses.back();
            for (size_t i = 0; i < join_clause_key_nodes_size; ++i)
            {
                table_join_clause.addKey(join_clause_left_key_nodes[i]->result_name,
                                         join_clause_right_key_nodes[i]->result_name,
                                         join_clause.isNullsafeCompareKey(i));
            }

            const auto & join_clause_get_left_filter_condition_nodes = join_clause.getLeftFilterConditionNodes();
            if (!join_clause_get_left_filter_condition_nodes.empty())
            {
                if (join_clause_get_left_filter_condition_nodes.size() != 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "JOIN {} left filter conditions size must be 1. Actual {}",
                        join_node.formatASTForErrorMessage(),
                        join_clause_get_left_filter_condition_nodes.size());

                if (!can_move_out_residuals)
                {
                    const auto & join_clause_left_filter_condition_name = join_clause_get_left_filter_condition_nodes[0]->result_name;
                    table_join_clause.analyzer_left_filter_condition_column_name = join_clause_left_filter_condition_name;
                }
            }

            const auto & join_clause_get_right_filter_condition_nodes = join_clause.getRightFilterConditionNodes();
            if (!join_clause_get_right_filter_condition_nodes.empty())
            {
                if (join_clause_get_right_filter_condition_nodes.size() != 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "JOIN {} right filter conditions size must be 1. Actual {}",
                        join_node.formatASTForErrorMessage(),
                        join_clause_get_right_filter_condition_nodes.size());

                if (!can_move_out_residuals)
                {
                    const auto & join_clause_right_filter_condition_name = join_clause_get_right_filter_condition_nodes[0]->result_name;
                    table_join_clause.analyzer_right_filter_condition_column_name = join_clause_right_filter_condition_name;
                }
            }

            if (is_asof)
            {
                if (!join_clause.hasASOF())
                    throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                        "JOIN {} no inequality in ASOF JOIN ON section",
                        join_node.formatASTForErrorMessage());
            }

            if (join_clause.hasASOF())
            {
                const auto & asof_conditions = join_clause.getASOFConditions();
                if (asof_conditions.size() > 1)
                {
                    throw Exception(
                        ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                        "JOIN {} ASOF JOIN expects exactly one inequality in ON section",
                        join_node.formatASTForErrorMessage());
                }

                const auto & asof_condition = asof_conditions[0];
                table_join->setAsofInequality(asof_condition.asof_inequality);

                /// Execution layer of JOIN algorithms expects that ASOF keys are last JOIN keys
                std::swap(table_join_clause.key_names_left.at(asof_condition.key_index), table_join_clause.key_names_left.back());
                std::swap(table_join_clause.key_names_right.at(asof_condition.key_index), table_join_clause.key_names_right.back());
            }
        }

        if (!can_move_out_residuals && join_clauses_and_actions.residual_join_expressions_actions)
        {
            /// Let join algorithm handle residual conditions
            ExpressionActionsPtr & mixed_join_expression = table_join->getMixedJoinExpression();
            mixed_join_expression = std::make_shared<ExpressionActions>(
                std::move(*join_clauses_and_actions.residual_join_expressions_actions),
                ExpressionActionsSettings(planner_context->getQueryContext()));

            appendSetsFromActionsDAG(mixed_join_expression->getActionsDAG(), left_join_tree_query_plan.useful_sets);
            join_clauses_and_actions.residual_join_expressions_actions.reset();
        }
    }
    else if (join_node.isUsingJoinExpression())
    {
        auto & table_join_clauses = table_join->getClauses();
        table_join_clauses.emplace_back();
        auto & table_join_clause = table_join_clauses.back();

        auto & using_list = join_node.getJoinExpression()->as<ListNode &>();

        for (auto & join_using_node : using_list.getNodes())
        {
            auto & join_using_column_node = join_using_node->as<ColumnNode &>();
            auto & using_join_columns_list = join_using_column_node.getExpressionOrThrow()->as<ListNode &>();
            auto & using_join_left_join_column_node = using_join_columns_list.getNodes().at(0);
            auto & using_join_right_join_column_node = using_join_columns_list.getNodes().at(1);

            const auto & left_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(using_join_left_join_column_node);
            const auto & right_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(using_join_right_join_column_node);

            table_join_clause.key_names_left.push_back(left_column_identifier);
            table_join_clause.key_names_right.push_back(right_column_identifier);
        }
    }

    auto [result_plan, join_algorithm] = buildJoinQueryPlan(
        std::move(left_plan),
        std::move(right_plan),
        table_join,
        join_clauses_and_actions,
        left_join_tree_query_plan.useful_sets,
        join_node.getRightTableExpression(),
        outer_scope_columns,
        planner_context,
        select_query_info);

    for (const auto & right_join_tree_query_plan_row_policy : right_join_tree_query_plan.used_row_policies)
        left_join_tree_query_plan.used_row_policies.insert(right_join_tree_query_plan_row_policy);

    /// Collect all required actions sets in `left_join_tree_query_plan.useful_sets`
    if (!join_algorithm->isFilled())
        for (const auto & useful_set : right_join_tree_query_plan.useful_sets)
            left_join_tree_query_plan.useful_sets.insert(useful_set);

    auto mapping = std::move(left_join_tree_query_plan.query_node_to_plan_step_mapping);
    auto & r_mapping = right_join_tree_query_plan.query_node_to_plan_step_mapping;
    mapping.insert(r_mapping.begin(), r_mapping.end());

    return JoinTreeQueryPlan{
        .query_plan = std::move(result_plan),
        .from_stage = QueryProcessingStage::FetchColumns,
        .used_row_policies = std::move(left_join_tree_query_plan.used_row_policies),
        .useful_sets = std::move(left_join_tree_query_plan.useful_sets),
        .query_node_to_plan_step_mapping = std::move(mapping),
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
        .from_stage = QueryProcessingStage::FetchColumns,
        .used_row_policies = std::move(result_used_row_policies),
        .useful_sets = std::move(result_useful_sets),
        .query_node_to_plan_step_mapping = std::move(result_mapping),
    };
}

JoinTreeQueryPlan buildQueryPlanForJoinNode(
    const QueryTreeNodePtr & join_table_expression,
    JoinTreeQueryPlan left_join_tree_query_plan,
    JoinTreeQueryPlan right_join_tree_query_plan,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context,
    const SelectQueryInfo & select_query_info)
{
    auto & join_node = join_table_expression->as<JoinNode &>();
    if (left_join_tree_query_plan.from_stage != QueryProcessingStage::FetchColumns)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "JOIN {} left table expression expected to process query to fetch columns stage. Actual {}",
            join_node.formatASTForErrorMessage(),
            QueryProcessingStage::toString(left_join_tree_query_plan.from_stage));

    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();
    if (!settings[Setting::query_plan_use_new_logical_join_step] || settings[Setting::allow_experimental_parallel_reading_from_replicas])
        return buildQueryPlanForJoinNodeLegacy(
            join_table_expression, std::move(left_join_tree_query_plan), std::move(right_join_tree_query_plan), outer_scope_columns, planner_context, select_query_info);

    auto join_step_logical = buildJoinStepLogical(
        left_join_tree_query_plan.query_plan.getCurrentHeader(),
        right_join_tree_query_plan.query_plan.getCurrentHeader(),
        outer_scope_columns,
        join_node,
        planner_context);

    PreparedJoinStorage prepared_join;
    bool allow_storage_join = right_join_tree_query_plan.used_row_policies.empty()
        && right_join_tree_query_plan.from_stage == QueryProcessingStage::FetchColumns
        && right_join_tree_query_plan.useful_sets.empty();
    if (allow_storage_join)
        prepared_join = tryGetStorageInTableJoin(join_node.getRightTableExpression(), planner_context);
    if (prepared_join)
    {
        auto join_lookup_step = std::make_unique<JoinStepLogicalLookup>(std::move(right_join_tree_query_plan.query_plan), std::move(prepared_join));
        right_join_tree_query_plan.query_plan = {};
        right_join_tree_query_plan.query_plan.addStep(std::move(join_lookup_step));
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
    if (join_tree_query_plan.from_stage != QueryProcessingStage::FetchColumns)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "ARRAY JOIN {} table expression expected to process query to fetch columns stage. Actual {}",
            array_join_node.formatASTForErrorMessage(),
            QueryProcessingStage::toString(join_tree_query_plan.from_stage));

    auto plan = std::move(join_tree_query_plan.query_plan);
    auto plan_output_columns = plan.getCurrentHeader().getColumnsWithTypeAndName();

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

    array_join_action_dag.appendInputsForUnusedColumns(plan.getCurrentHeader());

    auto array_join_actions = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(array_join_action_dag));
    array_join_actions->setStepDescription("ARRAY JOIN actions");
    appendSetsFromActionsDAG(array_join_actions->getExpression(), join_tree_query_plan.useful_sets);
    plan.addStep(std::move(array_join_actions));

    ActionsDAG drop_unused_columns_before_array_join_actions_dag(plan.getCurrentHeader().getColumnsWithTypeAndName());
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
        settings[Setting::max_block_size]);

    array_join_step->setStepDescription("ARRAY JOIN");
    plan.addStep(std::move(array_join_step));

    return JoinTreeQueryPlan{
        .query_plan = std::move(plan),
        .from_stage = QueryProcessingStage::FetchColumns,
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
            continue;
        }

        if (table_expression_type == QueryTreeNodeType::JOIN)
        {
            ++joins_count;
            const auto & join_node = table_expression->as<const JoinNode &>();
            if (join_node.getKind() == JoinKind::Full)
                is_full_join = true;

            continue;
        }

        prepareBuildQueryPlanForTableExpression(table_expression, planner_context);
    }

    /// disable parallel replicas for n-way join with FULL JOIN involved
    if (joins_count > 1 && is_full_join)
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
      * Examples: Distributed, LiveView, Merge storages.
      */
    auto left_table_expression = table_expressions_stack.front();
    auto left_table_expression_query_plan = buildQueryPlanForTableExpression(
        left_table_expression,
        parent_join_tree,
        select_query_info,
        select_query_options,
        planner_context,
        is_single_table_expression,
        false /*wrap_read_columns_in_subquery*/);
    if (left_table_expression_query_plan.from_stage != QueryProcessingStage::FetchColumns)
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
                planner_context,
                select_query_info));
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
                planner_context,
                select_query_info));
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
                if (node->getNodeType() == QueryTreeNodeType::JOIN || node->getNodeType() == QueryTreeNodeType::ARRAY_JOIN)
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
                join_tree_node,
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
