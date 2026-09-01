#include <Columns/IColumn.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/convertColumnToType.h>
#include <Planner/PlannerJoinTree.h>

#include <Core/Settings.h>

#include <Core/ParallelReplicasMode.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/quoteString.h>
#include <Common/scope_guard_safe.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Functions/FunctionFactory.h>

#include <AggregateFunctions/AggregateFunctionCount.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>

#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/IStorageCluster.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/SparsityFilter.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageView.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageMerge.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageValues.h>
#include <Storages/buildQueryTreeForShard.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/QueryTreeBuilder.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTSubquery.h>
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
#include <Processors/QueryPlan/ParallelReplicasLocalPlan.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/getCustomKeyFilterForParallelReplicas.h>
#include <Interpreters/ClusterProxy/executeQuery.h>

#include <Planner/CollectColumnIdentifiers.h>
#include <Planner/Planner.h>
#include <Planner/PlannerContext.h>
#include <Planner/PlannerJoins.h>
#include <Planner/PlannerJoinsLogical.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/Utils.h>
#include <Planner/CollectSets.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/collectSelectedColumnsFromTable.h>

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
    extern const SettingsBool optimize_trivial_view_pushdown_to_distributed;
    extern const SettingsUInt64 distributed_group_by_no_merge;
    extern const SettingsBool optimize_skip_unused_shards;
    extern const SettingsUInt64 force_optimize_skip_unused_shards;
    extern const SettingsBool extremes;
    extern const SettingsBool exact_rows_before_limit;
    extern const SettingsBool async_socket_for_remote;
    extern const SettingsBool empty_result_for_aggregation_by_empty_set;
    extern const SettingsBool enable_cascades_optimizer;
    extern const SettingsBool enable_unaligned_array_join;
    extern const SettingsBool join_use_nulls;
    extern const SettingsDouble limit;
    extern const SettingsBool make_distributed_plan;
    extern const SettingsDouble offset;
    extern const SettingsBool prefer_column_name_to_alias;
    extern const SettingsJoinAlgorithm join_algorithm;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 max_columns_to_read;
    extern const SettingsUInt64 max_distributed_connections;
    extern const SettingsUInt64 max_rows_in_set_to_optimize_join;
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsFloat max_streams_to_max_threads_ratio;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 max_threads_min_free_memory_per_thread;
    extern const SettingsBool optimize_sorting_by_input_stream_properties;
    extern const SettingsBool optimize_trivial_count_query;
    extern const SettingsBool optimize_trivial_count_with_sparsity_filter;
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
    extern const SettingsBool parallel_replicas_plan_based;
    extern const SettingsBool use_query_condition_cache;
    extern const SettingsBool use_query_condition_cache_for_top_k;
    extern const SettingsBool use_skip_indexes_for_top_k;
    extern const SettingsBool use_top_k_dynamic_filtering;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ACCESS_DENIED;
    extern const int ILLEGAL_PREWHERE;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int TOO_MANY_COLUMNS;
    extern const int UNSUPPORTED_METHOD;
}

namespace
{

/// Recursively find the first TableNode whose storage matches `target`.
QueryTreeNodePtr findTableNodeByStorage(const QueryTreeNodePtr & node, const StoragePtr & target)
{
    const auto * tn = node->as<TableNode>();
    if (tn && tn->getStorage() == target)
    {
        return node;
    }

    for (const auto & child : node->getChildren())
    {
        if (child)
        {
            auto found = findTableNodeByStorage(child, target);
            if (found)
            {
                return found;
            }
        }
    }
    return nullptr;
}

/// Returns true if `node` contains any function whose IFunctionBase::isDeterministic() is false.
/// Used by the view-pushdown gate: the optimization rewrites the outer query so its expressions
/// run on shards instead of the coordinator, which changes results for functions like hostName,
/// serverUUID, nowInBlock, blockNumber, rand, now etc. Aggregate and window functions are not
/// flagged here — FunctionNode::getFunction() only returns the resolved IFunctionBase for
/// ordinary functions, returning nullptr otherwise.
///
/// ConstantNode is also inspected because the analyzer constant-folds suitable function calls
/// (server-local constants such as hostName, serverUUID, version) into ConstantNodes that carry
/// is_deterministic = false (see resolveFunction.cpp:1766). Without this branch the visitor
/// would miss folded calls — only functions that vary per row (rand, now, ...) stay as
/// FunctionNodes after analysis. Plain literals (SELECT 1) use the default-deterministic
/// ConstantNode constructor and are not flagged.
bool containsNonDeterministicFunction(const QueryTreeNodePtr & node)
{
    if (!node)
        return false;

    if (const auto * function_node = node->as<FunctionNode>())
    {
        if (auto function = function_node->getFunction(); function && !function->isDeterministic())
            return true;
    }

    if (const auto * constant_node = node->as<ConstantNode>())
    {
        if (!constant_node->isDeterministic())
            return true;
    }

    for (const auto & child : node->getChildren())
    {
        if (containsNonDeterministicFunction(child))
            return true;
    }
    return false;
}

/// AST-level counterpart of containsNonDeterministicFunction. Used for predicates that the
/// pushdown injects as shard-side filters but that are NOT present in the outer query tree the
/// QueryTree-based check above runs on: the view's row policy and view-keyed
/// additional_table_filters. On the normal StorageView path these are coordinator-side filters;
/// the pushdown evaluates them on each shard, so a non-deterministic / server-local function in
/// them (hostName, serverUUID, rand, now, ...) would change results once the optimization fires.
/// A function is unsafe when its builder reports isDeterministic() == false (no constant-folding
/// has happened at the AST stage, so server-local constants are still ASTFunction nodes here).
bool astContainsNonDeterministicFunction(const ASTPtr & ast, const ContextPtr & context)
{
    if (!ast)
        return false;

    if (const auto * function = ast->as<ASTFunction>())
    {
        if (!function->name.empty() && function->name != "lambda")
        {
            auto builder = FunctionFactory::instance().tryGet(function->name, context);
            if (!builder || !builder->isDeterministic())
                return true;
        }
    }

    for (const auto & child : ast->children)
    {
        if (astContainsNonDeterministicFunction(child, context))
            return true;
    }
    return false;
}

/// Returns true if any descendant of `node` (the outer query) is a subquery, i.e. a QUERY or
/// UNION node. `node` itself is the outer query and is intentionally not treated as a subquery.
///
/// The pushdown ships the outer query to the shards. A subquery in it (e.g. the right-hand side
/// of `IN (SELECT ...)`) is evaluated once on the coordinator on the normal StorageView path, but
/// per-shard once the optimization fires; if it reads an initiator-local table, or one whose
/// contents/privileges differ between shards, the result can change or the query can start
/// throwing. So suppress the optimization whenever the outer query contains a subquery.
///
/// At this planner stage `IN (subquery)` still carries its subquery as a QUERY/UNION child of the
/// `in` function (see CollectSets.cpp), so it is found here. Scalar subqueries that the analyzer
/// already constant-folded live in ConstantNode::source_expression, which is NOT a child
/// (children_size == 0), so they are correctly ignored: by then they are constants evaluated on
/// the initiator and safe to ship.
bool containsSubqueryNode(const QueryTreeNodePtr & node)
{
    for (const auto & child : node->getChildren())
    {
        if (!child)
            continue;
        const auto child_node_type = child->getNodeType();
        if (child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION)
            return true;
        if (containsSubqueryNode(child))
            return true;
    }
    return false;
}

/// AST-level subquery detector, the counterpart of containsSubqueryNode for the predicates the
/// pushdown injects as shard-side filters (the view's row policy and view-keyed
/// additional_table_filters), which are not part of the outer query tree. They are coordinator-side
/// filters on the normal StorageView path; a subquery inside them would be evaluated per-shard once
/// the optimization fires, with the same divergence risk described above, so suppress the
/// optimization when present. Mirrors hasSubquery in StorageView.cpp.
bool astContainsSubquery(const ASTPtr & ast)
{
    if (!ast)
        return false;

    if (ast->as<ASTSubquery>())
        return true;

    for (const auto & child : ast->children)
    {
        if (astContainsSubquery(child))
            return true;
    }
    return false;
}

/// Check if current user has privileges to SELECT columns from table
/// Throws an exception if access to any column from `column_names` is not granted
/// If `column_names` is empty, check access to any columns and return names of accessible columns
NameSet checkAccessRights(const StoragePtr & storage, const StorageID & storage_id, const StorageSnapshotPtr & storage_snapshot, const Names & column_names, const ContextPtr & query_context)
{
    /// StorageDummy is created on preliminary stage, ignore access check for it.
    if (typeid_cast<const StorageDummy *>(storage.get()))
        return {};

    if (column_names.empty())
    {
        NameSet accessible_columns;
        /** For a trivial queries like "SELECT count() FROM table", "SELECT 1 FROM table" access is granted if at least
          * one table column is accessible.
          */
        auto access = query_context->getAccess();
        const auto * alias = storage->as<StorageAlias>();
        for (const auto & column : storage_snapshot->metadata->getColumns())
        {
            /// An `Alias` also requires access to the selected column of its target table.
            if (access->isGranted(AccessType::SELECT, storage_id.database_name, storage_id.table_name, column.name)
                && (!alias || alias->isTargetTableGranted(query_context, AccessType::SELECT, column.name)))
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
        /// Keep only the columns the user is allowed to read, so that a trivial query reads a column
        /// it has access to. But if none of the allowed columns is a physical column (e.g. the user is
        /// granted access only to ALIAS columns, which are not physical and therefore never appear in
        /// `column_names_and_types`), the filter below would remove everything. In that case we keep all
        /// physical columns: reading any of them just to determine the number of rows for a trivial query
        /// (such as `SELECT count()`) is allowed, because computing an accessible ALIAS column requires
        /// reading its physical source columns anyway and no column values are exposed to the user.
        bool has_allowed_physical_column = std::any_of(
            column_names_and_types.begin(),
            column_names_and_types.end(),
            [&](const auto & column) { return column_names_allowed_to_select.contains(column.name); });

        if (has_allowed_physical_column)
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
    {
        /// A table expression can resolve to no columns at all, for example a table function over a
        /// table whose schema is unavailable. `getSmallestColumn` treats an empty list as a logical error.
        if (column_names_and_types.empty())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Cannot read from table expression with no columns");

        /// If we have no information about columns sizes, choose a column of minimum size of its data type
        result = ExpressionActions::getSmallestColumn(column_names_and_types);
    }

    return result;
}

/// True if the table expression carries modifiers that prevent answering a
/// `SELECT count()` from whole-table statistics. `FINAL`, sampling and `STREAM`
/// all reshape the row set under the count, so the rewrite must be skipped.
bool hasTrivialCountIncompatibleModifiers(
    const TableNode * table_node, const TableFunctionNode * table_function_node)
{
    auto disqualifies = [](const std::optional<TableExpressionModifiers> & m)
    {
        return m.has_value()
            && (m->hasFinal() || m->hasSampleSizeRatio() || m->hasSampleOffsetRatio() || m->hasStream());
    };
    if (table_node && disqualifies(table_node->getTableExpressionModifiers()))
        return true;
    if (table_function_node && disqualifies(table_function_node->getTableExpressionModifiers()))
        return true;
    return false;
}

/// Returns the effective row policy filter for the table, or nullptr if the
/// table has no row policies for the current user or the combined filter is
/// always-true. Mirrors the effective-filter check used by
/// buildRowPolicyFilterIfNeeded.
RowPolicyFilterPtr getEffectiveRowPolicyFilter(const StoragePtr & storage, const ContextPtr & query_context)
{
    auto storage_id = storage->getStorageID();
    if (!storage_id.hasDatabase())
        return nullptr;
    auto row_policy_filter = query_context->getRowPolicyFilter(
        storage_id.getDatabaseName(), storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
    if (!row_policy_filter || row_policy_filter->isAlwaysTrue())
        return nullptr;
    return row_policy_filter;
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

    /// The rewrite produces a `ReadFromPreparedSource` leaf that the Cascades optimizer cannot
    /// clone; a distributed plan counts the rows with a distributed read instead.
    if (settings[Setting::make_distributed_plan] && settings[Setting::enable_cascades_optimizer])
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

    if (hasTrivialCountIncompatibleModifiers(table_node, table_function_node))
        return false;

    // TODO: It's possible to optimize count() given only partition predicates
    auto & main_query_node = query_tree->as<QueryNode &>();
    if (main_query_node.hasGroupBy() || main_query_node.hasPrewhere() || main_query_node.hasWhere())
        return false;

    if (settings[Setting::empty_result_for_aggregation_by_empty_set])
        return false;

    QueryTreeNodes aggregates = collectAggregateFunctionNodes(query_tree);
    if (aggregates.size() != 1)
        return false;

    const auto & function_node = aggregates.front().get()->as<const FunctionNode &>();
    chassert(function_node.getAggregateFunction() != nullptr);
    const auto * count_func = typeid_cast<const AggregateFunctionCount *>(function_node.getAggregateFunction().get());
    if (!count_func)
        return false;

    /// `arrayJoin` in the argument multiplies rows above the source read, so the aggregate does not
    /// observe `totalRows()` rows. Must precede `optimize_trivial_count`: storages that count in
    /// read() act on that flag even when this function later declines.
    if (hasFunctionNode(aggregates.front(), "arrayJoin"))
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

    /// Use the aggregate function's action node identifier (e.g. `count()`) as the column
    /// name so the emitted block already matches the header the outer planner expects at
    /// `WithMergeableState`. This lets the caller skip both the rename step and the
    /// recursive `Planner` that was only used to derive the expected header.
    String trivial_count_column_name = calculateActionNodeName(aggregates.front(), planner_context);
    if (trivial_count_column_name.empty())
        trivial_count_column_name = columns_names.front();

    auto block_with_count = std::make_shared<const Block>(Block{
        {createSingleCountStateColumn(function_node.getAggregateFunction(), num_rows.value()),
         std::make_shared<DataTypeAggregateFunction>(function_node.getAggregateFunction(), agg_count.getArgumentTypes(), Array{}),
         trivial_count_column_name}});

    auto source = std::make_shared<SourceFromSingleChunk>(block_with_count);
    auto prepared_count = std::make_unique<ReadFromPreparedSource>(Pipe(std::move(source)));
    prepared_count->setStepDescription("Optimized trivial count");
    query_plan.addStep(std::move(prepared_count));

    return true;
}

/// Serve `SELECT count() FROM t WHERE <predicate>` from per-column `(num_rows, num_defaults)`
/// stats when `<predicate>` partitions rows into defaults vs non-defaults of one column.
/// Sister of `applyTrivialCountIfPossible` with the same opt-outs, but this path *requires*
/// a `WHERE`. `SparsityFilter.h` documents the reliability rules and recognised shapes.
bool applyTrivialCountWithSparsityFilterIfPossible(
    QueryPlan & query_plan,
    SelectQueryInfo & select_query_info,
    const TableNode * table_node,
    const TableFunctionNode * table_function_node,
    const QueryTreeNodePtr & query_tree,
    const QueryTreeNodePtr & table_expression_node,
    ContextMutablePtr & query_context,
    const Names & columns_names,
    const PlannerContext & planner_context)
{
    const auto & settings = query_context->getSettingsRef();
    /// Extension of `optimize_trivial_count_query`: respect the base kill switch so
    /// disabling the parent setting also disables this variant.
    if (!settings[Setting::optimize_trivial_count_query]
        || !settings[Setting::optimize_trivial_count_with_sparsity_filter])
        return false;

    /// The rewrite produces a `ReadFromPreparedSource` leaf that the Cascades optimizer cannot
    /// clone; a distributed plan counts the rows with a distributed read instead.
    if (settings[Setting::make_distributed_plan] && settings[Setting::enable_cascades_optimizer])
        return false;

    const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
    if (!storage->supportsTrivialCountOptimization(
            table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot(), query_context))
        return false;

    if (getEffectiveRowPolicyFilter(storage, query_context))
        return false;

    if (select_query_info.additional_filter_ast)
        return false;

    if (query_context->getCurrentTransaction())
        return false;

    if (hasTrivialCountIncompatibleModifiers(table_node, table_function_node))
        return false;

    /// `WHERE` is required for classification; `GROUP BY` / `PREWHERE` / `HAVING` /
    /// `QUALIFY` would reshape the count we're trying to read off the stats.
    auto & main_query_node = query_tree->as<QueryNode &>();
    if (!main_query_node.hasWhere())
        return false;
    if (main_query_node.hasGroupBy() || main_query_node.hasPrewhere() || main_query_node.hasHaving() || main_query_node.hasQualify())
        return false;

    if (settings[Setting::empty_result_for_aggregation_by_empty_set])
        return false;

    QueryTreeNodes aggregates = collectAggregateFunctionNodes(query_tree);
    if (aggregates.size() != 1)
        return false;
    const auto & function_node = aggregates.front().get()->as<const FunctionNode &>();
    chassert(function_node.getAggregateFunction() != nullptr);
    const auto * count_func = typeid_cast<const AggregateFunctionCount *>(function_node.getAggregateFunction().get());
    if (!count_func)
        return false;

    /// Only zero-argument `count()` / `count(*)` counts rows. `count(expr)` counts non-null
    /// argument values, which the rewrite (which seeds the state with a row count derived from
    /// the per-column `num_defaults`) does not preserve for Nullable/expression counts.
    if (!function_node.getArguments().getNodes().empty())
        return false;

    auto classified = classifySparsityPredicate(main_query_node.getWhere(), table_expression_node);
    if (!classified)
        return false;

    auto stats = storage->getColumnDefaultnessStats(classified->column_name, query_context);
    if (!stats)
        return false;

    /// Disable parallel replicas: otherwise each remote shard would independently
    /// rewrite and the final result would be multiplied by the replica count.
    if (settings[Setting::allow_experimental_parallel_reading_from_replicas] > 0 && settings[Setting::max_parallel_replicas] > 1)
    {
        if (settings[Setting::parallel_replicas_mode] == ParallelReplicasMode::CUSTOM_KEY_RANGE ||
            settings[Setting::parallel_replicas_mode] == ParallelReplicasMode::CUSTOM_KEY_SAMPLING ||
            settings[Setting::parallel_replicas_mode] == ParallelReplicasMode::SAMPLING_KEY)
            return false;
        query_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
        LOG_TRACE(getLogger("Planner"), "Disabling parallel replicas to be able to use a trivial count with sparsity filter optimization");
    }

    select_query_info.optimize_trivial_count = true;

    UInt64 num_rows = (classified->predicate_class == SparsityPredicateClass::MatchesDefault)
        ? stats->num_defaults
        : (stats->num_rows - stats->num_defaults);

    const AggregateFunctionCount & agg_count = *count_func;

    String trivial_count_column_name = calculateActionNodeName(aggregates.front(), planner_context);
    if (trivial_count_column_name.empty())
        trivial_count_column_name = columns_names.front();

    auto block_with_count = std::make_shared<const Block>(Block{
        {createSingleCountStateColumn(function_node.getAggregateFunction(), num_rows),
         std::make_shared<DataTypeAggregateFunction>(function_node.getAggregateFunction(), agg_count.getArgumentTypes(), Array{}),
         trivial_count_column_name}});

    auto source = std::make_shared<SourceFromSingleChunk>(block_with_count);
    auto prepared_count = std::make_unique<ReadFromPreparedSource>(Pipe(std::move(source)));
    prepared_count->setStepDescription("Optimized trivial count with sparsity filter");
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
        columns_names_allowed_to_select = checkAccessRights(
            table_node->getStorage(), table_node->getStorageID(), table_node->getStorageSnapshot(), column_names_with_aliases, query_context);
    }
    else if (table_function_node)
    {
        /// A parameterized view is resolved as a `TableFunctionNode` that wraps a real `StorageView`, but no
        /// `ITableFunction::execute` runs for it, so the access check skipped above for regular table functions
        /// would let the query read the view without any `SELECT` grant. Enforce the same column-aware `SELECT`
        /// check the underlying view would receive as a `TableNode`.
        const auto & storage = table_function_node->getStorage();
        if (const auto * storage_view = storage ? storage->as<StorageView>() : nullptr; storage_view && storage_view->isParameterizedView())
        {
            const auto & column_names_with_aliases = table_expression_data.getSelectedColumnsNames();
            columns_names_allowed_to_select = checkAccessRights(
                storage, table_function_node->getStorageID(), table_function_node->getStorageSnapshot(), column_names_with_aliases, query_context);
        }
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
    std::set<std::string> & used_row_policies,
    NameSet required_names_without_filter = {})
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

    return buildFilterInfo(
        row_policy_filter->expression,
        table_expression_query_info.table_expression,
        planner_context,
        std::move(required_names_without_filter));
}

std::optional<FilterDAGInfo> buildCustomKeyFilterIfNeeded(const StoragePtr & storage,
    SelectQueryInfo & table_expression_query_info,
    PlannerContextPtr & planner_context)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    if (settings[Setting::parallel_replicas_count] <= 1)
        return {};

    /// An empty custom key is not skipped silently on purpose: the caller has already checked that the custom key
    /// filtering is requested, and this replica has been given an offset to read only its own part of the data.
    /// `parseCustomKeyForTable` fails on it, the same way it does on the initiator when the initiator builds the
    /// filter itself for a cluster with a single shard.
    auto custom_key_ast = parseCustomKeyForTable(settings[Setting::parallel_replicas_custom_key], *query_context);
    /// `parseCustomKeyForTable` either parses the key or throws, it never returns nothing.
    chassert(custom_key_ast);

    LOG_TRACE(getLogger("Planner"), "Processing query on a replica using custom_key '{}'", settings[Setting::parallel_replicas_custom_key].value);

    auto metadata_snapshot = storage->getInMemoryMetadataPtr(query_context, false);
    auto parallel_replicas_custom_filter_ast = getCustomKeyFilterForParallelReplica(
        settings[Setting::parallel_replicas_count],
        settings[Setting::parallel_replica_offset],
        std::move(custom_key_ast),
        {settings[Setting::parallel_replicas_mode],
         settings[Setting::parallel_replicas_custom_key_range_lower],
         settings[Setting::parallel_replicas_custom_key_range_upper]},
        metadata_snapshot->columns,
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
        const auto & limit_node = main_query_node.getLimit()->as<ConstantNode &>();
        ColumnPtr limit_uint = convertColumnToTypeOrNull(*limit_node.getColumn(), limit_node.getResultType(), std::make_shared<DataTypeUInt64>());

        // Negative LIMIT, skip optimization
        if (!limit_uint)
            return 0;

        limit_length = limit_uint->getUInt(0);
    }

    UInt64 limit_offset = 0;
    if (main_query_node.hasOffset())
    {
        const auto & offset_node = main_query_node.getOffset()->as<ConstantNode &>();
        ColumnPtr offset_uint = convertColumnToTypeOrNull(*offset_node.getColumn(), offset_node.getResultType(), std::make_shared<DataTypeUInt64>());

        // Negative OFFSET, skip optimization
        if (!offset_uint)
            return 0;

        limit_offset = offset_uint->getUInt(0);
    }

    /// `arrayJoin` in the projection expands one input row into several output rows after the
    /// source has run. Capping the source to `limit + offset` rows would truncate input BEFORE
    /// expansion, so hard consumers of `trivial_limit` (StorageLoop, system.zeros, generateRandom)
    /// could drop output rows that the LIMIT should keep. See issue #82279 and the sibling guard
    /// in `numbersLikeUtils::shouldPushdownLimit`. (The `ARRAY JOIN` clause is lowered to a
    /// separate table expression in the analyzer, so it is not a single-table read and never
    /// reaches this optimization.)
    if (hasFunctionNode(main_query_node.getProjectionNode(), "arrayJoin"))
        return 0;

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

/// Does the query condition cache have to be kept out of the pre-plan parallel-replicas estimate?
/// The estimate analyzes the read before `tryOptimizeTopK` has had a chance to stamp it, so for a query
/// which may still become a TopK read it cannot know whether the `use_query_condition_cache_for_top_k`
/// gate applies. With the gate off such a read must neither consult nor populate the cache, so analyze
/// without it. This is a deliberate over-approximation of `tryOptimizeTopK`'s plan pattern (which needs
/// the sorting and limit steps that do not exist yet): a query which turns out not to be a TopK read
/// only loses the cache for this throwaway estimate, never for the read that executes.
bool mustSkipQueryConditionCacheInParallelReplicasEstimate(const SelectQueryInfo & select_query_info, const Settings & settings)
{
    if (!settings[Setting::use_query_condition_cache] || settings[Setting::use_query_condition_cache_for_top_k])
        return false;

    /// `tryOptimizeTopK` stamps the read only if at least one of the two TopK mechanisms is enabled.
    if (!settings[Setting::use_skip_indexes_for_top_k] && !settings[Setting::use_top_k_dynamic_filtering])
        return false;

    const auto * main_query_node = select_query_info.query_tree->as<QueryNode>();
    return main_query_node && main_query_node->hasOrderBy() && main_query_node->hasLimit();
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

/// Recursively check whether `node` (or any descendant) contains an `OVER (...)`
/// window function call. Used to reject window-function VIEWs from `ORDER BY`
/// pushdown — per-shard window evaluation is not equivalent to a global one.
bool containsWindowFunction(const IAST & node)
{
    if (const auto * fn = node.as<ASTFunction>(); fn && fn->isWindowFunction())
        return true;
    for (const auto & child : node.children)
    {
        if (child && containsWindowFunction(*child))
            return true;
    }
    return false;
}

/// Push ORDER BY and LIMIT from outer query into simple VIEW's inner query.
/// This enables merge-sorted-streams optimization for views over `Distributed` tables.
///
/// Only safe when the VIEW is a "transparent projection" that does not change
/// ORDER BY/LIMIT semantics:
/// - Single SELECT from one table (no UNION)
/// - No row transformations (JOIN, GROUP BY, DISTINCT, named `WINDOW` clauses
///   or `OVER (...)` window function calls)
/// - No existing ORDER BY/LIMIT in the view
///
/// Outer query restrictions (to preserve semantics under shard-local truncation):
/// - Must have an outer LIMIT (without LIMIT, the outer planner still applies
///   a full ORDER BY on the coordinator, so pushing ORDER BY into the view only
///   adds a redundant inner sort and risks a regression)
/// - Single-table outer query (JOINs may filter/expand rows after per-shard truncation)
/// - No GROUP BY/HAVING/DISTINCT/window
/// - No LIMIT BY (per-shard truncation can drop candidates needed for global groups)
/// - No LIMIT ... OFFSET (pushing LIMIT_LENGTH would truncate before outer OFFSET)
/// - No LIMIT ... WITH TIES (ties are computed globally after merging)
/// - No ORDER BY ... WITH FILL (WITH FILL synthesizes rows; per-shard fills are wrong)
/// - ORDER BY items must be plain column references resolved to this view
void pushOrderByIntoView(
    const StoragePtr & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const SelectQueryInfo & select_query_info,
    const QueryTreeNodePtr & table_expression,
    bool is_single_table_expression,
    const ContextPtr & query_context,
    SelectQueryInfo & table_expression_query_info)
{
    /// Basic checks: must be a view with ORDER BY in a single-table outer query.
    /// JOINs are excluded because the outer JOIN may filter or expand rows, so
    /// truncating one input early can drop rows that belong to the global top-N.
    if (!storage->isView() || !select_query_info.has_order_by || !is_single_table_expression)
        return;

    /// Skip inline view() table functions - they're handled by remote()/Distributed
    if (storage->getStorageID().database_name == "_table_function")
        return;

    /// `SAMPLE` / `FINAL` applied to the view in the outer query (e.g.
    /// `SELECT id FROM v FINAL ORDER BY ts DESC LIMIT 10`) select which rows the
    /// view exposes: `SAMPLE` restricts it to a pseudo-random subset and `FINAL`
    /// collapses duplicates in the underlying MergeTree family. The pushed-down
    /// inner `ORDER BY`/`LIMIT` is rebuilt from the view's stored definition,
    /// which carries neither modifier, so it would sort and truncate the full,
    /// unsampled, non-final row set below the point where the modifier applies
    /// and could return the wrong top-N. Skip the pushdown whenever the view
    /// table expression carries such modifiers (mirrors the guard used for the
    /// trivial-count optimization above).
    if (const auto * table_node = table_expression->as<TableNode>();
        table_node && table_node->getTableExpressionModifiers().has_value()
        && (table_node->getTableExpressionModifiers()->hasFinal()
            || table_node->getTableExpressionModifiers()->hasSampleSizeRatio()
            || table_node->getTableExpressionModifiers()->hasSampleOffsetRatio()
            || table_node->getTableExpressionModifiers()->hasStream()))
        return;

    const auto * outer = select_query_info.query_tree->as<QueryNode>();
    if (!outer || !outer->hasOrderBy())
        return;

    /// Without an outer LIMIT, the outer planner still performs a full ORDER BY
    /// on the coordinator: it does not see the merge-sorted-streams produced by
    /// the inner view and re-sorts on top. Pushing ORDER BY into the view would
    /// only add a redundant per-shard sort with no benefit, and can regress
    /// plain `SELECT ... FROM view ORDER BY ...` queries.
    if (!outer->hasLimit())
        return;

    /// Skip when `extremes` is enabled. The outer planner adds an `ExtremesStep`
    /// before the final `LIMIT`, so with `extremes = 1` the extremes are supposed
    /// to be computed over the full pre-`LIMIT` stream. Pushing `LIMIT` into the
    /// view would truncate the stream first, so the outer `ExtremesStep` would
    /// only see the top-N rows and could report wrong min/max values.
    if (query_context->getSettingsRef()[Setting::extremes])
        return;

    /// Skip when `exact_rows_before_limit` is enabled. This setting promises an
    /// exact `rows_before_limit_at_least` counter by reading the full pre-`LIMIT`
    /// stream. The pushed inner `LIMIT` becomes a child `LimitTransform` under the
    /// outer `LimitTransform`, and `initRowsBeforeLimit` intentionally ignores
    /// child limits once it finds the outer limit, so the counter would be
    /// attached above the already-truncated view output and report only the
    /// per-shard top-N instead of the full pre-`LIMIT` row count.
    if (query_context->getSettingsRef()[Setting::exact_rows_before_limit])
        return;

    /// Skip when `prefer_column_name_to_alias` is enabled. The injected inner
    /// `ORDER BY` references view columns by bare identifier name (see below). By
    /// default an identifier in `ORDER BY` binds to a matching select-list alias,
    /// which is exactly the view column the outer query sorts by. But with
    /// `prefer_column_name_to_alias = 1` the same identifier prefers a source
    /// column of that name instead, so for a view like
    /// `SELECT b AS a, a AS b FROM dist` the injected inner `ORDER BY a` would
    /// bind to the source column `a` rather than the view column `a` (the inner
    /// expression `b`). The types can still match, so the type check below does
    /// not catch it, and shard-local truncation could keep the wrong rows.
    if (query_context->getSettingsRef()[Setting::prefer_column_name_to_alias])
        return;

    /// Skip when the outer query has filtration: `WHERE`, `PREWHERE`, or
    /// `QUALIFY`. The outer filter is materialized as a separate filter step
    /// above the view subquery, while `query_info.filter_actions_dag` is only
    /// used for analysis (e.g. skip-unused-shards), not as a runtime guarantee
    /// inside the view. Pushing `LIMIT` into the view would then truncate rows
    /// before the outer filter, potentially returning fewer rows than expected.
    if (outer->hasWhere() || outer->hasPrewhere() || outer->hasQualify())
        return;

    /// Skip when a row policy applies to the view itself: row policies are
    /// applied as planner `where_filters` above the view subquery (since
    /// `StorageView` does not support prewhere), so pushing `LIMIT` would
    /// truncate before the row-policy filter runs and could return fewer rows
    /// than expected.
    if (getEffectiveRowPolicyFilter(storage, query_context))
        return;

    /// Skip when `additional_table_filters` matches this view: the additional
    /// filter is applied above the view subquery for the same reason as row
    /// policies, so pushing `LIMIT` would truncate before the filter runs.
    if (table_expression_query_info.additional_filter_ast)
        return;

    /// Outer query must be a transparent SELECT — pushing ORDER BY through
    /// aggregation, DISTINCT or window functions changes semantics and can
    /// disable downstream optimizations (e.g. matching aggregate projections).
    ///
    /// `outer->hasWindow()` only reflects the named `WINDOW` clause, not inline
    /// `OVER (...)` calls in the projection. Use `select_query_info.has_window`,
    /// which is computed by `hasWindowFunctionNodes` over the whole query tree
    /// and catches both forms — otherwise an outer query like
    /// `SELECT row_number() OVER (ORDER BY id), id FROM v ORDER BY ts LIMIT 10`
    /// would still push `ORDER BY ts LIMIT 10` into the view and truncate rows
    /// before the outer window step is evaluated.
    if (outer->hasGroupBy() || outer->hasHaving() || outer->isDistinct() || select_query_info.has_window)
        return;

    /// `arrayJoin` used as a projection function changes row cardinality after
    /// the source read: it expands each input row into one row per array element
    /// and drops rows whose array is empty. Unlike an `ARRAY JOIN` clause (which
    /// the analyzer lowers to a separate table expression, so the outer query is
    /// no longer a single-table read and never reaches here), `arrayJoin` in the
    /// select list keeps `is_single_table_expression` true and slips past the
    /// guards above. Pushing `ORDER BY/LIMIT` into the view would then truncate
    /// source rows before the expansion runs, so if the top ordered rows have
    /// empty arrays the rewritten query would return too few rows instead of
    /// continuing to lower ordered rows to fill the `LIMIT`. Mirror the existing
    /// guard in `mainQueryNodeBlockSizeByLimit`.
    if (hasFunctionNode(outer->getProjectionNode(), "arrayJoin"))
        return;

    /// `LIMIT BY` is evaluated globally on the coordinator after merging.
    /// Pushing only `LIMIT_LENGTH` into the view would truncate per-shard before
    /// `LIMIT BY` is applied, so the coordinator may not see enough candidates
    /// to fill each `LIMIT BY` group.
    if (outer->hasLimitBy())
        return;

    /// Outer LIMIT ... OFFSET ... cannot be pushed safely: pushing only
    /// LIMIT_LENGTH would truncate too many rows before the outer OFFSET is
    /// applied, producing wrong results.
    if (outer->hasOffset())
        return;

    /// LIMIT ... WITH TIES decides ties globally after ordering. Pushing
    /// LIMIT_LENGTH into the view would truncate per-shard before the global
    /// tie set is known.
    if (outer->isLimitWithTies())
        return;

    /// Only push a plain non-negative integer LIMIT. A fractional LIMIT such as
    /// `LIMIT 0.1` is evaluated by `FractionalLimitStep`, which must count the
    /// full input before deciding how many rows to keep. Pushing it into the
    /// view would make the view return only a fraction of its rows, and then the
    /// outer fractional LIMIT would apply the same fraction again, yielding far
    /// too few rows. Negative LIMIT values are rejected for the same reason
    /// (they are not representable as a plain `UInt64`).
    const auto * limit_node = outer->getLimit()->as<ConstantNode>();
    if (!limit_node || !convertColumnToTypeOrNull(*limit_node->getColumn(), limit_node->getResultType(), std::make_shared<DataTypeUInt64>()))
        return;

    /// Validate ORDER BY: must be simple columns from this view, and must not
    /// use WITH FILL (which synthesizes rows from the sort range — per-shard
    /// fill would produce wrong results after merging).
    const auto & order_list = outer->getOrderBy();
    for (const auto & node : order_list.getNodes())
    {
        const auto * sort = node->as<SortNode>();
        if (!sort || sort->withFill())
            return;
        const auto * col = sort->getExpression()->as<ColumnNode>();
        if (!col || col->getColumnSource().get() != table_expression.get())
            return;
    }

    /// Validate view structure: must be simple SELECT from single table
    ASTPtr inner = storage_snapshot->metadata->getSelectQuery().inner_query;
    auto * union_ast = inner ? inner->as<ASTSelectWithUnionQuery>() : nullptr;
    if (!union_ast || !union_ast->list_of_selects || union_ast->list_of_selects->children.size() != 1)
        return;

    auto * sel = union_ast->list_of_selects->children[0]->as<ASTSelectQuery>();
    if (!sel)
        return;

    /// View must not have transformations that change ORDER BY semantics
    if (sel->hasJoin() || sel->groupBy() || sel->distinct)
        return;

    /// Window functions partition/order globally; with ORDER BY/LIMIT pushed
    /// per-shard each replica would compute its window over only its rows and
    /// then return the top-N, which is not equivalent to computing the window
    /// over all rows and taking the global top-N. Reject the view if it has
    /// a named `WINDOW` clause or any `OVER (...)` call inside the select list
    /// (or any nested expression of the select).
    if (sel->window())
        return;

    if (const auto & select_expr = sel->select(); select_expr && containsWindowFunction(*select_expr))
        return;

    /// `QUALIFY` filters rows by window-function results and is evaluated before
    /// `ORDER BY`/`LIMIT`. Its filter typically depends on a window computed over
    /// the whole row set (e.g. `QUALIFY row_number() OVER (ORDER BY id) > 50`),
    /// which the window guards above do not catch because the window lives in the
    /// `QUALIFY` clause rather than the select list. Pushing `ORDER BY`/`LIMIT`
    /// into such a view would let each shard evaluate the window over only its
    /// truncated rows, which is not equivalent to the global computation.
    if (sel->qualify())
        return;

    /// View must not already have ORDER BY/LIMIT
    if (sel->orderBy() || sel->limitBy() || sel->limitLength() || sel->limitOffset())
        return;

    /// View must not carry `LIMIT`/`OFFSET` through its own `SETTINGS` clause.
    /// `SETTINGS limit = N` / `offset = N` constrain which rows the view exposes,
    /// just like an explicit `LIMIT`/`OFFSET`. Pushing the outer `ORDER BY`/`LIMIT`
    /// into the inner query would re-sort and truncate around that setting and
    /// change which rows the view returns, so treat it like an existing inner
    /// `LIMIT` and skip the pushdown.
    ///
    /// Likewise reject `prefer_column_name_to_alias` here: the injected inner
    /// `ORDER BY` identifiers are resolved under the view's own `SETTINGS`
    /// clause, so it could re-introduce the alias-vs-source-column ambiguity
    /// that the outer-context guard above already excludes.
    if (const auto & settings_ast = sel->settings())
    {
        const auto & changes = settings_ast->as<ASTSetQuery &>().changes;
        if (changes.tryGet("limit") || changes.tryGet("offset") || changes.tryGet("prefer_column_name_to_alias"))
            return;
    }

    /// The pushed `ORDER BY`/`LIMIT` is evaluated by the view's inner query,
    /// before `StorageView` converts the inner result to the view's declared
    /// column structure (`StorageView::readImpl` adds a "Convert VIEW subquery
    /// result to VIEW table structure" step). When a view declares a column type
    /// that differs from the type produced by the inner expression — e.g.
    /// `CREATE VIEW v (x String) AS SELECT toUInt64(id) AS x FROM dist` — the
    /// conversion is not order-preserving: the inner query sorts numerically
    /// (`2` before `10`), while the view's `String` output should sort
    /// lexicographically (`'10'` before `'2'`). Pushing the sort/limit below the
    /// conversion would truncate the wrong rows, so the outer query could no
    /// longer recover the correct top-N.
    ///
    /// Only push when every `ORDER BY` column has the same type in the view's
    /// declared structure and in the inner query's output. Resolving the inner
    /// query may fail (e.g. `SQL SECURITY DEFINER`, table functions, missing
    /// access); in that case we conservatively skip the optimization.
    SharedHeader inner_header;
    try
    {
        auto view_context = storage_snapshot->metadata->getSQLSecurityOverriddenContext(query_context);

        /// The injected inner `ORDER BY`/`LIMIT` is analyzed and executed under the
        /// view's effective context (`getSQLSecurityOverriddenContext`), not the outer
        /// query context. The AST `SETTINGS` guard above only rejects `limit`/`offset`/
        /// `prefer_column_name_to_alias` written in the view definition; it does not see
        /// settings inherited through a `SQL SECURITY DEFINER` view's definer profile.
        /// A definer profile `limit`/`offset` constrains which rows the view exposes
        /// (just like an inner `LIMIT`/`OFFSET`), so re-sorting and truncating around it
        /// changes the result; a definer profile `prefer_column_name_to_alias` reintroduces
        /// the alias-vs-source-column ambiguity that the outer-context guard already excludes.
        /// Check the effective context here and skip the pushdown when any of these is set.
        const auto & view_settings = view_context->getSettingsRef();
        if (view_settings[Setting::limit] != 0 || view_settings[Setting::offset] != 0 || view_settings[Setting::prefer_column_name_to_alias])
            return;

        inner_header = InterpreterSelectQueryAnalyzer::getSampleBlock(inner, view_context, SelectQueryOptions().analyze());
    }
    catch (const Exception &)
    {
        return;
    }

    for (const auto & node : order_list.getNodes())
    {
        const auto * col = node->as<SortNode>()->getExpression()->as<ColumnNode>();
        if (!inner_header->has(col->getColumnName()))
            return;
        if (!col->getColumnType()->equals(*inner_header->getByName(col->getColumnName()).type))
            return;
    }

    /// Clone and add ORDER BY/LIMIT to the view's inner query.
    /// Preserve every ORDER BY modifier (direction, NULLS FIRST/LAST, COLLATE,
    /// etc.) by going through SortNode::toAST instead of rebuilding the AST.
    ASTPtr modified = inner->clone();
    sel = modified->as<ASTSelectWithUnionQuery>()->list_of_selects->children[0]->as<ASTSelectQuery>();

    auto order_ast = make_intrusive<ASTExpressionList>();
    for (const auto & node : order_list.getNodes())
    {
        const auto * sort = node->as<SortNode>();
        /// SortNode::toAST converts the inner column reference to its
        /// disambiguated identifier (e.g. `__table1.ts`), which is invalid in
        /// the view's inner AST. Build a fresh ASTOrderByElement and copy
        /// every modifier from SortNode explicitly.
        auto elem = make_intrusive<ASTOrderByElement>();
        elem->direction = sort->getSortDirection() == SortDirection::ASCENDING ? 1 : -1;
        if (auto nulls_dir = sort->getNullsSortDirection())
        {
            elem->nulls_direction = *nulls_dir == SortDirection::ASCENDING ? 1 : -1;
            elem->nulls_direction_was_explicitly_specified = true;
        }
        else
        {
            elem->nulls_direction = elem->direction;
        }
        elem->children.push_back(make_intrusive<ASTIdentifier>(sort->getExpression()->as<ColumnNode>()->getColumnName()));
        if (const auto & collator = sort->getCollator())
            elem->setCollation(make_intrusive<ASTLiteral>(Field(collator->getLocale())));
        order_ast->children.push_back(elem);
    }
    sel->setExpression(ASTSelectQuery::Expression::ORDER_BY, order_ast);
    sel->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, outer->getLimit()->toAST());

    table_expression_query_info.view_query = modified;
}

/// Storage-level eligibility check: is this storage on its own a candidate for
/// reading via parallel replicas?  Strips View / MaterializedView wrappers down
/// to the underlying MergeTree and applies the MergeTree / replication gates.
bool parallelReplicasEnabledForStorage(const StoragePtr & current_storage, const ContextPtr & context, const Settings & query_settings)
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
}

/// Join-tree-level eligibility check: can the leftmost leaf of this join tree
/// drive the parallel-replicas absorption of the entire join?  The contract is
/// that a single leaf (the leftmost) takes the WithMergeableState path with the
/// whole join query tree; the other leaves must take the plain `storage->read`
/// path.  Callers should compute this once for the join tree and gate the
/// per-leaf parallel-replicas activation accordingly.
bool allowParallelReplicasForJoinTree(const QueryTreeNodePtr & join_tree_node, const ContextPtr & context, const Settings & query_settings)
{
    if (!join_tree_node)
        return false;

    if (join_tree_node->as<CrossJoinNode>())
        return false;

    const JoinNode * join_node = join_tree_node->as<JoinNode>();
    if (!join_node)
        return true;

    const auto & left_table_expr = join_node->getLeftTableExpressionNode();
    const auto * left_table = typeid_cast<const TableNode *>(left_table_expr.get());
    if (left_table && left_table->getStorage()->isView())
        return false;

    const auto join_kind = join_node->getKind();
    const auto join_strictness = join_node->getStrictness();
    if ((join_kind == JoinKind::Inner && join_strictness == JoinStrictness::All) || join_kind == JoinKind::Left)
    {
        // check that left table expression can be used for parallel replicas
        if (left_table)
            return parallelReplicasEnabledForStorage(left_table->getStorage(), context, query_settings);

        const auto * left_table_function = left_table_expr->as<TableFunctionNode>();
        if (left_table_function)
            return parallelReplicasEnabledForStorage(left_table_function->getStorage(), context, query_settings);

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

        const auto & right_table_expr = join_node->getRightTableExpressionNode();
        const auto * right_table = right_table_expr->as<TableNode>();
        const auto * right_table_function = right_table_expr->as<TableFunctionNode>();
        if (!right_table && !right_table_function)
            return false;

        const auto right_storage = right_table ? right_table->getStorage() : right_table_function->getStorage();
        if (parallelReplicasEnabledForStorage(right_storage, context, query_settings))
        {
            const auto * left_table_function = left_table_expr->as<TableFunctionNode>();
            const auto left_storage = (left_table ? left_table->getStorage() : left_table_function->getStorage());
            if (!parallelReplicasEnabledForStorage(left_storage, context, query_settings))
                // TODO: support parallel replicas for (non_mt_table RIGHT JOIN mt_table) later
                return false;

            return true;
        }
    }

    return false;
}

JoinTreeQueryPlan buildQueryPlanForTableExpression(TableExpressionNodePtr table_expression,
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
        /// trivial-limit, ORDER BY pushdown into VIEW) can see `additional_filter_ast`
        /// before the actual filter DAG is built further down
        ///
        /// Skip under `only_analyze`, since we may not have the database in case of Distributed.
        if (!select_query_options.only_analyze)
        {
            parseAdditionalFilterAstIfNeeded(
                storage, table_expression->getOriginalAlias(), table_expression_query_info, query_context);

            /// `pushOrderByIntoView` depends on `additional_filter_ast` being parsed
            /// above, so it must run inside the same `!only_analyze` branch — otherwise
            /// the check at the top of the function would see a null `additional_filter_ast`
            /// and fail to block the pushdown when `additional_table_filters` are configured.
            pushOrderByIntoView(storage, storage_snapshot, select_query_info, table_expression, is_single_table_expression, query_context, table_expression_query_info);
        }

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

        /// Apply trivial_count if possible. The plain variant requires no `WHERE`; the
        /// sparsity-filter variant requires a `WHERE`, so at most one of them fires.
        is_trivial_count_applied = !select_query_options.only_analyze && !select_query_options.build_logical_plan && is_single_table_expression
            && (table_node || table_function_node) && select_query_info.has_aggregates
            && (applyTrivialCountIfPossible(
                    query_plan,
                    table_expression_query_info,
                    table_node,
                    table_function_node,
                    select_query_info.query_tree,
                    planner_context->getMutableQueryContext(),
                    table_expression_data.getColumnNames(),
                    *planner_context)
                || applyTrivialCountWithSparsityFilterIfPossible(
                    query_plan,
                    table_expression_query_info,
                    table_node,
                    table_function_node,
                    select_query_info.query_tree,
                    table_expression,
                    planner_context->getMutableQueryContext(),
                    table_expression_data.getColumnNames(),
                    *planner_context));

        if (is_trivial_count_applied)
        {
            till_stage = QueryProcessingStage::WithMergeableState;

            /// Log table access even when trivial count optimization skips reading
            if (query_context->hasQueryContext() && !select_query_options.is_internal)
            {
                auto local_storage_id = storage->getStorageID();
                query_context->getQueryContext()->addQueryAccessInfo(
                    backQuoteIfNeed(local_storage_id.getDatabaseName()),
                    local_storage_id.getFullTableName(),
                    table_expression_data.getColumnNames());
            }
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
                bool row_policy_filter_not_pushed = false;

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

                /// The row-level filter runs inside the reading step and must keep any column a later
                /// additional_table_filters step (applied on top) still needs, else that column is dropped
                /// from the block (#111077). Mirror the columns_needed_by_other_filters pre-collect used for
                /// PREWHERE above.
                NameSet row_policy_required_names;
                if (table_expression_query_info.additional_filter_ast)
                {
                    if (auto additional_filters_info_temp
                        = buildAdditionalFiltersIfNeeded(table_expression_query_info, prewhere_info, planner_context))
                    {
                        for (const auto * input : additional_filters_info_temp->actions.getInputs())
                            row_policy_required_names.insert(input->result_name);
                    }
                    /// buildFilterInfo treats an empty set as "keep all table columns", so seed it with the
                    /// columns the query already needs before adding the additional-filter columns.
                    if (!row_policy_required_names.empty())
                    {
                        const auto & current_column_names = table_expression_data.getColumnNames();
                        row_policy_required_names.insert(current_column_names.begin(), current_column_names.end());
                    }
                }

                auto row_policy_filter_info = buildRowPolicyFilterIfNeeded(
                    storage, table_expression_query_info, planner_context, used_row_policies, std::move(row_policy_required_names));
                if (row_policy_filter_info)
                {
                    table_expression_data.setRowLevelFilterActions(row_policy_filter_info->actions.clone());

                    /// The filter is built against this table's schema, but read() hands it to wrapper
                    /// storages' children (Merge, Buffer), which re-derive it against their own types.
                    /// Push it down only if every column it consumes is in the PREWHERE contract.
                    /// A remote storage cannot carry it at all: read() only ships query text to the
                    /// remote servers and never lowers the filter into it, so pushing would silently
                    /// drop an access-control filter. Refuse, and let the stage check fail closed.
                    bool can_push_down_filter = storage->supportsPrewhere() && !storage->isRemote();
                    if (can_push_down_filter)
                    {
                        if (const auto supported_prewhere_columns = storage->supportedPrewhereColumns())
                        {
                            const auto & table_columns = storage_snapshot->metadata->getColumns();
                            const bool include_subcolumns = storage->supportedPrewhereColumnsIncludeSubcolumns();
                            for (const auto & column_name : row_policy_filter_info->actions.getRequiredColumnsNames())
                            {
                                if (!prewhereSupportedColumnsContain(*supported_prewhere_columns, include_subcolumns, table_columns, column_name))
                                {
                                    can_push_down_filter = false;
                                    break;
                                }
                            }
                        }
                    }

                    /// TODO: Never put row-level security filter in WHERE clause for storages that do not support PREWHERE to avoid merging of filters.
                    if (can_push_down_filter)
                        row_level_filter = std::make_shared<FilterDAGInfo>(std::move(*row_policy_filter_info));
                    else
                    {
                        where_filters.emplace_back(std::move(*row_policy_filter_info), makeDescription("Row-level security filter"));
                        row_policy_filter_not_pushed = true;
                    }
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

                /// For trivial views over Distributed tables, inline the view body and use the
                /// underlying StorageDistributed directly. StorageDistributed will substitute the
                /// distributed table node (inside the inlined subquery) with the shard-local table,
                /// so each shard receives the full view body (aliases, WHERE, etc.) against its local table.
                StoragePtr effective_storage = storage;
                StorageSnapshotPtr effective_snapshot = storage_snapshot;
                /// Context used to ship the read into the underlying distributed table when the
                /// view-pushdown optimization fires. For SECURITY NONE it carries the no-user
                /// override built below (matching StorageView::readImpl, which uses the override for
                /// both the inner interpreter and the inner storage read); for INVOKER and the
                /// legacy unset case it is just a copy of query_context. Stays equal to
                /// query_context whenever the pushdown does not apply.
                ContextPtr effective_context = query_context;
                /// Only push down when the view is the sole table expression of the outer query.
                /// The pushdown ships the whole outer query to shards (replacing the view with the
                /// inlined body over the shard-local table), which is only meaningful when "the outer
                /// query" is exactly "read the view". When the view is one input of a join, the outer
                /// query references join columns by identifier (e.g. __table2.id) that the rewrite
                /// would leave dangling, and the other side may be a local table that cannot be
                /// shipped to shards at all. Joined queries fall back to the standard
                /// StorageView::readImpl path.
                ///
                /// Also skip when distributed_group_by_no_merge is set. A normal StorageView advertises
                /// FetchColumns, so an outer GROUP BY aggregates on the initiator over rows from all
                /// shards. Under the pushdown effective_storage is the Distributed table, and with
                /// distributed_group_by_no_merge its getQueryProcessingStage returns a stage where each
                /// shard aggregates independently and the initiator only concatenates — so a group key
                /// present on several shards yields one partial row per shard instead of one merged row.
                /// Fall back to the StorageView path, which still merges on the initiator.
                ///
                /// Also skip when the outer query is being lowered to a logical plan
                /// (`build_logical_plan`, set e.g. for the shard-side plan under
                /// `serialize_query_plan = 1` or a nested distributed read). That branch below only
                /// serializes a plain `ReadFromTableStep` for the original view's `TableNode` and
                /// cannot carry the rewritten distributed-read state (`effective_storage`,
                /// `effective_context`, the inlined `query_tree`), so the pushdown would be a silent
                /// no-op there — the remote side would resolve the leaf back through
                /// `StorageView::readImpl`. Suppressing it up front keeps the behavior explicit and
                /// avoids the wasted rewrite; results are unchanged because `readImpl` is the correct
                /// fallback. The common `serialize_query_plan = 1` case still benefits: the outer query
                /// is planned on the initiator with `build_logical_plan = false`, and only the inner
                /// body (no view) is later lowered to a logical plan on the shard.
                const auto * view = (is_single_table_expression
                                     && settings[Setting::optimize_trivial_view_pushdown_to_distributed]
                                     && !settings[Setting::distributed_group_by_no_merge]
                                     && !select_query_options.build_logical_plan)
                    ? typeid_cast<const StorageView *>(storage.get())
                    : nullptr;
                if (view)
                {
                    const auto & view_sql_security = storage_snapshot->metadata->sql_security_type;
                    /// Context the pushed-down read and the inner-query analysis run under. It is assigned
                    /// below, once the view is known to be over a `Distributed` table, and stays equal to
                    /// query_context otherwise.
                    ContextPtr inner_context = query_context;

                    auto underlying_dist = view->tryGetUnderlyingDistributed(storage_snapshot, query_context);
                    if (underlying_dist)
                    {
                        /// For `SQL SECURITY NONE`, the inner query normally executes with a no-user
                        /// (global) context via `getSQLSecurityOverriddenContext`, so caller-specific
                        /// row policies do not apply to the underlying distributed table. Use that
                        /// same context here to match `StorageView::readImpl`, which uses the override
                        /// for both the inner interpreter and the inner storage read. (`DEFINER` views
                        /// are rejected by `tryGetUnderlyingDistributed` outright.)
                        if (view_sql_security && *view_sql_security == SQLSecurityType::NONE)
                            inner_context = storage_snapshot->metadata->getSQLSecurityOverriddenContext(query_context);

                        /// Suppress the pushdown when it would move an expression from the coordinator
                        /// onto the shards that is unsafe to evaluate per-shard:
                        ///   * non-deterministic / server-local functions (hostName, serverUUID,
                        ///     nowInBlock, blockNumber, rand, now, ...) in the outer query — evaluated
                        ///     per-shard instead of once on the initiator, changing results;
                        ///   * subqueries in the outer query (e.g. the right-hand side of
                        ///     `IN (SELECT ...)`) — evaluated per-shard against shard-local tables
                        ///     instead of once on the coordinator (a scalar subquery the analyzer
                        ///     already folded into a constant is safe and is not flagged);
                        ///   * the same two hazards inside view-keyed additional_table_filters.
                        /// The view body itself needs no check: it is read through
                        /// StorageDistributed::read in both paths (and tryGetTrivialViewUnderlyingStorage
                        /// already rejects a body whose WHERE/SELECT contains a subquery).
                        ///
                        /// Also suppress when a row policy applies to the view or to the underlying
                        /// Distributed table. Row policies must be enforced in the view-output namespace;
                        /// splicing a policy into the inlined body can bind to a source column instead of
                        /// the view alias (e.g. `SELECT id + 1 AS id` with policy `id = 2` under
                        /// prefer_column_name_to_alias = 1). The canonical StorageView::readImpl path
                        /// enforces the view policy in the right namespace and handles the Distributed
                        /// policy (not propagated to shards — see issue #28334) and used_row_policies
                        /// bookkeeping correctly, so we fall back to it whenever any policy is present.
                        const auto & view_id = storage->getStorageID();
                        const auto & dist_id = underlying_dist->getStorageID();
                        auto view_row_policy = query_context->getRowPolicyFilter(
                            view_id.getDatabaseName(), view_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
                        auto dist_row_policy = query_context->getRowPolicyFilter(
                            dist_id.getDatabaseName(), dist_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
                        const bool has_row_policy = (view_row_policy && !view_row_policy->isAlwaysTrue())
                            || (dist_row_policy && !dist_row_policy->isAlwaysTrue());

                        /// Also suppress when shard pruning is forced. The pushdown ships the outer
                        /// query's WHERE in the view-output namespace, which cannot be safely mapped to
                        /// the underlying table's sharding key (the view may rewrite the key column while
                        /// keeping its name), so it forgoes optimize_skip_unused_shards pruning (see the
                        /// filter_actions_dag clear below). With force_optimize_skip_unused_shards the
                        /// inability to prune is a hard error, so fall back to the canonical
                        /// StorageView::readImpl path, which propagates the outer WHERE to the underlying
                        /// Distributed table and prunes correctly.
                        const bool force_skip_unused_shards = settings[Setting::force_optimize_skip_unused_shards] != 0;

                        /// The two read-time gates keyed off the caller's settings — `force_optimize_skip_unused_shards`
                        /// just above and `distributed_group_by_no_merge` in the `view` condition — are not
                        /// sufficient when the pushed-down read runs under a different settings profile. For
                        /// `SQL SECURITY NONE`, inner_context is copied from the global context, so its
                        /// no-user profile can have either setting enabled even though the caller has it off,
                        /// and both the processing-stage decision and `StorageDistributed::read` below use
                        /// that context. Re-evaluate the two gates against the effective read settings:
                        ///   * with `distributed_group_by_no_merge`, each shard would aggregate independently
                        ///     and the initiator would only concatenate, so an outer GROUP BY key present on
                        ///     several shards would yield one partial row per shard instead of the single
                        ///     merged row `StorageView::readImpl` produces;
                        ///   * with `force_optimize_skip_unused_shards`, the read would throw
                        ///     `UNABLE_TO_SKIP_UNUSED_SHARDS` once `filter_actions_dag` is cleared below,
                        ///     even though the non-pushdown path still propagates the outer `WHERE` to the
                        ///     underlying `Distributed` table and prunes correctly.
                        /// When inner_context is the caller's context these checks are simply redundant.
                        const auto & inner_settings = inner_context->getSettingsRef();
                        const bool inner_settings_forbid_pushdown = inner_settings[Setting::distributed_group_by_no_merge] != 0
                            || inner_settings[Setting::force_optimize_skip_unused_shards] != 0;

                        /// Also suppress when the outer query aggregates and max_rows_to_group_by is set.
                        /// StorageView::readImpl fetches the view's raw rows and performs the outer GROUP BY
                        /// entirely on the initiator, so the limit is checked once against the global set of
                        /// keys. The pushdown ships the whole GROUP BY to each shard instead, so each shard's
                        /// Aggregator enforces the limit independently against only its own local rows; for
                        /// group_by_overflow_mode = 'any' / 'break' the merge phase does not re-apply the cap
                        /// globally (see Aggregator::ensureLimitsFixedMapMerge), so shards could each keep a
                        /// different, locally-permitted set of keys and the initiator would return more groups
                        /// in total than the limit allows. Matches the precedent set by
                        /// `useDataParallelAggregation`, which disables independent aggregation for the same
                        /// reason.
                        const bool outer_group_by_forbids_pushdown = inner_settings[Setting::max_rows_to_group_by] != 0
                            && table_expression_query_info.query_tree->as<QueryNode &>().hasGroupBy();

                        if (has_row_policy
                            || force_skip_unused_shards
                            || inner_settings_forbid_pushdown
                            || outer_group_by_forbids_pushdown
                            || containsNonDeterministicFunction(table_expression_query_info.query_tree)
                            || containsSubqueryNode(table_expression_query_info.query_tree)
                            || astContainsNonDeterministicFunction(table_expression_query_info.additional_filter_ast, query_context)
                            || astContainsSubquery(table_expression_query_info.additional_filter_ast))
                            underlying_dist = nullptr;
                    }
                    if (underlying_dist)
                    {
                        /// Analyze the view's inner query to obtain its query tree. Row policies are not
                        /// injected here: queries against a view (or underlying Distributed table) with a
                        /// row policy were already excluded from the pushdown above, so the canonical
                        /// StorageView::readImpl path enforces them in the correct namespace.
                        const auto & inner_query_ast = storage_snapshot->metadata->getSelectQuery().inner_query;

                        auto options = SelectQueryOptions(QueryProcessingStage::FetchColumns).subquery();
                        InterpreterSelectQueryAnalyzer inner_interp(inner_query_ast, inner_context, options);
                        const auto & inner_query_tree = inner_interp.getQueryTree();
                        /// Mark as subquery so it serializes as (SELECT ...) in the FROM clause.
                        inner_query_tree->as<QueryNode &>().setIsSubquery(true);
                        /// Inherit the view's alias so outer ColumnNodes get the correct table qualifier.
                        inner_query_tree->setAlias(table_expression_query_info.table_expression->getAlias());
                        /// Find the underlying distributed table's node inside the inner query tree.
                        auto dist_table_node = findTableNodeByStorage(inner_query_tree, underlying_dist);

                        /// Two independent per-column hazards, checked together below:
                        ///   * A view may declare an explicit column schema whose types differ from the
                        ///     inner query's result types (e.g. CREATE VIEW v (id UInt8) AS SELECT id FROM
                        ///     dist where dist.id is UInt32). StorageView::readImpl converts the inner
                        ///     result to the view's declared structure ("Convert VIEW subquery result to
                        ///     VIEW table structure"); the pushdown ships the raw inner query and skips
                        ///     that conversion, so a shard would return the inner types and break the VIEW
                        ///     type contract.
                        ///   * `_table`/`_database`, see the comment at their check below.
                        /// Suppress the pushdown, falling back to readImpl, whenever either hazard applies.
                        bool column_checks_passed = true;
                        {
                            std::unordered_map<String, DataTypePtr> inner_types;
                            for (const auto & col : inner_query_tree->as<QueryNode &>().getProjectionColumns())
                                inner_types.emplace(col.name, col.type);
                            const auto & view_columns = storage_snapshot->metadata->getColumns();
                            for (const auto & name : columns_names)
                            {
                                if (!view_columns.has(name)) /// skip virtuals / non-declared columns
                                {
                                    /// `_table`/`_database` are materialized by StorageView's
                                    /// StorageWithCommonVirtualColumns::read as constants equal to the
                                    /// view's own name, not the underlying table's. The pushdown ships the
                                    /// inner query against the underlying Distributed table directly and
                                    /// bypasses that materialization entirely, so `_table`/`_database`
                                    /// would either resolve to the wrong (shard-local) name or, since the
                                    /// re-analyzed inner query has no such column in scope, fail outright
                                    /// with UNKNOWN_IDENTIFIER. Fall back to readImpl whenever the outer
                                    /// query reads either virtual column from the view.
                                    if (name == "_table" || name == "_database")
                                    {
                                        column_checks_passed = false;
                                        break;
                                    }
                                    continue;
                                }
                                auto it = inner_types.find(name);
                                if (it == inner_types.end() || !view_columns.get(name).type->equals(*it->second))
                                {
                                    column_checks_passed = false;
                                    break;
                                }
                            }
                        }

                        if (dist_table_node && column_checks_passed)
                        {
                            /// Column-aware access check for the underlying distributed table, gated on
                            /// the same security modes readImpl would check under (INVOKER and legacy
                            /// unset). The pushdown is suppressed entirely when a row policy applies to
                            /// either the view or the distributed table (see the gate above), so the
                            /// freshly-analyzed tree here carries no injected policy WHERE and the grant
                            /// requirement reflects only the columns the outer query actually reads.
                            if (!view_sql_security || *view_sql_security == SQLSecurityType::INVOKER)
                            {
                                /// Pass columns_names so the analyzer wraps the inner query as
                                /// SELECT <columns_names> FROM (<inner_query_ast>) and prunes columns
                                /// the outer caller does not read. Mirrors StorageView::readImpl, which
                                /// receives the same column list via IStorage::read and is the access
                                /// check this pushdown path is meant to be equivalent to. Without this,
                                /// collectSelectedColumnsFromTable would pick up every column the view
                                /// body mentions and over-require grants on the underlying table.
                                InterpreterSelectQueryAnalyzer access_check_interp(inner_query_ast, inner_context, options, columns_names);
                                auto access_check_tree = access_check_interp.getQueryTree();
                                auto access_check_dist_node = findTableNodeByStorage(access_check_tree, underlying_dist);
                                /// This tree is the same inner query re-analyzed with the read column list;
                                /// it cannot lose the dist table reference the earlier lookup found.
                                chassert(access_check_dist_node);
                                auto referenced_columns = collectSelectedColumnsFromTable(
                                    access_check_tree, underlying_dist->getStorageID(), query_context);
                                const auto & access_check_dist_table = access_check_dist_node->as<TableNode &>();
                                checkAccessRights(
                                    access_check_dist_table.getStorage(),
                                    access_check_dist_table.getStorageID(),
                                    access_check_dist_table.getStorageSnapshot(),
                                    referenced_columns,
                                    query_context);
                            }

                            /// Merge table expression modifiers (FINAL, SAMPLE) from the outer view
                            /// reference into the distributed table node. The outer modifiers come from
                            /// the caller (e.g. SELECT * FROM my_view FINAL SAMPLE 0.1); the inner
                            /// modifiers come from the view body itself. FINAL is OR-ed so either source
                            /// can enable it; for SAMPLE the outer value takes precedence over the inner.
                            auto & dist_table = dist_table_node->as<TableNode &>();
                            const auto outer_modifiers = table_expression_query_info.table_expression->as<const TableNode &>().getTableExpressionModifiers();
                            if (outer_modifiers)
                            {
                                const auto inner_modifiers = dist_table.getTableExpressionModifiers();
                                bool merged_final = outer_modifiers->hasFinal() || (inner_modifiers && inner_modifiers->hasFinal());
                                auto merged_sample_size = outer_modifiers->hasSampleSizeRatio()
                                    ? outer_modifiers->getSampleSizeRatio()
                                    : (inner_modifiers ? inner_modifiers->getSampleSizeRatio() : std::nullopt);
                                auto merged_sample_offset = outer_modifiers->hasSampleOffsetRatio()
                                    ? outer_modifiers->getSampleOffsetRatio()
                                    : (inner_modifiers ? inner_modifiers->getSampleOffsetRatio() : std::nullopt);
                                dist_table.setTableExpressionModifiers(TableExpressionModifiers{merged_final, merged_sample_size, merged_sample_offset});
                            }

                            /// Fold any `additional_table_filters` keyed by this view into the outer
                            /// QueryNode's WHERE clause BEFORE inlining the view body. The view's
                            /// TableNode is still table_expression at this point, so view-namespace
                            /// identifiers (including projection aliases) resolve correctly. After
                            /// cloneAndReplace below, the WHERE rides along inside the shipped query
                            /// and is evaluated on the shard prior to any partial aggregation, exactly
                            /// like a user-written WHERE clause — `SELECT count() FROM v` becomes
                            /// `SELECT count() FROM (inlined view body) WHERE additional_filter`.
                            /// We also clear additional_filter_ast on query_info to suppress
                            /// StorageDistributed::read's own propagation (which would otherwise
                            /// re-key the view-namespace AST to the shard-local table — fine for
                            /// unaliased views, but a hard error for aliased ones).
                            if (auto & additional_filter_ast = table_expression_query_info.additional_filter_ast; additional_filter_ast)
                            {
                                ASTPtr wrapped_filter_ast = additional_filter_ast;
                                if (wrapped_filter_ast->as<ASTSubquery>() || wrapped_filter_ast->as<ASTSelectWithUnionQuery>())
                                    wrapped_filter_ast = makeASTFunction("notEquals",
                                        wrapped_filter_ast,
                                        make_intrusive<ASTLiteral>(Field(UInt8(0))));

                                auto filter_query_tree = buildQueryTree(wrapped_filter_ast, query_context);
                                QueryAnalysisPass query_analysis_pass(table_expression_query_info.table_expression);
                                query_analysis_pass.run(filter_query_tree, query_context);

                                auto & outer_query_node = table_expression_query_info.query_tree->as<QueryNode &>();
                                if (outer_query_node.hasWhere())
                                    outer_query_node.getWhere() = mergeConditionNodes(
                                        {outer_query_node.getWhere(), std::move(filter_query_tree)},
                                        query_context);
                                else
                                    outer_query_node.getWhere() = std::move(filter_query_tree);

                                additional_filter_ast = nullptr;
                            }

                            /// additional_table_filters keyed by the underlying Distributed table are
                            /// parsed here so StorageDistributed::read can propagate them to the
                            /// shard-local table, exactly as the non-pushdown path does (its inner
                            /// planner parses the filter while planning the Distributed table). The
                            /// early parseAdditionalFilterAstIfNeeded ran only for the view's identifiers,
                            /// so a filter keyed by the Distributed table would otherwise be lost. This
                            /// runs after the view-keyed handling above cleared additional_filter_ast,
                            /// and parseAdditionalFilterAstIfNeeded is a no-op when no entry matches.
                            parseAdditionalFilterAstIfNeeded(
                                underlying_dist, dist_table_node->getAlias(), table_expression_query_info, inner_context);

                            /// Replace the view's table expression in the outer query with the
                            /// inlined inner query tree. StorageDistributed will then replace
                            /// the distributed table node (now deep inside the subquery) with
                            /// a StorageDummy for the shard-local table, so each shard receives
                            /// the full view body (aliases, WHERE, etc.) reading from its local table.
                            table_expression_query_info.query_tree = table_expression_query_info.query_tree->cloneAndReplace(
                                table_expression_query_info.table_expression, std::static_pointer_cast<ITableExpressionNode>(inner_query_tree));
                            table_expression_query_info.table_expression = std::static_pointer_cast<ITableExpressionNode>(dist_table_node);
                            effective_storage = underlying_dist;
                            auto dist_metadata_snapshot = underlying_dist->getInMemoryMetadataPtr(query_context, false);
                            effective_snapshot = underlying_dist->getStorageSnapshot(dist_metadata_snapshot, query_context);

                            /// filter_actions_dag was built from the VIEW's output columns. After
                            /// inlining, StorageDistributed::skipUnusedShardsWithAnalyzer would interpret
                            /// that view-namespace predicate against the underlying table's sharding key,
                            /// which is wrong when the view rewrites the sharding-key column but keeps its
                            /// name (e.g. SELECT id + 1 AS id FROM dist). Clear it so shard pruning is
                            /// skipped rather than applied to a stale predicate; the WHERE still rides in
                            /// the inlined query_tree shipped to the shards, so results stay correct — we
                            /// only forgo the optimize_skip_unused_shards pruning hint for this read.
                            if (table_expression_query_info.filter_actions_dag
                                && (settings[Setting::optimize_skip_unused_shards] || settings[Setting::force_optimize_skip_unused_shards]))
                            {
                                LOG_DEBUG(getLogger("Planner"),
                                    "optimize_trivial_view_pushdown_to_distributed is enabled; shard pruning "
                                    "(optimize_skip_unused_shards) is skipped for this query because the view's "
                                    "filter is in the view's output namespace and cannot be safely applied to "
                                    "the underlying table's sharding key. Disable "
                                    "optimize_trivial_view_pushdown_to_distributed to use shard pruning.");
                            }
                            table_expression_query_info.filter_actions_dag = nullptr;

                            /// Disable result-size limits and extremes for the underlying distributed
                            /// read, mirroring StorageView::readImpl (getViewContext). These settings
                            /// must be enforced only at the outer query boundary (the final result),
                            /// not on the inner view read: otherwise a query like
                            /// `SELECT id FROM v ORDER BY id LIMIT 1 SETTINGS max_result_rows = 1`
                            /// could throw on a shard whose intermediate result exceeds the limit,
                            /// before the coordinator applies the final ORDER BY/LIMIT. The outer query
                            /// still runs under query_context, so the limits remain enforced on the
                            /// result the client receives.
                            auto read_context = Context::createCopy(inner_context);
                            read_context->setSetting("max_result_rows", Field(0));
                            read_context->setSetting("max_result_bytes", Field(0));
                            read_context->setSetting("extremes", Field(false));

                            /// Disable the GROUP BY / DISTINCT / LIMIT BY sharding-key aggregation
                            /// optimization (optimize_distributed_group_by_sharding_key) for this read.
                            /// StorageDistributed::getOptimizedQueryProcessingStageAnalyzer compares the
                            /// shipped query's GROUP BY columns with the underlying table's sharding key
                            /// by name; when it matches it returns the Complete stage and skips the
                            /// coordinator merge. After inlining, a view that rewrites the sharding-key
                            /// column but keeps its name (e.g. `SELECT intDiv(id, 2) AS id FROM dist`,
                            /// sharded by `id`) would fool that name comparison: the view-output group can
                            /// span multiple shards, so skipping the merge returns per-shard partial
                            /// groups instead of one merged group. Clearing filter_actions_dag above only
                            /// handles shard pruning, not this stage decision, so disable the optimization
                            /// here to force the merge. The pushdown still ships the GROUP BY to the shards
                            /// (partial aggregation); only the unsafe merge-skipping is suppressed.
                            read_context->setSetting("optimize_distributed_group_by_sharding_key", Field(false));

                            /// StorageDistributed::read rewrites the shard query using
                            /// table_expression_query_info.planner_context's query context (see
                            /// buildQueryTreeForShard); rewrites such as ReplaceLongConstWithScalar
                            /// register scalars there. The distributed read, however, sends scalars from
                            /// the context it is invoked with (effective_context). Context::createCopy
                            /// snapshots the scalar map by value, so read_context and the original
                            /// planner context would diverge — the shard could receive __getScalar('..')
                            /// without the matching scalar ("Scalar doesn't exist"). Point the planner
                            /// context at read_context so the rewrite stores scalars into the very
                            /// context that sends them. The copy ctor keeps the shared
                            /// GlobalPlannerContext (prepared sets, column identifiers).
                            table_expression_query_info.planner_context
                                = std::make_shared<PlannerContext>(read_context, table_expression_query_info.planner_context);

                            effective_context = std::move(read_context);
                        }
                    }
                }

                if (!select_query_options.build_logical_plan)
                {
                    /// Use effective_context (not query_context) so the processing stage is computed
                    /// under the same SQL-security context the read runs in. For SQL SECURITY NONE the
                    /// pushdown reads via the no-user override context (effective_context); computing the
                    /// stage from the caller's context could otherwise return a stage inconsistent with
                    /// that read (e.g. settings like distributed_group_by_no_merge differing between the
                    /// two contexts, leading the initiator to skip a merge the read expects). When the
                    /// pushdown does not fire, effective_context == query_context, so this is unchanged.
                    till_stage = effective_storage->getQueryProcessingStage(
                        effective_context, select_query_options.to_stage, effective_snapshot, table_expression_query_info);

                    /// A row-level filter refused for push-down runs as a filter step right
                    /// above the read, but that step is only appended while the storage stops at
                    /// FetchColumns. A storage processing further (e.g. Distributed or a wrapper
                    /// over it) would silently skip the policy, so fail closed instead.
                    if (row_policy_filter_not_pushed && till_stage > QueryProcessingStage::FetchColumns)
                        throw Exception(ErrorCodes::ILLEGAL_PREWHERE,
                            "Row policy filter for {} cannot be pushed into the storage read, and the storage processes "
                            "the query remotely, so the filter cannot be applied. Define the policy on the underlying "
                            "tables instead; note that such a policy is not applied to reads shipped with "
                            "`serialize_query_plan = 1`",
                            storage->getStorageID().getNameForLogs());
                }

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
                    /// The chosen table and union children are TableNodes, so a table function matches
                    /// neither and equality against them is meaningless when table_node is null.
                    const bool no_tables_or_another_table_chosen_for_reading_with_parallel_replicas_mode
                        = query_context->canUseParallelReplicasOnFollower()
                        && (!table_node || table_node != planner_context->getGlobalPlannerContext()->parallel_replicas_table);
                    if (no_tables_or_another_table_chosen_for_reading_with_parallel_replicas_mode)
                    {
                        bool disable_parallel_replicas_for_storage = true;
                        ContextPtr updated_context = effective_context;
                        if (const UnionNode * table_union
                            = table_node ? planner_context->getGlobalPlannerContext()->parallel_replicas_table_union : nullptr)
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
                            auto mutable_context = Context::createCopy(effective_context);
                            mutable_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(0));
                            updated_context = mutable_context;
                            /// Source processors may hold only a weak_ptr to the context they read
                            /// with, so this copy has to outlive read() for the whole pipeline.
                            query_plan.addInterpreterContext(updated_context);
                        }

                        effective_storage->read(
                            query_plan,
                            columns_names,
                            effective_snapshot,
                            table_expression_query_info,
                            std::move(updated_context),
                            till_stage,
                            max_block_size,
                            max_streams);
                    }
                    else
                    {
                        effective_storage->read(
                            query_plan,
                            columns_names,
                            effective_snapshot,
                            table_expression_query_info,
                            effective_context,
                            till_stage,
                            max_block_size,
                            max_streams);
                    }
                }

                /// query_plan can be empty if there is nothing to read
                /// With `parallel_replicas_plan_based` the planner builds only the plain local plan;
                /// parallel replicas are applied later as a plan transformation (see QueryPlanOptimizations::applyParallelReplicas),
                /// so skip the parallel-replicas construction here.
                if (query_plan.isInitialized() && !select_query_options.build_logical_plan
                    && parallelReplicasEnabledForStorage(storage, query_context, settings))
                {
                    /// The custom-key read below replaces the plan with a remote read at the fixed stage
                    /// `WithMergeableStateAfterAggregationAndLimit`, so it is only allowed when the requested
                    /// stage is not below that: a plan built up to a partial stage - e.g. a `Merge` table plans
                    /// its children up to `WithMergeableState` when one of the underlying tables is read through
                    /// an interpreter - must not receive finalized (post-aggregation, post-LIMIT) data instead
                    /// of the partial aggregation states its consumer expects.
                    const bool to_stage_supports_custom_key = select_query_options.to_stage == QueryProcessingStage::Complete
                        || select_query_options.to_stage == QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;

                    if (query_context->canUseParallelReplicasCustomKey() && to_stage_supports_custom_key
                        && query_context->getClientInfo().distributed_depth == 0)
                    {
                        if (auto cluster = query_context->getClusterForParallelReplicas();
                            query_context->canUseParallelReplicasCustomKeyForCluster(*cluster))
                        {
                            planner_context->getMutableQueryContext()->setSetting("prefer_localhost_replica", Field{0});
                            auto modified_query_info = select_query_info;
                            modified_query_info.cluster = std::move(cluster);
                            till_stage = QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;
                            QueryPlan query_plan_parallel_replicas;
                            auto metadata_snapshot = storage->getInMemoryMetadataPtr(query_context, false);
                            ClusterProxy::executeQueryWithParallelReplicasCustomKey(
                                query_plan_parallel_replicas,
                                storage->getStorageID(),
                                modified_query_info,
                                metadata_snapshot->getColumns(),
                                storage_snapshot,
                                till_stage,
                                table_expression_query_info.query_tree,
                                query_context);
                            query_plan = std::move(query_plan_parallel_replicas);
                        }
                    }
                    else if (
                        ClusterProxy::canUseParallelReplicasOnInitiator(query_context)
                        && allowParallelReplicasForJoinTree(parent_join_tree, query_context, settings))
                    {
                        // (1) find read step

                        const bool allow_view_over_mergetree = settings[Setting::parallel_replicas_allow_view_over_mergetree];
                        auto reading_steps = findReadingSteps(query_plan.getRootNode(), allow_view_over_mergetree);
                        QueryPlan::Node * reading_node = nullptr;
                        if (!reading_steps.empty())
                        {
                            if (typeid_cast<ReadFromMergeTree*>(reading_steps.front()->step.get()))
                                reading_node = reading_steps.front();
                        }

                        // (2) if it's ReadFromMergeTree - run index analysis and check number of rows to read
                        // Note: reading_steps can have several steps in case of reading from view with UNION
                        // In such case, we avoid using parallel_replicas_min_number_of_rows_per_replica for all tables, -
                        // parallel_replicas_min_number_of_rows_per_replica will be replaced by automatic_parallel_replicas_mode
                        if (reading_node && reading_steps.size() == 1
                            && settings[Setting::parallel_replicas_min_number_of_rows_per_replica] > 0)
                        {
                            const auto * reading_step = typeid_cast<ReadFromMergeTree *>(reading_steps.front()->step.get());
                            auto result_ptr
                                = mustSkipQueryConditionCacheInParallelReplicasEstimate(select_query_info, settings)
                                ? reading_step->estimateRangesToReadWithoutQueryConditionCache()
                                : reading_step->selectRangesToRead();
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
                        if (reading_node && planner_context->getQueryContext()->canUseParallelReplicasOnInitiator())
                        {
                            if (!settings[Setting::parallel_replicas_plan_based])
                            {
                                till_stage = QueryProcessingStage::WithMergeableState;
                                QueryPlan query_plan_parallel_replicas;
                                QueryPlanStepPtr reading_step = std::move(reading_node->step);
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
                                /// With `parallel_replicas_plan_based` the planner builds only the plain local
                                /// plan. Deciding whether to use parallel replicas and where to place the
                                /// local/remote boundary is done later, as an analysis of the whole plan
                                /// (QueryPlanOptimizations::applyParallelReplicas), which inserts the split step.
                                QueryPlan query_plan_parallel_replicas;
                                storage->read(
                                    query_plan_parallel_replicas,
                                    columns_names,
                                    storage_snapshot,
                                    table_expression_query_info,
                                    query_context,
                                    till_stage,
                                    max_block_size,
                                    max_streams);
                                query_plan = std::move(query_plan_parallel_replicas);
                            }
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

    /// Filled by `buildShardCollapseFanOut` below: duplicate GROUP BY key columns the shard collapsed before bucketing.
    /// Propagated to the outer planner so a distributed aggregation merge buckets by only the representative keys.
    std::unordered_map<String, String> shard_collapse_duplicate_keys;

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
            /// If the shard deduplicated structurally-identical projection/sort/group expressions (e.g. several ALIAS
            /// columns expanding to the same expression), its header has fewer columns than the initiator expects.
            /// Reconstruct the missing columns by fanning out the deduplicated shard columns before the positional
            /// reconciliation below (which only handles renames, not different column counts).
            if (auto fan_out_actions_dag = buildShardCollapseFanOut(
                    select_query_info.query_tree,
                    select_query_info.planner_context,
                    *query_plan.getCurrentHeader(),
                    *expected_header,
                    &shard_collapse_duplicate_keys))
            {
                auto fan_out_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(*fan_out_actions_dag));
                fan_out_step->setStepDescription("Reconstruct deduplicated duplicate-ALIAS columns");
                query_plan.addStep(std::move(fan_out_step));
            }
        }

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

    /// Collect constants the storage actually returned, so the chain keeps them flowing (a shard must
    /// deliver every constant the initiator expects). ALIAS columns are excluded — re-creatable from
    /// their expression, propagated to the shard via AST (header is already renamed to identifiers).
    NameSet source_constants;
    for (const auto & column : query_plan.getCurrentHeader()->getColumnsWithTypeAndName())
    {
        if (!column.column || !isColumnConst(*column.column))
            continue;
        const auto * original_name = table_expression_data.getColumnNameOrNull(column.name);
        if (original_name && table_expression_data.hasAliasColumn(*original_name))
            continue;
        source_constants.insert(column.name);
    }

    return JoinTreeQueryPlan{
        .query_plan = std::move(query_plan),
        .stage = till_stage,
        .used_row_policies = std::move(used_row_policies),
        .useful_sets = std::move(useful_sets),
        .query_node_to_plan_step_mapping = std::move(query_node_to_plan_step_mapping),
        .source_constants = std::move(source_constants),
        .shard_collapse_duplicate_keys = std::move(shard_collapse_duplicate_keys),
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

    auto result_source_constants = std::move(left_join_tree_query_plan.source_constants);
    for (const auto & source_constant : right_join_tree_query_plan.source_constants)
        result_source_constants.insert(source_constant);

    return JoinTreeQueryPlan{
        .query_plan = std::move(result_plan),
        .stage = QueryProcessingStage::FetchColumns,
        .used_row_policies = std::move(result_used_row_policies),
        .useful_sets = std::move(result_useful_sets),
        .query_node_to_plan_step_mapping = std::move(result_mapping),
        .source_constants = std::move(result_source_constants),
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
            JoinSettings(settings, query_context->getJoinAnalyzeMode()),
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
    /// Only steps that support clone(), because the lookup plan below is cloned per lookup batch.
    bool is_allowed_storage = typeid_cast<const ReadFromMergeTree *>(children_step)
                           || typeid_cast<const ReadNothingStep *>(children_step);
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

    PreparedJoinStorage prepared_join;
    bool allow_storage_join = right_join_tree_query_plan.used_row_policies.empty()
        && right_join_tree_query_plan.stage == QueryProcessingStage::FetchColumns
        && right_join_tree_query_plan.useful_sets.empty();
    if (allow_storage_join)
        prepared_join = tryGetStorageInTableJoin(join_node.getRightTableExpressionNode(), planner_context);
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
        .source_constants = std::move(join_tree_query_plan.source_constants),
    };
}

}

JoinTreeQueryPlan buildJoinTreeQueryPlan(const QueryTreeNodePtr & query_node,
    const SelectQueryInfo & select_query_info,
    SelectQueryOptions & select_query_options,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context)
{
    const QueryTreeNodePtr & join_tree_node = query_node->as<QueryNode &>().getJoinTreeNode();
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
                const auto & right_expression_data = planner_context->getTableExpressionDataOrThrow(join_node.getRightTableExpressionNode());
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


    /// In case of n-way JOINs the table expression stack contains several join nodes;
    /// the parent JOIN node of the leftmost leaf is needed to evaluate parallel-replicas
    /// eligibility for the whole join tree.
    QueryTreeNodePtr parent_join_tree_for_leftmost = join_tree_node;
    for (const auto & node : table_expressions_stack)
    {
        if (node->getNodeType() == QueryTreeNodeType::JOIN ||
            node->getNodeType() == QueryTreeNodeType::CROSS_JOIN ||
            node->getNodeType() == QueryTreeNodeType::ARRAY_JOIN)
        {
            parent_join_tree_for_leftmost = node;
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
        parent_join_tree_for_leftmost,
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

            /** If table expression is remote and it is not left most table expression, we wrap read columns from such
              * table expression in subquery.
              */
            bool is_remote = planner_context->getTableExpressionDataOrThrow(table_expression).isRemote();
            query_plans_stack.push_back(buildQueryPlanForTableExpression(
                table_expression,
                nullptr /*parent_join_tree*/,
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
