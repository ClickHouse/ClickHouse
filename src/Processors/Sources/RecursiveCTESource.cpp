#include <Processors/Sources/RecursiveCTESource.h>

#include <Storages/IStorage.h>
#include <Storages/StorageMemory.h>

#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

#include <QueryPipeline/Chain.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>

#include <DataTypes/DataTypeTuple.h>

#include <Common/assert_cast.h>

#include <optional>
#include <set>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_recursive_cte_evaluation_depth;
    extern const SettingsUInt64 recursive_cte_max_in_filter_cardinality;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_DEEP_RECURSION;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

namespace
{

std::vector<TableNode *> collectTableNodesWithTemporaryTableName(const std::string & temporary_table_name, IQueryTreeNode * root)
{
    std::vector<TableNode *> result;

    std::vector<IQueryTreeNode *> nodes_to_process;
    nodes_to_process.push_back(root);

    while (!nodes_to_process.empty())
    {
        auto * subtree_node = nodes_to_process.back();
        nodes_to_process.pop_back();

        auto * table_node = subtree_node->as<TableNode>();
        if (table_node && table_node->getTemporaryTableName() == temporary_table_name)
            result.push_back(table_node);

        for (auto & child : subtree_node->getChildren())
        {
            if (child)
                nodes_to_process.push_back(child.get());
        }
    }

    return result;
}

/// Equi-join key between the recursive CTE working table and a real table,
/// tagged with the `QueryNode` whose join tree contains the join. The filter
/// will be injected into that `QueryNode`'s `WHERE` clause.
struct CTEJoinKey
{
    QueryNode * containing_query_node;
    String cte_column_name;
    DataTypePtr cte_column_type;
    ColumnNode * real_column_node;
};

bool isCTETableNode(const IQueryTreeNode * node, const std::vector<TableNode *> & recursive_table_nodes)
{
    for (const auto * table_node : recursive_table_nodes)
        if (node == table_node)
            return true;
    return false;
}

/// Extract equi-join key pairs from an `ON` join expression.
/// Handles single `equals` and `AND`-combined conditions.
void extractEquiJoinKeys(
    const QueryTreeNodePtr & expression,
    const std::vector<TableNode *> & recursive_table_nodes,
    QueryNode & containing_query_node,
    std::vector<CTEJoinKey> & result)
{
    const auto * function_node = expression->as<FunctionNode>();
    if (!function_node)
        return;

    if (function_node->getFunctionName() == "and")
    {
        for (const auto & arg : function_node->getArguments().getNodes())
            extractEquiJoinKeys(arg, recursive_table_nodes, containing_query_node, result);
        return;
    }

    if (function_node->getFunctionName() != "equals")
        return;

    const auto & args = function_node->getArguments().getNodes();
    if (args.size() != 2)
        return;

    auto * left_column = args[0]->as<ColumnNode>();
    auto * right_column = args[1]->as<ColumnNode>();
    if (!left_column || !right_column)
        return;

    auto left_source = left_column->getColumnSourceOrNull();
    auto right_source = right_column->getColumnSourceOrNull();
    if (!left_source || !right_source)
        return;

    bool left_is_cte = isCTETableNode(left_source.get(), recursive_table_nodes);
    bool right_is_cte = isCTETableNode(right_source.get(), recursive_table_nodes);

    /// Both columns from the same side (both CTE or both real) — skip.
    if (left_is_cte == right_is_cte)
        return;

    auto * cte_column = left_is_cte ? left_column : right_column;
    auto * real_column = left_is_cte ? right_column : left_column;
    const auto & real_source = left_is_cte ? right_source : left_source;

    /// Real side must be a physical table — filter pushdown only makes sense
    /// against a storage's primary key.
    if (!real_source->as<TableNode>())
        return;

    result.push_back({&containing_query_node, cte_column->getColumnName(), cte_column->getColumnType(), real_column});
}

/// Extract equi-join key pairs from a `USING` join expression.
///
/// A resolved `USING` expression is a `ListNode` of per-column `ColumnNode`s;
/// each carries an inner `ListNode` holding the actual `ColumnNode` from every
/// joined relation that contributes the column. `USING (c)` is thus equivalent
/// to `left.c = right.c = ...`, so we classify each inner column as CTE-side or
/// real-table-side and, for every real column, push a filter keyed by a CTE
/// column's name and type — mirroring the `ON`-expression handling.
void extractEquiJoinKeysFromUsing(
    const QueryTreeNodePtr & expression,
    const std::vector<TableNode *> & recursive_table_nodes,
    QueryNode & containing_query_node,
    std::vector<CTEJoinKey> & result)
{
    const auto * using_list = expression->as<ListNode>();
    if (!using_list)
        return;

    for (const auto & using_column : using_list->getNodes())
    {
        const auto * using_column_node = using_column->as<ColumnNode>();
        if (!using_column_node || !using_column_node->hasExpression())
            continue;

        const auto * inner_columns_list = using_column_node->getExpression()->as<ListNode>();
        if (!inner_columns_list)
            continue;

        ColumnNode * cte_column = nullptr;
        std::vector<ColumnNode *> real_columns;

        for (const auto & inner_column : inner_columns_list->getNodes())
        {
            auto * column = inner_column->as<ColumnNode>();
            if (!column)
                continue;

            auto source = column->getColumnSourceOrNull();
            if (!source)
                continue;

            if (isCTETableNode(source.get(), recursive_table_nodes))
                cte_column = column;
            else if (source->as<TableNode>())
                real_columns.push_back(column);
        }

        if (!cte_column)
            continue;

        for (auto * real_column : real_columns)
            result.push_back({&containing_query_node, cte_column->getColumnName(), cte_column->getColumnType(), real_column});
    }
}

/// Walk the join tree of a single `QueryNode` to collect equi-join keys.
///
/// The collected predicate is later injected into the `QueryNode`'s `WHERE`,
/// which applies after every join. That is semantics-preserving only when the
/// matched inner join is not nested on the nullable side of an outer join: a
/// `LEFT`/`RIGHT`/`FULL` join produces null-extended rows for unmatched outer
/// rows, and `real_column IN (...)` at `WHERE`-level would evaluate to NULL
/// (i.e. false) for those rows and silently drop them. To stay correct, the
/// walk tracks whether the current subtree sits on a nullable side and skips
/// inner joins reached through such a path.
void collectCTEJoinKeysInQuery(
    QueryNode & query_node,
    const std::vector<TableNode *> & recursive_table_nodes,
    std::vector<CTEJoinKey> & result)
{
    struct StackEntry
    {
        IQueryTreeNode * node;
        bool in_nullable_position;
    };

    std::vector<StackEntry> nodes_to_visit;
    nodes_to_visit.push_back({query_node.getJoinTree().get(), false});

    while (!nodes_to_visit.empty())
    {
        auto entry = nodes_to_visit.back();
        nodes_to_visit.pop_back();

        auto * join_node = entry.node->as<JoinNode>();
        if (!join_node)
            continue;

        const auto kind = join_node->getKind();

        if (kind == JoinKind::Inner
            && join_node->hasJoinExpression()
            && !entry.in_nullable_position)
        {
            if (join_node->isOnJoinExpression())
                extractEquiJoinKeys(join_node->getJoinExpression(), recursive_table_nodes, query_node, result);
            else if (join_node->isUsingJoinExpression())
                extractEquiJoinKeysFromUsing(join_node->getJoinExpression(), recursive_table_nodes, query_node, result);
        }

        const bool left_nullable = entry.in_nullable_position || kind == JoinKind::Right || kind == JoinKind::Full;
        const bool right_nullable = entry.in_nullable_position || kind == JoinKind::Left || kind == JoinKind::Full;

        nodes_to_visit.push_back({join_node->getLeftTableExpression().get(), left_nullable});
        nodes_to_visit.push_back({join_node->getRightTableExpression().get(), right_nullable});
    }
}

/// Collect all CTE join keys in the recursive query. When the recursive query
/// is a `UnionNode`, every branch is inspected independently so that filters
/// can later be injected into each branch's `WHERE` in isolation.
std::vector<CTEJoinKey> collectCTEJoinKeys(
    IQueryTreeNode & recursive_query,
    const std::vector<TableNode *> & recursive_table_nodes)
{
    std::vector<CTEJoinKey> result;

    if (auto * query_node = recursive_query.as<QueryNode>())
    {
        collectCTEJoinKeysInQuery(*query_node, recursive_table_nodes, result);
    }
    else if (auto * union_node = recursive_query.as<UnionNode>())
    {
        for (auto & subquery : union_node->getQueries().getNodes())
        {
            if (auto * sub_query_node = subquery->as<QueryNode>())
                collectCTEJoinKeysInQuery(*sub_query_node, recursive_table_nodes, result);
        }
    }

    return result;
}

/// Read deduplicated values of a column from a `StorageMemory`-backed temporary
/// table. Returns nullopt if the number of distinct values exceeds
/// `max_cardinality` — the caller then skips filter injection for the step.
std::optional<std::vector<Field>> readColumnValuesFromMemoryStorage(
    const StoragePtr & storage,
    const String & column_name,
    const ContextPtr & context,
    size_t max_cardinality)
{
    auto * memory_storage = typeid_cast<StorageMemory *>(storage.get());
    if (!memory_storage)
        return std::vector<Field>{};

    auto metadata = memory_storage->getInMemoryMetadataPtr(context, false);
    auto snapshot = memory_storage->getStorageSnapshot(metadata, context);
    const auto & snapshot_data = assert_cast<const StorageMemory::SnapshotData &>(*snapshot->data);

    if (!snapshot_data.blocks)
        return std::vector<Field>{};

    std::set<Field> unique_values;

    for (const auto & block : *snapshot_data.blocks)
    {
        if (!block.has(column_name))
            continue;

        const auto & column = block.getByName(column_name).column;
        for (size_t i = 0; i < column->size(); ++i)
        {
            Field value;
            column->get(i, value);
            unique_values.insert(std::move(value));

            if (unique_values.size() > max_cardinality)
                return std::nullopt;
        }
    }

    return std::vector<Field>(unique_values.begin(), unique_values.end());
}

/// Build a resolved query-tree expression equivalent to `real_column IN (values...)`.
///
/// The RHS tuple elements are typed using the CTE column's type (the type the
/// values were originally produced with), not the real column's type. This
/// matches the semantics of `JOIN ... ON real_col = cte_col`: the join is
/// resolved over a common comparison type, and values that are valid under
/// the join but not representable in the storage column's type (e.g.
/// `Int64(-1)` against a `UInt8` column, or `NULL` against a non-nullable
/// column) are correctly evaluated as no-match rather than triggering a
/// conversion exception while the filter is being built.
QueryTreeNodePtr buildInFilterNode(
    ColumnNode & real_column,
    const DataTypePtr & cte_column_type,
    const std::vector<Field> & values,
    const ContextPtr & context)
{
    Tuple tuple_values;
    tuple_values.reserve(values.size());
    DataTypes tuple_element_types;
    tuple_element_types.reserve(values.size());

    for (const auto & value : values)
    {
        tuple_values.push_back(value);
        tuple_element_types.push_back(cte_column_type);
    }

    auto rhs_node = std::make_shared<ConstantNode>(
        Field(std::move(tuple_values)),
        std::make_shared<DataTypeTuple>(std::move(tuple_element_types)));

    auto in_function_node = std::make_shared<FunctionNode>("in");
    in_function_node->markAsOperator();
    in_function_node->getArguments().getNodes() = {real_column.clone(), std::move(rhs_node)};
    resolveOrdinaryFunctionNodeByName(*in_function_node, "in", context);

    return in_function_node;
}

/// Conjoin a list of predicate nodes into a single `and(...)` expression.
QueryTreeNodePtr conjoinPredicates(std::vector<QueryTreeNodePtr> predicates, const ContextPtr & context)
{
    if (predicates.empty())
        return nullptr;
    if (predicates.size() == 1)
        return std::move(predicates.front());

    auto and_function_node = std::make_shared<FunctionNode>("and");
    and_function_node->markAsOperator();
    and_function_node->getArguments().getNodes() = std::move(predicates);
    resolveOrdinaryFunctionNodeByName(*and_function_node, "and", context);
    return and_function_node;
}

}

class RecursiveCTEChunkGenerator
{
public:
    RecursiveCTEChunkGenerator(SharedHeader header_, QueryTreeNodePtr recursive_cte_union_node_)
        : header(std::move(header_))
        , recursive_cte_union_node(std::move(recursive_cte_union_node_))
    {
        auto & recursive_cte_union_node_typed = recursive_cte_union_node->as<UnionNode &>();
        chassert(recursive_cte_union_node_typed.hasRecursiveCTETable());

        auto & recursive_cte_table = recursive_cte_union_node_typed.getRecursiveCTETable();

        const auto & cte_name = recursive_cte_union_node_typed.getCTEName();
        recursive_table_nodes = collectTableNodesWithTemporaryTableName(cte_name, recursive_cte_union_node.get());
        if (recursive_table_nodes.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UNION query {} is not recursive", recursive_cte_union_node->formatASTForErrorMessage());

        size_t recursive_cte_union_node_queries_size = recursive_cte_union_node_typed.getQueries().getNodes().size();
        chassert(recursive_cte_union_node_queries_size > 1);

        non_recursive_query = recursive_cte_union_node_typed.getQueries().getNodes()[0];
        recursive_query = recursive_cte_union_node_typed.getQueries().getNodes()[1];

        if (recursive_cte_union_node_queries_size > 2)
        {
            auto working_union_query = std::make_shared<UnionNode>(recursive_cte_union_node_typed.getMutableContext(),
                recursive_cte_union_node_typed.getUnionMode());
            auto & working_union_query_subqueries = working_union_query->getQueries().getNodes();

            for (size_t i = 1; i < recursive_cte_union_node_queries_size; ++i)
                working_union_query_subqueries.push_back(recursive_cte_union_node_typed.getQueries().getNodes()[i]);

            recursive_query = std::move(working_union_query);
        }

        /// Disable parallel replicas in every `QueryNode`/`UnionNode` of the recursive query.
        /// When parallel replicas is enabled, the planner rewrites JOINs to GLOBAL JOIN and
        /// materializes the right-side subquery into a cached external table keyed by tree
        /// hash. Across recursive steps the tree structure is identical (only the working
        /// table's data changes), so the cache key collides and stale data is reused —
        /// producing wrong results. The planner reads this setting from each node's
        /// `mutable_context` (see `Planner::buildPlannerContext`), not from the outer
        /// interpreter context, so overriding only the interpreter context is not enough.
        /// We rewrite the contexts once at construction time and preserve sharing: nodes
        /// that originally pointed to the same context will share the same copy afterwards.
        std::map<Context *, ContextMutablePtr> context_copies;
        auto rewrite_context = [&context_copies](ContextMutablePtr & ctx)
        {
            if (auto it = context_copies.find(ctx.get()); it != context_copies.end())
            {
                ctx = it->second;
                return;
            }
            auto new_ctx = Context::createCopy(ctx);
            new_ctx->setSetting("allow_experimental_parallel_reading_from_replicas", Field(UInt64(0)));
            context_copies.emplace(ctx.get(), new_ctx);
            ctx = std::move(new_ctx);
        };

        std::vector<IQueryTreeNode *> nodes_to_visit;
        nodes_to_visit.push_back(recursive_query.get());
        while (!nodes_to_visit.empty())
        {
            auto * node = nodes_to_visit.back();
            nodes_to_visit.pop_back();

            if (auto * qn = node->as<QueryNode>())
                rewrite_context(qn->getMutableContext());
            else if (auto * un = node->as<UnionNode>())
                rewrite_context(un->getMutableContext());

            for (auto & child : node->getChildren())
                if (child)
                    nodes_to_visit.push_back(child.get());
        }

        recursive_query_context = recursive_query->as<QueryNode>() ? recursive_query->as<QueryNode &>().getMutableContext() :
            recursive_query->as<UnionNode &>().getMutableContext();

        const auto & recursive_query_projection_columns = recursive_query->as<QueryNode>() ? recursive_query->as<QueryNode &>().getProjectionColumns() :
            recursive_query->as<UnionNode &>().computeProjectionColumns();

        if (recursive_cte_table->columns.size() != recursive_query_projection_columns.size())
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Recursive CTE subquery {}. Expected projection columns to have same size in recursive and non recursive subquery.",
            recursive_cte_union_node->formatASTForErrorMessage());

        working_temporary_table_holder = recursive_cte_table->holder;
        working_temporary_table_storage = recursive_cte_table->storage;

        intermediate_temporary_table_holder = std::make_shared<TemporaryTableHolder>(
            recursive_query_context,
            ColumnsDescription{NamesAndTypesList{recursive_cte_table->columns.begin(), recursive_cte_table->columns.end()}},
            ConstraintsDescription{},
            nullptr /*query*/,
            true /*create_for_global_subquery*/);
        intermediate_temporary_table_storage = intermediate_temporary_table_holder->getTable();

        /// Collect equi-join keys between the CTE table and physical tables.
        /// Filters built from working-table values will be ANDed into each
        /// containing `QueryNode`'s WHERE during the recursive step.
        cte_join_keys = collectCTEJoinKeys(*recursive_query, recursive_table_nodes);
        for (const auto & key : cte_join_keys)
        {
            /// Snapshot `WHERE`, `HAVING`, and `QUALIFY` together. The planner
            /// mutates all three in place when building the pipeline: it can
            /// merge `QUALIFY` into `HAVING` (no window functions) and `HAVING`
            /// into `WHERE` (no aggregation), clearing the source clause in
            /// each case (see `Planner::buildPlanForQueryNode`). Restoring only
            /// `WHERE` between steps would drop those merged predicates on
            /// step 3+, because on step 2 they get moved into the WHERE we
            /// then overwrite with the snapshot.
            auto * qn = key.containing_query_node;
            original_clauses.emplace(qn, OriginalClauses{qn->getWhere(), qn->getHaving(), qn->getQualify()});
        }
    }

    Chunk generate()
    {
        Chunk current_chunk;

        while (!finished)
        {
            if (!executor.has_value())
                buildStepExecutor();

            while (current_chunk.getNumRows() == 0 && executor->pull(current_chunk))
            {
            }

            read_rows_during_recursive_step += current_chunk.getNumRows();

            if (current_chunk.getNumRows() > 0)
                break;

            executor.reset();

            if (read_rows_during_recursive_step == 0)
            {
                finished = true;
                truncateTemporaryTable(intermediate_temporary_table_storage);
                continue;
            }

            read_rows_during_recursive_step = 0;

            for (auto & recursive_table_node : recursive_table_nodes)
                recursive_table_node->updateStorage(intermediate_temporary_table_storage, recursive_query_context);

            truncateTemporaryTable(working_temporary_table_storage);

            std::swap(intermediate_temporary_table_holder, working_temporary_table_holder);
            std::swap(intermediate_temporary_table_storage, working_temporary_table_storage);
        }

        return current_chunk;
    }

private:
    /// Inject `WHERE original_where AND col IN (values)` into each affected
    /// `QueryNode` before executing a recursive step. Each step rebuilds the
    /// filter from the pristine original WHERE saved at construction time, so
    /// nothing accumulates across steps.
    ///
    /// Returns true on success, false if the join-key cardinality exceeded the
    /// configured cap for some key — in that case the recursive step runs
    /// without any CTE-derived filter (the caller restores original clauses).
    bool injectFiltersIntoRecursiveQuery(size_t max_in_filter_cardinality)
    {
        if (cte_join_keys.empty() || max_in_filter_cardinality == 0)
            return false;

        /// Group join keys by their containing `QueryNode`. A `QueryNode` may
        /// have multiple joins against the CTE — their predicates are combined
        /// with `AND`.
        std::map<QueryNode *, std::vector<QueryTreeNodePtr>> predicates_by_query;

        for (const auto & key : cte_join_keys)
        {
            auto values = readColumnValuesFromMemoryStorage(
                working_temporary_table_storage, key.cte_column_name, recursive_query_context, max_in_filter_cardinality);

            if (!values.has_value())
                return false;

            if (values->empty())
                continue;

            predicates_by_query[key.containing_query_node]
                .push_back(buildInFilterNode(*key.real_column_node, key.cte_column_type, *values, recursive_query_context));
        }

        bool injected_any = false;
        for (auto & [query_node, predicates] : predicates_by_query)
        {
            if (predicates.empty())
                continue;

            auto cte_filter = conjoinPredicates(std::move(predicates), recursive_query_context);

            const auto & original_where = original_clauses.at(query_node).where;
            if (original_where)
                query_node->getWhere() = conjoinPredicates({original_where, std::move(cte_filter)}, recursive_query_context);
            else
                query_node->getWhere() = std::move(cte_filter);

            injected_any = true;
        }

        return injected_any;
    }

    void restoreOriginalClauses()
    {
        for (auto & [query_node, original] : original_clauses)
        {
            query_node->getWhere() = original.where;
            query_node->getHaving() = original.having;
            query_node->getQualify() = original.qualify;
        }
    }

    void buildStepExecutor()
    {
        const auto & recursive_subquery_settings = recursive_query_context->getSettingsRef();

        if (recursive_step > recursive_subquery_settings[Setting::max_recursive_cte_evaluation_depth])
            throw Exception(
                ErrorCodes::TOO_DEEP_RECURSION,
                "Maximum recursive CTE evaluation depth ({}) exceeded, during evaluation of {}. Consider raising "
                "max_recursive_cte_evaluation_depth setting.",
                recursive_subquery_settings[Setting::max_recursive_cte_evaluation_depth].value,
                recursive_cte_union_node->formatASTForErrorMessage());

        auto & query_to_execute = recursive_step > 0 ? recursive_query : non_recursive_query;
        ++recursive_step;

        SelectQueryOptions select_query_options;
        select_query_options.merge_tree_enable_remove_parts_from_snapshot_optimization = false;

        const auto & recursive_table_name = recursive_cte_union_node->as<UnionNode &>().getCTEName();
        recursive_query_context->addOrUpdateExternalTable(recursive_table_name, working_temporary_table_holder);

        const auto & interpreter_context = recursive_query_context;

        /// recursive_step was already incremented above — `>1` means we are
        /// executing the recursive query (the seed query is step `1`).
        if (recursive_step > 1)
        {
            const auto max_in_filter_cardinality
                = recursive_subquery_settings[Setting::recursive_cte_max_in_filter_cardinality].value;

            injectFiltersIntoRecursiveQuery(max_in_filter_cardinality);
        }

        try
        {
            auto interpreter = std::make_unique<InterpreterSelectQueryAnalyzer>(query_to_execute, interpreter_context, select_query_options);
            auto pipeline_builder = interpreter->buildQueryPipeline();

            pipeline_builder.addSimpleTransform([&](const SharedHeader & in_header)
            {
                return std::make_shared<MaterializingTransform>(in_header);
            });

            auto convert_to_temporary_tables_header_actions_dag = ActionsDAG::makeConvertingActions(
                pipeline_builder.getHeader().getColumnsWithTypeAndName(),
                header->getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Position,
                interpreter->getContext());
            auto convert_to_temporary_tables_header_actions = std::make_shared<ExpressionActions>(std::move(convert_to_temporary_tables_header_actions_dag));
            pipeline_builder.addSimpleTransform([&](const SharedHeader & input_header)
            {
                return std::make_shared<ExpressionTransform>(input_header, convert_to_temporary_tables_header_actions);
            });

            /// TODO: Support squashing transform

            auto intermediate_temporary_table_storage_sink = intermediate_temporary_table_storage->write(
                {},
                intermediate_temporary_table_storage->getInMemoryMetadataPtr(recursive_query_context, false),
                recursive_query_context,
                false /*async_insert*/);

            pipeline_builder.addChain(Chain(std::move(intermediate_temporary_table_storage_sink)));

            pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
            pipeline.setProgressCallback(recursive_query_context->getProgressCallback());
            pipeline.setProcessListElement(recursive_query_context->getProcessListElement());

            executor.emplace(pipeline);
        }
        catch (...)
        {
            restoreOriginalClauses();
            throw;
        }

        /// The pipeline was built and captured the (filter-injected) state of
        /// the query tree. The tree itself is reused across steps, so restore
        /// the original clauses now to leave it pristine for the next step.
        restoreOriginalClauses();
    }

    void truncateTemporaryTable(StoragePtr & temporary_table)
    {
        /// TODO: Support proper locking
        TableExclusiveLockHolder table_exclusive_lock;
        temporary_table->truncate({},
            temporary_table->getInMemoryMetadataPtr(recursive_query_context, false),
            recursive_query_context,
            table_exclusive_lock);
    }

    SharedHeader header;
    QueryTreeNodePtr recursive_cte_union_node;
    std::vector<TableNode *> recursive_table_nodes;

    QueryTreeNodePtr non_recursive_query;
    QueryTreeNodePtr recursive_query;
    ContextMutablePtr recursive_query_context;

    TemporaryTableHolderPtr working_temporary_table_holder;
    StoragePtr working_temporary_table_storage;

    TemporaryTableHolderPtr intermediate_temporary_table_holder;
    StoragePtr intermediate_temporary_table_storage;

    QueryPipeline pipeline;
    std::optional<PullingAsyncPipelineExecutor> executor;

    std::vector<CTEJoinKey> cte_join_keys;
    /// Pristine `WHERE`, `HAVING`, and `QUALIFY` clauses captured at
    /// construction time, one entry per affected `QueryNode`. The recursive
    /// step rebuilds `WHERE = original_where AND in(...)` from these, then
    /// restores all three after the pipeline has been built — the planner
    /// folds `QUALIFY` into `HAVING` and `HAVING` into `WHERE` in place, and
    /// not restoring `HAVING`/`QUALIFY` would lose those clauses on step 3+.
    struct OriginalClauses
    {
        QueryTreeNodePtr where;
        QueryTreeNodePtr having;
        QueryTreeNodePtr qualify;
    };
    std::map<QueryNode *, OriginalClauses> original_clauses;

    size_t recursive_step = 0;
    size_t read_rows_during_recursive_step = 0;
    bool finished = false;
};

RecursiveCTESource::RecursiveCTESource(SharedHeader header, QueryTreeNodePtr recursive_cte_union_node_)
    : ISource(header)
    , generator(std::make_unique<RecursiveCTEChunkGenerator>(std::move(header), std::move(recursive_cte_union_node_)))
{}

RecursiveCTESource::~RecursiveCTESource() = default;

Chunk RecursiveCTESource::generate()
{
    return generator->generate();
}

}
