#include <Storages/buildQueryTreeForShard.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/createUniqueAliasesIfNecessary.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <IO/WriteHelpers.h>
#include <Planner/PlannerContext.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/removeGroupingFunctionSpecializations.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageDummy.h>
#include <Analyzer/UnionNode.h>

#include <stack>


namespace DB
{

namespace Setting
{
    extern const SettingsDistributedProductMode distributed_product_mode;
    extern const SettingsUInt64 min_external_table_block_size_rows;
    extern const SettingsUInt64 min_external_table_block_size_bytes;
    extern const SettingsBool parallel_replicas_prefer_local_join;
    extern const SettingsBool prefer_global_in_and_join;
    extern const SettingsBool enable_add_distinct_to_in_subqueries;
    extern const SettingsInt64 optimize_const_name_size;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCOMPATIBLE_TYPE_OF_JOIN;
    extern const int DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED;
}

namespace
{

/// Visitor that collect column source to columns mapping from query and all subqueries
class CollectColumnSourceToColumnsVisitor : public InDepthQueryTreeVisitor<CollectColumnSourceToColumnsVisitor>
{
public:
    struct Columns
    {
        NameSet column_names;
        NamesAndTypes columns;

        void addColumn(NameAndTypePair column)
        {
            if (column_names.contains(column.name))
                return;

            column_names.insert(column.name);
            columns.push_back(std::move(column));
        }
    };

    const std::unordered_map<QueryTreeNodePtr, Columns> & getColumnSourceToColumns() const
    {
        return column_source_to_columns;
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        auto column_source = column_node->getColumnSourceOrNull();
        if (!column_source)
            return;

        auto it = column_source_to_columns.find(column_source);
        if (it == column_source_to_columns.end())
        {
            auto [insert_it, _] = column_source_to_columns.emplace(column_source, Columns());
            it = insert_it;
        }

        it->second.addColumn(column_node->getColumn());
    }

private:
    std::unordered_map<QueryTreeNodePtr, Columns> column_source_to_columns;
};

/** Visitor that rewrites IN and JOINs in query and all subqueries according to distributed_product_mode and
  * prefer_global_in_and_join settings.
  *
  * Additionally collects GLOBAL JOIN and GLOBAL IN query nodes.
  *
  * If distributed_product_mode = deny, then visitor throws exception if there are multiple distributed tables.
  * If distributed_product_mode = local, then visitor collects replacement map for tables that must be replaced
  * with local tables.
  * If distributed_product_mode = global or prefer_global_in_and_join setting is true, then visitor rewrites JOINs and IN functions that
  * contain distributed tables to GLOBAL JOINs and GLOBAL IN functions.
  * If distributed_product_mode = allow, then visitor does not rewrite query if there are multiple distributed tables.
  */
class DistributedProductModeRewriteInJoinVisitor : public InDepthQueryTreeVisitorWithContext<DistributedProductModeRewriteInJoinVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<DistributedProductModeRewriteInJoinVisitor>;
    using Base::Base;

    explicit DistributedProductModeRewriteInJoinVisitor(const ContextPtr & context_)
        : Base(context_)
    {}

    struct InFunctionOrJoin
    {
        QueryTreeNodePtr query_node;
        size_t subquery_depth = 0;
    };

    const std::unordered_map<const IQueryTreeNode *, QueryTreeNodePtr> & getReplacementMap() const
    {
        return replacement_map;
    }

    const std::vector<InFunctionOrJoin> & getGlobalInOrJoinNodes() const
    {
        return global_in_or_join_nodes;
    }

    static bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr & child)
    {
        auto * function_node = parent->as<FunctionNode>();
        if (function_node && isNameOfGlobalInFunction(function_node->getFunctionName()))
            return false;

        auto * join_node = parent->as<JoinNode>();
        if (join_node && join_node->getLocality() == JoinLocality::Global && join_node->getRightTableExpression() == child)
            return false;

        return true;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        auto * join_node = node->as<JoinNode>();

        if ((function_node && isNameOfGlobalInFunction(function_node->getFunctionName())) ||
            (join_node && join_node->getLocality() == JoinLocality::Global))
        {
            InFunctionOrJoin in_function_or_join_entry;
            in_function_or_join_entry.query_node = node;
            in_function_or_join_entry.subquery_depth = getSubqueryDepth();
            global_in_or_join_nodes.push_back(std::move(in_function_or_join_entry));
            return;
        }

        if ((function_node && isNameOfLocalInFunction(function_node->getFunctionName())) ||
            (join_node && join_node->getLocality() != JoinLocality::Global))
        {
            InFunctionOrJoin in_function_or_join_entry;
            in_function_or_join_entry.query_node = node;
            in_function_or_join_entry.subquery_depth = getSubqueryDepth();
            in_function_or_join_stack.push_back(in_function_or_join_entry);
            return;
        }

        if (node->getNodeType() == QueryTreeNodeType::TABLE)
            tryRewriteTableNodeIfNeeded(node);
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (!in_function_or_join_stack.empty() && node.get() == in_function_or_join_stack.back().query_node.get())
            in_function_or_join_stack.pop_back();
    }

private:
    void tryRewriteTableNodeIfNeeded(const QueryTreeNodePtr & table_node)
    {
        const auto & table_node_typed = table_node->as<TableNode &>();
        const auto * distributed_storage = typeid_cast<const StorageDistributed *>(table_node_typed.getStorage().get());
        if (!distributed_storage)
            return;

        bool distributed_valid_for_rewrite = distributed_storage->getShardCount() >= 2;
        if (!distributed_valid_for_rewrite)
            return;

        auto distributed_product_mode = getSettings()[Setting::distributed_product_mode];

        if (distributed_product_mode == DistributedProductMode::LOCAL)
        {
            std::optional<StorageID> resolved_remote_storage_id;

            bool database_can_be_changed = distributed_storage->getCluster()->maybeCrossReplication();
            if (database_can_be_changed)
                resolved_remote_storage_id = StorageID{{}, distributed_storage->getRemoteTableName()};
            else
            {
                StorageID remote_storage_id = StorageID{distributed_storage->getRemoteDatabaseName(),
                    distributed_storage->getRemoteTableName()};
                resolved_remote_storage_id = getContext()->resolveStorageID(remote_storage_id);
            }

            const auto & distributed_storage_columns = table_node_typed.getStorageSnapshot()->metadata->getColumns();
            auto storage = std::make_shared<StorageDummy>(*resolved_remote_storage_id, distributed_storage_columns);
            auto replacement_table_expression = std::make_shared<TableNode>(std::move(storage), getContext());
            if (auto table_expression_modifiers = table_node_typed.getTableExpressionModifiers())
                replacement_table_expression->setTableExpressionModifiers(*table_expression_modifiers);
            replacement_map.emplace(table_node.get(), std::move(replacement_table_expression));
        }
        else if ((distributed_product_mode == DistributedProductMode::GLOBAL || getSettings()[Setting::prefer_global_in_and_join]) &&
            !in_function_or_join_stack.empty())
        {
            auto * in_or_join_node_to_modify = in_function_or_join_stack.back().query_node.get();

            if (auto * in_function_to_modify = in_or_join_node_to_modify->as<FunctionNode>())
            {
                auto global_in_function_name = getGlobalInFunctionNameForLocalInFunctionName(in_function_to_modify->getFunctionName());
                auto global_in_function_resolver = FunctionFactory::instance().get(global_in_function_name, getContext());
                in_function_to_modify->resolveAsFunction(global_in_function_resolver->build(in_function_to_modify->getArgumentColumns()));
            }
            else if (auto * join_node_to_modify = in_or_join_node_to_modify->as<JoinNode>())
            {
                join_node_to_modify->setLocality(JoinLocality::Global);
            }

            global_in_or_join_nodes.push_back(in_function_or_join_stack.back());
        }
        else if (distributed_product_mode == DistributedProductMode::ALLOW)
        {
            return;
        }
        else if (distributed_product_mode == DistributedProductMode::DENY)
        {
            throw Exception(ErrorCodes::DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED,
                "Double-distributed IN/JOIN subqueries is denied (distributed_product_mode = 'deny'). "
                "You may rewrite query to use local tables "
                "in subqueries, or use GLOBAL keyword, or set distributed_product_mode to suitable value.");
        }
    }

    std::vector<InFunctionOrJoin> in_function_or_join_stack;
    std::unordered_map<const IQueryTreeNode *, QueryTreeNodePtr> replacement_map;
    std::vector<InFunctionOrJoin> global_in_or_join_nodes;
};

/** Replaces large constant values with `__getScalar` function calls to avoid
  * serializing them directly in the query text sent to remote shards.
  *
  * When a query contains large constants (e.g., large arrays or strings),
  * sending them as literals in the query text is inefficient. Instead, we store
  * the constant in a scalar context and replace it with a `__getScalar('hash')`
  * function call. The remote shard will retrieve the actual value from the scalar context.
  *
  * The `optimize_const_name_size` setting controls the threshold for this optimization.
  */
class ReplaceLongConstWithScalarVisitor : public InDepthQueryTreeVisitorWithContext<ReplaceLongConstWithScalarVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ReplaceLongConstWithScalarVisitor>;
    using Base::Base;

    explicit ReplaceLongConstWithScalarVisitor(const ContextPtr & context, Int64 max_size_)
        : Base(context)
        , max_size(max_size_)
    {}

    static bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr & child)
    {
        if (auto * function_node = parent->as<FunctionNode>())
        {
            /// Do not traverse into `__getScalar` - it's already been processed.
            if (function_node->getFunctionName() == "__getScalar")
                return false;

            /// Do not visit parameters node.
            if (function_node->getParametersNode() == child)
                return false;
        }

        if (auto * query_node = parent->as<QueryNode>())
        {
            /// Do not replace constants in LIMIT, OFFSET, LIMIT BY LIMIT, and LIMIT BY OFFSET clauses.
            /// These must remain as `ConstantNode` because the query planner accesses their values
            /// directly via `as<ConstantNode &>()`. Replacing them with `__getScalar` function nodes
            /// would cause a bad cast exception during query planning.
            if (query_node->hasLimit() && query_node->getLimit() == child)
                return false;
            if (query_node->hasOffset() && query_node->getOffset() == child)
                return false;
            if (query_node->hasLimitByLimit() && query_node->getLimitByLimit() == child)
                return false;
            if (query_node->hasLimitByOffset() && query_node->getLimitByOffset() == child)
                return false;
        }

        return true;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        // Do not visit second argument of "in" functions
        if (!in_second_argument.empty() && in_second_argument.top() == node)
        {
            in_second_argument.pop();
            return;
        }

        if (auto * function_node = node->as<FunctionNode>(); function_node && isNameOfInFunction(function_node->getFunctionName()))
        {
            in_second_argument.push(function_node->getArguments().getNodes()[1]);
            return;
        }

        auto * constant_node = node->as<ConstantNode>();

        if (!constant_node)
            return;

        const auto * col_const = typeid_cast<const ColumnConst *>(constant_node->getColumn().get());

        if (max_size > 0)
        {
            WriteBufferFromOwnString name_buf;
            IColumn::Options options {.optimize_const_name_size = max_size};
            col_const->getValueNameAndTypeImpl(name_buf, 0, options);
            if (options.notFull(name_buf))
                return;
        }

        const auto & context = getContext();

        auto node_without_alias = constant_node->clone();
        node_without_alias->removeAlias();

        QueryTreeNodePtrWithHash node_with_hash(node_without_alias);
        auto str_hash = DB::toString(node_with_hash.hash);

        Block scalar_block({{constant_node->getColumn(), constant_node->getResultType(), "_constant"}});

        context->getQueryContext()->addScalar(str_hash, scalar_block);

        auto scalar_query_hash_string = DB::toString(node_with_hash.hash);

        auto scalar_query_hash_constant_node = std::make_shared<ConstantNode>(std::move(scalar_query_hash_string), std::make_shared<DataTypeString>());

        auto get_scalar_function_node = std::make_shared<FunctionNode>("__getScalar");
        get_scalar_function_node->getArguments().getNodes().push_back(std::move(scalar_query_hash_constant_node));

        auto get_scalar_function = FunctionFactory::instance().get("__getScalar", context);
        get_scalar_function_node->resolveAsFunction(get_scalar_function->build(get_scalar_function_node->getArgumentColumns()));

        node = std::move(get_scalar_function_node);
    }

private:
    Int64 max_size = 0;
    std::stack<QueryTreeNodePtr> in_second_argument;
};

// Helper function to add DISTINCT to all QueryNode objects inside a query/union subtree
void addDistinctRecursively(const QueryTreeNodePtr & node)
{
    if (auto * query_node = node->as<QueryNode>())
    {
        if (!query_node->isDistinct())
            query_node->setIsDistinct(true);
    }
    else if (auto * union_node = node->as<UnionNode>())
    {
        auto union_mode = union_node->getUnionMode();
        // Only add DISTINCT recursively for UNION operations where it's beneficial
        // For UNION_DISTINCT, the union already ensures distinctness, so adding DISTINCT to subqueries is redundant
        // For EXCEPT and INTERSECT operations, adding DISTINCT can change the result set semantics
        if (union_mode == SelectUnionMode::UNION_DEFAULT ||
            union_mode == SelectUnionMode::UNION_ALL)
        {
            for (auto & child : union_node->getQueries().getNodes())
                addDistinctRecursively(child);
        }
    }
}

/** Execute subquery node and put result in mutable context temporary table.
  * Returns table node that is initialized with temporary table storage.
  */
TableNodePtr executeSubqueryNode(const QueryTreeNodePtr & subquery_node,
    ContextMutablePtr & mutable_context,
    size_t subquery_depth)
{
    const auto subquery_hash = subquery_node->getTreeHash();
    const auto temporary_table_name = fmt::format("_data_{}", toString(subquery_hash));

    const auto & external_tables = mutable_context->getExternalTables();
    auto external_table_it = external_tables.find(temporary_table_name);
    if (external_table_it != external_tables.end())
    {
        auto temporary_table_expression_node = std::make_shared<TableNode>(external_table_it->second, mutable_context);
        temporary_table_expression_node->setTemporaryTableName(temporary_table_name);
        return temporary_table_expression_node;
    }

    auto subquery_options = SelectQueryOptions(QueryProcessingStage::Complete, subquery_depth, true /*is_subquery*/);
    auto context_copy = Context::createCopy(mutable_context);
    updateContextForSubqueryExecution(context_copy);

    InterpreterSelectQueryAnalyzer interpreter(subquery_node, context_copy, subquery_options);
    auto & query_plan = interpreter.getQueryPlan();

    auto sample_block_with_unique_names = *query_plan.getCurrentHeader();
    makeUniqueColumnNamesInBlock(sample_block_with_unique_names);

    if (!blocksHaveEqualStructure(sample_block_with_unique_names, *query_plan.getCurrentHeader()))
    {
        auto actions_dag = ActionsDAG::makeConvertingActions(
            query_plan.getCurrentHeader()->getColumnsWithTypeAndName(),
            sample_block_with_unique_names.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position,
            context_copy);
        auto converting_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(actions_dag));
        query_plan.addStep(std::move(converting_step));
    }

    auto sample = interpreter.getSampleBlock();
    NamesAndTypesList columns = sample->getNamesAndTypesList();

    auto external_storage_holder = TemporaryTableHolder(
        mutable_context,
        ColumnsDescription(columns, false),
        ConstraintsDescription{},
        nullptr /*query*/,
        true /*create_for_global_subquery*/);

    StoragePtr external_storage = external_storage_holder.getTable();
    auto temporary_table_expression_node = std::make_shared<TableNode>(external_storage, mutable_context);
    temporary_table_expression_node->setTemporaryTableName(temporary_table_name);

    auto table_out = external_storage->write({}, external_storage->getInMemoryMetadataPtr(), mutable_context, /*async_insert=*/false);

    QueryPlanOptimizationSettings optimization_settings(mutable_context);
    BuildQueryPipelineSettings build_pipeline_settings(mutable_context);
    auto builder = query_plan.buildQueryPipeline(optimization_settings, build_pipeline_settings);

    size_t min_block_size_rows = mutable_context->getSettingsRef()[Setting::min_external_table_block_size_rows];
    size_t min_block_size_bytes = mutable_context->getSettingsRef()[Setting::min_external_table_block_size_bytes];
    auto squashing = std::make_shared<SimpleSquashingChunksTransform>(builder->getSharedHeader(), min_block_size_rows, min_block_size_bytes);

    builder->resize(1);
    builder->addTransform(std::move(squashing));

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));

    pipeline.complete(std::move(table_out));
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();
    mutable_context->addExternalTable(temporary_table_name, std::move(external_storage_holder));

    return temporary_table_expression_node;
}

QueryTreeNodePtr getSubqueryFromTableExpression(
    const QueryTreeNodePtr & join_table_expression,
    const std::unordered_map<QueryTreeNodePtr, CollectColumnSourceToColumnsVisitor::Columns> & column_source_to_columns,
    const ContextPtr & context)
{
    auto join_table_expression_node_type = join_table_expression->getNodeType();
    QueryTreeNodePtr subquery_node;

    if (join_table_expression_node_type == QueryTreeNodeType::QUERY || join_table_expression_node_type == QueryTreeNodeType::UNION)
    {
        subquery_node = join_table_expression;
    }
    else if (join_table_expression_node_type == QueryTreeNodeType::TABLE || join_table_expression_node_type == QueryTreeNodeType::TABLE_FUNCTION)
    {
        auto columns_it = column_source_to_columns.find(join_table_expression);
        const NamesAndTypes & columns = columns_it != column_source_to_columns.end() ? columns_it->second.columns : NamesAndTypes();
        subquery_node = buildSubqueryToReadColumnsFromTableExpression(columns, join_table_expression, context);
    }
    else if (join_table_expression_node_type == QueryTreeNodeType::ARRAY_JOIN)
    {
        /// ARRAY_JOIN columns have multiple sources: the ARRAY_JOIN itself provides
        /// the array-joined columns, while the inner table provides pass-through columns.
        /// We must preserve per-source attribution for correct resolution.
        QueryTreeNodes subquery_projection_nodes;
        NamesAndTypes projection_columns;
        NameSet seen_column_names;

        std::vector<QueryTreeNodePtr> nodes_to_visit = {join_table_expression};
        while (!nodes_to_visit.empty())
        {
            auto current = nodes_to_visit.back();
            nodes_to_visit.pop_back();

            auto columns_it = column_source_to_columns.find(current);
            if (columns_it != column_source_to_columns.end())
            {
                for (const auto & col : columns_it->second.columns)
                {
                    if (seen_column_names.insert(col.name).second)
                    {
                        subquery_projection_nodes.push_back(std::make_shared<ColumnNode>(col, current));
                        projection_columns.push_back(col);
                    }
                }
            }

            for (const auto & child : current->getChildren())
                if (child)
                    nodes_to_visit.push_back(child);
        }

        if (subquery_projection_nodes.empty())
        {
            auto constant_data_type = std::make_shared<DataTypeUInt64>();
            subquery_projection_nodes.push_back(std::make_shared<ConstantNode>(1UL, constant_data_type));
            projection_columns.push_back({"1", std::move(constant_data_type)});
        }

        auto context_copy = Context::createCopy(context);
        updateContextForSubqueryExecution(context_copy);

        auto query_node = std::make_shared<QueryNode>(std::move(context_copy));
        query_node->getProjection().getNodes() = std::move(subquery_projection_nodes);
        query_node->resolveProjectionColumns(std::move(projection_columns));
        query_node->getJoinTree() = join_table_expression;
        query_node->setIsSubquery(true);

        subquery_node = query_node;
    }
    else
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected JOIN table expression to be table, table function, query or union node. Actual {}",
            join_table_expression->formatASTForErrorMessage());
    }

    return subquery_node;
}

}

QueryTreeNodePtr buildQueryTreeForShard(const PlannerContextPtr & planner_context, QueryTreeNodePtr query_tree_to_modify, bool allow_global_join_for_right_table)
{
    CollectColumnSourceToColumnsVisitor collect_column_source_to_columns_visitor;
    collect_column_source_to_columns_visitor.visit(query_tree_to_modify);

    const auto & column_source_to_columns = collect_column_source_to_columns_visitor.getColumnSourceToColumns();

    DistributedProductModeRewriteInJoinVisitor visitor(planner_context->getQueryContext());
    visitor.visit(query_tree_to_modify);

    auto replacement_map = visitor.getReplacementMap();
    const auto & global_in_or_join_nodes = visitor.getGlobalInOrJoinNodes();

    QueryTreeNodePtrWithHashMap<TableNodePtr> global_in_temporary_tables;

    bool enable_add_distinct_to_in_subqueries = planner_context->getQueryContext()->getSettingsRef()[Setting::enable_add_distinct_to_in_subqueries];

    for (const auto & global_in_or_join_node : global_in_or_join_nodes)
    {
        if (auto * join_node = global_in_or_join_node.query_node->as<JoinNode>())
        {
            QueryTreeNodePtr join_table_expression;
            const auto join_kind = join_node->getKind();
            if (!allow_global_join_for_right_table || join_kind == JoinKind::Left || join_kind == JoinKind::Inner)
            {
                join_table_expression = join_node->getRightTableExpression();
            }
            else if (join_kind == JoinKind::Right)
            {
                join_table_expression = join_node->getLeftTableExpression();
            }
            else
            {
                throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN, "Unexpected global join kind: {}", toString(join_kind));
            }

            auto subquery_node = getSubqueryFromTableExpression(join_table_expression, column_source_to_columns, planner_context->getQueryContext());

            auto temporary_table_expression_node = executeSubqueryNode(subquery_node,
                planner_context->getMutableQueryContext(),
                global_in_or_join_node.subquery_depth);
            temporary_table_expression_node->setAlias(join_table_expression->getAlias());

            /** When a compound node like ARRAY_JOIN is replaced, its descendants (e.g., the inner TABLE)
              * are not traversed by cloneAndReplace. Column nodes that reference these descendants
              * as their source would get dangling weak pointers when the original tree is released.
              * Map all descendants of the replaced node to the temporary table so that
              * weak pointer updates in cloneAndReplace can find them.
              */
            std::vector<const IQueryTreeNode *> descendants_to_map;
            for (const auto & child : join_table_expression->getChildren())
                if (child)
                    descendants_to_map.push_back(child.get());

            while (!descendants_to_map.empty())
            {
                const auto * descendant = descendants_to_map.back();
                descendants_to_map.pop_back();

                replacement_map.emplace(descendant, temporary_table_expression_node);

                for (const auto & child : descendant->getChildren())
                    if (child)
                        descendants_to_map.push_back(child.get());
            }

            replacement_map.emplace(join_table_expression.get(), std::move(temporary_table_expression_node));
            continue;
        }
        if (auto * in_function_node = global_in_or_join_node.query_node->as<FunctionNode>())
        {
            auto & in_function_subquery_node = in_function_node->getArguments().getNodes().at(1);
            auto in_function_node_type = in_function_subquery_node->getNodeType();
            if (in_function_node_type != QueryTreeNodeType::QUERY && in_function_node_type != QueryTreeNodeType::UNION
                && in_function_node_type != QueryTreeNodeType::TABLE)
                continue;

            QueryTreeNodePtr replacement_table_expression;
            auto & temporary_table_expression_node = global_in_temporary_tables[in_function_subquery_node];
            if (!temporary_table_expression_node)
            {
                auto subquery_to_execute = in_function_subquery_node;
                if (subquery_to_execute->as<TableNode>())
                    subquery_to_execute = buildSubqueryToReadColumnsFromTableExpression(
                        subquery_to_execute,
                        planner_context->getQueryContext());

                // If DISTINCT optimization is enabled, add DISTINCT before executing the subquery
                if (enable_add_distinct_to_in_subqueries)
                    addDistinctRecursively(subquery_to_execute);

                temporary_table_expression_node = executeSubqueryNode(
                    subquery_to_execute,
                    planner_context->getMutableQueryContext(),
                    global_in_or_join_node.subquery_depth);
                replacement_table_expression = temporary_table_expression_node;
            }
            else
            {
                replacement_table_expression = temporary_table_expression_node->clone();
            }

            replacement_map.emplace(in_function_subquery_node.get(), replacement_table_expression);
        }
        else
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected global IN or JOIN query node. Actual {}",
                global_in_or_join_node.query_node->formatASTForErrorMessage());
        }
    }

    if (!replacement_map.empty())
        query_tree_to_modify = query_tree_to_modify->cloneAndReplace(replacement_map);

    removeGroupingFunctionSpecializations(query_tree_to_modify);

    createUniqueAliasesIfNecessary(query_tree_to_modify, planner_context->getQueryContext());

    // Get rid of the settings clause so we don't send them to remote. Thus newly non-important
    // settings won't break any remote parser. It's also more reasonable since the query settings
    // are written into the query context and will be sent by the query pipeline.
    if (auto * query_node = query_tree_to_modify->as<QueryNode>())
        query_node->clearSettingsChanges();

    auto max_const_name_size = planner_context->getQueryContext()->getSettingsRef()[Setting::optimize_const_name_size];
    if (max_const_name_size >= 0)
    {
        ReplaceLongConstWithScalarVisitor scalar_visitor(planner_context->getQueryContext(), max_const_name_size);
        scalar_visitor.visit(query_tree_to_modify);
    }

    return query_tree_to_modify;
}

class CollectStoragesVisitor : public InDepthQueryTreeVisitor<CollectStoragesVisitor>
{
public:
    using Base = InDepthQueryTreeVisitor<CollectStoragesVisitor>;
    using Base::Base;

    void visitImpl(QueryTreeNodePtr & node)
    {
        if (auto * table_node = node->as<TableNode>())
            storages.push_back(table_node->getStorage());
        else if (auto * table_func_node = node->as<TableFunctionNode>())
        {
            /// See https://github.com/ClickHouse/ClickHouse/issues/77990
            /// Now that parallel replicas support TableFunctionRemote, GlobalJoin also needs to support TableFunctionRemote.
            const auto & name = table_func_node->getTableFunctionName();
            if (name == "cluster" || name == "clusterAllReplicas" || name == "remote" || name == "remoteSecure")
                storages.push_back(table_func_node->getStorage());
        }
    }

    std::vector<StoragePtr> storages;
};

class RewriteJoinToGlobalJoinVisitor : public InDepthQueryTreeVisitorWithContext<RewriteJoinToGlobalJoinVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteJoinToGlobalJoinVisitor>;
    using Base::Base;

    static bool allStoragesAreMergeTree(QueryTreeNodePtr & node)
    {
        CollectStoragesVisitor collect_storages;
        collect_storages.visit(node);
        for (const auto & storage : collect_storages.storages)
            if (!storage->isMergeTree())
                return false;

        return true;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (auto * join_node = node->as<JoinNode>())
        {
            bool prefer_local_join = getContext()->getSettingsRef()[Setting::parallel_replicas_prefer_local_join];
            bool should_use_global_join = !prefer_local_join || !allStoragesAreMergeTree(join_node->getRightTableExpression());
            if (should_use_global_join)
                join_node->setLocality(JoinLocality::Global);
        }
    }

    static bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr & child)
    {
        auto * join_node = parent->as<JoinNode>();
        if (join_node && join_node->getRightTableExpression() == child)
            return false;

        return true;
    }
};

void rewriteJoinToGlobalJoin(QueryTreeNodePtr query_tree_to_modify, ContextPtr context)
{
    RewriteJoinToGlobalJoinVisitor visitor(context);
    visitor.visit(query_tree_to_modify);
}

}
