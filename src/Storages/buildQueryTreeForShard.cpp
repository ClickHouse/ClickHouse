
#include <Storages/buildQueryTreeForShard.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/Utils.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Storages/removeGroupingFunctionSpecializations.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageDummy.h>
#include <Planner/Utils.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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

        auto distributed_product_mode = getSettings().distributed_product_mode;

        if (distributed_product_mode == DistributedProductMode::LOCAL)
        {
            StorageID remote_storage_id = StorageID{distributed_storage->getRemoteDatabaseName(),
                distributed_storage->getRemoteTableName()};
            auto resolved_remote_storage_id = getContext()->resolveStorageID(remote_storage_id);
            const auto & distributed_storage_columns = table_node_typed.getStorageSnapshot()->metadata->getColumns();
            auto storage = std::make_shared<StorageDummy>(resolved_remote_storage_id, distributed_storage_columns);
            auto replacement_table_expression = std::make_shared<TableNode>(std::move(storage), getContext());
            replacement_map.emplace(table_node.get(), std::move(replacement_table_expression));
        }
        else if ((distributed_product_mode == DistributedProductMode::GLOBAL || getSettings().prefer_global_in_and_join) &&
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

    auto sample_block_with_unique_names = query_plan.getCurrentDataStream().header;
    makeUniqueColumnNamesInBlock(sample_block_with_unique_names);

    if (!blocksHaveEqualStructure(sample_block_with_unique_names, query_plan.getCurrentDataStream().header))
    {
        auto actions_dag = ActionsDAG::makeConvertingActions(
            query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName(),
            sample_block_with_unique_names.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position);
        auto converting_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), std::move(actions_dag));
        query_plan.addStep(std::move(converting_step));
    }

    Block sample = interpreter.getSampleBlock();
    NamesAndTypesList columns = sample.getNamesAndTypesList();

    auto external_storage_holder = TemporaryTableHolder(
        mutable_context,
        ColumnsDescription{columns},
        ConstraintsDescription{},
        nullptr /*query*/,
        true /*create_for_global_subquery*/);

    StoragePtr external_storage = external_storage_holder.getTable();
    auto temporary_table_expression_node = std::make_shared<TableNode>(external_storage, mutable_context);
    temporary_table_expression_node->setTemporaryTableName(temporary_table_name);

    auto table_out = external_storage->write({}, external_storage->getInMemoryMetadataPtr(), mutable_context, /*async_insert=*/false);

    auto optimization_settings = QueryPlanOptimizationSettings::fromContext(mutable_context);
    auto build_pipeline_settings = BuildQueryPipelineSettings::fromContext(mutable_context);
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*query_plan.buildQueryPipeline(optimization_settings, build_pipeline_settings)));

    pipeline.complete(std::move(table_out));
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();
    mutable_context->addExternalTable(temporary_table_name, std::move(external_storage_holder));

    return temporary_table_expression_node;
}

}

QueryTreeNodePtr buildQueryTreeForShard(SelectQueryInfo & query_info, QueryTreeNodePtr query_tree_to_modify)
{
    auto & planner_context = query_info.planner_context;

    CollectColumnSourceToColumnsVisitor collect_column_source_to_columns_visitor;
    collect_column_source_to_columns_visitor.visit(query_tree_to_modify);

    const auto & column_source_to_columns = collect_column_source_to_columns_visitor.getColumnSourceToColumns();

    DistributedProductModeRewriteInJoinVisitor visitor(planner_context->getQueryContext());
    visitor.visit(query_tree_to_modify);

    auto replacement_map = visitor.getReplacementMap();
    const auto & global_in_or_join_nodes = visitor.getGlobalInOrJoinNodes();

    for (const auto & global_in_or_join_node : global_in_or_join_nodes)
    {
        if (auto * join_node = global_in_or_join_node.query_node->as<JoinNode>())
        {
            auto join_right_table_expression = join_node->getRightTableExpression();
            auto join_right_table_expression_node_type = join_right_table_expression->getNodeType();

            QueryTreeNodePtr subquery_node;

            if (join_right_table_expression_node_type == QueryTreeNodeType::QUERY ||
                join_right_table_expression_node_type == QueryTreeNodeType::UNION)
            {
                subquery_node = join_right_table_expression;
            }
            else if (join_right_table_expression_node_type == QueryTreeNodeType::TABLE ||
                join_right_table_expression_node_type == QueryTreeNodeType::TABLE_FUNCTION)
            {
                const auto & columns = column_source_to_columns.at(join_right_table_expression).columns;
                subquery_node = buildSubqueryToReadColumnsFromTableExpression(columns,
                    join_right_table_expression,
                    planner_context->getQueryContext());
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expected JOIN right table expression to be table, table function, query or union node. Actual {}",
                    join_right_table_expression->formatASTForErrorMessage());
            }

            auto temporary_table_expression_node = executeSubqueryNode(subquery_node,
                planner_context->getMutableQueryContext(),
                global_in_or_join_node.subquery_depth);
            temporary_table_expression_node->setAlias(join_right_table_expression->getAlias());

            replacement_map.emplace(join_right_table_expression.get(), std::move(temporary_table_expression_node));
            continue;
        }
        else if (auto * in_function_node = global_in_or_join_node.query_node->as<FunctionNode>())
        {
            auto & in_function_subquery_node = in_function_node->getArguments().getNodes().at(1);
            auto in_function_node_type = in_function_subquery_node->getNodeType();
            if (in_function_node_type != QueryTreeNodeType::QUERY && in_function_node_type != QueryTreeNodeType::UNION)
                continue;

            auto temporary_table_expression_node = executeSubqueryNode(in_function_subquery_node,
                planner_context->getMutableQueryContext(),
                global_in_or_join_node.subquery_depth);

            in_function_subquery_node = std::move(temporary_table_expression_node);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expected global IN or JOIN query node. Actual {}",
                global_in_or_join_node.query_node->formatASTForErrorMessage());
        }
    }

    if (!replacement_map.empty())
        query_tree_to_modify = query_tree_to_modify->cloneAndReplace(replacement_map);

    removeGroupingFunctionSpecializations(query_tree_to_modify);

    return query_tree_to_modify;
}

}
