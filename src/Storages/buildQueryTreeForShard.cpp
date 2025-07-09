
#include <Storages/buildQueryTreeForShard.h>

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


namespace DB
{

namespace Setting
{
    extern const SettingsDistributedProductMode distributed_product_mode;
    extern const SettingsUInt64 min_external_table_block_size_rows;
    extern const SettingsUInt64 min_external_table_block_size_bytes;
    extern const SettingsBool parallel_replicas_prefer_local_join;
    extern const SettingsBool prefer_global_in_and_join;
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

    auto sample_block_with_unique_names = query_plan.getCurrentHeader();
    makeUniqueColumnNamesInBlock(sample_block_with_unique_names);

    if (!blocksHaveEqualStructure(sample_block_with_unique_names, query_plan.getCurrentHeader()))
    {
        auto actions_dag = ActionsDAG::makeConvertingActions(
            query_plan.getCurrentHeader().getColumnsWithTypeAndName(),
            sample_block_with_unique_names.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position);
        auto converting_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(actions_dag));
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

    QueryPlanOptimizationSettings optimization_settings(mutable_context);
    BuildQueryPipelineSettings build_pipeline_settings(mutable_context);
    auto builder = query_plan.buildQueryPipeline(optimization_settings, build_pipeline_settings);

    size_t min_block_size_rows = mutable_context->getSettingsRef()[Setting::min_external_table_block_size_rows];
    size_t min_block_size_bytes = mutable_context->getSettingsRef()[Setting::min_external_table_block_size_bytes];
    auto squashing = std::make_shared<SimpleSquashingChunksTransform>(builder->getHeader(), min_block_size_rows, min_block_size_bytes);

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
    else if (
        join_table_expression_node_type == QueryTreeNodeType::TABLE || join_table_expression_node_type == QueryTreeNodeType::TABLE_FUNCTION)
    {
        auto columns_it = column_source_to_columns.find(join_table_expression);
        const NamesAndTypes & columns = columns_it != column_source_to_columns.end() ? columns_it->second.columns : NamesAndTypes();
        subquery_node = buildSubqueryToReadColumnsFromTableExpression(columns, join_table_expression, context);
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

            auto subquery_node
                = getSubqueryFromTableExpression(join_table_expression, column_source_to_columns, planner_context->getQueryContext());

            auto temporary_table_expression_node = executeSubqueryNode(subquery_node,
                planner_context->getMutableQueryContext(),
                global_in_or_join_node.subquery_depth);
            temporary_table_expression_node->setAlias(join_table_expression->getAlias());

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
