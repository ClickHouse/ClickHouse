#include <Storages/ProjectionPlanner.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/TableNode.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Planner/CollectSets.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/PlannerAggregation.h>
#include <Planner/PlannerCorrelatedSubqueries.h>
#include <Processors/ISource.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

namespace
{

/// Provides source data for the projection pipeline
class ProjectionDataSource final : public ISource
{
public:
    explicit ProjectionDataSource(SharedHeader block)
        : ISource(std::make_shared<const Block>(block->cloneEmpty()))
        , chunk(block->getColumns(), block->rows())
    {
    }

    String getName() const override { return "ProjectionDataSource"; }

    /// Avoid tracking read progress for projection calculation pipeline.
    std::optional<ReadProgress> getReadProgress() override { return std::nullopt; }

protected:
    Chunk generate() override { return std::move(chunk); }

private:
    Chunk chunk;
};

/// Fake storage which stands in for the missing `FROM` clause of a projection query,
/// both when the projection metadata is built and when the projection is calculated.
///
/// It borrows the name of `system.one`, which is what a `SELECT` without a `FROM` clause reads from
/// anyway. The Analyzer plans a read from a table expression as a read from a table, so it checks the
/// access rights and applies the quotas for it, and calculating a projection must not require
/// anything beyond the rights of the query which writes the data: `system.one` is readable by every
/// user and is exempt from quotas and limits.
class StorageProjectionSource final : public IStorage
{
public:
    StorageProjectionSource(const ColumnsDescription & columns_description, VirtualColumnsDescription virtuals, Block data_)
        : IStorage({DatabaseCatalog::SYSTEM_DATABASE, "one"})
        , data(std::move(data_))
    {
        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(columns_description);
        storage_metadata.setVirtuals(std::move(virtuals));
        setInMemoryMetadata(storage_metadata);
    }

    std::string getName() const override { return "ProjectionSource"; }

    /// The storage is not a table of the catalog, it only borrows the name of `system.one`, so the
    /// row policies and the `additional_table_filters` of that name must not be applied to it: they
    /// would silently reshape the data written into the projection part.
    bool isSyntheticDataSource() const override { return true; }

    bool supportsSubcolumns() const override { return true; }

    bool supportsColumnsWithDynamicStructure() const override { return true; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo &,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processing_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override
    {
        if (data.columns() == 0)
            return Pipe(std::make_shared<NullSource>(std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names))));

        Block block;
        for (const auto & name : column_names)
            block.insert(data.getColumnOrSubcolumnByName(name));

        return Pipe(std::make_shared<ProjectionDataSource>(std::make_shared<const Block>(std::move(block))));
    }

private:
    Block data;
};

/// Remembers the names of the functions of `node` as they are written in the projection query.
void collectUnresolvedNodeNames(const QueryTreeNodePtr & node, QueryTreeNodeToName & result)
{
    if (node->getNodeType() == QueryTreeNodeType::FUNCTION)
    {
        /// A subquery is named by the hash of its AST, which `toAST` does not reproduce.
        if (node->hasOriginalAST())
        {
            result.emplace(node, node->getOriginalAST()->getColumnName());
        }
        else
        {
            ConvertToASTOptions ast_options;
            ast_options.add_cast_for_constants = false;
            ast_options.fully_qualified_identifiers = false;
            result.emplace(node, node->toAST(ast_options)->getColumnName());
        }
    }

    for (const auto & child : node->getChildren())
        if (child)
            collectUnresolvedNodeNames(child, result);
}

}

StoragePtr createProjectionQuerySource(const ColumnsDescription & columns, VirtualColumnsDescription virtuals, Block data)
{
    return std::make_shared<StorageProjectionSource>(columns, std::move(virtuals), std::move(data));
}

QueryTreeNodePtr buildProjectionQueryTree(
    const ASTPtr & query_ast,
    const StoragePtr & source,
    const ContextPtr & context,
    QueryTreeNodeToName * unresolved_node_names)
{
    auto query_tree = buildQueryTree(query_ast, context);
    auto & query_node = query_tree->as<QueryNode &>();

    /// A projection query is stored without a `FROM` clause, so its join tree is a reference to
    /// `system.one` which has to be replaced with the table expression providing the input columns.
    query_node.getJoinTreeNode() = std::make_shared<TableNode>(source, context);

    /** The Analyzer unwraps a `tuple` at the top level of `GROUP BY` into separate keys, while a
      * projection stores such a key as a single column named after the whole tuple. Wrap the key into
      * one more `tuple` so that unwrapping gives back the key the projection stores.
      */
    if (query_node.hasGroupBy() && !query_node.isGroupByWithGroupingSets())
    {
        for (auto & group_by_key_node : query_node.getGroupBy().getNodes())
        {
            const auto * function_node = group_by_key_node->as<FunctionNode>();
            if (!function_node || function_node->getFunctionName() != "tuple")
                continue;

            auto wrapped_key_node = std::make_shared<FunctionNode>("tuple");
            wrapped_key_node->getArguments().getNodes().push_back(group_by_key_node);
            group_by_key_node = std::move(wrapped_key_node);
        }
    }

    if (unresolved_node_names)
        collectUnresolvedNodeNames(query_tree, *unresolved_node_names);

    QueryTreePassManager query_tree_pass_manager(context);
    addQueryTreePasses(query_tree_pass_manager, /*only_analyze=*/true);
    /// Optimization passes are not applied because some of them are invalid for projections.
    /// Example: 'SELECT min(c0), max(c0), count() GROUP BY -c0' for minmax_count projection can be rewritten to
    /// 'SELECT min(c0), max(c0), count() GROUP BY c0' which is incorrect cause we store a column '-c0' in projection.
    query_tree_pass_manager.runOnlyResolve(query_tree);

    return query_tree;
}

ProjectionPlanner::ProjectionPlanner(
    const ASTPtr & query_ast,
    const ColumnsDescription & source_columns,
    VirtualColumnsDescription virtuals,
    const ContextPtr & context_)
{
    auto mut_context = Context::createCopy(context_);
    /// The projection query is analyzed as it is stored: the settings which rewrite the aggregate
    /// functions and `IN` of a query would change the columns of the projection.
    mut_context->setSetting("aggregate_functions_null_for_empty", Field(0));
    mut_context->setSetting("transform_null_in", Field(0));
    /// Building the metadata replaces the positional arguments of `query_ast`, so resolving them
    /// again would re-interpret the integer literals they were replaced with.
    mut_context->setSetting("enable_positional_arguments", false);
    /// Analyze the projection query as an initial one, as its metadata is built, also when the data
    /// comes from a remote `INSERT`, which is executed as a `SECONDARY_QUERY`.
    mut_context->setQueryKindInitial();
    context = mut_context;

    auto source = createProjectionQuerySource(source_columns, std::move(virtuals));
    QueryTreeNodeToName unresolved_node_names;
    query_tree = buildProjectionQueryTree(query_ast, source, context, &unresolved_node_names);

    auto select_query_options = SelectQueryOptions{}.ignoreASTOptimizations().ignoreSettingConstraints();
    planner_context = std::make_shared<PlannerContext>(
        mut_context, std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{}), select_query_options);
    collectSets(query_tree, *planner_context);
    collectTableExpressionData(query_tree, planner_context);
    collectNodeNames(query_tree, unresolved_node_names);

    auto & query_node = query_tree->as<QueryNode &>();
    is_aggregate = query_node.hasGroupBy() || hasAggregateFunctionNodes(query_tree);
    if (!is_aggregate)
        return;

    NameSet unique_aggregation_keys;
    for (const auto & key_node : query_node.getGroupBy().getNodes())
    {
        auto key_name = calculateActionNodeName(key_node, *planner_context, node_to_name, /*use_column_identifier_as_action_node_name=*/false);
        if (unique_aggregation_keys.emplace(key_name).second)
            aggregation_keys.push_back(std::move(key_name));
    }

    aggregates = extractAggregateDescriptions(
        collectAggregateFunctionNodes(query_tree), *planner_context, node_to_name, /*use_column_identifier_as_action_node_name=*/false);
}

void ProjectionPlanner::collectNodeNames(const QueryTreeNodePtr & node, const QueryTreeNodeToName & unresolved_node_names)
{
    auto node_type = node->getNodeType();
    if (node_type == QueryTreeNodeType::COLUMN || node_type == QueryTreeNodeType::CONSTANT || node_type == QueryTreeNodeType::FUNCTION)
    {
        /// A constant folded from an expression is named by that expression, as it is written.
        const QueryTreeNodePtr * written_node = &node;
        if (const auto * constant_node = node->as<ConstantNode>(); constant_node && constant_node->hasSourceExpression())
            written_node = &constant_node->getSourceExpression();

        if (auto it = unresolved_node_names.find(*written_node); it != unresolved_node_names.end())
        {
            node_to_name.emplace(node, it->second);
        }
        else
        {
            /// A node the analysis created: a column, which is not qualified with its table expression,
            /// or a constant, which is written as a plain literal (`NULL`, never `_CAST(NULL, 'Nullable(Nothing)')`).
            ConvertToASTOptions ast_options;
            ast_options.use_source_expression_for_constants = true;
            ast_options.add_cast_for_constants = false;
            ast_options.fully_qualified_identifiers = false;
            node_to_name.emplace(node, node->toAST(ast_options)->getColumnName());
        }
    }

    for (const auto & child : node->getChildren())
        if (child)
            collectNodeNames(child, unresolved_node_names);
}

ActionsDAG ProjectionPlanner::buildBeforeAggregationActions() const
{
    ActionsDAG actions_dag;
    ColumnNodePtrWithHashSet empty_correlated_columns_set;
    PlannerActionsVisitor actions_visitor(planner_context, empty_correlated_columns_set, /*use_column_identifier_as_action_node_name=*/false, node_to_name);

    auto & outputs = actions_dag.getOutputs();
    NameSet output_names;
    auto add_output = [&](const QueryTreeNodePtr & expression_node)
    {
        auto [expression_dag_nodes, correlated_subtrees] = actions_visitor.visit(actions_dag, expression_node);
        correlated_subtrees.assertEmpty("in a projection");
        for (const auto * dag_node : expression_dag_nodes)
            if (output_names.emplace(dag_node->result_name).second)
                outputs.push_back(dag_node);
    };

    const auto & query_node = query_tree->as<const QueryNode &>();
    for (const auto & key_node : query_node.getGroupBy().getNodes())
        add_output(key_node);

    for (const auto & aggregate_function_node : collectAggregateFunctionNodes(query_tree))
        for (const auto & argument_node : aggregate_function_node->as<FunctionNode &>().getArguments().getNodes())
            add_output(argument_node);

    return actions_dag;
}

}
