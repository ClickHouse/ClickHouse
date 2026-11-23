#include <Planner/PlannerProjectionAnalysis.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/Passes/NormalizeCountVariantsPass.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool prefer_column_name_to_alias;
}

namespace
{

ASTPtr makeStoredNameAST(const QueryTreeNodePtr & node, const ContextPtr & context);

ASTPtr makeStoredNameASTs(const QueryTreeNodes & nodes, const ContextPtr & context)
{
    auto list = make_intrusive<ASTExpressionList>();
    for (const auto & node : nodes)
        list->children.push_back(makeStoredNameAST(node, context));
    return list;
}

/// the expression as the old analyzer kept it: its column name is the stored name
ASTPtr makeStoredNameAST(const QueryTreeNodePtr & node, const ContextPtr & context)
{
    if (const auto * column_node = node->as<ColumnNode>())
        return make_intrusive<ASTIdentifier>(column_node->getColumnName());

    if (const auto * constant_node = node->as<ConstantNode>())
    {
        /// folded from a written expression: named as that expression
        if (constant_node->hasSourceExpression() && constant_node->getSourceExpression()->hasOriginalAST())
            return makeStoredNameAST(constant_node->getSourceExpression(), context);

        /// made by the analyzer: named by its value
        if (constant_node->hasSourceExpression() || !node->hasOriginalAST())
            return make_intrusive<ASTLiteral>(constant_node->getValue());
    }

    if (const auto * function_node = node->as<FunctionNode>())
    {
        /// keep the written aggregate name (`median`, not `quantile`), unless the analyzer changed the arguments (`count(*)`)
        const auto * written_function
            = function_node->isAggregationCall() && node->hasOriginalAST() ? node->getOriginalAST()->as<ASTFunction>() : nullptr;
        bool has_written_name = written_function
            && (written_function->arguments ? written_function->arguments->children.size() : 0) == function_node->getArguments().getNodes().size();

        auto function = make_intrusive<ASTFunction>();
        function->setNullsAction(has_written_name ? written_function->getNullsAction() : function_node->getNullsAction());
        function->name = has_written_name
            ? getAggregateFunctionCanonicalNameIfAny(QueryAnalyzer::rewriteAggregateFunctionNameIfNeeded(
                written_function->name, function->getNullsAction(), function_node->getArgumentTypes(), function_node->getAggregateFunction()->getParameters(), context))
            : function_node->getFunctionName();

        if (!function_node->getParameters().getNodes().empty())
        {
            function->parameters = makeStoredNameASTs(function_node->getParameters().getNodes(), context);
            function->children.push_back(function->parameters);
        }

        function->arguments = makeStoredNameASTs(function_node->getArguments().getNodes(), context);
        function->children.push_back(function->arguments);
        return function;
    }

    if (const auto * lambda_node = node->as<LambdaNode>())
    {
        ASTs arguments;
        for (const auto & name : lambda_node->getArguments().getNames())
            arguments.push_back(make_intrusive<ASTIdentifier>(name));

        return makeASTFunction("lambda", makeASTFunction("tuple", std::move(arguments)), makeStoredNameAST(lambda_node->getExpression(), context));
    }

    /// literals and subqueries are named as written
    if (node->hasOriginalAST())
        return node->getOriginalAST();

    if (!isQueryOrUnionNode(node))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expression {} of a projection has no original AST", node->formatASTForErrorMessage());

    /// the subquery the analyzer makes for `exists`
    auto hash = node->getTreeHash();
    return make_intrusive<ASTIdentifier>(fmt::format("__subquery_{}_{}", hash.low64, hash.high64));
}

/// `ALIAS` columns and constants folded over a column become expressions again, as in the old analyzer
bool restoreExpressionsOverColumns(QueryTreeNodePtr & node, const QueryTreeNodePtr & table_node)
{
    while (const auto * column_node = node->as<ColumnNode>())
    {
        if (!column_node->hasExpression())
            break;

        auto expression = column_node->getExpression();
        node = std::move(expression);
    }

    if (const auto * constant_node = node->as<ConstantNode>();
        constant_node && constant_node->hasSourceExpression() && !isQueryOrUnionNode(constant_node->getSourceExpression()))
    {
        auto source_expression = constant_node->getSourceExpression();
        if (!restoreExpressionsOverColumns(source_expression, table_node))
            return false;

        node = std::move(source_expression);
        return true;
    }

    const auto * column_node = node->as<ColumnNode>();
    bool reads_column = column_node && column_node->getColumnSourceOrNull() == table_node;
    for (auto & child : node->getChildren())
        if (child && !isQueryOrUnionNode(child))
            reads_column |= restoreExpressionsOverColumns(child, table_node);

    return reads_column;
}

/// constant keys are dropped; one key stays if there are no aggregate functions
void removeConstantGroupByKeys(QueryNode & query_node, bool has_aggregate_functions)
{
    if (!query_node.hasGroupBy() || query_node.isGroupByWithGroupingSets())
        return;

    auto & keys = query_node.getGroupBy().getNodes();
    for (size_t i = 0; i < keys.size();)
    {
        if (!keys[i]->as<ConstantNode>() || (!has_aggregate_functions && keys.size() == 1))
        {
            ++i;
            continue;
        }

        /// the last key takes its place: existing parts are sorted in this order
        if (i + 1 < keys.size())
            keys[i] = std::move(keys.back());
        keys.pop_back();
    }
}

bool hasSubquery(const ASTPtr & ast)
{
    if (ast->as<ASTSubquery>())
        return true;

    for (const auto & child : ast->children)
        if (child && hasSubquery(child))
            return true;

    return false;
}

using ScalarSubqueryValues = std::unordered_map<const IAST *, ASTPtr>;

void collectScalarSubqueryValues(const QueryTreeNodePtr & node, ScalarSubqueryValues & values)
{
    if (const auto * constant_node = node->as<ConstantNode>(); constant_node && constant_node->hasSourceExpression())
    {
        const auto & source_expression = constant_node->getSourceExpression();
        if (isQueryOrUnionNode(source_expression))
        {
            if (!source_expression->hasOriginalAST())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Scalar subquery {} of a projection has no original AST", source_expression->formatASTForErrorMessage());

            values.emplace(
                source_expression->getOriginalAST().get(),
                makeASTFunction(
                    "_CAST",
                    make_intrusive<ASTLiteral>(constant_node->getValue()),
                    make_intrusive<ASTLiteral>(constant_node->getResultType()->getName())));
            return;
        }

        collectScalarSubqueryValues(source_expression, values);
    }

    for (const auto & child : node->getChildren())
        if (child)
            collectScalarSubqueryValues(child, values);
}

size_t replaceScalarSubqueries(const ASTPtr & ast, const ScalarSubqueryValues & values)
{
    size_t replaced = 0;
    for (auto & child : ast->children)
    {
        if (!child)
            continue;

        if (auto it = values.find(child.get()); it != values.end())
        {
            it->second->setAlias(child->tryGetAlias());
            child = it->second;
            ++replaced;
        }
        else
            replaced += replaceScalarSubqueries(child, values);
    }

    return replaced;
}

}

ContextMutablePtr createProjectionAnalysisContext(const ASTPtr & query_ast, const ContextPtr & context)
{
    auto query_context = Context::createCopy(context);
    query_context->setSetting("enable_positional_arguments", false);
    query_context->setSetting("rewrite_in_to_join", false);
    query_context->setSetting("rewrite_count_distinct_if_with_count_distinct_implementation", false);
    /// keep `exists` as `IN`, not as a scalar constant
    query_context->setSetting("execute_exists_as_scalar_subquery", false);
    /// keep scalar values in the tree, not references to query scalars
    query_context->setSetting("enable_scalar_subquery_optimization", false);
    /// parallel replicas would turn a `GLOBAL IN` set into a temporary table
    query_context->setSetting("enable_parallel_replicas", Field(0));
    /// resolve aliases with the server settings, as a table reload does
    query_context->setSetting(
        "prefer_column_name_to_alias", context->getGlobalContext()->getSettingsRef()[Setting::prefer_column_name_to_alias].value);
    /// positional arguments are resolved only in an initial query
    query_context->setQueryKindInitial();

    /// settings stored with the query win over the session
    if (const auto & settings = query_ast->as<ASTSelectQuery &>().settings())
        for (const auto & change : settings->as<ASTSetQuery &>().changes)
            query_context->setSetting(change.name, change.value);

    return query_context;
}

QueryTreeNodePtr buildProjectionQueryTree(const ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context, bool only_analyze)
{
    auto query_tree = buildQueryTree(query_ast, context);
    auto & query_node = query_tree->as<QueryNode &>();

    /// the stored query has no `FROM`: put the source in place of `system.one`
    query_node.getJoinTreeNode() = std::make_shared<TableNode>(source, context);

    /// the analyzer splits a `tuple` key into its elements, but the part stores it as one column: wrap it once more
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

    QueryTreePassManager query_tree_pass_manager(context);
    addQueryTreePasses(query_tree_pass_manager, only_analyze);
    /// resolve only: an optimization pass can change a stored expression (`GROUP BY -c0` into `GROUP BY c0`)
    query_tree_pass_manager.runOnlyResolve(query_tree);

    /// `count(1)` and `sum(1)` are stored as `count()`
    NormalizeCountVariantsPass().run(query_tree, context);

    auto table_node = query_node.getJoinTreeNode();
    for (auto & child : query_node.getChildren())
        if (child)
            restoreExpressionsOverColumns(child, table_node);

    removeConstantGroupByKeys(query_node, hasAggregateFunctionNodes(query_tree));

    return query_tree;
}

String getStoredName(const QueryTreeNodePtr & node, const ContextPtr & context)
{
    return makeStoredNameAST(node, context)->getColumnName();
}

void storeValuesOfScalarSubqueries(ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context)
{
    if (!hasSubquery(query_ast))
        return;

    auto query_tree = buildProjectionQueryTree(query_ast, source, createProjectionAnalysisContext(query_ast, context), /*only_analyze=*/false);
    ScalarSubqueryValues values;
    collectScalarSubqueryValues(query_tree, values);

    /// the tree points into the analyzer's copy of the query, so that copy becomes the stored query
    query_ast = query_tree->getOriginalAST();
    if (replaceScalarSubqueries(query_ast, values) != values.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A scalar subquery of a projection is not in the query {}", query_ast->formatForErrorMessage());
}

}
