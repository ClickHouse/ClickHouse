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
    extern const SettingsBool enable_positional_arguments_for_projections;
    extern const SettingsBool optimize_if_chain_to_multiif;
    extern const SettingsBool optimize_if_transform_const_strings_to_lowcardinality;
    extern const SettingsBool optimize_if_transform_strings_to_enum;
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

/// the conditions that the old analysis took for a constant: a number, NULL, and a cast of them to `UInt8`
std::optional<bool> getConstantCondition(const QueryTreeNodePtr & node)
{
    if (const auto * constant_node = node->as<ConstantNode>())
    {
        /// the analyzer folds an expression to a constant, but the old analysis took only what the query writes
        if (constant_node->hasSourceExpression())
            return getConstantCondition(constant_node->getSourceExpression());

        const auto & value = constant_node->getValue();
        if (value.isNull())
            return false;
        if (isBool(constant_node->getResultType()))
            return {};
        if (value.getType() == Field::Types::UInt64)
            return value.safeGet<UInt64>() != 0;
        if (value.getType() == Field::Types::Int64)
            return value.safeGet<Int64>() != 0;
        return {};
    }

    const auto * function_node = node->as<FunctionNode>();
    if (!function_node)
        return {};

    const auto & name = function_node->getFunctionName();
    const auto & arguments = function_node->getArguments().getNodes();
    if ((name == "CAST" || name == "_CAST") && arguments.size() == 2)
    {
        const auto * type_node = arguments[1]->as<ConstantNode>();
        if (!type_node || type_node->getValue().getType() != Field::Types::String)
            return {};

        const auto & type_name = type_node->getValue().safeGet<String>();
        return type_name == "UInt8" || type_name == "Nullable(UInt8)" ? getConstantCondition(arguments[0]) : std::nullopt;
    }

    if ((name == "toUInt8" || name == "toInt8" || name == "identity" || name == "__scalarSubqueryResult") && arguments.size() == 1)
        return getConstantCondition(arguments[0]);

    return {};
}

bool isIfFunction(const QueryTreeNodePtr & node)
{
    const auto * function_node = node->as<FunctionNode>();
    return function_node && function_node->getFunctionName() == "if" && function_node->getArguments().getNodes().size() == 3;
}

/// `if(1, a, b)` becomes `a`: the old analysis did this before it found the types, so the stored names and types depend on it
void replaceIfWithConstantCondition(QueryTreeNodePtr & node)
{
    for (auto & child : node->getChildren())
        if (child)
            replaceIfWithConstantCondition(child);

    if (!isIfFunction(node))
        return;

    const auto & arguments = node->as<FunctionNode &>().getArguments().getNodes();
    auto condition = getConstantCondition(arguments[0]);
    if (!condition)
        return;

    auto branch = arguments[*condition ? 1 : 2];
    if (branch->hasAlias())
        branch = branch->clone();
    if (node->hasAlias())
        branch->setAlias(node->getAlias());
    node = std::move(branch);
}

/// `if(x, 1, if(y, 2, 3))` becomes `multiIf(x, 1, y, 2, 3)`, also before the types; a changed chain is not looked into, as before
void replaceIfChainsWithMultiIf(QueryTreeNodePtr & node)
{
    for (auto & child : node->getChildren())
    {
        if (!child)
            continue;

        if (!isIfFunction(child) || !isIfFunction(child->as<FunctionNode &>().getArguments().getNodes()[2]))
        {
            replaceIfChainsWithMultiIf(child);
            continue;
        }

        auto multi_if_node = std::make_shared<FunctionNode>("multiIf");
        auto & multi_if_arguments = multi_if_node->getArguments().getNodes();
        for (auto if_node = child; ; if_node = multi_if_arguments.back())
        {
            if (!multi_if_arguments.empty())
                multi_if_arguments.pop_back();

            const auto & if_arguments = if_node->as<FunctionNode &>().getArguments().getNodes();
            multi_if_arguments.insert(multi_if_arguments.end(), if_arguments.begin(), if_arguments.end());
            if (!isIfFunction(multi_if_arguments.back()))
                break;
        }

        if (child->hasAlias())
            multi_if_node->setAlias(child->getAlias());
        child = std::move(multi_if_node);
    }
}

/// for an `if` (the key is its text): the expressions of the arguments that are aliases of the query
using AliasedIfArguments = std::map<String, std::array<ASTPtr, 3>>;

/// the old analysis put the expression of an alias in its place before it changed `if`; the analyzer finds that expression
/// returns true if an `if` has a new argument to replace
bool collectAliasedIfArguments(const QueryTreeNodePtr & node, bool with_chains, AliasedIfArguments & result)
{
    bool found = false;
    for (const auto & child : node->getChildren())
        if (child && !isQueryOrUnionNode(child))
            found |= collectAliasedIfArguments(child, with_chains, result);

    if (!isIfFunction(node) || !node->hasOriginalAST())
        return found;

    const auto * if_ast = node->getOriginalAST()->as<ASTFunction>();
    if (!if_ast || !if_ast->arguments || if_ast->arguments->children.size() != 3)
        return found;

    const auto & arguments = node->as<FunctionNode &>().getArguments().getNodes();
    auto add = [&](size_t position)
    {
        if (!if_ast->arguments->children[position]->as<ASTIdentifier>())
            return;

        const auto * constant_node = arguments[position]->as<ConstantNode>();
        const auto & written_node = constant_node && constant_node->hasSourceExpression() ? constant_node->getSourceExpression() : arguments[position];
        if (!written_node->hasOriginalAST())
            return;

        auto & expression = result[if_ast->getColumnNameWithoutAlias()][position];
        if (!expression)
        {
            expression = written_node->getOriginalAST();
            found = true;
        }
    };

    if (getConstantCondition(arguments[0]))
        add(0);
    if (with_chains && isIfFunction(arguments[2]))
        add(2);

    return found;
}

/// true if an `if` of the query (not of a subquery) has an identifier as its condition, or as its last argument when a chain matters
bool hasIfWithIdentifierArgument(const ASTPtr & ast, bool with_chains)
{
    if (ast->as<ASTSubquery>())
        return false;

    const auto * function = ast->as<ASTFunction>();
    if (function && function->name == "if" && function->arguments && function->arguments->children.size() == 3
        && (function->arguments->children[0]->as<ASTIdentifier>() || (with_chains && function->arguments->children[2]->as<ASTIdentifier>())))
        return true;

    return std::ranges::any_of(ast->children, [&](const auto & child) { return child && hasIfWithIdentifierArgument(child, with_chains); });
}

void collectIdentifierNames(const ASTPtr & ast, Names & names)
{
    if (const auto * identifier = ast->as<ASTIdentifier>())
        names.push_back(identifier->name());

    for (const auto & child : ast->children)
        if (child)
            collectIdentifierNames(child, names);
}

/// a parameter of a lambda hides an alias with its name, and a subquery has its own aliases
/// returns true if it replaced an argument
bool replaceAliasedIfArguments(const ASTPtr & ast, const AliasedIfArguments & aliased_arguments, Names & lambda_parameters)
{
    if (ast->as<ASTSubquery>())
        return false;

    bool replaced = false;
    size_t parameters_before = lambda_parameters.size();
    if (const auto * function = ast->as<ASTFunction>(); function && function->arguments)
    {
        auto & arguments = function->arguments->children;
        if (function->name == "lambda" && !arguments.empty())
            collectIdentifierNames(arguments[0], lambda_parameters);

        auto it = function->name == "if" && arguments.size() == 3 ? aliased_arguments.find(function->getColumnNameWithoutAlias()) : aliased_arguments.end();
        for (size_t i = 0; it != aliased_arguments.end() && i < arguments.size(); ++i)
        {
            const auto * identifier = arguments[i]->as<ASTIdentifier>();
            if (!it->second[i] || !identifier || std::ranges::contains(lambda_parameters, identifier->name()))
                continue;

            auto expression = it->second[i]->clone();
            expression->setAlias(arguments[i]->tryGetAlias());
            arguments[i] = std::move(expression);
            replaced = true;
        }
    }

    for (const auto & child : ast->children)
        if (child)
            replaced |= replaceAliasedIfArguments(child, aliased_arguments, lambda_parameters);

    lambda_parameters.resize(parameters_before);
    return replaced;
}

/// the constant that a key is for the analyzer; an `ALIAS` column is the expression that it has
QueryTreeNodePtr getConstantOfKey(QueryTreeNodePtr key)
{
    while (const auto * column_node = key->as<ColumnNode>())
    {
        if (!column_node->hasExpression())
            return nullptr;

        key = column_node->getExpression();
    }

    return key->as<ConstantNode>() ? key : nullptr;
}

/// constant keys are dropped; one key stays if there are no aggregate functions
void removeConstantGroupByKeys(QueryNode & query_node, bool has_aggregate_functions)
{
    if (!query_node.hasGroupBy() || query_node.isGroupByWithGroupingSets())
        return;

    auto & keys = query_node.getGroupBy().getNodes();
    for (size_t i = 0; i < keys.size();)
    {
        auto constant = getConstantOfKey(keys[i]);
        if (!constant || (!has_aggregate_functions && keys.size() == 1))
        {
            ++i;
            continue;
        }

        /// the query still writes the key, so the columns that it reads stay columns of the query
        const auto & constant_node = constant->as<ConstantNode &>();
        if (constant_node.hasSourceExpression() && !isQueryOrUnionNode(constant_node.getSourceExpression()))
            query_node.getProjection().getNodes().push_back(keys[i]);

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

/// the settings that change what a projection query means: the stored query has their values that are not the default
constexpr std::array kept_settings{
    "count_distinct_implementation",
    "rewrite_count_distinct_if_with_count_distinct_implementation",
    "optimize_normalize_count_variants",
    "optimize_if_chain_to_multiif"};

/// session settings must not change what a projection means
ContextMutablePtr createProjectionAnalysisContext(const ASTPtr & query_ast, const ContextPtr & context)
{
    auto query_context = Context::createCopy(context);
    query_context->setSetting("enable_positional_arguments", false);
    query_context->setSetting("rewrite_in_to_join", false);
    query_context->resetSettingsToDefaultValue(Strings(kept_settings.begin(), kept_settings.end()));
    /// `*` in a projection never has `MATERIALIZED` and `ALIAS` columns
    query_context->setSetting("asterisk_include_materialized_columns", false);
    query_context->setSetting("asterisk_include_alias_columns", false);
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

/// the query as the analyzer resolves it, before the changes that give the stored columns
QueryTreeNodePtr resolveProjectionQuery(const ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context, bool only_analyze)
{
    auto query_context = createProjectionAnalysisContext(query_ast, context);
    auto query_tree = buildQueryTree(query_ast, query_context);
    auto & query_node = query_tree->as<QueryNode &>();

    replaceIfWithConstantCondition(query_tree);
    if (query_context->getSettingsRef()[Setting::optimize_if_chain_to_multiif])
        replaceIfChainsWithMultiIf(query_tree);

    /// the stored query has no `FROM`: put the source in place of `system.one`
    query_node.getJoinTreeNode() = std::make_shared<TableNode>(source, query_context);

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

    QueryTreePassManager query_tree_pass_manager(query_context);
    addQueryTreePasses(query_tree_pass_manager, only_analyze);
    /// resolve only: an optimization pass can change a stored expression (`GROUP BY -c0` into `GROUP BY c0`)
    query_tree_pass_manager.runOnlyResolve(query_tree);

    return query_tree;
}

}

void storeSettingsOfProjectionQuery(const ASTPtr & query_ast, const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    auto & stored_settings = query_ast->as<ASTSelectQuery &>().settings()->as<ASTSetQuery &>().changes;
    if (settings[Setting::enable_positional_arguments_for_projections])
        stored_settings.setSetting("enable_positional_arguments", true);
    for (const auto * setting : kept_settings)
        if (settings.isChanged(setting))
            stored_settings.setSetting(setting, settings.get(setting));

    /// the old analysis kept a chain of `if` with these settings: `multiIf` of constant strings would be `LowCardinality(String)`
    if (settings[Setting::optimize_if_transform_const_strings_to_lowcardinality]
        && !settings[Setting::optimize_if_transform_strings_to_enum])
        stored_settings.removeSetting("optimize_if_chain_to_multiif");
}

QueryTreeNodePtr buildProjectionQueryTree(const ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context, bool only_analyze)
{
    auto query_tree = resolveProjectionQuery(query_ast, source, context, only_analyze);
    auto & query_node = query_tree->as<QueryNode &>();

    /// first, while the analyzer's constants are constants: the old analysis did not keep such a key
    removeConstantGroupByKeys(query_node, hasAggregateFunctionNodes(query_tree));

    /// before the pass: it must not remove an expression that reads a column (`count(b IS NOT NULL)`)
    auto table_node = query_node.getJoinTreeNode();
    for (auto & child : query_node.getChildren())
        if (child)
            restoreExpressionsOverColumns(child, table_node);

    /// the old analysis changed `count` and `sum` only of a literal, not of a folded expression (`count(1 + 1)`)
    for (const auto & aggregate_function_node : collectAggregateFunctionNodes(query_tree))
        for (auto & argument : aggregate_function_node->as<FunctionNode &>().getArguments().getNodes())
            if (const auto * constant_node = argument->as<ConstantNode>();
                constant_node && constant_node->hasSourceExpression() && !isQueryOrUnionNode(constant_node->getSourceExpression()))
                argument = constant_node->getSourceExpression();

    /// `count(1)` and `sum(1)` are stored as `count()`
    NormalizeCountVariantsPass().run(query_tree, query_node.getContext());

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

    /// only resolved: a later change can remove the expression that holds a subquery (`count((SELECT 1))`)
    auto query_tree = resolveProjectionQuery(query_ast, source, context, /*only_analyze=*/false);
    ScalarSubqueryValues values;
    collectScalarSubqueryValues(query_tree, values);

    /// the tree points into the analyzer's copy of the query, so that copy becomes the stored query
    query_ast = query_tree->getOriginalAST();
    if (replaceScalarSubqueries(query_ast, values) != values.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A scalar subquery of a projection is not in the query {}", query_ast->formatForErrorMessage());
}

void storeExpressionsOfAliasesInIf(const ASTPtr & query_ast, const StoragePtr & source, const ContextPtr & context)
{
    bool with_chains = createProjectionAnalysisContext(query_ast, context)->getSettingsRef()[Setting::optimize_if_chain_to_multiif];

    /// the analyzer finds what an alias is; the expression of an alias can have an alias too, so this repeats
    while (hasIfWithIdentifierArgument(query_ast, with_chains))
    {
        AliasedIfArguments aliased_arguments;
        Names lambda_parameters;
        if (!collectAliasedIfArguments(resolveProjectionQuery(query_ast, source, context, /*only_analyze=*/true), with_chains, aliased_arguments)
            || !replaceAliasedIfArguments(query_ast, aliased_arguments, lambda_parameters))
            return;
    }
}

}
