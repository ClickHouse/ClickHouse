#include <Analyzer/QueryAnalysisPass.h>

#include <boost/algorithm/string/case_conv.hpp>

#include <Common/FieldVisitorToString.h>

#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWithElement.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>

#include <Columns/ColumnNullable.h>

#include <Analyzer/IdentifierNode.h>
#include <Analyzer/MatcherNode.h>
#include <Analyzer/ColumnTransformers.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryTreeBuilder.h>

#include <Databases/IDatabase.h>

#include <Interpreters/StorageID.h>
#include <Interpreters/Context.h>
#include <Interpreters/UserDefinedExecutableFunctionFactory.h>
#include <Interpreters/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>

#include <Functions/FunctionFactory.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int LOGICAL_ERROR;
    extern const int CYCLIC_ALIASES;
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
    extern const int BAD_ARGUMENTS;
    extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
}

/** Query analyzer implementation overview. Please check documentation in QueryAnalysisPass.h before.
  * And additional documentation is writter for each method, where special cases are described in detail.
  *
  * Each node in query must be resolved. For each query tree node resolved state is specific.
  *
  * For constant node no resolve process exists, it is resolved during construction.
  *
  * For table node no resolve process exists, it is resolved during construction.
  *
  * For function node to be resolved parameters and arguments must be resolved, must be initialized with concrete aggregate or
  * non aggregate function and with result type.
  *
  * For lambda node there can be 2 different cases.
  * 1. Standalone: WITH (x -> x + 1) AS lambda SELECT lambda(1); Such lambdas are inlined in query tree during query analysis pass.
  * 2. Function arguments: WITH (x -> x + 1) AS lambda SELECT arrayMap(lambda, [1, 2, 3]); For such lambda resolution must
  * set concrete lambda arguments (initially they are identifier nodes) and resolve lambda expression body.
  *
  * For query node resolve process must resolve all its inner nodes.
  *
  * For matcher node resolve process must replace it with matched nodes.
  *
  * For identifier node resolve process must replace it with concrete non identifier node. This part is most complex because
  * for identifier resolution scopes and identifier lookup context play important part.
  *
  * ClickHouse SQL support lexical scoping for identifier resolution. Scope can be defined by query node or by expression node.
  * Expression nodes that can define scope are lambdas and table ALIAS columns.
  *
  * Identifier lookup context can be expression, function, table.
  *
  * Examples: WITH (x -> x + 1) as func SELECT func() FROM func; During function `func` resolution identifier lookup is performed
  * in function context.
  *
  * If there are no information of identifier context rules are following:
  * 1. Try to resolve identifier in expression context.
  * 2. Try to resolve identifier in function context, if it is allowed. Example: SELECT func; Here func identifier cannot be resolved in function context
  * because query projection does not support that.
  * 3. Try to resolve identifier in talbe context, if it is allowed. Example: SELECT table; Here table identifier cannot be resolved in function context
  * because query projection does not support that.
  *
  * TODO: This does not supported properly before, because matchers could not be resolved from aliases.
  *
  * Identifiers are resolved with following resules:
  * Resolution starts with current scope.
  * 1. Try to resolve identifier from expression scope arguments. Lambda expression arguments are greatest priority.
  * 2. Try to resolve identifier from aliases.
  * 3. Try to resolve identifier from tables if scope is query.
  * Steps 2 and 3 can be changed using prefer_column_name_to_alias setting.
  * If identifier could not be resolved in current scope, resolution must be continued in parent scopes.
  * 4. Try to resolve identifier from parent scopes.
  *
  * Additional rules about aliases and scopes.
  * 1. Parent scope cannot refer alias from child scope.
  * 2. Child scope can refer to alias in parent scope.
  *
  * Example: SELECT arrayMap(x -> x + 1 AS a, [1,2,3]), a; Identifier a is unknown in parent scope.
  * Example: SELECT a FROM (SELECT 1 as a); Here we do not refer to alias a from child query scope. But we query it projection result, similar to tables.
  * Example: WITH 1 as a SELECT (SELECT a) as b; Here in child scope identifier a is resolved using alias from parent scope.
  *
  * Additional rules about identifier binding.
  * Bind for identifier to entity means that identifier first part match some node during analysis.
  * If other parts of identifier cannot be resolved in that node, exception must be throwed.
  *
  * Example:
  * CREATE TABLE test_table (id UInt64, compound_value Tuple(value UInt64)) ENGINE=TinyLog;
  * SELECT compound_value.value, 1 AS compound_value FROM test_table;
  * Identifier first part compound_value bound to entity with alias compound_value, but nested identifier part cannot be resolved from entity,
  * lookup should not be continued, and exception must be throwed because if lookup continues that way identifier can be resolved from tables.
  *
  * TODO: This was not supported properly before analyzer because nested identifier could not be resolved from alias.
  *
  * More complex example:
  * CREATE TABLE test_table (id UInt64, value UInt64) ENGINE=TinyLog;
  * WITH cast(('Value'), 'Tuple (value UInt64') AS value SELECT (SELECT value FROM test_table);
  * Identifier first part value bound to test_table column value, but nested identifier part cannot be resolved from it,
  * lookup should not be continued, and exception must be throwed because if lookup continues identifier can be resolved from parent scope.
  *
  * TODO: Add expression name into query tree node. Example: SELECT plus(1, 1). Result: SELECT 2. Expression name of constant node should be 2.
  * TODO: Table identifiers with optional UUID.
  * TODO: Table ALIAS columns
  * TODO: Lookup functions arrayReduce(sum, [1, 2, 3]);
  * TODO: SELECT (compound_expression).*, (compound_expression).COLUMNS are not supported on parser level.
  * TODO: SELECT a.b.c.*, a.b.c.COLUMNS. Qualified matcher where identifier size is greater than 2 are not supported on parser level.
  * TODO: CTE
  * TODO: JOIN, ARRAY JOIN
  * TODO: bulding sets
  * TODO: Special functions grouping, in.
  */

/// Identifier lookup context
enum class IdentifierLookupContext : uint8_t
{
    EXPRESSION = 0,
    FUNCTION,
    TABLE,
};

static const char * toString(IdentifierLookupContext identifier_lookup_context)
{
    switch (identifier_lookup_context)
    {
        case IdentifierLookupContext::EXPRESSION: return "EXPRESSION";
        case IdentifierLookupContext::FUNCTION: return "FUNCTION";
        case IdentifierLookupContext::TABLE: return "TABLE";
    }
}

static const char * toStringLowercase(IdentifierLookupContext identifier_lookup_context)
{
    switch (identifier_lookup_context)
    {
        case IdentifierLookupContext::EXPRESSION: return "expression";
        case IdentifierLookupContext::FUNCTION: return "function";
        case IdentifierLookupContext::TABLE: return "table";
    }
}

/** Structure that represent identifier lookup during query analysis.
  * Lookup can be in query expression, function, table context.
  */
struct IdentifierLookup
{
    Identifier identifier;
    IdentifierLookupContext lookup_context;

    bool isExpressionLookup() const
    {
        return lookup_context == IdentifierLookupContext::EXPRESSION;
    }

    bool isFunctionLookup() const
    {
        return lookup_context == IdentifierLookupContext::FUNCTION;
    }

    bool isTableLookup() const
    {
        return lookup_context == IdentifierLookupContext::TABLE;
    }

    String dump() const
    {
        return identifier.getFullName() + ' ' + toString(lookup_context);
    }
};

inline bool operator==(const IdentifierLookup & lhs, const IdentifierLookup & rhs)
{
    return lhs.identifier.getFullName() == rhs.identifier.getFullName() && lhs.lookup_context == rhs.lookup_context;
}

inline bool operator!=(const IdentifierLookup & lhs, const IdentifierLookup & rhs)
{
    return !(lhs == rhs);
}

struct IdentifierLookupHash
{
    size_t operator()(const IdentifierLookup & identifier_lookup) const
    {
        return std::hash<std::string>()(identifier_lookup.identifier.getFullName()) ^ static_cast<uint8_t>(identifier_lookup.lookup_context);
    }
};

struct StorageIDHash
{
    size_t operator()(const StorageID & storage_id) const
    {
        return std::hash<std::string>()(storage_id.getFullNameNotQuoted());
    }
};

struct StorageColumns
{
    NamesAndTypesList column_names_and_types;
    std::unordered_set<std::string> column_identifier_first_parts;

    bool canBindIdentifier(IdentifierView identifier)
    {
        return column_identifier_first_parts.find(std::string(identifier.at(0))) != column_identifier_first_parts.end();
    }
};

class ExpressionsStack
{
public:
    void pushNode(const QueryTreeNodePtr & node)
    {
        if (node->hasAlias())
        {
            expressions.emplace_back(node.get(), node->getAlias());
            ++alias_name_to_expressions_size[expressions.back().second];
            return;
        }

        expressions.emplace_back(node.get(), std::string());
    }

    void popNode()
    {
        const auto & [_, top_expression_alias] = expressions.back();
        if (!top_expression_alias.empty())
        {
            auto it = alias_name_to_expressions_size.find(top_expression_alias);
            --it->second;

            if (it->second == 0)
                alias_name_to_expressions_size.erase(it);
        }

        expressions.pop_back();
    }

    const IQueryTreeNode * getRoot() const
    {
        if (expressions.empty())
            return nullptr;

        return expressions.front().first;
    }

    const IQueryTreeNode * getTop() const
    {
        if (expressions.empty())
            return nullptr;

        return expressions.back().first;
    }

    bool hasExpressionWithAlias(const std::string & alias) const
    {
        return alias_name_to_expressions_size.find(alias) != alias_name_to_expressions_size.end();
    }

    size_t size() const
    {
        return expressions.size();
    }

    bool empty() const
    {
        return expressions.empty();
    }

    void dump(WriteBuffer & buffer)
    {
        buffer << expressions.size() << '\n';

        for (auto & [expression, alias] : expressions)
        {
            buffer << "Expression ";
            buffer << expression->formatASTForErrorMessage();

            if (!alias.empty())
                buffer << " alias " << alias;

            buffer << '\n';
        }
    }

private:
    std::vector<std::pair<const IQueryTreeNode *, std::string>> expressions;
    std::unordered_map<std::string, size_t> alias_name_to_expressions_size;
};

struct IdentifierResolveScope
{
    /// Construct identifier resolve scope using scope node, and parent scope
    IdentifierResolveScope(QueryTreeNodePtr scope_node_, IdentifierResolveScope * parent_scope_)
        : scope_node(std::move(scope_node_))
        , parent_scope(parent_scope_)
    {}

    QueryTreeNodePtr scope_node;
    IdentifierResolveScope * parent_scope = nullptr;

    std::unordered_map<IdentifierLookup, QueryTreeNodePtr, IdentifierLookupHash> identifier_lookup_to_node;

    /// Lambda argument can be expression like constant, column, or it can be function
    std::unordered_map<std::string, QueryTreeNodePtr> expression_argument_name_to_node;

    /// Alias name to query expression node
    std::unordered_map<std::string, QueryTreeNodePtr> alias_name_to_expression_node;

    /// Alias name to lambda node
    std::unordered_map<std::string, QueryTreeNodePtr> alias_name_to_lambda_node;

    /// Alias name to table expression node
    std::unordered_map<std::string, QueryTreeNodePtr> alias_name_to_table_expression_node;

    /// Nodes with duplicated identifiers
    std::unordered_set<QueryTreeNodePtr> nodes_with_duplicated_aliases;

    /// Current scope expression in resolve process stack
    ExpressionsStack expressions_in_resolve_process_stack;

    /// Current scope expression
    std::unordered_set<IdentifierLookup, IdentifierLookupHash> non_cached_identifier_lookups_during_expression_resolve;

    /// Allow to check parent scopes if identifier cannot be resolved in current scope
    bool allow_to_check_parent_scopes = true;

    /// Dump identifier resolve scope
    void dump(WriteBuffer & buffer)
    {
        buffer << "Scope node " << scope_node->formatASTForErrorMessage() << '\n';
        buffer << "Identifier lookup to node " << identifier_lookup_to_node.size() << '\n';
        for (const auto & [identifier, node] : identifier_lookup_to_node)
            buffer << "Identifier " << identifier.dump() << " node " << node->formatASTForErrorMessage() << '\n';

        buffer << "Expression argument name to node " << expression_argument_name_to_node.size() << '\n';
        for (const auto & [alias_name, node] : expression_argument_name_to_node)
            buffer << "Alias name " << alias_name << " node " << node->formatASTForErrorMessage() << '\n';

        buffer << "Alias name to expression node " << alias_name_to_expression_node.size() << '\n';
        for (const auto & [alias_name, node] : alias_name_to_expression_node)
            buffer << "Alias name " << alias_name << " expression node " << node->formatASTForErrorMessage() << '\n';

        buffer << "Alias name to function node " << alias_name_to_lambda_node.size() << '\n';
        for (const auto & [alias_name, node] : alias_name_to_lambda_node)
            buffer << "Alias name " << alias_name << " lambda node " << node->formatASTForErrorMessage() << '\n';

        buffer << "Alias name to table expression node " << alias_name_to_table_expression_node.size() << '\n';
        for (const auto & [alias_name, node] : alias_name_to_table_expression_node)
            buffer << "Alias name " << alias_name << " table node " << node->formatASTForErrorMessage() << '\n';

        buffer << "Nodes with duplicated aliases " << nodes_with_duplicated_aliases.size() << '\n';
        for (const auto & node : nodes_with_duplicated_aliases)
            buffer << "Alias name " << node->getAlias() << " node " << node->formatASTForErrorMessage() << '\n';

        buffer << "Expression resolve process stack " << '\n';
        expressions_in_resolve_process_stack.dump(buffer);

        buffer << "Allow to check parent scopes " << allow_to_check_parent_scopes << '\n';
        // buffer << "Parent scope " << parent_scope << '\n';
    }
};

class QueryAnalyzer
{
public:
    explicit QueryAnalyzer(ContextPtr context_)
        : context(std::move(context_))
    {}

    void resolve(QueryTreeNodePtr node)
    {
        IdentifierResolveScope scope(node, nullptr /*parent_scope*/);

        if (node->getNodeType() == QueryTreeNodeType::QUERY)
        {
            resolveQuery(node, scope);
        }
        else if (node->getNodeType() == QueryTreeNodeType::LIST)
        {
            resolveExpressionNodeList(node, scope, false /*allow_lambda_expression*/);
        }
        else if (node->getNodeType() == QueryTreeNodeType::FUNCTION)
        {
            resolveExpressionNode(node, scope, false /*allow_lambda_expression*/);
        }
        else if (node->getNodeType() == QueryTreeNodeType::LAMBDA)
        {
            resolveLambda(node, {}, scope);
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Node {} with type {} is not supported by query analyzer. Supported nodes are query, list, function, lambda.",
                node->formatASTForErrorMessage(),
                node->getNodeTypeName());
        }
    }

private:
    /// Utility functions

    static bool isNodePartOfSubtree(const IQueryTreeNode * node, const IQueryTreeNode * root);

    static QueryTreeNodePtr wrapExpressionNodeInTupleElement(QueryTreeNodePtr expression_node, IdentifierView nested_path);

    static QueryTreeNodePtr tryGetLambdaFromSQLUserDefinedFunction(const std::string & function_name);

    void evaluateScalarSubquery(QueryTreeNodePtr & query_tree_node);

    /// Resolve identifier functions

    QueryTreeNodePtr tryResolveIdentifierFromExpressionArguments(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromAliases(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromTables(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierInCurrentScope(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & current_scope);

    QueryTreeNodePtr tryResolveIdentifierInParentScopes(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifier(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    /// Resolve query tree nodes functions

    QueryTreeNodePtr resolveMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope);

    void resolveLambda(QueryTreeNodePtr & lambda_node, const QueryTreeNodes & lambda_arguments, IdentifierResolveScope & scope);

    void resolveFunction(QueryTreeNodePtr & function_node, IdentifierResolveScope & scope);

    void resolveExpressionNode(QueryTreeNodePtr & node, IdentifierResolveScope & scope, bool allow_lambda_expression);

    void resolveExpressionNodeList(QueryTreeNodePtr & node_list, IdentifierResolveScope & scope, bool allow_lambda_expression);

    void resolveQuery(QueryTreeNodePtr & query_tree_node, IdentifierResolveScope & scope);

    /// Query analyzer context
    ContextPtr context;

    /// Lambdas that are currently in resolve process
    std::unordered_set<IQueryTreeNode *> lambdas_in_resolve_process;

    /// Storage id to storage columns cache
    std::unordered_map<StorageID, StorageColumns, StorageIDHash> storage_id_to_columns_cache;
};

/// Utility functions implementation

bool QueryAnalyzer::isNodePartOfSubtree(const IQueryTreeNode * node, const IQueryTreeNode * root)
{
    std::vector<const IQueryTreeNode *> nodes_to_process;
    nodes_to_process.push_back(root);

    while (!nodes_to_process.empty())
    {
        const auto * subtree_node = nodes_to_process.back();
        nodes_to_process.pop_back();

        if (subtree_node == node)
            return true;

        for (const auto & child : subtree_node->getChildren())
        {
            if (child)
                nodes_to_process.push_back(child.get());
        }
    }

    return false;
}

/** Wrap expression node in tuple element function calls for nested paths.
  * Example: Expression node: compound_expression. Nested path: nested_path_1.nested_path_2.
  * Result: tupleElement(tupleElement(compound_expression, 'nested_path_1'), 'nested_path_2').
  */
QueryTreeNodePtr QueryAnalyzer::wrapExpressionNodeInTupleElement(QueryTreeNodePtr expression_node, IdentifierView nested_path)
{
    size_t nested_path_parts_size = nested_path.getPartsSize();
    for (size_t i = 0; i < nested_path_parts_size; ++i)
    {
        const auto & nested_path_part = nested_path[i];
        auto tuple_element_function = std::make_shared<FunctionNode>("tupleElement");
        tuple_element_function->getArguments().getNodes().push_back(expression_node);
        tuple_element_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(nested_path_part));
        expression_node = tuple_element_function;
    }

    return expression_node;
}

/** Try to get lambda node from sql user defined functions if sql user defined function with function name exists.
  * Returns lambda node if function exists, nullptr otherwise.
  */
QueryTreeNodePtr tryGetLambdaFromSQLUserDefinedFunction(const std::string & function_name, const ContextPtr & context)
{
    auto user_defined_function = UserDefinedSQLFunctionFactory::instance().tryGet(function_name);
    if (!user_defined_function)
        return {};

    const auto & create_function_query = user_defined_function->as<ASTCreateFunctionQuery>();
    auto result_node = buildQueryTree(create_function_query->function_core, context);
    if (result_node->getNodeType() != QueryTreeNodeType::LAMBDA)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "SQL user defined function {} must represent lambda expression. Actual {}",
            function_name,
            create_function_query->function_core->formatForErrorMessage());

    return result_node;
}

/** Evaluate scalar subquery.
  * In result of this function node will be replaced by constant node.
  */
void QueryAnalyzer::evaluateScalarSubquery(QueryTreeNodePtr & node)
{
    auto subquery_context = Context::createCopy(context);
    Settings subquery_settings = context->getSettings();
    subquery_settings.max_result_rows = 1;
    subquery_settings.extremes = false;
    subquery_context->setSettings(subquery_settings);

    if (!subquery_context->hasQueryContext())
    {
        // auto subquery_query_context = subquery_context->getQueryContext();
        // for (const auto & it : data.scalars)
        //     context->addScalar(it.first, it.second);
    }

    size_t subquery_depth = 0;
    auto options = SelectQueryOptions(QueryProcessingStage::Complete, subquery_depth + 1, true);

    auto interpreter = std::make_unique<InterpreterSelectQueryAnalyzer>(node->toAST(), options, subquery_context);

    auto io = interpreter->execute();

    Block block;
    PullingAsyncPipelineExecutor executor(io.pipeline);
    io.pipeline.setProgressCallback(context->getProgressCallback());
    while (block.rows() == 0 && executor.pull(block))
    {
    }

    if (block.rows() == 0)
    {
        auto types = interpreter->getSampleBlock().getDataTypes();
        if (types.size() != 1)
            types = {std::make_shared<DataTypeTuple>(types)};

        auto & type = types[0];
        if (!type->isNullable())
        {
            if (!type->canBeInsideNullable())
                throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY,
                    "Scalar subquery returned empty result of type {} which cannot be Nullable.",
                    type->getName());

            type = makeNullable(type);
        }

        node = std::make_shared<ConstantNode>(Null());
        return;
    }

    if (block.rows() != 1)
        throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one row");

    Block tmp_block;
    while (tmp_block.rows() == 0 && executor.pull(tmp_block))
    {
    }

    if (tmp_block.rows() != 0)
        throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one row");

    block = materializeBlock(block);
    size_t columns = block.columns();

    // Block scalar;
    Field scalar_value;
    DataTypePtr scalar_type;

    if (columns == 1)
    {
        auto & column = block.getByPosition(0);
        /// Here we wrap type to nullable if we can.
        /// It is needed cause if subquery return no rows, it's result will be Null.
        /// In case of many columns, do not check it cause tuple can't be nullable.
        if (!column.type->isNullable() && column.type->canBeInsideNullable())
        {
            column.type = makeNullable(column.type);
            column.column = makeNullable(column.column);
        }

        column.column->get(0, scalar_value);
        scalar_type = column.type;
    }
    else
    {
        auto tuple_column = ColumnTuple::create(block.getColumns());
        tuple_column->get(0, scalar_value);
        scalar_type = std::make_shared<DataTypeTuple>(block.getDataTypes());
    }

    auto original_ast = node->getOriginalAST();
    node = std::make_shared<ConstantNode>(std::move(scalar_value), std::move(scalar_type));
    node->setOriginalAST(std::move(original_ast));
}

/// Resolve identifier functions implementation

/** Resolve identifier from expression arguments.
  *
  * Expression arguments can be initialized during lambda analysis or they could be provided externally.
  * Expression arguments must be already resolved nodes. This is client responsibility to resolve them during scope initialization.
  *
  * Resolve strategy:
  * 1. Try to bind identifier to scope argument name to node map.
  * 2. If identifier is binded but expression context and node type are incompatible return nullptr.
  *
  * It is important to support edge cases, where we lookup for table or function node, but argument has same name.
  * Example: WITH (x -> x + 1) AS func, (func -> func(1) + func) AS lambda SELECT lambda(1);
  *
  * 3. If identifier is compound and identifier lookup is in expression context, pop first part from identifier lookup and wrap node
  * using nested parts of identifier using `wrapExpressionNodeInTupleElement` function.
  */
QueryTreeNodePtr QueryAnalyzer::tryResolveIdentifierFromExpressionArguments(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope)
{
    const auto & identifier_bind_part = identifier_lookup.identifier.front();

    auto it = scope.expression_argument_name_to_node.find(identifier_bind_part);
    if (it == scope.expression_argument_name_to_node.end())
        return {};

    if (identifier_lookup.isExpressionLookup() && it->second->getNodeType() == QueryTreeNodeType::LAMBDA)
        return {};
    else if (identifier_lookup.isTableLookup() && it->second->getNodeType() != QueryTreeNodeType::TABLE)
        return {};

    if (identifier_lookup.identifier.isCompound() && identifier_lookup.isExpressionLookup())
    {
        auto nested_path = IdentifierView(identifier_lookup.identifier);
        nested_path.popFirst();

        auto tuple_element_result = wrapExpressionNodeInTupleElement(it->second, nested_path);
        resolveFunction(tuple_element_result, scope);

        return tuple_element_result;
    }

    return it->second;
}

/** Visitor that extracts expression and function aliases from node and initialize scope tables with it.
  * Does not go into child lambdas and queries.
  *
  * TODO: Maybe better for this visitor to handle QueryNode. Handle table nodes.
  *
  * Important:
  * Identifier nodes with aliases are added both in alias to expression and alias to function map.
  *
  * These is necessary because identifier with alias can give alias name to any query tree node.
  *
  * Example:
  * WITH (x -> x + 1) AS id, id AS value SELECT value(1);
  * In this example id as value is identifier node that has alias, during scope initialization we cannot derive
  * that id is actually lambda or expression.
  *
  *
  * TODO: Disable identifier with alias node propagation for table nodes. This can occur only for special functions
  * if their argument can be table.
  *
  * There are no easy solution here, without trying to make full featured expression resolution at this stage.
  * Example:
  * WITH (x -> x + 1) AS id, id AS id_1, id_1 AS id_2 SELECT id_2(1);
  *
  * It is client responsibility after resolving identifier node with alias, make following actions:
  * 1. If identifier node was resolved in function scope, remove alias from scope expression map.
  * 2. If identifier node was resolved in expression scope, remove alias from scope function map.
  *
  * That way we separate alias map initialization and expressions resolution.
  */
class ScopeAliasVisitorMatcher
{
public:
    using Visitor = InDepthQueryTreeVisitor<ScopeAliasVisitorMatcher, true, true>;

    struct Data
    {
        IdentifierResolveScope & scope;
    };

    static void visit(QueryTreeNodePtr & node, Data & data)
    {
        updateAliasesIfNeeded(data, node, false);
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child, Data & data)
    {
        if (auto * lambda_node = child->as<LambdaNode>())
        {
            updateAliasesIfNeeded(data, child, true);
            return false;
        }
        else if (auto * query_tree_node = child->as<QueryNode>())
        {
            updateAliasesIfNeeded(data, child, false);
            return false;
        }

        return !(child->as<TableNode>());
    }
private:
    static void updateAliasesIfNeeded(Data & data, const QueryTreeNodePtr & node, bool function_node)
    {
        if (!node->hasAlias())
            return;

        const auto & alias = node->getAlias();

        if (function_node)
        {
            if (data.scope.alias_name_to_expression_node.contains(alias))
                data.scope.nodes_with_duplicated_aliases.insert(node);

            auto [_, inserted] = data.scope.alias_name_to_lambda_node.insert(std::make_pair(alias, node));
            if (!inserted)
                data.scope.nodes_with_duplicated_aliases.insert(node);

            return;
        }

        if (data.scope.alias_name_to_lambda_node.contains(alias))
            data.scope.nodes_with_duplicated_aliases.insert(node);

        auto [_, inserted] = data.scope.alias_name_to_expression_node.insert(std::make_pair(alias, node));
        if (!inserted)
            data.scope.nodes_with_duplicated_aliases.insert(node);

        /// If node is identifier put it also in scope alias name to lambda node map
        if (node->getNodeType() == QueryTreeNodeType::IDENTIFIER)
            data.scope.alias_name_to_lambda_node.insert(std::make_pair(alias, node));
    }
};

using ScopeAliasVisitor = ScopeAliasVisitorMatcher::Visitor;

/** Resolve identifier from scope aliases.
  *
  * Resolve strategy:
  * 1. If alias is registered current expressions that are in resolve process and if last expression is not part of first expression subtree
  * throw cyclic aliases exception.
  * Otherwise prevent cache usage for identifier lookup and return nullptr.
  *
  * This is special scenario where identifier has name the same as alias name in one of its parent expressions including itself.
  * In such case we cannot resolve identifier from aliases because of recursion. It is client responsibility to register and deregister alias
  * names during expressions resolve.
  *
  * We must prevent cache usage for lookup because lookup outside of expression is supposed to return other value.
  * Example: SELECT (id + 1) AS id, id + 2. Lookup for id inside (id + 1) as id should return id from table, but lookup (id + 2) should return
  * (id + 1) AS id.
  *
  * Below cases should work:
  * Example:
  * SELECT id AS id FROM test_table;
  * SELECT value.value1 AS value FROM test_table;
  * SELECT (id + 1) AS id FROM test_table;
  * SELECT (1 + (1 + id)) AS id FROM test_table;
  *
  * Below cases should throw cyclic aliases exception:
  * SELECT (id + b) AS id, id as b FROM test_table;
  * SELECT (1 + b + 1 + id) AS id, b as c, id as b FROM test_table;
  *
  * 2. Depending on IdentifierLookupContext get alias name to node map from IdentifierResolveScope.
  * 3. Try to bind identifier to alias name in map. If there are no such binding return nullptr.
  * 4. Add node into current expressions to resolve. TODO: Handle lambdas and tables properly.
  *
  * 5. If node in map is not resolved, resolve it. It is important because for result type of identifier lookup node can depend on it.
  * Example: SELECT value.a, cast('(1)', 'Tuple(a UInt64)') AS value;
  *
  * Special case for IdentifierNode, if node is identifier depending on lookup context we need to erase entry from expression or lambda map.
  * Check ScopeAliasVisitorMatcher documentation.
  *
  * Special case for QueryNode, if lookup context is expression, evaluate it as scalar subquery.
  *
  * 6. Pop node from current expressions to resolve.
  * 7. If identifier is compound and identifier lookup is in expression context, pop first part from identifier lookup and wrap alias node
  * using nested parts of identifier using `wrapExpressionNodeInTupleElement` function.
  *
  * Example: SELECT value AS alias, alias.nested_path.
  * Result: SELECT value AS alias, tupleElement(value, 'nested_path') value.nested_path.
  *
  * 8. If identifier lookup is in expression context, clone result expression.
  */
QueryTreeNodePtr QueryAnalyzer::tryResolveIdentifierFromAliases(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope)
{
    const auto & identifier_bind_part = identifier_lookup.identifier.front();
    auto get_alias_name_to_node_map = [&]() -> std::unordered_map<std::string, QueryTreeNodePtr> &
    {
        if (identifier_lookup.isExpressionLookup())
            return scope.alias_name_to_expression_node;
        else if (identifier_lookup.isFunctionLookup())
            return scope.alias_name_to_lambda_node;

        return scope.alias_name_to_table_expression_node;
    };

    auto & alias_name_to_node_map = get_alias_name_to_node_map();
    auto it = alias_name_to_node_map.find(identifier_bind_part);

    if (it == alias_name_to_node_map.end())
        return {};

    if (!it->second)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Node with alias {} is not valid. In scope {}",
            identifier_bind_part,
            scope.scope_node->formatASTForErrorMessage());

    if (scope.expressions_in_resolve_process_stack.hasExpressionWithAlias(identifier_bind_part))
    {
        const auto * root_expression = scope.expressions_in_resolve_process_stack.getRoot();
        const auto * top_expression = scope.expressions_in_resolve_process_stack.getTop();

        if (!isNodePartOfSubtree(top_expression, root_expression))
        {
            throw Exception(ErrorCodes::CYCLIC_ALIASES,
                "Cyclic aliases for identifier {}. In scope {}",
                identifier_lookup.identifier.getFullName(),
                scope.scope_node->formatASTForErrorMessage());
        }

        scope.non_cached_identifier_lookups_during_expression_resolve.insert(identifier_lookup);
        return {};
    }

    scope.expressions_in_resolve_process_stack.pushNode(it->second);

    /// Resolve expression if necessary
    if (auto * alias_identifier = it->second->as<IdentifierNode>())
    {
        it->second = tryResolveIdentifier(IdentifierLookup{alias_identifier->getIdentifier(), identifier_lookup.lookup_context}, scope);

        if (!it->second)
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
               "Unknown {} identifier {}. In scope {}",
               toStringLowercase(identifier_lookup.lookup_context),
               identifier_lookup.identifier.getFullName(),
               scope.scope_node->formatASTForErrorMessage());

        /** During collection of aliases if node is identifier and has alias, we cannot say if it is
          * column or function node. Check ScopeAliasVisitor documentation for clarification.
          *
          * If we resolved identifier node as expression, we must remove identifier node alias from
          * function alias map.
          * If we resolved identifier node as function, we must remove identifier node alias from
          * expression alias map.
          */
        if (identifier_lookup.isExpressionLookup())
            scope.alias_name_to_lambda_node.erase(identifier_bind_part);
        else if (identifier_lookup.isFunctionLookup() && it->second)
            scope.alias_name_to_expression_node.erase(identifier_bind_part);
    }
    else if (auto * function = it->second->as<FunctionNode>())
    {
        resolveFunction(it->second, scope);
    }
    else if (auto * query = it->second->as<QueryNode>())
    {
        IdentifierResolveScope subquery_scope(it->second, &scope /*parent_scope*/);
        resolveQuery(it->second, subquery_scope);

        if (identifier_lookup.isExpressionLookup())
            evaluateScalarSubquery(it->second);
    }

    scope.expressions_in_resolve_process_stack.popNode();

    QueryTreeNodePtr result = it->second;

    if (identifier_lookup.identifier.isCompound() && identifier_lookup.isExpressionLookup() && result)
    {
        auto nested_path = IdentifierView(identifier_lookup.identifier);
        nested_path.popFirst();

        auto tuple_element_result = wrapExpressionNodeInTupleElement(result, nested_path);
        resolveFunction(tuple_element_result, scope);

        result = tuple_element_result;
    }
    else if (identifier_lookup.isExpressionLookup() && result)
    {
        /** If expression node was resolved throught aliases we must clone it to keep query tree state valid.
          *
          * Example:
          * If we have query SELECT 1 as a, a
          * If we do not clone node, after query analysis query will look like SELECT 1 as a, 1 as a.
          *
          * This query is broken because multiple aliases with same name are not allowed in query tree.
          */
        result = result->clone();
        result->removeAlias();
    }

    return result;
}

/** Resolve identifier from scope tables.
  *
  * 1. If there is no table node in scope, or identifier is in function lookup context return nullptr.
  * 2. If identifier is in table lookup context, check if it has 1 or 2 parts, otherwise throw exception.
  * If identifer has 2 parts try to match it with database_name and table_name.
  * If identifier has 1 part try to match it with table_name, then try to match it with table alias.
  * 3. If identifier is in expression lookup context, we first need to bind identifier to some table column using identifier first part.
  * Start with identifier first part, if it match some column name in table try to get column with full identifier name.
  * TODO: Need to check if it is okay to throw exception if compound identifier first part bind to column but column is not valid.
  */
QueryTreeNodePtr QueryAnalyzer::tryResolveIdentifierFromTables(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope)
{
    if (identifier_lookup.isFunctionLookup())
        return {};

    auto * query_scope_node = scope.scope_node->as<QueryNode>();
    if (!query_scope_node || !query_scope_node->getFrom())
        return {};

    auto from_node = query_scope_node->getFrom();
    auto * table_node = query_scope_node->getFrom()->as<TableNode>();

    if (!table_node)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "From does not contain table node");

    const auto & identifier = identifier_lookup.identifier;
    const auto & path_start = identifier.getParts().front();
    auto storage_id = table_node->getStorageID();
    auto & table_name = storage_id.table_name;
    auto & database_name = storage_id.database_name;

    if (identifier_lookup.isTableLookup())
    {
        size_t parts_size = identifier_lookup.identifier.getPartsSize();
        if (parts_size != 1 && parts_size != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Expected identifier {} to contain 1 or 2 parts size to be resolved as table. In scope {}",
                identifier_lookup.identifier.getFullName(),
                table_node->formatASTForErrorMessage());

        if (parts_size == 1 && path_start == table_name)
            return from_node;
        else if (parts_size == 2 && path_start == database_name && identifier[1] == table_name)
            return from_node;
        else
            return {};
    }

    /// TODO: Check if this cache is safe to use in case of different settings

    auto storage_columns_it = storage_id_to_columns_cache.find(table_node->getStorageID());
    if (storage_columns_it == storage_id_to_columns_cache.end())
    {
        StorageColumns storage_columns;
        storage_columns.column_names_and_types = table_node->getStorageSnapshot()->getColumns(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns());

        for (auto & name_and_type_pair : storage_columns.column_names_and_types)
        {
            Identifier column_name_identifier(name_and_type_pair.name);
            storage_columns.column_identifier_first_parts.insert(column_name_identifier.at(0));
        }

        auto [inserted_it, _] = storage_id_to_columns_cache.emplace(storage_id, std::move(storage_columns));
        storage_columns_it = inserted_it;
    }

    auto & storage_columns = storage_columns_it->second;

    /** If identifier first part binds to some column start. Then we can try to find whole identifier in table.
      * 1. Try to bind identifier first part to column in table, if true get full identifier from table or throw exception.
      * 2. Try to bind identifier first part to table name or table alias, if true remove first part and try to get full identifier from table or throw exception.
      * 3. Try to bind identifier first parts to database name and table name, if true remove first two parts and try to get full identifier from table or throw exception.
      */
    auto resolve_identifier_from_table_or_throw = [&](size_t drop_first_parts_size)
    {
        auto identifier_view = IdentifierView(identifier);
        identifier_view.popFirst(drop_first_parts_size);

        auto result_column = storage_columns.column_names_and_types.tryGetByName(std::string(identifier_view.getFullName()));
        if (!result_column)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Identifier {} cannot be resolved from table {}. In scope {}",
                identifier.getFullName(),
                storage_id.getFullTableName(),
                scope.scope_node->formatASTForErrorMessage());

        return result_column;
    };

    if (storage_columns.canBindIdentifier(IdentifierView(identifier)))
    {
        auto result_column = resolve_identifier_from_table_or_throw(0 /*drop_first_parts_size*/);
        return std::make_shared<ColumnNode>(*result_column, from_node);
    }

    if (identifier.getPartsSize() == 1)
        return {};

    if (path_start == table_name || (table_node->hasAlias() && path_start == table_node->getAlias()))
    {
        auto result_column = resolve_identifier_from_table_or_throw(1 /*drop_first_parts_size*/);
        return std::make_shared<ColumnNode>(*result_column, from_node);
    }

    if (identifier.getPartsSize() == 2)
        return {};

    if (path_start == database_name && identifier[1] == table_name)
    {
        auto result_column = resolve_identifier_from_table_or_throw(2 /*drop_first_parts_size*/);
        return std::make_shared<ColumnNode>(*result_column, from_node);
    }

    return {};
}

/** Resolve identifier in current scope.
  * 1. Try resolve identifier from expression arguments.
  * If prefer_column_name_to_alias = true.
  * 2. Try to resolve identifier from tables.
  * 3. Try to resolve identifier from aliases.
  * Otherwise.
  * 2. Try to resolve identifier from aliases.
  * 3. Try to resolve identifier from tables.
  */
QueryTreeNodePtr QueryAnalyzer::tryResolveIdentifierInCurrentScope(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & current_scope)
{
    auto resolved_identifier = tryResolveIdentifierFromExpressionArguments(identifier_lookup, current_scope);
    if (resolved_identifier)
        return resolved_identifier;

    bool prefer_column_name_to_alias = context->getSettingsRef().prefer_column_name_to_alias;
    if (unlikely(prefer_column_name_to_alias))
    {
        resolved_identifier = tryResolveIdentifierFromTables(identifier_lookup, current_scope);
        if (resolved_identifier)
            return resolved_identifier;

        return tryResolveIdentifierFromAliases(identifier_lookup, current_scope);
    }

    resolved_identifier = tryResolveIdentifierFromAliases(identifier_lookup, current_scope);
    if (resolved_identifier)
        return resolved_identifier;

    return tryResolveIdentifierFromTables(identifier_lookup, current_scope);
}

/** Try resolve identifier in current scope parent scopes.
  * If initial scope is query. Then return nullptr.
  * TODO: CTE, constants can be used from parent query with statement.
  * TODO: If column is matched, throw exception that nested subqueries are not supported.
  *
  * If iniital scope is expression. Then try to resolve identifier in parent scopes until query scope is hit.
  * For query scope resolve strategy is same as if initial scope if query.
  */
QueryTreeNodePtr QueryAnalyzer::tryResolveIdentifierInParentScopes(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope)
{
    if (!scope.allow_to_check_parent_scopes)
        return {};

    bool initial_scope_is_query = scope.scope_node->getNodeType() == QueryTreeNodeType::QUERY;
    bool initial_scope_is_expression = !initial_scope_is_query;

    if (initial_scope_is_expression)
    {
        IdentifierResolveScope * scope_to_check = scope.parent_scope;
        while (scope_to_check != nullptr)
        {
            auto resolved_identifier = tryResolveIdentifier(identifier_lookup, *scope_to_check);
            if (resolved_identifier)
                return resolved_identifier;

            if (scope_to_check->scope_node->getNodeType() == QueryTreeNodeType::QUERY)
                break;

            scope_to_check = scope_to_check->parent_scope;
        }
    }

    return {};
}

/** Resolve identifier in scope.
  * Steps:
  * 1. Register identifier lookup in scope identifier_lookup_to_resolve_status table.
  * If entry is already registered and is not resolved, that means that we have cyclic aliases for identifier.
  * Example: SELECT a AS b, b AS a;
  * 2. Try resolve identifier in current scope.
  * 3. If identifier is not resolved in current scope, try to resolve it in parent scopes.
  * 4. If identifier was not resolved remove it from identifier_lookup_to_resolve_status table.
  * It is okay for identifier to be not resolved, in case we want first try to lookup identifier in one context,
  * then if there is no identifier in this context, try to lookup in another context.
  * Example: Try to lookup identifier as function, if it is not found lookup as expression.
  * Example: Try to lookup identifier as expression, if it is not found lookup as table.
  */
QueryTreeNodePtr QueryAnalyzer::tryResolveIdentifier(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope)
{
    auto it = scope.identifier_lookup_to_node.find(identifier_lookup);
    if (it != scope.identifier_lookup_to_node.end())
    {
        if (!it->second)
            throw Exception(ErrorCodes::CYCLIC_ALIASES,
                "Cyclic aliases for identifier {}. In scope {}",
                identifier_lookup.identifier.getFullName(),
                scope.scope_node->formatASTForErrorMessage());

        if (scope.non_cached_identifier_lookups_during_expression_resolve.count(identifier_lookup) == 0)
            return it->second;
    }

    auto [insert_it, _] = scope.identifier_lookup_to_node.insert({identifier_lookup, QueryTreeNodePtr()});
    it = insert_it;

    QueryTreeNodePtr resolved_identifier = tryResolveIdentifierInCurrentScope(identifier_lookup, scope);

    if (!resolved_identifier)
        resolved_identifier = tryResolveIdentifierInParentScopes(identifier_lookup, scope);

    it->second = resolved_identifier;

    if (!resolved_identifier || scope.non_cached_identifier_lookups_during_expression_resolve.count(identifier_lookup) > 0)
        scope.identifier_lookup_to_node.erase(it);

    return resolved_identifier;
}

/// Resolve query tree nodes functions implementation

/** Resolve query tree matcher. Check MatcherNode.h for detailed matcher description. Check ColumnTransformers.h for detailed transformers description.
  *
  * 1. Populate matcher_expression_nodes.
  *
  * If we resolve qualified matcher, first try to match qualified identifier to expression. If qualified identifier matched expression node then
  * if expression is compound match it column names using matcher `isMatchingColumn` method, if expression is not compound, throw exception.
  * If qualified identifier did not match expression in query tree, try to lookup qualified identifier in table context.
  *
  * If we resolve non qualified matcher, use current scope join tree node.
  *
  * 2. Apply column transformers to matched expression nodes. For strict column transformers save used column names.
  * 3. Validate strict column transformers.
  */
QueryTreeNodePtr QueryAnalyzer::resolveMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope)
{
    auto & matcher_node_typed = matcher_node->as<MatcherNode &>();

    std::vector<QueryTreeNodePtr> matcher_expression_nodes;

    if (matcher_node_typed.isQualified())
    {
        auto expression_query_tree_node = tryResolveIdentifier({matcher_node_typed.getQualifiedIdentifier(), IdentifierLookupContext::EXPRESSION}, scope);
        if (expression_query_tree_node)
        {
            auto result_type = expression_query_tree_node->getResultType();

            while (const auto * array_type = typeid_cast<const DataTypeArray *>(result_type.get()))
                result_type = array_type->getNestedType();

            const auto * tuple_data_type = typeid_cast<const DataTypeTuple *>(result_type.get());
            if (!tuple_data_type)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Qualified matcher {} find non compound expression {} with type {}. Expected tuple or array of tuples. In scope {}",
                    matcher_node->formatASTForErrorMessage(),
                    expression_query_tree_node->formatASTForErrorMessage(),
                    expression_query_tree_node->getResultType()->getName(),
                    scope.scope_node->formatASTForErrorMessage());

            const auto & element_names = tuple_data_type->getElementNames();

            for (const auto & element_name : element_names)
            {
                if (!matcher_node_typed.isMatchingColumn(element_name))
                    continue;

                auto tuple_element_function = std::make_shared<FunctionNode>("tupleElement");
                tuple_element_function->getArguments().getNodes().push_back(expression_query_tree_node);
                tuple_element_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(element_name));

                QueryTreeNodePtr function_query_node = tuple_element_function;
                resolveFunction(function_query_node, scope);
                matcher_expression_nodes.push_back(std::move(function_query_node));
            }
        }
        else
        {
            auto table_query_tree_node = tryResolveIdentifier({matcher_node_typed.getQualifiedIdentifier(), IdentifierLookupContext::TABLE}, scope);

            if (!table_query_tree_node || table_query_tree_node->getNodeType() != QueryTreeNodeType::TABLE)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Qualified matcher {} does not find table. In scope {}",
                    matcher_node->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            const auto & table_node = table_query_tree_node->as<TableNode &>();
            auto initial_matcher_columns = table_node.getStorageSnapshot()->getColumns(GetColumnsOptions(GetColumnsOptions::All));

            for (auto & column : initial_matcher_columns)
            {
                const auto & column_name = column.name;
                if (matcher_node_typed.isMatchingColumn(column_name))
                    matcher_expression_nodes.push_back(std::make_shared<ColumnNode>(column, table_query_tree_node));
            }
        }
    }
    else
    {
        /** There can be edge case if matcher is inside lambda expression.
          * Try to find parent query expression using parent scopes.
          */
        IQueryTreeNode * scope_node = scope.scope_node.get();
        auto * scope_query_node = scope_node->as<QueryNode>();

        while (!scope_query_node)
        {
            if (!scope.parent_scope)
                break;

            scope_node = scope.parent_scope->scope_node.get();
            scope_query_node = scope_node->as<QueryNode>();
        }

        /// If there are no parent scope that has tables or query scope does not have FROM section
        if (!scope_query_node || !scope_query_node->getFrom())
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Unqualified matcher {} cannot be resolved. There are no table sources. In scope {}",
                matcher_node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }

        const auto & table_node = scope_query_node->getFrom()->as<const TableNode &>();

        UInt8 get_column_options_kind = 0;

        if (matcher_node_typed.isAsteriskMatcher())
        {
            get_column_options_kind = GetColumnsOptions::Ordinary;

            if (context->getSettingsRef().asterisk_include_alias_columns)
                get_column_options_kind |= GetColumnsOptions::Kind::Aliases;

            if (context->getSettingsRef().asterisk_include_materialized_columns)
                get_column_options_kind |= GetColumnsOptions::Kind::Materialized;
        }
        else
        {
            /// TODO: Check if COLUMNS select aliases column by default
            get_column_options_kind = GetColumnsOptions::All;
        }

        auto get_columns_options = GetColumnsOptions(static_cast<GetColumnsOptions::Kind>(get_column_options_kind));
        auto initial_matcher_columns = table_node.getStorageSnapshot()->getColumns(get_columns_options);

        for (auto & column : initial_matcher_columns)
        {
            const auto & column_name = column.name;
            if (matcher_node_typed.isMatchingColumn(column_name))
                matcher_expression_nodes.push_back(std::make_shared<ColumnNode>(column, scope_query_node->getFrom()));
        }
    }

    std::unordered_map<const IColumnTransformerNode *, std::unordered_set<std::string>> strict_transformer_to_used_column_names;
    auto add_strict_transformer_column_name = [&](const IColumnTransformerNode * transformer, const std::string & column_name)
    {
        auto it = strict_transformer_to_used_column_names.find(transformer);
        if (it == strict_transformer_to_used_column_names.end())
        {
            auto [inserted_it, _] = strict_transformer_to_used_column_names.emplace(transformer, std::unordered_set<std::string>());
            it = inserted_it;
        }

        it->second.insert(column_name);
    };

    ListNodePtr list = std::make_shared<ListNode>();

    for (auto & node : matcher_expression_nodes)
    {
        for (const auto & transformer : matcher_node_typed.getColumnTransformers().getNodes())
        {
            if (auto * apply_transformer = transformer->as<ApplyColumnTransformerNode>())
            {
                const auto & expression_node = apply_transformer->getExpressionNode();

                if (apply_transformer->getApplyTransformerType() == ApplyColumnTransformerType::LAMBDA)
                {
                    auto lambda_expression_to_resolve = expression_node->clone();
                    IdentifierResolveScope lambda_scope(expression_node, &scope /*parent_scope*/);
                    resolveLambda(lambda_expression_to_resolve, {node}, lambda_scope);
                    auto & lambda_expression_to_resolve_typed = lambda_expression_to_resolve->as<LambdaNode &>();
                    node = lambda_expression_to_resolve_typed.getExpression();
                }
                else if (apply_transformer->getApplyTransformerType() == ApplyColumnTransformerType::FUNCTION)
                {
                    auto function_to_resolve_untyped = expression_node->clone();
                    auto & function_to_resolve_typed = function_to_resolve_untyped->as<FunctionNode &>();
                    function_to_resolve_typed.getArguments().getNodes().push_back(node);
                    resolveFunction(function_to_resolve_untyped, scope);
                    node = function_to_resolve_untyped;
                }
                else
                {
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                        "Unsupported apply matcher expression type. Expected lambda or function apply transformer. Actual {}. In scope {}",
                        transformer->formatASTForErrorMessage(),
                        scope.scope_node->formatASTForErrorMessage());
                }
            }
            else if (auto * except_transformer = transformer->as<ExceptColumnTransformerNode>())
            {
                auto node_name = node->getName();

                if (except_transformer->isColumnMatching(node_name))
                {
                    if (except_transformer->isStrict())
                        add_strict_transformer_column_name(except_transformer, node_name);

                    node = {};
                    break;
                }
            }
            else if (auto * replace_transformer = transformer->as<ReplaceColumnTransformerNode>())
            {
                auto node_name = node->getName();
                auto replace_expression = replace_transformer->findReplacementExpression(node_name);
                if (!replace_expression)
                    continue;

                if (replace_transformer->isStrict())
                    add_strict_transformer_column_name(replace_transformer, node_name);

                node = replace_expression;
                resolveExpressionNode(node, scope, false /*allow_lambda_expression*/);
            }
        }

        if (node)
            list->getNodes().push_back(node);
    }

    for (auto & [strict_transformer, used_column_names] : strict_transformer_to_used_column_names)
    {
        auto strict_transformer_type = strict_transformer->getTransformerType();
        const Names * strict_transformer_column_names = nullptr;

        switch (strict_transformer_type)
        {
            case ColumnTransfomerType::EXCEPT:
            {
                const auto * except_transformer = static_cast<const ExceptColumnTransformerNode *>(strict_transformer);
                const auto & except_names = except_transformer->getExceptColumnNames();

                if (except_names.size() != used_column_names.size())
                    strict_transformer_column_names = &except_transformer->getExceptColumnNames();

                break;
            }
            case ColumnTransfomerType::REPLACE:
            {
                const auto * replace_transformer = static_cast<const ReplaceColumnTransformerNode *>(strict_transformer);
                const auto & replacement_names = replace_transformer->getReplacementsNames();

                if (replacement_names.size() != used_column_names.size())
                    strict_transformer_column_names = &replace_transformer->getReplacementsNames();

                break;
            }
            default:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expected strict EXCEPT or REPLACE column transformer. Actual type {}. In scope {}",
                    toString(strict_transformer_type),
                    scope.scope_node->formatASTForErrorMessage());
            }
        }

        if (!strict_transformer_column_names)
            continue;

        Names non_matched_column_names;
        size_t strict_transformer_column_names_size = strict_transformer_column_names->size();
        for (size_t i = 0; i < strict_transformer_column_names_size; ++i)
        {
            const auto & column_name = (*strict_transformer_column_names)[i];
            if (used_column_names.find(column_name) == used_column_names.end())
                non_matched_column_names.push_back(column_name);
        }

        WriteBufferFromOwnString non_matched_column_names_buffer;
        size_t non_matched_column_names_size = non_matched_column_names.size();
        for (size_t i = 0; i < non_matched_column_names_size; ++i)
        {
            const auto & column_name = non_matched_column_names[i];

            non_matched_column_names_buffer << column_name;
            if (i + 1 != non_matched_column_names_size)
                non_matched_column_names_buffer << ", ";
        }

        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Strict {} column transformer {} expects following column(s) {}",
            toString(strict_transformer_type),
            strict_transformer->formatASTForErrorMessage(),
            non_matched_column_names_buffer.str());
    }

    return list;
}


/** Resolve lambda function.
  * This function modified lambda_node during resolve. It is caller responsibility to clone lambda before resolve
  * if it is needed for later use.
  *
  * lambda_node - node that must have LambdaNode type.
  * arguments - lambda arguments.
  * scope - lambda scope. It is client responsibility to create it.
  *
  * Resolve steps:
  * 1. Valide arguments.
  * 2. Register lambda in lambdas in resolve process. This is necessary to prevent recursive lambda resolving.
  * 3. Initialize scope with lambda aliases.
  * 4. Validate lambda argument names, and scope expressions.
  * 5. Resolve lambda body expression.
  * 6. Deregister lambda from lambdas in resolve process.
  */
void QueryAnalyzer::resolveLambda(QueryTreeNodePtr & lambda_node, const QueryTreeNodes & lambda_arguments, IdentifierResolveScope & scope)
{
    auto & lambda = lambda_node->as<LambdaNode &>();
    auto & lambda_arguments_nodes = lambda.getArguments().getNodes();
    size_t lambda_argument_nodes_size = lambda_arguments_nodes.size();

    /** Register lambda as being resolved, to prevent recursive lambdas resolution.
      * Example: WITH (x -> x + lambda_2(x)) AS lambda_1, (x -> x + lambda_1(x)) AS lambda_2 SELECT 1;
      */
    auto it = lambdas_in_resolve_process.find(lambda_node.get());
    if (it != lambdas_in_resolve_process.end())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Recursive lambda {}. In scope {}",
            lambda.formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());

    size_t arguments_size = lambda_arguments.size();
    if (lambda_argument_nodes_size != arguments_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Lambda {} expect {} arguments. Actual {}. In scope {}",
            lambda.formatASTForErrorMessage(),
            arguments_size,
            lambda_argument_nodes_size,
            scope.scope_node->formatASTForErrorMessage());

    /// Initialize aliases in lambda scope
    ScopeAliasVisitorMatcher::Data data{scope};
    ScopeAliasVisitorMatcher::Visitor visitor(data);
    visitor.visit(lambda_node);

    /** Replace lambda arguments with new arguments.
      * Additionally validate that there are no aliases with same name as lambda arguments.
      * Arguments are registered in current scope expression_argument_name_to_node map.
      */
    auto lambda_new_arguments = std::make_shared<ListNode>();
    lambda_new_arguments->getNodes().reserve(lambda_argument_nodes_size);

    for (size_t i = 0; i < lambda_argument_nodes_size; ++i)
    {
        auto & lambda_argument_node = lambda_arguments_nodes[i];
        auto & lambda_argument_node_typed = lambda_argument_node->as<IdentifierNode &>();
        const auto & lambda_argument_name = lambda_argument_node_typed.getIdentifier().getFullName();

        bool has_expression_node = data.scope.alias_name_to_expression_node.count(lambda_argument_name) > 0;
        bool has_alias_node = data.scope.alias_name_to_lambda_node.count(lambda_argument_name) > 0;

        if (has_expression_node || has_alias_node)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Alias name {} inside lambda cannot have same name as lambda argument. In scope {}",
                lambda_argument_name,
                lambda_argument_node_typed.formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }

        scope.expression_argument_name_to_node.emplace(lambda_argument_name, lambda_arguments[i]);
        lambda_new_arguments->getNodes().push_back(lambda_arguments[i]);
    }

    lambda.getArgumentsNode() = std::move(lambda_new_arguments);

    /** Lambda body expression is resolved as standard query expression node.
      * After that lambda is resolved, because its expression node is resolved.
      */
    resolveExpressionNode(lambda.getExpression(), scope, false /*allow_lambda_expression*/);

    /** TODO: Lambda body can be resolved in expression list. And for standalone lambdas it will work.
      * TODO: It can potentially be resolved into table or another lambda.
      * Example: WITH (x -> untuple(x)) AS lambda SELECT untuple(compound_expression).
      */
    // if (lambda.getExpression()->getNodeType() == QueryTreeNodeType::LIST)
    //     throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
    //         "Lambda {} expression body cannot contain list of expressions. In scope {}",
    //         lambda_node->formatASTForErrorMessage(),
    //         scope.scope_node->formatASTForErrorMessage());

    lambdas_in_resolve_process.erase(lambda_node.get());
}

/** Resolve function node in scope.
  * During function node resolve, function node can be replaced with another expression (if it match lambda or sql user defined function),
  * with constant (if it allow constant folding), or with expression list. It is caller responsibility to handle such cases appropriately.
  *
  * Steps:
  * 1. Resolve function parameters. Validate that each function parameter must be constant node.
  * 2. Resolve function arguments list, lambda expressions are allowed as function arguments.
  * 3. Initialize argument_columns, argument_types, function_lambda_arguments_indexes arrays from function arguments.
  * 4. Try to resolve function name as identifier as function.
  * 5. If function name identifier was not resolved as function, try to lookup lambda from sql user defined functions factory.
  * 6. If function was resolve as lambda from step 4, or 5, then resolve lambda using function arguments and replace function node with lambda result.
  * After than function node is resolved.
  * 7. If function was not resolved during step 6 as lambda, then try to resolve function as executable user defined function or aggregate function or
  * non aggregate function.
  *
  * Special case is `untuple` function that takes single compound argument expression. If argument is not compound expression throw exception.
  * Wrap compound expression subcolumns into `tupleElement` and replace function node with them. After that `untuple` function node is resolved.
  *
  * If function is resolved as executable user defined function or aggregate function, function node is resolved
  * no additional special handling is required.
  *
  * 8. If function was resolved as non aggregate function. Then if on step 3 there were lambdas, their result types need to be initialized and
  * they must be resolved.
  * 9. If function is suitable for constant folding, try to replace function node with constant result.
  *
  * TODO: Special `in` function.
  * TODO: Special `grouping` function.
  * TODO: Window functions.
  */
void QueryAnalyzer::resolveFunction(QueryTreeNodePtr & node, IdentifierResolveScope & scope)
{
    FunctionNode & function_node = node->as<FunctionNode &>();
    if (function_node.isResolved())
        return;

    /// Resolve function parameters

    resolveExpressionNodeList(function_node.getParametersNode(), scope, false /*allow_lambda_expression*/);

    /// Convert function parameters into constant parameters array

    Array parameters;

    auto & parameters_nodes = function_node.getParameters().getNodes();
    parameters.reserve(parameters_nodes.size());

    for (auto & parameter : parameters_nodes)
    {
        auto * constant_parameter = parameter->as<ConstantNode>();
        if (constant_parameter)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Parameter for function {} expected to be constant expression. Actual {}. In scope {}",
            function_node.getFunctionName(),
            parameter->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());

        parameters.push_back(constant_parameter);
    }

    /// Resolve function arguments

    resolveExpressionNodeList(function_node.getArgumentsNode(), scope, true /*allow_lambda_expression*/);

    /// Initialize function argument columns

    ColumnsWithTypeAndName argument_columns;
    DataTypes argument_types;
    bool all_arguments_constants = true;
    std::vector<size_t> function_lambda_arguments_indexes;

    auto & function_arguments = function_node.getArguments().getNodes();
    size_t function_arguments_size = function_arguments.size();

    for (size_t function_argument_index = 0; function_argument_index < function_arguments_size; ++function_argument_index)
    {
        auto & function_argument = function_arguments[function_argument_index];

        ColumnWithTypeAndName argument_column;
        argument_column.name = function_argument->getName();

        /** If function argument is lambda, save lambda argument index and initialize argument type as DataTypeFunction
          * where function argument types are initialized with empty array of lambda arguments size.
          */
        if (auto * lambda_query_tree_node = function_argument->as<LambdaNode>())
        {
            size_t lambda_arguments_size = lambda_query_tree_node->getArguments().getNodes().size();
            argument_column.type = std::make_shared<DataTypeFunction>(DataTypes(lambda_arguments_size, nullptr), nullptr);
            function_lambda_arguments_indexes.push_back(function_argument_index);
        }
        else
        {
            argument_column.type = function_argument->getResultType();
        }

        if (!argument_column.type)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Function {} argument is not resolved. In scope {}",
                function_node.getFunctionName(),
                scope.scope_node->formatASTForErrorMessage());

        if (auto * constant_query_tree_node = function_argument->as<ConstantNode>())
            argument_column.column = argument_column.type->createColumnConst(1, constant_query_tree_node->getConstantValue());
        else
            all_arguments_constants = false;

        argument_types.push_back(argument_column.type);
        argument_columns.emplace_back(std::move(argument_column));
    }

    /** Lookup function node name as lambda identifier.
      * If no lambda node exists with function node name identifier, try to resolve it as lambda from sql user defined functions.
      */
    auto lambda_expression_untyped = tryResolveIdentifier({Identifier{function_node.getFunctionName()}, IdentifierLookupContext::FUNCTION}, scope);
    // if (!lambda_expression_untyped)
        // lambda_expression_untyped = tryGetLambdaFromSQLUserDefinedFunction(function_node.getFunctionName());

    /** If function is resolved as lambda.
      * Clone lambda before resolve.
      * Initialize lambda arguments as function arguments
      * Resolve lambda and then replace function node with resolved lambda expression body.
      * Example: WITH (x -> x + 1) AS lambda SELECT lambda(value) FROM test_table;
      * Result: SELECT value + 1 FROM test_table;
      */
    if (lambda_expression_untyped)
    {
        auto * lambda_expression = lambda_expression_untyped->as<LambdaNode>();
        if (!lambda_expression)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Function identifier {} must be resolved as lambda. Actual {}. In scope {}",
                function_node.getFunctionName(),
                lambda_expression_untyped->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());

        auto lambda_expression_clone = lambda_expression_untyped->clone();

        IdentifierResolveScope lambda_scope(lambda_expression_clone, &scope /*parent_scope*/);
        resolveLambda(lambda_expression_clone, function_arguments, lambda_scope);

        auto & resolved_lambda = lambda_expression_clone->as<LambdaNode &>();
        node = resolved_lambda.getExpression();
        return;
    }

    const auto & function_name = function_node.getFunctionName();

    /// Special handling of `untuple` function

    if (function_name == "untuple")
    {
        if (function_arguments.size() != 1)
             throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Function 'untuple' must have 1 argument. In scope {}",
                scope.scope_node->formatASTForErrorMessage());

        const auto & tuple_argument = function_arguments[0];
        auto result_type = tuple_argument->getResultType();
        const auto * tuple_data_type = typeid_cast<const DataTypeTuple *>(result_type.get());
        if (!tuple_data_type)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Function untuple argument must be have compound type. Actual type {}. In scope {}",
                result_type->getName(),
                scope.scope_node->formatASTForErrorMessage());

        const auto & element_names = tuple_data_type->getElementNames();

        auto result_list = std::make_shared<ListNode>();
        result_list->getNodes().reserve(element_names.size());

        for (const auto & element_name : element_names)
        {
            auto tuple_element_function = std::make_shared<FunctionNode>("tupleElement");
            tuple_element_function->getArguments().getNodes().push_back(tuple_argument);
            tuple_element_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(element_name));

            QueryTreeNodePtr function_query_node = tuple_element_function;
            resolveFunction(function_query_node, scope);
            result_list->getNodes().push_back(std::move(function_query_node));
        }

        node = result_list;
        return;
    }

    /** Try to resolve function as
      * 1. Executable user defined function.
      * 2. Aggregate function.
      * 3. Non aggregate function.
      * TODO: Provide better hints.
      */
    FunctionOverloadResolverPtr function = UserDefinedExecutableFunctionFactory::instance().tryGet(function_name, context, parameters);

    if (!function)
        function = FunctionFactory::instance().tryGet(function_name, context);

    if (!function)
    {
        if (!AggregateFunctionFactory::instance().isAggregateFunctionName(function_name))
           throw Exception(ErrorCodes::BAD_ARGUMENTS,
               "Function with name {} does not exists. In scope {}",
               function_name,
               scope.scope_node->formatASTForErrorMessage());

        AggregateFunctionProperties properties;
        auto aggregate_function = AggregateFunctionFactory::instance().get(function_name, argument_types, parameters, properties);
        function_node.resolveAsAggregateFunction(aggregate_function, aggregate_function->getReturnType());
        return;
    }

    /** For lambda arguments we need to initialize lambda argument types DataTypeFunction using `getLambdaArgumentTypes` function.
      * Then each lambda arguments are initalized with columns, where column source is lambda.
      * This information is important for later steps of query processing.
      * Example: SELECT arrayMap(x -> x + 1, [1, 2, 3]).
      * lambda node x -> x + 1 identifier x is resolved as column where source is lambda node.
      */
    bool has_lambda_arguments = !function_lambda_arguments_indexes.empty();
    if (has_lambda_arguments)
    {
        function->getLambdaArgumentTypes(argument_types);

        for (auto & function_lambda_argument_index : function_lambda_arguments_indexes)
        {
            auto lambda_to_resolve = function_arguments[function_lambda_argument_index]->clone();
            auto & lambda_to_resolve_typed = lambda_to_resolve->as<LambdaNode &>();

            const auto & lambda_argument_names = lambda_to_resolve_typed.getArgumentNames();
            size_t lambda_arguments_size = lambda_to_resolve_typed.getArguments().getNodes().size();

            const auto * function_data_type = typeid_cast<const DataTypeFunction *>(argument_types[function_lambda_argument_index].get());
            if (!function_data_type)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Function {} expected function data type for lambda argument with index {}. Actual {}. In scope {}",
                    function_name,
                    function_lambda_argument_index,
                    argument_types[function_lambda_argument_index]->getName(),
                    scope.scope_node->formatASTForErrorMessage());

            const auto & function_data_type_argument_types = function_data_type->getArgumentTypes();
            size_t function_data_type_arguments_size = function_data_type_argument_types.size();
            if (function_data_type_arguments_size != lambda_arguments_size)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Function {} function data type for lambda argument wiht index {} arguments size mismatch. Actual {}. Expected {}. In scope {}",
                    function_name,
                    function_data_type_arguments_size,
                    lambda_arguments_size,
                    argument_types[function_lambda_argument_index]->getName(),
                    scope.scope_node->formatASTForErrorMessage());

            QueryTreeNodes lambda_arguments;
            lambda_arguments.reserve(lambda_arguments_size);

            for (size_t i = 0; i < lambda_arguments_size; ++i)
            {
                const auto & argument_type = function_data_type_argument_types[i];
                auto column_name_and_type = NameAndTypePair{lambda_argument_names[i], argument_type};
                lambda_arguments.push_back(std::make_shared<ColumnNode>(std::move(column_name_and_type), lambda_to_resolve));
            }

            IdentifierResolveScope lambda_scope(lambda_to_resolve, &scope /*parent_scope*/);
            resolveLambda(lambda_to_resolve, lambda_arguments, lambda_scope);

            argument_types[function_lambda_argument_index] = std::make_shared<DataTypeFunction>(function_data_type_argument_types, lambda_to_resolve->getResultType());
            argument_columns[function_lambda_argument_index].type = argument_types[function_lambda_argument_index];
            function_arguments[function_lambda_argument_index] = std::move(lambda_to_resolve);
        }
    }

    DataTypePtr result_type;

    try
    {
        auto function_base = function->build(argument_columns);
        result_type = function_base->getResultType();

        /** If function is suitable for constant folding try to convert it to constant.
          * Example: SELECT plus(1, 1);
          * Result: SELECT 2;
          */
        if (function_base->isSuitableForConstantFolding())
        {
            auto executable_function = function_base->prepare(argument_columns);

            ColumnPtr column;

            if (all_arguments_constants)
            {
                size_t num_rows = function_arguments.empty() ? 0 : argument_columns.front().column->size();
                column = executable_function->execute(argument_columns, result_type, num_rows, true);
            }
            else
            {
                column = function_base->getConstantResultForNonConstArguments(argument_columns, result_type);
            }

            if (column && isColumnConst(*column))
            {
                /// Replace function node with result constant node
                Field constant_value;
                column->get(0, constant_value);

                auto original_ast = function_node.getOriginalAST();
                node = std::make_shared<ConstantNode>(std::move(constant_value), result_type);
                node->setOriginalAST(std::move(original_ast));
                return;
            }
        }
    }
    catch (Exception & e)
    {
        e.addMessage("In scope {}", scope.scope_node->formatASTForErrorMessage());
        throw;
    }

    function_node.resolveAsFunction(std::move(function), std::move(result_type));
}

/** Resolve expression node.
  * Argument node can be replaced with different node, or even with list node in case of mather resolution.
  * Example: SELECT * FROM test_table;
  * * - is matcher node, and it can be resolved into ListNode.
  *
  * Steps:
  * 1. If node has alias, replace node with its value in scope alias map. Register alias in expression_aliases_in_resolve_process, to prevent resolving identifier
  * which can bind to expression alias name. Check tryResolveIdentifierFromAliases documentation for additional explanation.
  * Example:
  * SELECT id AS id FROM test_table;
  * SELECT value.value1 AS value FROM test_table;
  *
  * 2. Call specific resolve method depending on node type.
  * TODO: Need a way to prevent scalar subquery evaluation, for special functions like `in`.
  * 3. Special case identifier node. Try first resolve it as expression identifier. Then if allow_lambda_expression = true
  * try to resolve it as function. TODO: Add tables.
  * 4. If node has alias, update its value in scope alias map. Deregister alias from expression_aliases_in_resolve_process.
  */
void QueryAnalyzer::resolveExpressionNode(QueryTreeNodePtr & node, IdentifierResolveScope & scope, bool allow_lambda_expression)
{
    String node_alias = node->getAlias();

    /** Do not use alias table if node has alias same as some other node.
      * Example: WITH x -> x + 1 AS lambda SELECT 1 AS lambda;
      * During 1 AS lambda resolve if we use alias table we replace node with x -> x + 1 AS lambda.
      */
    bool use_alias_table = !scope.nodes_with_duplicated_aliases.contains(node);

    if (!node_alias.empty() && use_alias_table)
    {
        /** Node could be potentially resolved by resolving other nodes.
          * SELECT b, a as b FROM test_table;
          *
          * To resolve b we need to resolve a.
          */
        auto it = scope.alias_name_to_expression_node.find(node_alias);
        if (it != scope.alias_name_to_expression_node.end())
            node = it->second;

        if (allow_lambda_expression)
        {
            it = scope.alias_name_to_lambda_node.find(node_alias);
            if (it != scope.alias_name_to_lambda_node.end())
                node = it->second;
        }
    }

    scope.expressions_in_resolve_process_stack.pushNode(node);

    if (auto * identifier_node = node->as<IdentifierNode>())
    {
        auto unresolved_identifier = identifier_node->getIdentifier();
        node = tryResolveIdentifier({unresolved_identifier, IdentifierLookupContext::EXPRESSION}, scope);

        if (node && !node_alias.empty())
            scope.alias_name_to_lambda_node.erase(node_alias);

        if (!node && allow_lambda_expression)
        {
            node = tryResolveIdentifier({unresolved_identifier, IdentifierLookupContext::FUNCTION}, scope);

            if (node && !node_alias.empty())
                scope.alias_name_to_expression_node.erase(node_alias);
        }

        if (!node)
        {
            std::string lambda_message_clarification;
            if (allow_lambda_expression)
                lambda_message_clarification = std::string(" or ") + toStringLowercase(IdentifierLookupContext::FUNCTION);

            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
               "Unknown {}{} identifier {} in scope {}",
               toStringLowercase(IdentifierLookupContext::EXPRESSION),
               lambda_message_clarification,
               unresolved_identifier.getFullName(),
               scope.scope_node->formatASTForErrorMessage());
        }
    }
    else if (auto * function_node = node->as<FunctionNode>())
    {
        resolveFunction(node, scope);
    }
    else if (auto * constant_function_node = node->as<ConstantNode>())
    {
        /// Already resolved
    }
    else if (auto * column_function_node = node->as<ColumnNode>())
    {
        /// Already resolved
    }
    else if (auto * lambda_node = node->as<LambdaNode>())
    {
        if (!allow_lambda_expression)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Lambda is not allowed {} in expression context. In scope {}",
                lambda_node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());

        /// Must be resolved by caller
    }
    else if (auto * asterisk_node = node->as<MatcherNode>())
    {
        node = resolveMatcher(node, scope);
    }
    else if (auto * query_node = node->as<QueryNode>())
    {
        IdentifierResolveScope subquery_scope(node, &scope /*parent_scope*/);
        resolveQuery(node, subquery_scope);
        evaluateScalarSubquery(node);
    }
    else if (auto * list_node = node->as<ListNode>())
    {
        /** Edge case if list expression has alias.
          * Matchers cannot have aliases, but `untuple` function can.
          * Example: SELECT a, untuple(CAST(('hello', 1) AS Tuple(name String, count UInt32))) AS a;
          * During resolveFunction `untuple` function is replaced by list of 2 constants 'hello', 1.
          */
        resolveExpressionNodeList(node, scope, allow_lambda_expression);
    }
    else
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Node with type {} is not supported {}. In scope {}",
            node->getNodeTypeName(),
            node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());
    }

    /** Update aliases after expression node was resolved.
      * Do not update node in alias table if we resolve it for duplicate alias.
      */
    if (!node_alias.empty() && use_alias_table)
    {
        auto it = scope.alias_name_to_expression_node.find(node_alias);
        if (it != scope.alias_name_to_expression_node.end())
            it->second = node;

        if (allow_lambda_expression)
        {
            it = scope.alias_name_to_lambda_node.find(node_alias);
            if (it != scope.alias_name_to_lambda_node.end())
                it->second = node;
        }
    }

    scope.expressions_in_resolve_process_stack.popNode();
    bool expression_was_root = scope.expressions_in_resolve_process_stack.empty();
    if (expression_was_root)
        scope.non_cached_identifier_lookups_during_expression_resolve.clear();
}

/** Resolve expression node list.
  * If expression is resolved in list, it is flattened into initial node list.
  *
  * Such examples must work:
  * Example: CREATE TABLE test_table (id UInt64, value UInt64) ENGINE=TinyLog; SELECT plus(*) FROM test_table;
  * Example: SELECT *** FROM system.one;
  */
void QueryAnalyzer::resolveExpressionNodeList(QueryTreeNodePtr & node_list, IdentifierResolveScope & scope, bool allow_lambda_expression)
{
    auto & initial_node_list = node_list->as<ListNode &>();
    size_t initial_node_list_size = initial_node_list.getNodes().size();

    auto result_node_list = std::make_shared<ListNode>();
    result_node_list->getNodes().reserve(initial_node_list_size);

    for (auto & node : initial_node_list.getNodes())
    {
        resolveExpressionNode(node, scope, allow_lambda_expression);

        if (auto * expression_list = node->as<ListNode>())
        {
            for (auto & expression_list_node : expression_list->getNodes())
                result_node_list->getNodes().push_back(std::move(expression_list_node));
        }
        else
        {
            result_node_list->getNodes().push_back(std::move(node));
        }
    }

    node_list = std::move(result_node_list);
}

/** Resolve query.
  * This function modifies query node during resolve. It is caller responsibility to clone query node before resolve
  * if it is needed for later use.
  *
  * lambda_node - query_tree_node that must have QueryNode type.
  * scope - query scope. It is caller responsibility to create it.
  *
  * Resolve steps:
  * 1. Initialize query scope with aliases.
  * 2. Resolve expressions in query parts.
  * 3. Remove WITH section from query.
  */
void QueryAnalyzer::resolveQuery(QueryTreeNodePtr & query_tree_node, IdentifierResolveScope & scope)
{
    auto & query_tree = query_tree_node->as<QueryNode &>();

    /// Initialize aliases in query node scope

    ScopeAliasVisitorMatcher::Data data{scope};
    ScopeAliasVisitorMatcher::Visitor visitor(data);

    if (query_tree.getWithNode())
        visitor.visit(query_tree.getWithNode());

    if (query_tree.getProjectionNode())
        visitor.visit(query_tree.getProjectionNode());

    if (query_tree.getPrewhere())
        visitor.visit(query_tree.getPrewhere());

    if (query_tree.getWhere())
        visitor.visit(query_tree.getWhere());

    auto from_node_alias = query_tree.getFrom()->getAlias();
    if (!from_node_alias.empty())
    {
        auto [_, inserted] = scope.alias_name_to_table_expression_node.insert(std::make_pair(from_node_alias, query_tree.getFrom()));
        if (!inserted)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Multiple aliases does not point to same entity {}", from_node_alias);
    }

    /// Resolve query node sections.

    if (query_tree.getWithNode())
        resolveExpressionNodeList(query_tree.getWithNode(), scope, true /*allow_lambda_expression*/);

    resolveExpressionNodeList(query_tree.getProjectionNode(), scope, false /*allow_lambda_expression*/);

    if (query_tree.getPrewhere())
        resolveExpressionNode(query_tree.getPrewhere(), scope, false /*allow_lambda_expression*/);

    if (query_tree.getWhere())
        resolveExpressionNode(query_tree.getWhere(), scope, false /*allow_lambda_expression*/);

    /** WITH section can be safely removed, because  WITH section only can provide aliases to expressions
      * and CTE for other sections to use.
      *
      * Example: WITH 1 AS constant, (x -> x + 1) AS lambda, a AS (SELECT * FROM test_table);
      */
    query_tree.getWithNode() = std::make_shared<ListNode>();

    /** Resolve nodes with duplicate aliases.
      *
      * Such nodes during scope aliases collection are placed into duplicated array.
      * After scope nodes are resolved, we can compare node with duplicate alias with
      * node from scope alias table.
      */
    for (const auto & node_with_duplicated_alias : scope.nodes_with_duplicated_aliases)
    {
        auto node = node_with_duplicated_alias;
        auto node_alias = node->getAlias();
        resolveExpressionNode(node, scope, true /*allow_lambda_expression*/);

        bool has_node_in_alias_table = false;

        auto it = scope.alias_name_to_expression_node.find(node_alias);
        if (it != scope.alias_name_to_expression_node.end())
        {
            has_node_in_alias_table = true;

            if (!it->second->isEqual(*node))
                throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
                    "Multiple expressions {} and {} for alias {}. In scope {}",
                    node->dumpTree(),
                    it->second->dumpTree(),
                    node_alias,
                    scope.scope_node->formatASTForErrorMessage());
        }

        it = scope.alias_name_to_lambda_node.find(node_alias);
        if (it != scope.alias_name_to_lambda_node.end())
        {
            has_node_in_alias_table = true;

            if (!it->second->isEqual(*node))
                throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
                    "Multiple expressions {} and {} for alias {}. In scope {}",
                    node->formatASTForErrorMessage(),
                    it->second->formatASTForErrorMessage(),
                    node_alias,
                    scope.scope_node->formatASTForErrorMessage());
        }

        if (!has_node_in_alias_table)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Node {} with duplicate alias {} does not exists in alias table. In scope {}",
                node->formatASTForErrorMessage(),
                node_alias,
                scope.scope_node->formatASTForErrorMessage());
    }
}

void QueryAnalysisPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    if (query_tree_node->getNodeType() != QueryTreeNodeType::QUERY)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "QueryAnalysis pass requires query node");

    QueryAnalyzer analyzer(std::move(context));
    analyzer.resolve(query_tree_node);
}

}
