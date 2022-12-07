#include <Analyzer/Passes/QueryAnalysisPass.h>

#include <Common/NamePrompter.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/getLeastSupertype.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>

#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/grouping.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <TableFunctions/TableFunctionFactory.h>

#include <Databases/IDatabase.h>

#include <Storages/IStorage.h>
#include <Storages/StorageSet.h>

#include <Interpreters/convertFieldToType.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/Context.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Set.h>
#include <Interpreters/misc.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

#include <Analyzer/Utils.h>
#include <Analyzer/SetUtils.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/MatcherNode.h>
#include <Analyzer/ColumnTransformers.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/InterpolateNode.h>
#include <Analyzer/WindowNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryTreeBuilder.h>

#include <Common/checkStackSize.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNKNOWN_FUNCTION;
    extern const int LOGICAL_ERROR;
    extern const int CYCLIC_ALIASES;
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
    extern const int BAD_ARGUMENTS;
    extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
    extern const int TYPE_MISMATCH;
    extern const int AMBIGUOUS_IDENTIFIER;
    extern const int INVALID_WITH_FILL_EXPRESSION;
    extern const int INVALID_LIMIT_EXPRESSION;
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int TOO_DEEP_SUBQUERIES;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int NOT_AN_AGGREGATE;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_FINAL;
    extern const int SAMPLING_NOT_SUPPORTED;
    extern const int NO_COMMON_TYPE;
    extern const int NOT_IMPLEMENTED;
    extern const int ALIAS_REQUIRED;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** Query analyzer implementation overview. Please check documentation in QueryAnalysisPass.h before.
  * And additional documentation for each method, where special cases are described in detail.
  *
  * Each node in query must be resolved. For each query tree node resolved state is specific.
  *
  * For constant node no resolve process exists, it is resolved during construction.
  *
  * For table node no resolve process exists, it is resolved during construction.
  *
  * For function node to be resolved parameters and arguments must be resolved, function node must be initialized with concrete aggregate or
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
  * 2. Try to resolve identifier in function context, if it is allowed. Example: SELECT func(arguments); Here func identifier cannot be resolved in function context
  * because query projection does not support that.
  * 3. Try to resolve identifier in table context, if it is allowed. Example: SELECT table; Here table identifier cannot be resolved in function context
  * because query projection does not support that.
  *
  * TODO: This does not supported properly before, because matchers could not be resolved from aliases.
  *
  * Identifiers are resolved with following resules:
  * Resolution starts with current scope.
  * 1. Try to resolve identifier from expression scope arguments. Lambda expression arguments are greatest priority.
  * 2. Try to resolve identifier from aliases.
  * 3. Try to resolve identifier from join tree if scope is query, or if there are registered table columns in scope.
  * Steps 2 and 3 can be changed using prefer_column_name_to_alias setting.
  * 4. If it is table lookup, try to resolve identifier from CTE.
  * If identifier could not be resolved in current scope, resolution must be continued in parent scopes.
  * 5. Try to resolve identifier from parent scopes.
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
  * If other parts of identifier cannot be resolved in that node, exception must be thrown.
  *
  * Example:
  * CREATE TABLE test_table (id UInt64, compound_value Tuple(value UInt64)) ENGINE=TinyLog;
  * SELECT compound_value.value, 1 AS compound_value FROM test_table;
  * Identifier first part compound_value bound to entity with alias compound_value, but nested identifier part cannot be resolved from entity,
  * lookup should not be continued, and exception must be thrown because if lookup continues that way identifier can be resolved from join tree.
  *
  * TODO: This was not supported properly before analyzer because nested identifier could not be resolved from alias.
  *
  * More complex example:
  * CREATE TABLE test_table (id UInt64, value UInt64) ENGINE=TinyLog;
  * WITH cast(('Value'), 'Tuple (value UInt64') AS value SELECT (SELECT value FROM test_table);
  * Identifier first part value bound to test_table column value, but nested identifier part cannot be resolved from it,
  * lookup should not be continued, and exception must be thrown because if lookup continues identifier can be resolved from parent scope.
  *
  * TODO: Update exception messages
  * TODO: JOIN TREE subquery constant columns
  * TODO: Table identifiers with optional UUID.
  * TODO: Lookup functions arrayReduce(sum, [1, 2, 3]);
  * TODO: SELECT (compound_expression).*, (compound_expression).COLUMNS are not supported on parser level.
  * TODO: SELECT a.b.c.*, a.b.c.COLUMNS. Qualified matcher where identifier size is greater than 2 are not supported on parser level.
  * TODO: Support function identifier resolve from parent query scope, if lambda in parent scope does not capture any columns.
  * TODO: Support group_by_use_nulls.
  * TODO: Scalar subqueries cache.
  */

namespace
{

/// Identifier lookup context
enum class IdentifierLookupContext : uint8_t
{
    EXPRESSION = 0,
    FUNCTION,
    TABLE_EXPRESSION,
};

const char * toString(IdentifierLookupContext identifier_lookup_context)
{
    switch (identifier_lookup_context)
    {
        case IdentifierLookupContext::EXPRESSION: return "EXPRESSION";
        case IdentifierLookupContext::FUNCTION: return "FUNCTION";
        case IdentifierLookupContext::TABLE_EXPRESSION: return "TABLE_EXPRESSION";
    }
}

const char * toStringLowercase(IdentifierLookupContext identifier_lookup_context)
{
    switch (identifier_lookup_context)
    {
        case IdentifierLookupContext::EXPRESSION: return "expression";
        case IdentifierLookupContext::FUNCTION: return "function";
        case IdentifierLookupContext::TABLE_EXPRESSION: return "table expression";
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

    bool isTableExpressionLookup() const
    {
        return lookup_context == IdentifierLookupContext::TABLE_EXPRESSION;
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

[[maybe_unused]] inline bool operator!=(const IdentifierLookup & lhs, const IdentifierLookup & rhs)
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

enum class IdentifierResolvePlace : UInt8
{
    NONE = 0,
    EXPRESSION_ARGUMENTS,
    ALIASES,
    JOIN_TREE,
    /// Valid only for table lookup
    CTE,
    /// Valid only for table lookup
    DATABASE_CATALOG
};

const char * toString(IdentifierResolvePlace resolved_identifier_place)
{
    switch (resolved_identifier_place)
    {
        case IdentifierResolvePlace::NONE: return "NONE";
        case IdentifierResolvePlace::EXPRESSION_ARGUMENTS: return "EXPRESSION_ARGUMENTS";
        case IdentifierResolvePlace::ALIASES: return "ALIASES";
        case IdentifierResolvePlace::JOIN_TREE: return "JOIN_TREE";
        case IdentifierResolvePlace::CTE: return "CTE";
        case IdentifierResolvePlace::DATABASE_CATALOG: return "DATABASE_CATALOG";
    }
}

struct IdentifierResolveResult
{
    IdentifierResolveResult() = default;

    QueryTreeNodePtr resolved_identifier;
    IdentifierResolvePlace resolve_place = IdentifierResolvePlace::NONE;
    bool resolved_from_parent_scopes = false;

    [[maybe_unused]] bool isResolved() const
    {
        return resolve_place != IdentifierResolvePlace::NONE;
    }

    [[maybe_unused]] bool isResolvedFromParentScopes() const
    {
        return resolved_from_parent_scopes;
    }

    [[maybe_unused]] bool isResolvedFromExpressionArguments() const
    {
        return resolve_place == IdentifierResolvePlace::EXPRESSION_ARGUMENTS;
    }

    [[maybe_unused]] bool isResolvedFromAliases() const
    {
        return resolve_place == IdentifierResolvePlace::ALIASES;
    }

    [[maybe_unused]] bool isResolvedFromJoinTree() const
    {
        return resolve_place == IdentifierResolvePlace::JOIN_TREE;
    }

    [[maybe_unused]] bool isResolvedFromCTEs() const
    {
        return resolve_place == IdentifierResolvePlace::CTE;
    }

    void dump(WriteBuffer & buffer) const
    {
        if (!resolved_identifier)
        {
            buffer << "unresolved";
            return;
        }

        buffer << resolved_identifier->formatASTForErrorMessage() << " place " << toString(resolve_place) << " resolved from parent scopes " << resolved_from_parent_scopes;
    }

    [[maybe_unused]] String dump() const
    {
        WriteBufferFromOwnString buffer;
        dump(buffer);

        return buffer.str();
    }
};

struct IdentifierResolveSettings
{
    /// Allow to check join tree during identifier resolution
    bool allow_to_check_join_tree = true;

    /// Allow to check CTEs during table identifier resolution
    bool allow_to_check_cte = true;

    /// Allow to check parent scopes during identifier resolution
    bool allow_to_check_parent_scopes = true;

    /// Allow to check database catalog during table identifier resolution
    bool allow_to_check_database_catalog = true;

    /// Allow to resolve subquery during identifier resolution
    bool allow_to_resolve_subquery_during_identifier_resolution = true;
};

struct StringTransparentHash
{
    using is_transparent = void;
    using hash = std::hash<std::string_view>;

    [[maybe_unused]] size_t operator()(const char * data) const
    {
        return hash()(data);
    }

    size_t operator()(std::string_view data) const
    {
        return hash()(data);
    }

    size_t operator()(const std::string & data) const
    {
        return hash()(data);
    }
};

using ColumnNameToColumnNodeMap = std::unordered_map<std::string, ColumnNodePtr, StringTransparentHash, std::equal_to<>>;

struct TableExpressionData
{
    std::string table_expression_name;
    std::string table_expression_description;
    std::string table_name;
    std::string database_name;
    ColumnNameToColumnNodeMap column_name_to_column_node;
    std::unordered_set<std::string, StringTransparentHash, std::equal_to<>> column_identifier_first_parts;

    bool hasFullIdentifierName(IdentifierView identifier) const
    {
        return column_name_to_column_node.contains(std::string_view(identifier.getFullName()));
    }

    bool canBindIdentifier(IdentifierView identifier) const
    {
        return column_identifier_first_parts.contains(std::string_view(identifier.at(0)));
    }

    [[maybe_unused]] void dump(WriteBuffer & buffer) const
    {
        buffer << "Columns size " << column_name_to_column_node.size() << '\n';

        for (const auto & [column_name, column_node] : column_name_to_column_node)
            buffer << "Column name " << column_name << " column node " << column_node->dumpTree() << '\n';
    }

    [[maybe_unused]] String dump() const
    {
        WriteBufferFromOwnString buffer;
        dump(buffer);

        return buffer.str();
    }
};

class ExpressionsStack
{
public:
    void pushNode(const QueryTreeNodePtr & node)
    {
        if (node->hasAlias())
        {
            const auto & node_alias = node->getAlias();
            alias_name_to_expressions[node_alias].push_back(node);
        }

        expressions.emplace_back(node);
    }

    void popNode()
    {
        const auto & top_expression = expressions.back();
        const auto & top_expression_alias = top_expression->getAlias();

        if (!top_expression_alias.empty())
        {
            auto it = alias_name_to_expressions.find(top_expression_alias);
            auto & alias_expressions = it->second;
            alias_expressions.pop_back();

            if (alias_expressions.empty())
                alias_name_to_expressions.erase(it);
        }

        expressions.pop_back();
    }

    [[maybe_unused]] const QueryTreeNodePtr & getRoot() const
    {
        return expressions.front();
    }

    const QueryTreeNodePtr & getTop() const
    {
        return expressions.back();
    }

    [[maybe_unused]] bool hasExpressionWithAlias(const std::string & alias) const
    {
        return alias_name_to_expressions.contains(alias);
    }

    QueryTreeNodePtr getExpressionWithAlias(const std::string & alias) const
    {
        auto expression_it = alias_name_to_expressions.find(alias);
        if (expression_it == alias_name_to_expressions.end())
            return {};

        return expression_it->second.front();
    }

    [[maybe_unused]] size_t size() const
    {
        return expressions.size();
    }

    bool empty() const
    {
        return expressions.empty();
    }

    void dump(WriteBuffer & buffer) const
    {
        buffer << expressions.size() << '\n';

        for (const auto & expression : expressions)
        {
            buffer << "Expression ";
            buffer << expression->formatASTForErrorMessage();

            const auto & alias = expression->getAlias();
            if (!alias.empty())
                buffer << " alias " << alias;

            buffer << '\n';
        }
    }

    [[maybe_unused]] String dump() const
    {
        WriteBufferFromOwnString buffer;
        dump(buffer);

        return buffer.str();
    }

private:
    QueryTreeNodes expressions;
    std::unordered_map<std::string, QueryTreeNodes> alias_name_to_expressions;
};

/** Projection names is name of query tree node that is used in projection part of query node.
  * Example: SELECT id FROM test_table;
  * `id` is projection name of column node
  *
  * Example: SELECT id AS id_alias FROM test_table;
  * `id_alias` is projection name of column node
  *
  * Calculation of projection names is done during expression nodes resolution. This is done this way
  * because after identifier node is resolved we lose information about identifier name. We could
  * potentially save this information in query tree node itself, but that would require to clone it in some cases.
  * Example: SELECT big_scalar_subquery AS a, a AS b, b AS c;
  * All 3 nodes in projection are the same big_scalar_subquery, but they have different projection names.
  * If we want to save it in query tree node, we have to clone subquery node that could lead to performance degradation.
  *
  * Possible solution is to separate query node metadata and query node content. So only node metadata could be cloned
  * if we want to change projection name. This solution does not seem to be easy for client of query tree because projection
  * name will be part of interface. If we potentially could hide projection names calculation in analyzer without introducing additional
  * changes in query tree structure that would be preferable.
  *
  * Currently each resolve method returns projection names array. Resolve method must compute projection names of node.
  * If node is resolved as list node this is case for `untuple` function or `matcher` result projection names array must contain projection names
  * for result nodes.
  * If node is not resolved as list node, projection names array contain single projection name for node.
  *
  * Rules for projection names:
  * 1. If node has alias. It is node projection name.
  * Except scenario where `untuple` function has alias. Example: SELECT untuple(expr) AS alias, alias.
  *
  * 2. For constant it is constant value string representation.
  *
  * 3. For identifier:
  * If identifier is resolved from JOIN TREE, we want to remove additional identifier qualifications.
  * Example: SELECT default.test_table.id FROM test_table.
  * Result projection name is `id`.
  *
  * Example: SELECT t1.id FROM test_table_1 AS t1, test_table_2 AS t2
  * In example both test_table_1, test_table_2 have `id` column.
  * In such case projection name is `t1.id` because if additional qualification is removed then column projection name `id` will be ambiguous.
  *
  * Example: SELECT default.test_table_1.id FROM test_table_1 AS t1, test_table_2 AS t2
  * In such case projection name is `test_table_1.id` because we remove unnecessary database qualification, but table name qualification cannot be removed
  * because otherwise column projection name `id` will be ambiguous.
  *
  * If identifier is not resolved from JOIN TREE. Identifier name is projection name.
  * Except scenario where `untuple` function resolved using identifier. Example: SELECT untuple(expr) AS alias, alias.
  * Example: SELECT sum(1, 1) AS value, value.
  * In such case both nodes have `value` projection names.
  *
  * Example: SELECT id AS value, value FROM test_table.
  * In such case both nodes have have `value` projection names.
  *
  * Special case is `untuple` function. If `untuple` function specified with alias, then result nodes will have alias.tuple_column_name projection names.
  * Example: SELECT cast(tuple(1), 'Tuple(id UInt64)') AS value, untuple(value) AS a;
  * Result projection names are `value`, `a.id`.
  *
  * If `untuple` function does not have alias then result nodes will have `tupleElement(untuple_expression_projection_name, 'tuple_column_name') projection names.
  *
  * Example: SELECT cast(tuple(1), 'Tuple(id UInt64)') AS value, untuple(value);
  * Result projection names are `value`, `tupleElement(value, 'id')`;
  *
  * 4. For function:
  * Projection name consists from function_name(parameters_projection_names)(arguments_projection_names).
  * Additionally if function is window function. Window node projection name is used with OVER clause.
  * Example: function_name (parameters_names)(argument_projection_names) OVER window_name;
  * Example: function_name (parameters_names)(argument_projection_names) OVER (PARTITION BY id ORDER BY id).
  * Example: function_name (parameters_names)(argument_projection_names) OVER (window_name ORDER BY id).
  *
  * 5. For lambda:
  * If it is standalone lambda that returns single expression, function projection name is used.
  * Example: WITH (x -> x + 1) AS lambda SELECT lambda(1).
  * Projection name is `lambda(1)`.
  *
  * If is it standalone lambda that returns list, projection names of list nodes are used.
  * Example: WITH (x -> *) AS lambda SELECT lambda(1) FROM test_table;
  * If test_table has two columns `id`, `value`. Then result projection names are `id`, `value`.
  *
  * If lambda is argument of function.
  * Then projection name consists from lambda(tuple(lambda_arguments)(lambda_body_projection_name));
  *
  * 6. For matcher:
  * Matched nodes projection names are used as matcher projection names.
  *
  * Matched nodes must be qualified if needed.
  * Example: SELECT * FROM test_table_1 AS t1, test_table_2 AS t2.
  * In example table test_table_1 and test_table_2 both have `id`, `value` columns.
  * Matched nodes after unqualified matcher resolve must be qualified to avoid ambiguous projection names.
  * Result projection names must be `t1.id`, `t1.value`, `t2.id`, `t2.value`.
  *
  * There are special cases
  * 1. For lambda inside APPLY matcher transformer:
  * Example: SELECT * APPLY x -> toString(x) FROM test_table.
  * In such case lambda argument projection name `x` will be replaced by matched node projection name.
  * If table has two columns `id` and `value`. Then result projection names are `toString(id)`, `toString(value)`;
  *
  * 2. For unqualified matcher when JOIN tree contains JOIN with USING.
  * Example: SELECT * FROM test_table_1 AS t1 INNER JOIN test_table_2 AS t2 USING(id);
  * Result projection names must be `id`, `t1.value`, `t2.value`.
  *
  * 7. For subquery:
  * For subquery projection name consists of `_subquery_` prefix and implementation specific unique number suffix.
  * Example: SELECT (SELECT 1), (SELECT 1 UNION DISTINCT SELECT 1);
  * Result projection name can be `_subquery_1`, `subquery_2`;
  *
  * 8. For table:
  * Table node can be used in expression context only as right argument of IN function. In that case identifier is used
  * as table node projection name.
  * Example: SELECT id IN test_table FROM test_table;
  * Result projection name is `in(id, test_table)`.
  */
using ProjectionName = String;
using ProjectionNames = std::vector<ProjectionName>;
constexpr auto PROJECTION_NAME_PLACEHOLDER = "__projection_name_placeholder";

struct IdentifierResolveScope
{
    /// Construct identifier resolve scope using scope node, and parent scope
    IdentifierResolveScope(QueryTreeNodePtr scope_node_, IdentifierResolveScope * parent_scope_)
        : scope_node(std::move(scope_node_))
        , parent_scope(parent_scope_)
    {
        if (parent_scope)
        {
            subquery_depth = parent_scope->subquery_depth;
            context = parent_scope->context;
        }
    }

    QueryTreeNodePtr scope_node;

    IdentifierResolveScope * parent_scope = nullptr;

    ContextPtr context;

    /// Identifier lookup to result
    std::unordered_map<IdentifierLookup, IdentifierResolveResult, IdentifierLookupHash> identifier_lookup_to_result;

    /// Lambda argument can be expression like constant, column, or it can be function
    std::unordered_map<std::string, QueryTreeNodePtr> expression_argument_name_to_node;

    /// Alias name to query expression node
    std::unordered_map<std::string, QueryTreeNodePtr> alias_name_to_expression_node;

    /// Alias name to lambda node
    std::unordered_map<std::string, QueryTreeNodePtr> alias_name_to_lambda_node;

    /// Alias name to table expression node
    std::unordered_map<std::string, QueryTreeNodePtr> alias_name_to_table_expression_node;

    /// Table column name to column node. Valid only during table ALIAS columns resolve.
    ColumnNameToColumnNodeMap column_name_to_column_node;

    /// CTE name to query node
    std::unordered_map<std::string, QueryTreeNodePtr> cte_name_to_query_node;

    /// Window name to window node
    std::unordered_map<std::string, QueryTreeNodePtr> window_name_to_window_node;

    /// Nodes with duplicated aliases
    std::unordered_set<QueryTreeNodePtr> nodes_with_duplicated_aliases;

    /// Current scope expression in resolve process stack
    ExpressionsStack expressions_in_resolve_process_stack;

    /// Table expressions in resolve process
    std::unordered_set<const IQueryTreeNode *> table_expressions_in_resolve_process;

    /// Current scope expression
    std::unordered_set<IdentifierLookup, IdentifierLookupHash> non_cached_identifier_lookups_during_expression_resolve;

    /// Table expression node to data
    std::unordered_map<QueryTreeNodePtr, TableExpressionData> table_expression_node_to_data;

    /// Use identifier lookup to result cache
    bool use_identifier_lookup_to_result_cache = true;

    /// Subquery depth
    size_t subquery_depth = 0;

    /** Scope join tree node for expression.
      * Valid only during analysis construction for single expression.
      */
    QueryTreeNodePtr expression_join_tree_node;

    [[maybe_unused]] const IdentifierResolveScope * getNearestQueryScope() const
    {
        const IdentifierResolveScope * scope_to_check = this;
        while (scope_to_check != nullptr)
        {
            if (scope_to_check->scope_node->getNodeType() == QueryTreeNodeType::QUERY)
                break;

            scope_to_check = scope_to_check->parent_scope;
        }

        return scope_to_check;
    }

    IdentifierResolveScope * getNearestQueryScope()
    {
        IdentifierResolveScope * scope_to_check = this;
        while (scope_to_check != nullptr)
        {
            if (scope_to_check->scope_node->getNodeType() == QueryTreeNodeType::QUERY)
                break;

            scope_to_check = scope_to_check->parent_scope;
        }

        return scope_to_check;
    }

    TableExpressionData & getTableExpressionDataOrThrow(QueryTreeNodePtr table_expression_node)
    {
        auto it = table_expression_node_to_data.find(table_expression_node);
        if (it == table_expression_node_to_data.end())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Table expression {} data must be initialized. In scope {}",
                table_expression_node->formatASTForErrorMessage(),
                scope_node->formatASTForErrorMessage());
        }

        return it->second;
    }

    /// Dump identifier resolve scope
    [[maybe_unused]] void dump(WriteBuffer & buffer) const
    {
        buffer << "Scope node " << scope_node->formatASTForErrorMessage() << '\n';
        buffer << "Identifier lookup to result " << identifier_lookup_to_result.size() << '\n';
        for (const auto & [identifier, result] : identifier_lookup_to_result)
        {
            buffer << "Identifier " << identifier.dump() << " resolve result ";
            result.dump(buffer);
            buffer << '\n';
        }

        buffer << "Expression argument name to node " << expression_argument_name_to_node.size() << '\n';
        for (const auto & [alias_name, node] : expression_argument_name_to_node)
            buffer << "Alias name " << alias_name << " node " << node->formatASTForErrorMessage() << '\n';

        buffer << "Alias name to expression node table size " << alias_name_to_expression_node.size() << '\n';
        for (const auto & [alias_name, node] : alias_name_to_expression_node)
            buffer << "Alias name " << alias_name << " expression node " << node->dumpTree() << '\n';

        buffer << "Alias name to function node table size " << alias_name_to_lambda_node.size() << '\n';
        for (const auto & [alias_name, node] : alias_name_to_lambda_node)
            buffer << "Alias name " << alias_name << " lambda node " << node->formatASTForErrorMessage() << '\n';

        buffer << "Alias name to table expression node table size " << alias_name_to_table_expression_node.size() << '\n';
        for (const auto & [alias_name, node] : alias_name_to_table_expression_node)
            buffer << "Alias name " << alias_name << " node " << node->formatASTForErrorMessage() << '\n';

        buffer << "CTE name to query node table size " << cte_name_to_query_node.size() << '\n';
        for (const auto & [cte_name, node] : cte_name_to_query_node)
            buffer << "CTE name " << cte_name << " node " << node->formatASTForErrorMessage() << '\n';

        buffer << "WINDOW name to window node table size " << window_name_to_window_node.size() << '\n';
        for (const auto & [window_name, node] : window_name_to_window_node)
            buffer << "CTE name " << window_name << " node " << node->formatASTForErrorMessage() << '\n';

        buffer << "Nodes with duplicated aliases size " << nodes_with_duplicated_aliases.size() << '\n';
        for (const auto & node : nodes_with_duplicated_aliases)
            buffer << "Alias name " << node->getAlias() << " node " << node->formatASTForErrorMessage() << '\n';

        buffer << "Expression resolve process stack " << '\n';
        expressions_in_resolve_process_stack.dump(buffer);

        buffer << "Table expressions in resolve process size " << table_expressions_in_resolve_process.size() << '\n';
        for (const auto & node : table_expressions_in_resolve_process)
            buffer << "Table expression " << node->formatASTForErrorMessage() << '\n';

        buffer << "Non cached identifier lookups during expression resolve " << non_cached_identifier_lookups_during_expression_resolve.size() << '\n';
        for (const auto & identifier_lookup : non_cached_identifier_lookups_during_expression_resolve)
            buffer << "Identifier lookup " << identifier_lookup.dump() << '\n';

        buffer << "Table expression node to data " << table_expression_node_to_data.size() << '\n';
        for (const auto & [table_expression_node, table_expression_data] : table_expression_node_to_data)
            buffer << "Table expression node " << table_expression_node->formatASTForErrorMessage() << " data " << table_expression_data.dump() << '\n';

        buffer << "Use identifier lookup to result cache " << use_identifier_lookup_to_result_cache << '\n';
        buffer << "Subquery depth " << subquery_depth << '\n';
    }

    [[maybe_unused]] String dump() const
    {
        WriteBufferFromOwnString buffer;
        dump(buffer);

        return buffer.str();
    }
};


/** Visitor that extracts expression and function aliases from node and initialize scope tables with it.
  * Does not go into child lambdas and queries.
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
  * There are no easy solution here, without trying to make full featured expression resolution at this stage.
  * Example:
  * WITH (x -> x + 1) AS id, id AS id_1, id_1 AS id_2 SELECT id_2(1);
  * Example: SELECT a, b AS a, b AS c, 1 AS c;
  *
  * It is client responsibility after resolving identifier node with alias, make following actions:
  * 1. If identifier node was resolved in function scope, remove alias from scope expression map.
  * 2. If identifier node was resolved in expression scope, remove alias from scope function map.
  *
  * That way we separate alias map initialization and expressions resolution.
  */
class QueryExpressionsAliasVisitor : public InDepthQueryTreeVisitor<QueryExpressionsAliasVisitor>
{
public:
    explicit QueryExpressionsAliasVisitor(IdentifierResolveScope & scope_)
        : scope(scope_)
    {}

    void visitImpl(QueryTreeNodePtr & node)
    {
        updateAliasesIfNeeded(node, false /*is_lambda_node*/);
    }

    bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child)
    {
        if (auto * lambda_node = child->as<LambdaNode>())
        {
            updateAliasesIfNeeded(child, true /*is_lambda_node*/);
            return false;
        }
        else if (auto * query_tree_node = child->as<QueryNode>())
        {
            if (query_tree_node->isCTE())
                return false;

            updateAliasesIfNeeded(child, false /*is_lambda_node*/);
            return false;
        }
        else if (auto * union_node = child->as<UnionNode>())
        {
            if (union_node->isCTE())
                return false;

            updateAliasesIfNeeded(child, false /*is_lambda_node*/);
            return false;
        }

        return true;
    }
private:
    void updateAliasesIfNeeded(const QueryTreeNodePtr & node, bool is_lambda_node)
    {
        if (!node->hasAlias())
            return;

        const auto & alias = node->getAlias();

        if (is_lambda_node)
        {
            if (scope.alias_name_to_expression_node.contains(alias))
                scope.nodes_with_duplicated_aliases.insert(node);

            auto [_, inserted] = scope.alias_name_to_lambda_node.insert(std::make_pair(alias, node));
            if (!inserted)
                scope.nodes_with_duplicated_aliases.insert(node);

            return;
        }

        if (scope.alias_name_to_lambda_node.contains(alias))
            scope.nodes_with_duplicated_aliases.insert(node);

        auto [_, inserted] = scope.alias_name_to_expression_node.insert(std::make_pair(alias, node));
        if (!inserted)
            scope.nodes_with_duplicated_aliases.insert(node);

        /// If node is identifier put it also in scope alias name to lambda node map
        if (node->getNodeType() == QueryTreeNodeType::IDENTIFIER)
            scope.alias_name_to_lambda_node.insert(std::make_pair(alias, node));
    }

    IdentifierResolveScope & scope;
};

class TableExpressionsAliasVisitor : public InDepthQueryTreeVisitor<TableExpressionsAliasVisitor>
{
public:
    explicit TableExpressionsAliasVisitor(IdentifierResolveScope & scope_)
        : scope(scope_)
    {}

    void visitImpl(QueryTreeNodePtr & node)
    {
        updateAliasesIfNeeded(node);
    }

    static bool needChildVisit(const QueryTreeNodePtr & node, const QueryTreeNodePtr & child)
    {
        auto node_type = node->getNodeType();

        switch (node_type)
        {
            case QueryTreeNodeType::ARRAY_JOIN:
            {
                const auto & array_join_node = node->as<const ArrayJoinNode &>();
                return child.get() == array_join_node.getTableExpression().get();
            }
            case QueryTreeNodeType::JOIN:
            {
                const auto & join_node = node->as<const JoinNode &>();
                return child.get() == join_node.getLeftTableExpression().get() || child.get() == join_node.getRightTableExpression().get();
            }
            default:
            {
                break;
            }
        }

        return false;
    }

private:
    void updateAliasesIfNeeded(const QueryTreeNodePtr & node)
    {
        if (!node->hasAlias())
            return;

        const auto & node_alias = node->getAlias();
        auto [_, inserted] = scope.alias_name_to_table_expression_node.emplace(node_alias, node);
        if (!inserted)
            throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
                "Multiple table expressions with same alias {}. In scope {}",
                node_alias,
                scope.scope_node->formatASTForErrorMessage());
    }

    IdentifierResolveScope & scope;
};

class QueryAnalyzer
{
public:
    void resolve(QueryTreeNodePtr node, const QueryTreeNodePtr & table_expression, ContextPtr context)
    {
        IdentifierResolveScope scope(node, nullptr /*parent_scope*/);
        scope.context = context;

        auto node_type = node->getNodeType();

        switch (node_type)
        {
            case QueryTreeNodeType::QUERY:
            {
                if (table_expression)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "For query analysis table expression must be empty");

                resolveQuery(node, scope);
                break;
            }
            case QueryTreeNodeType::UNION:
            {
                if (table_expression)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "For union analysis table expression must be empty");

                resolveUnion(node, scope);
                break;
            }
            case QueryTreeNodeType::IDENTIFIER:
                [[fallthrough]];
            case QueryTreeNodeType::CONSTANT:
                [[fallthrough]];
            case QueryTreeNodeType::COLUMN:
                [[fallthrough]];
            case QueryTreeNodeType::FUNCTION:
                [[fallthrough]];
            case QueryTreeNodeType::LIST:
            {
                if (!table_expression)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "For expression analysis table expression must not be empty");

                scope.expression_join_tree_node = table_expression;
                validateTableExpressionModifiers(scope.expression_join_tree_node, scope);
                initializeTableExpressionColumns(scope.expression_join_tree_node, scope);

                if (node_type == QueryTreeNodeType::LIST)
                    resolveExpressionNodeList(node, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
                else
                    resolveExpressionNode(node, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

                break;
            }
            default:
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Node {} with type {} is not supported by query analyzer. Supported nodes are query, union, identifier, constant, column, function, list.",
                    node->formatASTForErrorMessage(),
                    node->getNodeTypeName());
            }
        }
    }

private:
    /// Utility functions

    static bool isExpressionNodeType(QueryTreeNodeType node_type);

    static bool isFunctionExpressionNodeType(QueryTreeNodeType node_type);

    static bool isTableExpressionNodeType(QueryTreeNodeType node_type);

    static ProjectionName calculateFunctionProjectionName(const QueryTreeNodePtr & function_node,
        const ProjectionNames & parameters_projection_names,
        const ProjectionNames & arguments_projection_names);

    static ProjectionName calculateWindowProjectionName(const QueryTreeNodePtr & window_node,
        const QueryTreeNodePtr & parent_window_node,
        const String & parent_window_name,
        const ProjectionNames & partition_by_projection_names,
        const ProjectionNames & order_by_projection_names,
        const ProjectionName & frame_begin_offset_projection_name,
        const ProjectionName & frame_end_offset_projection_name);

    static ProjectionName calculateSortColumnProjectionName(const QueryTreeNodePtr & sort_column_node,
        const ProjectionName & sort_expression_projection_name,
        const ProjectionName & fill_from_expression_projection_name,
        const ProjectionName & fill_to_expression_projection_name,
        const ProjectionName & fill_step_expression_projection_name);

    static void collectCompoundExpressionValidIdentifiersForTypoCorrection(const Identifier & unresolved_identifier,
        const DataTypePtr & compound_expression_type,
        const Identifier & valid_identifier_prefix,
        std::unordered_set<Identifier> & valid_identifiers_result);

    static void collectTableExpressionValidIdentifiersForTypoCorrection(const Identifier & unresolved_identifier,
        const QueryTreeNodePtr & table_expression,
        const TableExpressionData & table_expression_data,
        std::unordered_set<Identifier> & valid_identifiers_result);

    static void collectScopeValidIdentifiersForTypoCorrection(const Identifier & unresolved_identifier,
        const IdentifierResolveScope & scope,
        bool allow_expression_identifiers,
        bool allow_function_identifiers,
        bool allow_table_expression_identifiers,
        std::unordered_set<Identifier> & valid_identifiers_result);

    static void collectScopeWithParentScopesValidIdentifiersForTypoCorrection(const Identifier & unresolved_identifier,
        const IdentifierResolveScope & scope,
        bool allow_expression_identifiers,
        bool allow_function_identifiers,
        bool allow_table_expression_identifiers,
        std::unordered_set<Identifier> & valid_identifiers_result);

    static std::vector<String> collectIdentifierTypoHints(const Identifier & unresolved_identifier, const std::unordered_set<Identifier> & valid_identifiers);

    static QueryTreeNodePtr wrapExpressionNodeInTupleElement(QueryTreeNodePtr expression_node, IdentifierView nested_path);

    static QueryTreeNodePtr tryGetLambdaFromSQLUserDefinedFunctions(const std::string & function_name, ContextPtr context);

    static void evaluateScalarSubqueryIfNeeded(QueryTreeNodePtr & query_tree_node, size_t subquery_depth, ContextPtr context);

    static void mergeWindowWithParentWindow(const QueryTreeNodePtr & window_node, const QueryTreeNodePtr & parent_window_node, IdentifierResolveScope & scope);

    static void replaceNodesWithPositionalArguments(QueryTreeNodePtr & node_list, const QueryTreeNodes & projection_nodes, IdentifierResolveScope & scope);

    static void validateLimitOffsetExpression(QueryTreeNodePtr & expression_node, const String & expression_description, IdentifierResolveScope & scope);

    static void validateTableExpressionModifiers(const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    static void validateJoinTableExpressionWithoutAlias(const QueryTreeNodePtr & join_node, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    static void expandGroupByAll(QueryNode & query_tree_node_typed);

    static std::pair<bool, UInt64> recursivelyCollectMaxOrdinaryExpressions(QueryTreeNodePtr & node, QueryTreeNodes & into);

    /// Resolve identifier functions

    static QueryTreeNodePtr tryResolveTableIdentifierFromDatabaseCatalog(const Identifier & table_identifier, ContextPtr context);

    QueryTreeNodePtr tryResolveIdentifierFromCompoundExpression(const Identifier & expression_identifier,
        size_t identifier_bind_size,
        const QueryTreeNodePtr & compound_expression,
        String compound_expression_source,
        IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromExpressionArguments(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    static bool tryBindIdentifierToAliases(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromAliases(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope, IdentifierResolveSettings identifier_resolve_settings = {});

    QueryTreeNodePtr tryResolveIdentifierFromTableColumns(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    static bool tryBindIdentifierToTableExpression(const IdentifierLookup & identifier_lookup, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromTableExpression(const IdentifierLookup & identifier_lookup, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromJoin(const IdentifierLookup & identifier_lookup, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromArrayJoin(const IdentifierLookup & identifier_lookup, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromJoinTreeNode(const IdentifierLookup & identifier_lookup, const QueryTreeNodePtr & join_tree_node, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromJoinTree(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    IdentifierResolveResult tryResolveIdentifierInParentScopes(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    IdentifierResolveResult tryResolveIdentifier(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope, IdentifierResolveSettings identifier_resolve_settings = {});

    /// Resolve query tree nodes functions

    using QueryTreeNodesWithNames = std::vector<std::pair<QueryTreeNodePtr, std::string>>;

    void qualifyMatchedColumnsProjectionNamesIfNeeded(QueryTreeNodesWithNames & matched_nodes_with_column_names,
        const QueryTreeNodePtr & table_expression_node,
        IdentifierResolveScope & scope);

    QueryTreeNodesWithNames resolveQualifiedMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope);

    QueryTreeNodesWithNames resolveUnqualifiedMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope);

    ProjectionNames resolveMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope);

    ProjectionName resolveWindow(QueryTreeNodePtr & window_node, IdentifierResolveScope & scope);

    ProjectionNames resolveLambda(const QueryTreeNodePtr & lambda_node,
        const QueryTreeNodePtr & lambda_node_to_resolve,
        const QueryTreeNodes & lambda_arguments,
        IdentifierResolveScope & scope);

    ProjectionNames resolveFunction(QueryTreeNodePtr & function_node, IdentifierResolveScope & scope);

    ProjectionNames resolveExpressionNode(QueryTreeNodePtr & node, IdentifierResolveScope & scope, bool allow_lambda_expression, bool allow_table_expression);

    ProjectionNames resolveExpressionNodeList(QueryTreeNodePtr & node_list, IdentifierResolveScope & scope, bool allow_lambda_expression, bool allow_table_expression);

    ProjectionNames resolveSortNodeList(QueryTreeNodePtr & sort_node_list, IdentifierResolveScope & scope);

    void resolveInterpolateColumnsNodeList(QueryTreeNodePtr & interpolate_node_list, IdentifierResolveScope & scope);

    void resolveWindowNodeList(QueryTreeNodePtr & window_node_list, IdentifierResolveScope & scope);

    NamesAndTypes resolveProjectionExpressionNodeList(QueryTreeNodePtr & projection_node_list, IdentifierResolveScope & scope);

    void initializeQueryJoinTreeNode(QueryTreeNodePtr & join_tree_node, IdentifierResolveScope & scope);

    void initializeTableExpressionColumns(const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    void resolveQueryJoinTreeNode(QueryTreeNodePtr & join_tree_node, IdentifierResolveScope & scope, QueryExpressionsAliasVisitor & expressions_visitor);

    void resolveQuery(const QueryTreeNodePtr & query_node, IdentifierResolveScope & scope);

    void resolveUnion(const QueryTreeNodePtr & union_node, IdentifierResolveScope & scope);

    /// Lambdas that are currently in resolve process
    std::unordered_set<IQueryTreeNode *> lambdas_in_resolve_process;

    /// Array join expressions counter
    size_t array_join_expressions_counter = 0;

    /// Subquery counter
    size_t subquery_counter = 0;

    /// Global expression node to projection name map
    std::unordered_map<QueryTreeNodePtr, ProjectionName> node_to_projection_name;

    /// Global resolve expression node to projection names map
    std::unordered_map<QueryTreeNodePtr, ProjectionNames> resolved_expressions;

};

/// Utility functions implementation


bool QueryAnalyzer::isExpressionNodeType(QueryTreeNodeType node_type)
{
    return node_type == QueryTreeNodeType::CONSTANT || node_type == QueryTreeNodeType::COLUMN || node_type == QueryTreeNodeType::FUNCTION
        || node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION;
}

bool QueryAnalyzer::isFunctionExpressionNodeType(QueryTreeNodeType node_type)
{
    return node_type == QueryTreeNodeType::LAMBDA;
}

bool QueryAnalyzer::isTableExpressionNodeType(QueryTreeNodeType node_type)
{
    return node_type == QueryTreeNodeType::TABLE || node_type == QueryTreeNodeType::TABLE_FUNCTION ||
        node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION;
}

ProjectionName QueryAnalyzer::calculateFunctionProjectionName(const QueryTreeNodePtr & function_node, const ProjectionNames & parameters_projection_names,
    const ProjectionNames & arguments_projection_names)
{
    const auto & function_node_typed = function_node->as<FunctionNode &>();

    WriteBufferFromOwnString buffer;
    buffer << function_node_typed.getFunctionName();

    if (!parameters_projection_names.empty())
    {
        buffer << '(';

        size_t function_parameters_projection_names_size = parameters_projection_names.size();
        for (size_t i = 0; i < function_parameters_projection_names_size; ++i)
        {
            buffer << parameters_projection_names[i];

            if (i + 1 != function_parameters_projection_names_size)
                buffer << ", ";
        }

        buffer << ')';
    }

    buffer << '(';

    size_t function_arguments_projection_names_size = arguments_projection_names.size();
    for (size_t i = 0; i < function_arguments_projection_names_size; ++i)
    {
        buffer << arguments_projection_names[i];

        if (i + 1 != function_arguments_projection_names_size)
            buffer << ", ";
    }

    buffer << ')';

    return buffer.str();
}

ProjectionName QueryAnalyzer::calculateWindowProjectionName(const QueryTreeNodePtr & window_node,
    const QueryTreeNodePtr & parent_window_node,
    const String & parent_window_name,
    const ProjectionNames & partition_by_projection_names,
    const ProjectionNames & order_by_projection_names,
    const ProjectionName & frame_begin_offset_projection_name,
    const ProjectionName & frame_end_offset_projection_name)
{
    const auto & window_node_typed = window_node->as<WindowNode &>();
    const auto & window_frame = window_node_typed.getWindowFrame();

    bool parent_window_node_has_partition_by = false;
    bool parent_window_node_has_order_by = false;

    if (parent_window_node)
    {
        const auto & parent_window_node_typed = parent_window_node->as<WindowNode &>();
        parent_window_node_has_partition_by = parent_window_node_typed.hasPartitionBy();
        parent_window_node_has_order_by = parent_window_node_typed.hasOrderBy();
    }

    WriteBufferFromOwnString buffer;

    if (!parent_window_name.empty())
        buffer << parent_window_name;

    if (!partition_by_projection_names.empty() && !parent_window_node_has_partition_by)
    {
        if (!parent_window_name.empty())
            buffer << ' ';

        buffer << "PARTITION BY ";

        size_t partition_by_projection_names_size = partition_by_projection_names.size();
        for (size_t i = 0; i < partition_by_projection_names_size; ++i)
        {
            buffer << partition_by_projection_names[i];
            if (i + 1 != partition_by_projection_names_size)
                buffer << ", ";
        }
    }

    if (!order_by_projection_names.empty() && !parent_window_node_has_order_by)
    {
        if (!partition_by_projection_names.empty() || !parent_window_name.empty())
            buffer << ' ';

        buffer << "ORDER BY ";

        size_t order_by_projection_names_size = order_by_projection_names.size();
        for (size_t i = 0; i < order_by_projection_names_size; ++i)
        {
            buffer << order_by_projection_names[i];
            if (i + 1 != order_by_projection_names_size)
                buffer << ", ";
        }
    }

    if (!window_frame.is_default)
    {
        if (!partition_by_projection_names.empty() || !order_by_projection_names.empty() || !parent_window_name.empty())
            buffer << ' ';

        buffer << window_frame.type << " BETWEEN ";
        if (window_frame.begin_type == WindowFrame::BoundaryType::Current)
        {
            buffer << "CURRENT ROW";
        }
        else if (window_frame.begin_type == WindowFrame::BoundaryType::Unbounded)
        {
            buffer << "UNBOUNDED";
            buffer << " " << (window_frame.begin_preceding ? "PRECEDING" : "FOLLOWING");
        }
        else
        {
            buffer << frame_begin_offset_projection_name;
            buffer << " " << (window_frame.begin_preceding ? "PRECEDING" : "FOLLOWING");
        }

        buffer << " AND ";

        if (window_frame.end_type == WindowFrame::BoundaryType::Current)
        {
            buffer << "CURRENT ROW";
        }
        else if (window_frame.end_type == WindowFrame::BoundaryType::Unbounded)
        {
            buffer << "UNBOUNDED";
            buffer << " " << (window_frame.end_preceding ? "PRECEDING" : "FOLLOWING");
        }
        else
        {
            buffer << frame_end_offset_projection_name;
            buffer << " " << (window_frame.end_preceding ? "PRECEDING" : "FOLLOWING");
        }
    }

    return buffer.str();
}

ProjectionName QueryAnalyzer::calculateSortColumnProjectionName(const QueryTreeNodePtr & sort_column_node, const ProjectionName & sort_expression_projection_name,
    const ProjectionName & fill_from_expression_projection_name, const ProjectionName & fill_to_expression_projection_name, const ProjectionName & fill_step_expression_projection_name)
{
    auto & sort_node_typed = sort_column_node->as<SortNode &>();

    WriteBufferFromOwnString sort_column_projection_name_buffer;
    sort_column_projection_name_buffer << sort_expression_projection_name;

    auto sort_direction = sort_node_typed.getSortDirection();
    sort_column_projection_name_buffer << (sort_direction == SortDirection::ASCENDING ? " ASC" : " DESC");

    auto nulls_sort_direction = sort_node_typed.getNullsSortDirection();

    if (nulls_sort_direction)
        sort_column_projection_name_buffer << " NULLS " << (nulls_sort_direction == sort_direction ? "LAST" : "FIRST");

    if (auto collator = sort_node_typed.getCollator())
        sort_column_projection_name_buffer << " COLLATE " << collator->getLocale();

    if (sort_node_typed.withFill())
    {
        sort_column_projection_name_buffer << " WITH FILL";

        if (sort_node_typed.hasFillFrom())
            sort_column_projection_name_buffer << " FROM " << fill_from_expression_projection_name;

        if (sort_node_typed.hasFillTo())
            sort_column_projection_name_buffer << " TO " << fill_to_expression_projection_name;

        if (sort_node_typed.hasFillStep())
            sort_column_projection_name_buffer << " STEP " << fill_step_expression_projection_name;
    }

    return sort_column_projection_name_buffer.str();
}

/// Get valid identifiers for typo correction from compound expression
void QueryAnalyzer::collectCompoundExpressionValidIdentifiersForTypoCorrection(
    const Identifier & unresolved_identifier,
    const DataTypePtr & compound_expression_type,
    const Identifier & valid_identifier_prefix,
    std::unordered_set<Identifier> & valid_identifiers_result)
{
    std::vector<std::pair<Identifier, const IDataType *>> identifiers_with_types_to_process;
    identifiers_with_types_to_process.emplace_back(valid_identifier_prefix, compound_expression_type.get());

    while (!identifiers_with_types_to_process.empty())
    {
        auto [identifier, type] = identifiers_with_types_to_process.back();
        identifiers_with_types_to_process.pop_back();

        if (identifier.getPartsSize() + 1 > unresolved_identifier.getPartsSize())
            continue;

        while (const DataTypeArray * array = checkAndGetDataType<DataTypeArray>(type))
            type = array->getNestedType().get();

        const DataTypeTuple * tuple = checkAndGetDataType<DataTypeTuple>(type);

        if (!tuple)
            continue;

        const auto & tuple_element_names = tuple->getElementNames();
        size_t tuple_element_names_size = tuple_element_names.size();

        for (size_t i = 0; i < tuple_element_names_size; ++i)
        {
            const auto & element_name = tuple_element_names[i];
            const auto & element_type = tuple->getElements()[i];

            identifier.push_back(element_name);

            valid_identifiers_result.insert(identifier);
            identifiers_with_types_to_process.emplace_back(identifier, element_type.get());

            identifier.pop_back();
        }
    }
}

/// Get valid identifiers for typo correction from table expression
void QueryAnalyzer::collectTableExpressionValidIdentifiersForTypoCorrection(
    const Identifier & unresolved_identifier,
    const QueryTreeNodePtr & table_expression,
    const TableExpressionData & table_expression_data,
    std::unordered_set<Identifier> & valid_identifiers_result)
{
    for (const auto & [column_name, column_node] : table_expression_data.column_name_to_column_node)
    {
        Identifier column_identifier(column_name);
        if (unresolved_identifier.getPartsSize() == column_identifier.getPartsSize())
            valid_identifiers_result.insert(column_identifier);

        collectCompoundExpressionValidIdentifiersForTypoCorrection(unresolved_identifier,
            column_node->getColumnType(),
            column_identifier,
            valid_identifiers_result);

        if (table_expression->hasAlias())
        {
            Identifier column_identifier_with_alias({table_expression->getAlias()});
            for (const auto & column_identifier_part : column_identifier)
                column_identifier_with_alias.push_back(column_identifier_part);

            if (unresolved_identifier.getPartsSize() == column_identifier_with_alias.getPartsSize())
                valid_identifiers_result.insert(column_identifier_with_alias);

            collectCompoundExpressionValidIdentifiersForTypoCorrection(unresolved_identifier,
                column_node->getColumnType(),
                column_identifier_with_alias,
                valid_identifiers_result);
        }

        if (!table_expression_data.table_name.empty())
        {
            Identifier column_identifier_with_table_name({table_expression_data.table_name});
            for (const auto & column_identifier_part : column_identifier)
                column_identifier_with_table_name.push_back(column_identifier_part);

            if (unresolved_identifier.getPartsSize() == column_identifier_with_table_name.getPartsSize())
                valid_identifiers_result.insert(column_identifier_with_table_name);

            collectCompoundExpressionValidIdentifiersForTypoCorrection(unresolved_identifier,
                column_node->getColumnType(),
                column_identifier_with_table_name,
                valid_identifiers_result);
        }

        if (!table_expression_data.database_name.empty() && !table_expression_data.table_name.empty())
        {
            Identifier column_identifier_with_table_name_and_database_name({table_expression_data.database_name, table_expression_data.table_name});
            for (const auto & column_identifier_part : column_identifier)
                column_identifier_with_table_name_and_database_name.push_back(column_identifier_part);

            if (unresolved_identifier.getPartsSize() == column_identifier_with_table_name_and_database_name.getPartsSize())
                valid_identifiers_result.insert(column_identifier_with_table_name_and_database_name);

            collectCompoundExpressionValidIdentifiersForTypoCorrection(unresolved_identifier,
                column_node->getColumnType(),
                column_identifier_with_table_name_and_database_name,
                valid_identifiers_result);
        }
    }
}

/// Get valid identifiers for typo correction from scope without looking at parent scopes
void QueryAnalyzer::collectScopeValidIdentifiersForTypoCorrection(
    const Identifier & unresolved_identifier,
    const IdentifierResolveScope & scope,
    bool allow_expression_identifiers,
    bool allow_function_identifiers,
    bool allow_table_expression_identifiers,
    std::unordered_set<Identifier> & valid_identifiers_result)
{
    bool identifier_is_short = unresolved_identifier.isShort();
    bool identifier_is_compound = unresolved_identifier.isCompound();

    if (allow_expression_identifiers)
    {
        for (const auto & [name, expression] : scope.alias_name_to_expression_node)
        {
            assert(expression);
            auto expression_identifier = Identifier(name);
            valid_identifiers_result.insert(expression_identifier);

            auto expression_node_type = expression->getNodeType();

            if (identifier_is_compound && isExpressionNodeType(expression_node_type))
            {
                collectCompoundExpressionValidIdentifiersForTypoCorrection(unresolved_identifier,
                    expression->getResultType(),
                    expression_identifier,
                    valid_identifiers_result);
            }
        }

        for (const auto & [table_expression, table_expression_data] : scope.table_expression_node_to_data)
        {
            collectTableExpressionValidIdentifiersForTypoCorrection(unresolved_identifier,
                table_expression,
                table_expression_data,
                valid_identifiers_result);
        }
    }

    if (identifier_is_short)
    {
        if (allow_function_identifiers)
        {
            for (const auto & [name, _] : scope.alias_name_to_expression_node)
                valid_identifiers_result.insert(Identifier(name));
        }

        if (allow_table_expression_identifiers)
        {
            for (const auto & [name, _] : scope.alias_name_to_table_expression_node)
                valid_identifiers_result.insert(Identifier(name));
        }
    }

    for (const auto & [argument_name, expression] : scope.expression_argument_name_to_node)
    {
        auto expression_node_type = expression->getNodeType();

        if (allow_expression_identifiers && isExpressionNodeType(expression_node_type))
        {
            auto expression_identifier = Identifier(argument_name);

            if (identifier_is_compound)
            {
                collectCompoundExpressionValidIdentifiersForTypoCorrection(unresolved_identifier,
                    expression->getResultType(),
                    expression_identifier,
                    valid_identifiers_result);
            }

            valid_identifiers_result.insert(expression_identifier);
        }
        else if (identifier_is_short && allow_function_identifiers && isFunctionExpressionNodeType(expression_node_type))
        {
            valid_identifiers_result.insert(Identifier(argument_name));
        }
        else if (allow_table_expression_identifiers && isTableExpressionNodeType(expression_node_type))
        {
            valid_identifiers_result.insert(Identifier(argument_name));
        }
    }
}

void QueryAnalyzer::collectScopeWithParentScopesValidIdentifiersForTypoCorrection(
    const Identifier & unresolved_identifier,
    const IdentifierResolveScope & scope,
    bool allow_expression_identifiers,
    bool allow_function_identifiers,
    bool allow_table_expression_identifiers,
    std::unordered_set<Identifier> & valid_identifiers_result)
{
    const IdentifierResolveScope * current_scope = &scope;

    while (current_scope)
    {
        collectScopeValidIdentifiersForTypoCorrection(unresolved_identifier,
            *current_scope,
            allow_expression_identifiers,
            allow_function_identifiers,
            allow_table_expression_identifiers,
            valid_identifiers_result);

        current_scope = current_scope->parent_scope;
    }
}

std::vector<String> QueryAnalyzer::collectIdentifierTypoHints(const Identifier & unresolved_identifier, const std::unordered_set<Identifier> & valid_identifiers)
{
    std::vector<String> prompting_strings;
    prompting_strings.reserve(valid_identifiers.size());

    for (const auto & valid_identifier : valid_identifiers)
        prompting_strings.push_back(valid_identifier.getFullName());

    NamePrompter<1> prompter;
    return prompter.getHints(unresolved_identifier.getFullName(), prompting_strings);
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

        auto & tuple_element_function_arguments_nodes = tuple_element_function->getArguments().getNodes();
        tuple_element_function_arguments_nodes.reserve(2);
        tuple_element_function_arguments_nodes.push_back(expression_node);
        tuple_element_function_arguments_nodes.push_back(std::make_shared<ConstantNode>(nested_path_part));

        expression_node = std::move(tuple_element_function);
    }

    return expression_node;
}

/** Try to get lambda node from sql user defined functions if sql user defined function with function name exists.
  * Returns lambda node if function exists, nullptr otherwise.
  */
QueryTreeNodePtr QueryAnalyzer::tryGetLambdaFromSQLUserDefinedFunctions(const std::string & function_name, ContextPtr context)
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

/// Evaluate scalar subquery and perform constant folding if scalar subquery does not have constant value
void QueryAnalyzer::evaluateScalarSubqueryIfNeeded(QueryTreeNodePtr & node, size_t subquery_depth, ContextPtr context)
{
    auto * query_node = node->as<QueryNode>();
    auto * union_node = node->as<UnionNode>();
    if (!query_node && !union_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Node must have query or union type. Actual {} {}",
            node->getNodeTypeName(),
            node->formatASTForErrorMessage());

    auto subquery_context = Context::createCopy(context);

    Settings subquery_settings = context->getSettings();
    subquery_settings.max_result_rows = 1;
    subquery_settings.extremes = false;
    subquery_context->setSettings(subquery_settings);

    auto options = SelectQueryOptions(QueryProcessingStage::Complete, subquery_depth, true /*is_subquery*/);
    auto interpreter = std::make_unique<InterpreterSelectQueryAnalyzer>(node, options, subquery_context);

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

        auto constant_value = std::make_shared<ConstantValue>(Null(), std::move(type));
        node = std::make_shared<ConstantNode>(std::move(constant_value), node);
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
        scalar_type = std::make_shared<DataTypeTuple>(block.getDataTypes(), block.getNames());
    }

    auto constant_value = std::make_shared<ConstantValue>(std::move(scalar_value), std::move(scalar_type));
    node = std::make_shared<ConstantNode>(std::move(constant_value), node);
}

void QueryAnalyzer::mergeWindowWithParentWindow(const QueryTreeNodePtr & window_node, const QueryTreeNodePtr & parent_window_node, IdentifierResolveScope & scope)
{
    auto & window_node_typed = window_node->as<WindowNode &>();
    auto parent_window_name = window_node_typed.getParentWindowName();

    auto & parent_window_node_typed = parent_window_node->as<WindowNode &>();

    /** If an existing_window_name is specified it must refer to an earlier
      * entry in the WINDOW list; the new window copies its partitioning clause
      * from that entry, as well as its ordering clause if any. In this case
      * the new window cannot specify its own PARTITION BY clause, and it can
      * specify ORDER BY only if the copied window does not have one. The new
      * window always uses its own frame clause; the copied window must not
      * specify a frame clause.
      * https://www.postgresql.org/docs/current/sql-select.html
      */
    if (window_node_typed.hasPartitionBy())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Derived window definition '{}' is not allowed to override PARTITION BY. In scope {}",
            window_node_typed.formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());
    }

    if (window_node_typed.hasOrderBy() && parent_window_node_typed.hasOrderBy())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Derived window definition '{}' is not allowed to override a non-empty ORDER BY. In scope {}",
            window_node_typed.formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());
    }

    if (!parent_window_node_typed.getWindowFrame().is_default)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Parent window '{}' is not allowed to define a frame: while processing derived window definition '{}'. In scope {}",
            parent_window_name,
            window_node_typed.formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());
    }

    window_node_typed.getPartitionByNode() = parent_window_node_typed.getPartitionBy().clone();

    if (parent_window_node_typed.hasOrderBy())
        window_node_typed.getOrderByNode() = parent_window_node_typed.getOrderBy().clone();
}

/** Replace nodes in node list with positional arguments.
  *
  * Example: SELECT id, value FROM test_table GROUP BY 1, 2;
  * Example: SELECT id, value FROM test_table ORDER BY 1, 2;
  * Example: SELECT id, value FROM test_table LIMIT 5 BY 1, 2;
  */
void QueryAnalyzer::replaceNodesWithPositionalArguments(QueryTreeNodePtr & node_list, const QueryTreeNodes & projection_nodes, IdentifierResolveScope & scope)
{
    auto & node_list_typed = node_list->as<ListNode &>();

    for (auto & node : node_list_typed.getNodes())
    {
        auto * constant_node = node->as<ConstantNode>();
        if (!constant_node)
            continue;

        if (!isNativeNumber(removeNullable(constant_node->getResultType())))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Positional argument must be constant with numeric type. Actual {}. In scope {}",
                constant_node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());

        Field converted = convertFieldToType(constant_node->getValue(), DataTypeUInt64());
        if (converted.isNull())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Positional argument numeric constant expression is not representable as UInt64. In scope {}",
                scope.scope_node->formatASTForErrorMessage());

        UInt64 positional_argument_number = converted.safeGet<UInt64>();
        if (positional_argument_number == 0 || positional_argument_number > projection_nodes.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Positional argument number {} is out of bounds. Expected in range [1, {}]. In scope {}",
                positional_argument_number,
                projection_nodes.size(),
                scope.scope_node->formatASTForErrorMessage());

        --positional_argument_number;
        node = projection_nodes[positional_argument_number];
    }
}

void QueryAnalyzer::validateLimitOffsetExpression(QueryTreeNodePtr & expression_node, const String & expression_description, IdentifierResolveScope & scope)
{
    const auto * limit_offset_constant_node = expression_node->as<ConstantNode>();
    if (!limit_offset_constant_node || !isNativeNumber(removeNullable(limit_offset_constant_node->getResultType())))
        throw Exception(ErrorCodes::INVALID_LIMIT_EXPRESSION,
            "{} expression must be constant with numeric type. Actual {}. In scope {}",
            expression_description,
            expression_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());

    Field converted = convertFieldToType(limit_offset_constant_node->getValue(), DataTypeUInt64());
    if (converted.isNull())
        throw Exception(ErrorCodes::INVALID_LIMIT_EXPRESSION,
            "{} numeric constant expression is not representable as UInt64",
            expression_description);
}

void QueryAnalyzer::validateTableExpressionModifiers(const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope)
{
    auto * table_node = table_expression_node->as<TableNode>();
    auto * table_function_node = table_expression_node->as<TableFunctionNode>();
    auto * query_node = table_expression_node->as<QueryNode>();
    auto * union_node = table_expression_node->as<UnionNode>();

    if (!table_node && !table_function_node && !query_node && !union_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Unexpected table expression. Expected table, table function, query or union node. Actual {}",
        table_expression_node->formatASTForErrorMessage(),
        scope.scope_node->formatASTForErrorMessage());

    if (table_node || table_function_node)
    {
        auto table_expression_modifiers = table_node ? table_node->getTableExpressionModifiers() : table_function_node->getTableExpressionModifiers();

        if (table_expression_modifiers.has_value())
        {
            const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
            if (table_expression_modifiers->hasFinal() && !storage->supportsFinal())
                throw Exception(ErrorCodes::ILLEGAL_FINAL,
                    "Storage {} doesn't support FINAL",
                    storage->getName());

            if (table_expression_modifiers->hasSampleSizeRatio() && !storage->supportsSampling())
                throw Exception(ErrorCodes::SAMPLING_NOT_SUPPORTED,
                    "Storage {} doesn't support sampling",
                    storage->getStorageID().getFullNameNotQuoted());
        }
    }
}

void QueryAnalyzer::validateJoinTableExpressionWithoutAlias(const QueryTreeNodePtr & join_node, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope)
{
    if (!scope.context->getSettingsRef().joined_subquery_requires_alias)
        return;

    bool table_expression_has_alias = table_expression_node->hasAlias();
    if (table_expression_has_alias)
        return;

    auto table_expression_node_type = table_expression_node->getNodeType();
    if (table_expression_node_type == QueryTreeNodeType::TABLE_FUNCTION ||
        table_expression_node_type == QueryTreeNodeType::QUERY ||
        table_expression_node_type == QueryTreeNodeType::UNION)
        throw Exception(ErrorCodes::ALIAS_REQUIRED,
            "JOIN {} no alias for subquery or table function {}. In scope {} (set joined_subquery_requires_alias = 0 to disable restriction)",
            join_node->formatASTForErrorMessage(),
            table_expression_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());
}

std::pair<bool, UInt64> QueryAnalyzer::recursivelyCollectMaxOrdinaryExpressions(QueryTreeNodePtr & node, QueryTreeNodes & into)
{
    checkStackSize();

    if (node->as<ColumnNode>())
    {
        into.push_back(node);
        return {false, 1};
    }

    auto * function = node->as<FunctionNode>();

    if (!function)
        return {false, 0};

    if (function->isAggregateFunction())
        return {true, 0};

    UInt64 pushed_children = 0;
    bool has_aggregate = false;

    for (auto & child : function->getArguments().getNodes())
    {
        auto [child_has_aggregate, child_pushed_children] = recursivelyCollectMaxOrdinaryExpressions(child, into);
        has_aggregate |= child_has_aggregate;
        pushed_children += child_pushed_children;
    }

    /// The current function is not aggregate function and there is no aggregate function in its arguments,
    /// so use the current function to replace its arguments
    if (!has_aggregate)
    {
        for (UInt64 i = 0; i < pushed_children; i++)
            into.pop_back();

        into.push_back(node);
        pushed_children = 1;
    }

    return {has_aggregate, pushed_children};
}

/** Expand GROUP BY ALL by extracting all the SELECT-ed expressions that are not aggregate functions.
  *
  * For a special case that if there is a function having both aggregate functions and other fields as its arguments,
  * the `GROUP BY` keys will contain the maximum non-aggregate fields we can extract from it.
  *
  * Example:
  * SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t GROUP BY ALL
  * will expand as
  * SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t GROUP BY substring(a, 4, 2), substring(a, 1, 2)
  */
void QueryAnalyzer::expandGroupByAll(QueryNode & query_tree_node_typed)
{
    auto & group_by_nodes = query_tree_node_typed.getGroupBy().getNodes();
    auto & projection_list = query_tree_node_typed.getProjection();

    for (auto & node : projection_list.getNodes())
        recursivelyCollectMaxOrdinaryExpressions(node, group_by_nodes);

}


/// Resolve identifier functions implementation

/// Try resolve table identifier from database catalog
QueryTreeNodePtr QueryAnalyzer::tryResolveTableIdentifierFromDatabaseCatalog(const Identifier & table_identifier, ContextPtr context)
{
    size_t parts_size = table_identifier.getPartsSize();
    if (parts_size < 1 || parts_size > 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected table identifier to contain 1 or 2 parts. Actual '{}'",
            table_identifier.getFullName());

    std::string database_name;
    std::string table_name;

    if (table_identifier.isCompound())
    {
        database_name = table_identifier[0];
        table_name = table_identifier[1];
    }
    else
    {
        table_name = table_identifier[0];
    }

    StorageID storage_id(database_name, table_name);
    storage_id = context->resolveStorageID(storage_id);
    auto storage = DatabaseCatalog::instance().getTable(storage_id, context);
    auto storage_lock = storage->lockForShare(context->getInitialQueryId(), context->getSettingsRef().lock_acquire_timeout);
    auto storage_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(), context);

    return std::make_shared<TableNode>(std::move(storage), storage_lock, storage_snapshot);
}

/// Resolve identifier from compound expression
QueryTreeNodePtr QueryAnalyzer::tryResolveIdentifierFromCompoundExpression(const Identifier & expression_identifier,
    size_t identifier_bind_size,
    const QueryTreeNodePtr & compound_expression,
    String compound_expression_source,
    IdentifierResolveScope & scope)
{
    Identifier compound_expression_identifier;
    for (size_t i = 0; i < identifier_bind_size; ++i)
        compound_expression_identifier.push_back(expression_identifier[i]);

    IdentifierView nested_path(expression_identifier);
    nested_path.popFirst(identifier_bind_size);

    auto expression_type = compound_expression->getResultType();

    if (!nestedIdentifierCanBeResolved(expression_type, nested_path))
    {
        std::unordered_set<Identifier> valid_identifiers;
        collectCompoundExpressionValidIdentifiersForTypoCorrection(expression_identifier,
            expression_type,
            compound_expression_identifier,
            valid_identifiers);

        auto hints = collectIdentifierTypoHints(expression_identifier, valid_identifiers);

        String compound_expression_from_error_message;
        if (!compound_expression_source.empty())
        {
            compound_expression_from_error_message += " from ";
            compound_expression_from_error_message += compound_expression_source;
        }

        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Identifier {} nested path {} cannot be resolved from type {}{}. In scope {}{}",
            expression_identifier,
            nested_path,
            expression_type->getName(),
            compound_expression_from_error_message,
            scope.scope_node->formatASTForErrorMessage(),
            getHintsErrorMessageSuffix(hints));
    }

    auto tuple_element_result = wrapExpressionNodeInTupleElement(compound_expression, nested_path);
    resolveFunction(tuple_element_result, scope);

    return tuple_element_result;
}

/** Resolve identifier from expression arguments.
  *
  * Expression arguments can be initialized during lambda analysis or they could be provided externally.
  * Expression arguments must be already resolved nodes. This is client responsibility to resolve them.
  *
  * Example: SELECT arrayMap(x -> x + 1, [1,2,3]);
  * For lambda x -> x + 1, `x` is lambda expression argument.
  *
  * Resolve strategy:
  * 1. Try to bind identifier to scope argument name to node map.
  * 2. If identifier is binded but expression context and node type are incompatible return nullptr.
  *
  * It is important to support edge cases, where we lookup for table or function node, but argument has same name.
  * Example: WITH (x -> x + 1) AS func, (func -> func(1) + func) AS lambda SELECT lambda(1);
  *
  * 3. If identifier is compound and identifier lookup is in expression context use `tryResolveIdentifierFromCompoundExpression`.
  */
QueryTreeNodePtr QueryAnalyzer::tryResolveIdentifierFromExpressionArguments(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope)
{
    auto it = scope.expression_argument_name_to_node.find(identifier_lookup.identifier.getFullName());
    bool resolve_full_identifier = it != scope.expression_argument_name_to_node.end();

    if (!resolve_full_identifier)
    {
        const auto & identifier_bind_part = identifier_lookup.identifier.front();

        it = scope.expression_argument_name_to_node.find(identifier_bind_part);
        if (it == scope.expression_argument_name_to_node.end())
            return {};
    }

    auto node_type = it->second->getNodeType();
    if (identifier_lookup.isExpressionLookup() && !isExpressionNodeType(node_type))
        return {};
    else if (identifier_lookup.isTableExpressionLookup() && !isTableExpressionNodeType(node_type))
        return {};
    else if (identifier_lookup.isFunctionLookup() && !isFunctionExpressionNodeType(node_type))
        return {};

    if (!resolve_full_identifier && identifier_lookup.identifier.isCompound() && identifier_lookup.isExpressionLookup())
        return tryResolveIdentifierFromCompoundExpression(identifier_lookup.identifier, 1 /*identifier_bind_size*/, it->second, {}, scope);

    return it->second;
}

bool QueryAnalyzer::tryBindIdentifierToAliases(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope)
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
        return false;

    return true;
}

/** Resolve identifier from scope aliases.
  *
  * Resolve strategy:
  * 1. If alias is registered in current expressions that are in resolve process and if top expression is not part of bottom expression with the same alias subtree
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
  * 4. If node in map is not resolved, resolve it. It is important in case of compound expressions.
  * Example: SELECT value.a, cast('(1)', 'Tuple(a UInt64)') AS value;
  *
  * Special case if node is identifier node.
  * Example: SELECT value, id AS value FROM test_table;
  *
  * Add node in current scope expressions in resolve process stack.
  * Try to resolve identifier.
  * If identifier is resolved, depending on lookup context, erase entry from expression or lambda map. Check QueryExpressionsAliasVisitor documentation.
  * Pop node from current scope expressions in resolve process stack.
  *
  * 5. If identifier is compound and identifier lookup is in expression context, use `tryResolveIdentifierFromCompoundExpression`.
  */
QueryTreeNodePtr QueryAnalyzer::tryResolveIdentifierFromAliases(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope, IdentifierResolveSettings identifier_resolve_settings)
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

    if (auto root_expression_wih_alias = scope.expressions_in_resolve_process_stack.getExpressionWithAlias(identifier_bind_part))
    {
        const auto top_expression = scope.expressions_in_resolve_process_stack.getTop();

        if (!isNodePartOfTree(top_expression.get(), root_expression_wih_alias.get()))
            throw Exception(ErrorCodes::CYCLIC_ALIASES,
                "Cyclic aliases for identifier '{}'. In scope {}",
                identifier_lookup.identifier.getFullName(),
                scope.scope_node->formatASTForErrorMessage());

        scope.non_cached_identifier_lookups_during_expression_resolve.insert(identifier_lookup);
        return {};
    }

    auto node_type = it->second->getNodeType();

    /// Resolve expression if necessary
    if (node_type == QueryTreeNodeType::IDENTIFIER)
    {
        scope.expressions_in_resolve_process_stack.pushNode(it->second);

        auto & alias_identifier_node = it->second->as<IdentifierNode &>();
        auto identifier = alias_identifier_node.getIdentifier();
        auto lookup_result = tryResolveIdentifier(IdentifierLookup{identifier, identifier_lookup.lookup_context}, scope, identifier_resolve_settings);
        if (!lookup_result.resolved_identifier)
        {
            std::unordered_set<Identifier> valid_identifiers;
            collectScopeWithParentScopesValidIdentifiersForTypoCorrection(identifier, scope, true, false, false, valid_identifiers);
            auto hints = collectIdentifierTypoHints(identifier, valid_identifiers);

            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown {} identifier '{}'. In scope {}{}",
                toStringLowercase(identifier_lookup.lookup_context),
                identifier.getFullName(),
                scope.scope_node->formatASTForErrorMessage(),
                getHintsErrorMessageSuffix(hints));
        }

        it->second = lookup_result.resolved_identifier;

        /** During collection of aliases if node is identifier and has alias, we cannot say if it is
          * column or function node. Check QueryExpressionsAliasVisitor documentation for clarification.
          *
          * If we resolved identifier node as expression, we must remove identifier node alias from
          * function alias map.
          * If we resolved identifier node as function, we must remove identifier node alias from
          * expression alias map.
          */
        if (identifier_lookup.isExpressionLookup())
            scope.alias_name_to_lambda_node.erase(identifier_bind_part);
        else if (identifier_lookup.isFunctionLookup())
            scope.alias_name_to_expression_node.erase(identifier_bind_part);

        scope.expressions_in_resolve_process_stack.popNode();
    }
    else if (node_type == QueryTreeNodeType::FUNCTION)
    {
        resolveExpressionNode(it->second, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
    }
    else if (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION)
    {
        if (identifier_resolve_settings.allow_to_resolve_subquery_during_identifier_resolution)
            resolveExpressionNode(it->second, scope, false /*allow_lambda_expression*/, identifier_lookup.isTableExpressionLookup() /*allow_table_expression*/);
    }

    QueryTreeNodePtr result = it->second;

    if (identifier_lookup.identifier.isCompound() && result)
    {
        if (identifier_lookup.isExpressionLookup())
        {
            return tryResolveIdentifierFromCompoundExpression(identifier_lookup.identifier, 1 /*identifier_bind_size*/, it->second, {}, scope);
        }
        else if (identifier_lookup.isFunctionLookup() || identifier_lookup.isTableExpressionLookup())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Compound identifier '{}' cannot be resolved as {}. In scope {}",
                identifier_lookup.identifier.getFullName(),
                identifier_lookup.isFunctionLookup() ? "function" : "table expression",
                scope.scope_node->formatASTForErrorMessage());
        }
    }

    return result;
}

/** Resolve identifier from table columns.
  *
  * 1. If table column nodes are empty or identifier is not expression lookup return nullptr.
  * 2. If identifier full name match table column use column. Save information that we resolve identifier using full name.
  * 3. Else if identifier binds to table column, use column.
  * 4. Try to resolve column ALIAS expression if it exists.
  * 5. If identifier was compound and was not resolved using full name during step 1 use `tryResolveIdentifierFromCompoundExpression`.
  * This can be the case with compound ALIAS columns.
  *
  * Example:
  * CREATE TABLE test_table (id UInt64, value Tuple(id UInt64, value String), alias_value ALIAS value.id) ENGINE=TinyLog;
  */
QueryTreeNodePtr QueryAnalyzer::tryResolveIdentifierFromTableColumns(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope)
{
    if (scope.column_name_to_column_node.empty() || !identifier_lookup.isExpressionLookup())
        return {};

    const auto & identifier = identifier_lookup.identifier;
    auto it = scope.column_name_to_column_node.find(identifier.getFullName());
    bool full_column_name_match = it != scope.column_name_to_column_node.end();

    if (!full_column_name_match)
    {
        it = scope.column_name_to_column_node.find(identifier_lookup.identifier[0]);
        if (it == scope.column_name_to_column_node.end())
            return {};
    }

    if (it->second->hasExpression())
        resolveExpressionNode(it->second->getExpression(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

    QueryTreeNodePtr result = it->second;

    if (!full_column_name_match && identifier.isCompound())
        return tryResolveIdentifierFromCompoundExpression(identifier_lookup.identifier, 1 /*identifier_bind_size*/, it->second, {}, scope);

    return result;
}

bool QueryAnalyzer::tryBindIdentifierToTableExpression(const IdentifierLookup & identifier_lookup, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope)
{
    auto table_expression_node_type = table_expression_node->getNodeType();

    if (table_expression_node_type != QueryTreeNodeType::TABLE &&
        table_expression_node_type != QueryTreeNodeType::TABLE_FUNCTION &&
        table_expression_node_type != QueryTreeNodeType::QUERY &&
        table_expression_node_type != QueryTreeNodeType::UNION)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
        "Unexpected table expression. Expected table, table function, query or union node. Actual {}. In scope {}",
        table_expression_node->formatASTForErrorMessage(),
        scope.scope_node->formatASTForErrorMessage());

    const auto & identifier = identifier_lookup.identifier;
    const auto & path_start = identifier.getParts().front();

    auto & table_expression_data = scope.getTableExpressionDataOrThrow(table_expression_node);

    const auto & table_name = table_expression_data.table_name;
    const auto & database_name = table_expression_data.database_name;

    if (identifier_lookup.isTableExpressionLookup())
    {
        size_t parts_size = identifier_lookup.identifier.getPartsSize();
        if (parts_size != 1 && parts_size != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Expected identifier '{}' to contain 1 or 2 parts to be resolved as table expression. In scope {}",
                identifier_lookup.identifier.getFullName(),
                table_expression_node->formatASTForErrorMessage());

        if (parts_size == 1 && path_start == table_name)
            return true;
        else if (parts_size == 2 && path_start == database_name && identifier[1] == table_name)
            return true;
        else
            return false;
    }

    if (table_expression_data.hasFullIdentifierName(IdentifierView(identifier)) || table_expression_data.canBindIdentifier(IdentifierView(identifier)))
        return true;

    if (identifier.getPartsSize() == 1)
        return false;

    if ((!table_name.empty() && path_start == table_name) || (table_expression_node->hasAlias() && path_start == table_expression_node->getAlias()))
        return true;

    if (identifier.getPartsSize() == 2)
        return false;

    if (!database_name.empty() && path_start == database_name && identifier[1] == table_name)
        return true;

    return false;
}

QueryTreeNodePtr QueryAnalyzer::tryResolveIdentifierFromTableExpression(const IdentifierLookup & identifier_lookup, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope)
{
    auto table_expression_node_type = table_expression_node->getNodeType();

    if (table_expression_node_type != QueryTreeNodeType::TABLE &&
        table_expression_node_type != QueryTreeNodeType::TABLE_FUNCTION &&
        table_expression_node_type != QueryTreeNodeType::QUERY &&
        table_expression_node_type != QueryTreeNodeType::UNION)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Unexpected table expression. Expected table, table function, query or union node. Actual {}. In scope {}",
            table_expression_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());

    const auto & identifier = identifier_lookup.identifier;
    const auto & path_start = identifier.getParts().front();

    auto & table_expression_data = scope.getTableExpressionDataOrThrow(table_expression_node);

    if (identifier_lookup.isTableExpressionLookup())
    {
        size_t parts_size = identifier_lookup.identifier.getPartsSize();
        if (parts_size != 1 && parts_size != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Expected identifier '{}' to contain 1 or 2 parts to be resolved as table expression. In scope {}",
                identifier_lookup.identifier.getFullName(),
                table_expression_node->formatASTForErrorMessage());

        const auto & table_name = table_expression_data.table_name;
        const auto & database_name = table_expression_data.database_name;

        if (parts_size == 1 && path_start == table_name)
            return table_expression_node;
        else if (parts_size == 2 && path_start == database_name && identifier[1] == table_name)
            return table_expression_node;
        else
            return {};
    }

    auto resolve_identifier_from_storage_or_throw = [&](size_t identifier_column_qualifier_parts) -> QueryTreeNodePtr
    {
        auto identifier_view = IdentifierView(identifier);
        identifier_view.popFirst(identifier_column_qualifier_parts);

        /** Compound identifier cannot be resolved directly from storage if storage is not table.
          *
          * Example: SELECT test_table.id.value1.value2 FROM test_table;
          * In table storage column test_table.id.value1.value2 will exists.
          *
          * Example: SELECT test_subquery.compound_expression.value FROM (SELECT compound_expression AS value) AS test_subquery;
          * Here there is no column with name test_subquery.compound_expression.value, and additional wrap in tuple element is required.
          */

        ColumnNodePtr result_column;
        bool compound_identifier = identifier_view.getPartsSize() > 1;
        bool match_full_identifier = false;

        auto it = table_expression_data.column_name_to_column_node.find(std::string(identifier_view.getFullName()));
        if (it != table_expression_data.column_name_to_column_node.end())
        {
            match_full_identifier = true;
            result_column = it->second;
        }
        else
        {
            it = table_expression_data.column_name_to_column_node.find(std::string(identifier_view.at(0)));

            if (it != table_expression_data.column_name_to_column_node.end())
                result_column = it->second;
        }

        QueryTreeNodePtr result_expression = result_column;
        bool clone_is_needed = true;

        String table_expression_source = table_expression_data.table_expression_description;
        if (!table_expression_data.table_expression_name.empty())
            table_expression_source += " with name " + table_expression_data.table_expression_name;

        if (result_column && !match_full_identifier && compound_identifier)
        {
            size_t identifier_bind_size = identifier_column_qualifier_parts + 1;
            result_expression = tryResolveIdentifierFromCompoundExpression(identifier_lookup.identifier, identifier_bind_size, result_column, table_expression_source, scope);
            clone_is_needed = false;
        }

        if (!result_expression)
        {
            std::unordered_set<Identifier> valid_identifiers;
            collectTableExpressionValidIdentifiersForTypoCorrection(identifier,
                table_expression_node,
                table_expression_data,
                valid_identifiers);

            auto hints = collectIdentifierTypoHints(identifier, valid_identifiers);

            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Identifier '{}' cannot be resolved from {}. In scope {}{}",
                identifier.getFullName(),
                table_expression_source,
                scope.scope_node->formatASTForErrorMessage(),
                getHintsErrorMessageSuffix(hints));
        }

        if (clone_is_needed)
            result_expression = result_expression->clone();

        auto qualified_identifier = identifier;
        for (size_t i = 0; i < identifier_column_qualifier_parts; ++i)
        {
            auto qualified_identifier_with_removed_part = qualified_identifier;
            qualified_identifier_with_removed_part.popFirst();

            if (qualified_identifier_with_removed_part.empty())
                break;

            IdentifierLookup bind_to_aliases_identifier_lookup = {qualified_identifier_with_removed_part, IdentifierLookupContext::EXPRESSION};
            if (tryBindIdentifierToAliases(bind_to_aliases_identifier_lookup, scope))
                break;

            bool can_remove_qualificator = true;

            for (auto & table_expression_to_check_data : scope.table_expression_node_to_data)
            {
                const auto & table_expression_to_check = table_expression_to_check_data.first;
                if (table_expression_to_check.get() == table_expression_node.get())
                    continue;

                IdentifierLookup column_identifier_lookup{qualified_identifier_with_removed_part, IdentifierLookupContext::EXPRESSION};
                bool can_bind_identifier_to_table_expression = tryBindIdentifierToTableExpression(column_identifier_lookup, table_expression_to_check, scope);

                if (can_bind_identifier_to_table_expression)
                {
                    can_remove_qualificator = false;
                    break;
                }
            }

            if (!can_remove_qualificator)
                break;

            qualified_identifier = std::move(qualified_identifier_with_removed_part);
        }

        auto qualified_identifier_full_name = qualified_identifier.getFullName();
        node_to_projection_name.emplace(result_expression, std::move(qualified_identifier_full_name));

        return result_expression;
    };

     /** If identifier first part binds to some column start or table has full identifier name. Then we can try to find whole identifier in table.
       * 1. Try to bind identifier first part to column in table, if true get full identifier from table or throw exception.
       * 2. Try to bind identifier first part to table name or storage alias, if true remove first part and try to get full identifier from table or throw exception.
       * Storage alias works for subquery, table function as well.
       * 3. Try to bind identifier first parts to database name and table name, if true remove first two parts and try to get full identifier from table or throw exception.
       */
    if (table_expression_data.hasFullIdentifierName(IdentifierView(identifier)))
        return resolve_identifier_from_storage_or_throw(0 /*identifier_column_qualifier_parts*/);

    if (table_expression_data.canBindIdentifier(IdentifierView(identifier)))
        return resolve_identifier_from_storage_or_throw(0 /*identifier_column_qualifier_parts*/);

    if (identifier.getPartsSize() == 1)
        return {};

    const auto & table_name = table_expression_data.table_name;
    if ((!table_name.empty() && path_start == table_name) || (table_expression_node->hasAlias() && path_start == table_expression_node->getAlias()))
        return resolve_identifier_from_storage_or_throw(1 /*identifier_column_qualifier_parts*/);

    if (identifier.getPartsSize() == 2)
        return {};

    const auto & database_name = table_expression_data.database_name;
    if (!database_name.empty() && path_start == database_name && identifier[1] == table_name)
        return resolve_identifier_from_storage_or_throw(2 /*identifier_column_qualifier_parts*/);

    return {};
}

QueryTreeNodePtr QueryAnalyzer::tryResolveIdentifierFromJoin(const IdentifierLookup & identifier_lookup, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope)
{
    const auto & from_join_node = table_expression_node->as<const JoinNode &>();
    auto left_resolved_identifier = tryResolveIdentifierFromJoinTreeNode(identifier_lookup, from_join_node.getLeftTableExpression(), scope);
    auto right_resolved_identifier = tryResolveIdentifierFromJoinTreeNode(identifier_lookup, from_join_node.getRightTableExpression(), scope);

    if (!identifier_lookup.isExpressionLookup())
    {
        if (left_resolved_identifier && right_resolved_identifier)
            throw Exception(ErrorCodes::AMBIGUOUS_IDENTIFIER,
                "JOIN {} ambiguous identifier {}. In scope {}",
                table_expression_node->formatASTForErrorMessage(),
                identifier_lookup.dump(),
                scope.scope_node->formatASTForErrorMessage());

        return left_resolved_identifier ? left_resolved_identifier : right_resolved_identifier;
    }

    bool join_node_in_resolve_process = scope.table_expressions_in_resolve_process.contains(table_expression_node.get());

    std::unordered_map<std::string, ColumnNodePtr> join_using_column_name_to_column_node;

    if (!join_node_in_resolve_process && from_join_node.isUsingJoinExpression())
    {
        auto & join_using_list = from_join_node.getJoinExpression()->as<ListNode &>();

        for (auto & join_using_node : join_using_list.getNodes())
        {
            auto & column_node = join_using_node->as<ColumnNode &>();
            join_using_column_name_to_column_node.emplace(column_node.getColumnName(), std::static_pointer_cast<ColumnNode>(join_using_node));
        }
    }

    std::optional<JoinTableSide> resolved_side;
    QueryTreeNodePtr resolved_identifier;

    JoinKind join_kind = from_join_node.getKind();

    if (left_resolved_identifier && right_resolved_identifier)
    {
        auto & left_resolved_column = left_resolved_identifier->as<ColumnNode &>();
        auto & right_resolved_column = right_resolved_identifier->as<ColumnNode &>();

        auto using_column_node_it = join_using_column_name_to_column_node.find(left_resolved_column.getColumnName());
        if (using_column_node_it != join_using_column_name_to_column_node.end()
            && left_resolved_column.getColumnName() == right_resolved_column.getColumnName())
        {
            JoinTableSide using_column_inner_column_table_side = isRight(join_kind) ? JoinTableSide::Right : JoinTableSide::Left;
            auto & using_column_node = using_column_node_it->second->as<ColumnNode &>();
            auto & using_expression_list = using_column_node.getExpression()->as<ListNode &>();

            size_t inner_column_node_index = using_column_inner_column_table_side == JoinTableSide::Left ? 0 : 1;
            const auto & inner_column_node = using_expression_list.getNodes().at(inner_column_node_index);

            auto result_column_node = inner_column_node->clone();
            auto & result_column = result_column_node->as<ColumnNode &>();
            result_column.setColumnType(using_column_node.getColumnType());

            resolved_identifier = std::move(result_column_node);
        }
        else
        {
            throw Exception(ErrorCodes::AMBIGUOUS_IDENTIFIER,
                "JOIN {} ambiguous identifier '{}'. In scope {}",
                table_expression_node->formatASTForErrorMessage(),
                identifier_lookup.identifier.getFullName(),
                scope.scope_node->formatASTForErrorMessage());
        }
    }
    else if (left_resolved_identifier)
    {
        resolved_side = JoinTableSide::Left;
        auto & left_resolved_column = left_resolved_identifier->as<ColumnNode &>();

        resolved_identifier = left_resolved_identifier;

        auto using_column_node_it = join_using_column_name_to_column_node.find(left_resolved_column.getColumnName());
        if (using_column_node_it != join_using_column_name_to_column_node.end() &&
            !using_column_node_it->second->getColumnType()->equals(*left_resolved_column.getColumnType()))
        {
            auto left_resolved_column_clone = std::static_pointer_cast<ColumnNode>(left_resolved_column.clone());
            left_resolved_column_clone->setColumnType(using_column_node_it->second->getColumnType());
            resolved_identifier = std::move(left_resolved_column_clone);
        }
        else
        {
            resolved_identifier = left_resolved_identifier;
        }
    }
    else if (right_resolved_identifier)
    {
        resolved_side = JoinTableSide::Right;
        auto & right_resolved_column = right_resolved_identifier->as<ColumnNode &>();

        auto using_column_node_it = join_using_column_name_to_column_node.find(right_resolved_column.getColumnName());
        if (using_column_node_it != join_using_column_name_to_column_node.end() &&
            !using_column_node_it->second->getColumnType()->equals(*right_resolved_column.getColumnType()))
        {
            auto right_resolved_column_clone = std::static_pointer_cast<ColumnNode>(right_resolved_column.clone());
            right_resolved_column_clone->setColumnType(using_column_node_it->second->getColumnType());
            resolved_identifier = std::move(right_resolved_column_clone);
        }
        else
        {
            resolved_identifier = right_resolved_identifier;
        }
    }

    if (join_node_in_resolve_process || !resolved_identifier)
        return resolved_identifier;

    bool join_use_nulls = scope.context->getSettingsRef().join_use_nulls;

    if (join_use_nulls
        && (isFull(join_kind) ||
            (isLeft(join_kind) && resolved_side && *resolved_side == JoinTableSide::Right) ||
            (isRight(join_kind) && resolved_side && *resolved_side == JoinTableSide::Left)))
    {
        resolved_identifier = resolved_identifier->clone();
        auto & resolved_column = resolved_identifier->as<ColumnNode &>();
        resolved_column.setColumnType(makeNullable(resolved_column.getColumnType()));
    }

    return resolved_identifier;
}

QueryTreeNodePtr QueryAnalyzer::tryResolveIdentifierFromArrayJoin(const IdentifierLookup & identifier_lookup, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope)
{
    const auto & from_array_join_node = table_expression_node->as<const ArrayJoinNode &>();
    auto resolved_identifier = tryResolveIdentifierFromJoinTreeNode(identifier_lookup, from_array_join_node.getTableExpression(), scope);

    if (scope.table_expressions_in_resolve_process.contains(table_expression_node.get()))
        return resolved_identifier;

    const auto & array_join_column_expressions = from_array_join_node.getJoinExpressions();
    const auto & array_join_column_expressions_nodes = array_join_column_expressions.getNodes();

    /** Allow JOIN with USING with ARRAY JOIN.
      *
      * SELECT * FROM test_table_1 AS t1 ARRAY JOIN [1,2,3] AS id INNER JOIN test_table_2 AS t2 ON t1.id = t2.id
      * SELECT * FROM test_table_1 AS t1 ARRAY JOIN t1.id AS id INNER JOIN test_table_2 AS t2 ON t1.id = t2.id
      */
    for (const auto & array_join_column_expression : array_join_column_expressions_nodes)
    {
        auto & array_join_column_expression_typed = array_join_column_expression->as<ColumnNode &>();

        if (identifier_lookup.identifier.isShort() &&
            array_join_column_expression_typed.getAlias() == identifier_lookup.identifier.getFullName())
            return array_join_column_expression;
    }

    if (!resolved_identifier)
        return nullptr;

    /** Special case when qualified or unqualified identifier point to array join expression without alias.
      *
      * CREATE TABLE test_table (id UInt64, value String, value_array Array(UInt8)) ENGINE=TinyLog;
      * SELECT id, value, value_array, test_table.value_array, default.test_table.value_array FROM test_table ARRAY JOIN value_array;
      *
      * value_array, test_table.value_array, default.test_table.value_array must be resolved into array join expression.
      */
    for (const auto & array_join_column_expression : array_join_column_expressions_nodes)
    {
        auto & array_join_column_expression_typed = array_join_column_expression->as<ColumnNode &>();

        if (array_join_column_expression_typed.hasAlias())
            continue;

        auto & array_join_column_inner_expression = array_join_column_expression_typed.getExpressionOrThrow();
        if (array_join_column_inner_expression.get() == resolved_identifier.get() ||
            array_join_column_inner_expression->isEqual(*resolved_identifier))
        {
            resolved_identifier = array_join_column_expression;
            break;
        }
    }

    return resolved_identifier;
}

QueryTreeNodePtr QueryAnalyzer::tryResolveIdentifierFromJoinTreeNode(const IdentifierLookup & identifier_lookup, const QueryTreeNodePtr & join_tree_node, IdentifierResolveScope & scope)
{
    auto join_tree_node_type = join_tree_node->getNodeType();

    switch (join_tree_node_type)
    {
        case QueryTreeNodeType::JOIN:
            return tryResolveIdentifierFromJoin(identifier_lookup, join_tree_node, scope);
        case QueryTreeNodeType::ARRAY_JOIN:
            return tryResolveIdentifierFromArrayJoin(identifier_lookup, join_tree_node, scope);
        case QueryTreeNodeType::QUERY:
            [[fallthrough]];
        case QueryTreeNodeType::UNION:
            [[fallthrough]];
        case QueryTreeNodeType::TABLE:
            [[fallthrough]];
        case QueryTreeNodeType::TABLE_FUNCTION:
        {
            /** Edge case scenario when subquery in FROM node try to resolve identifier from parent scopes, when FROM is not resolved.
              * SELECT subquery.b AS value FROM (SELECT value, 1 AS b) AS subquery;
              * TODO: This can be supported
              */
            if (scope.table_expressions_in_resolve_process.contains(join_tree_node.get()))
                return {};

            return tryResolveIdentifierFromTableExpression(identifier_lookup, join_tree_node, scope);
        }
        default:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Scope FROM section expected table, table function, query, union, join or array join. Actual {}. In scope {}",
                join_tree_node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }
    }
}

/** Resolve identifier from scope join tree.
  *
  * 1. If identifier is in function lookup context return nullptr.
  * 2. Try to resolve identifier from table columns.
  * 3. If there is no FROM section return nullptr.
  * 4. If identifier is in table lookup context, check if it has 1 or 2 parts, otherwise throw exception.
  * If identifier has 2 parts try to match it with database_name and table_name.
  * If identifier has 1 part try to match it with table_name, then try to match it with table alias.
  * 5. If identifier is in expression lookup context, we first need to bind identifier to some table column using identifier first part.
  * Start with identifier first part, if it match some column name in table try to get column with full identifier name.
  * TODO: Need to check if it is okay to throw exception if compound identifier first part bind to column but column is not valid.
  */
QueryTreeNodePtr QueryAnalyzer::tryResolveIdentifierFromJoinTree(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope)
{
    if (identifier_lookup.isFunctionLookup())
        return {};

    /// Try to resolve identifier from table columns
    if (auto resolved_identifier = tryResolveIdentifierFromTableColumns(identifier_lookup, scope))
        return resolved_identifier;

    if (scope.expression_join_tree_node)
        return tryResolveIdentifierFromJoinTreeNode(identifier_lookup, scope.expression_join_tree_node, scope);

    auto * query_scope_node = scope.scope_node->as<QueryNode>();
    if (!query_scope_node || !query_scope_node->getJoinTree())
        return {};

    const auto & join_tree_node = query_scope_node->getJoinTree();
    return tryResolveIdentifierFromJoinTreeNode(identifier_lookup, join_tree_node, scope);
}

/** Try resolve identifier in current scope parent scopes.
  *
  * TODO: If column is matched, throw exception that nested subqueries are not supported.
  *
  * If initial scope is expression. Then try to resolve identifier in parent scopes until query scope is hit.
  * For query scope resolve strategy is same as if initial scope if query.
  */
IdentifierResolveResult QueryAnalyzer::tryResolveIdentifierInParentScopes(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope)
{
    bool initial_scope_is_query = scope.scope_node->getNodeType() == QueryTreeNodeType::QUERY;
    bool initial_scope_is_expression = !initial_scope_is_query;

    IdentifierResolveSettings identifier_resolve_settings;
    identifier_resolve_settings.allow_to_check_parent_scopes = false;
    identifier_resolve_settings.allow_to_check_database_catalog = false;

    IdentifierResolveScope * scope_to_check = scope.parent_scope;

    if (initial_scope_is_expression)
    {
        while (scope_to_check != nullptr)
        {
            auto resolve_result = tryResolveIdentifier(identifier_lookup, *scope_to_check, identifier_resolve_settings);
            if (resolve_result.resolved_identifier)
                return resolve_result;

            bool scope_was_query = scope_to_check->scope_node->getNodeType() == QueryTreeNodeType::QUERY;
            scope_to_check = scope_to_check->parent_scope;

            if (scope_was_query)
                break;
        }
    }

    /** Nested subqueries cannot access outer subqueries table expressions from JOIN tree because
      * that can prevent resolution of table expression from CTE.
      *
      * Example: WITH a AS (SELECT number FROM numbers(1)), b AS (SELECT number FROM a) SELECT * FROM a as l, b as r;
      */
    if (identifier_lookup.isTableExpressionLookup())
        identifier_resolve_settings.allow_to_check_join_tree = false;

    while (scope_to_check != nullptr)
    {
        auto lookup_result = tryResolveIdentifier(identifier_lookup, *scope_to_check, identifier_resolve_settings);
        const auto & resolved_identifier = lookup_result.resolved_identifier;

        scope_to_check = scope_to_check->parent_scope;

        if (resolved_identifier)
        {
            bool is_cte = resolved_identifier->as<QueryNode>() && resolved_identifier->as<QueryNode>()->isCTE();

            /** From parent scopes we can resolve table identifiers only as CTE.
              * Example: SELECT (SELECT 1 FROM a) FROM test_table AS a;
              *
              * During child scope table identifier resolve a, table node test_table with alias a from parent scope
              * is invalid.
              */
            if (identifier_lookup.isTableExpressionLookup() && !is_cte)
                continue;

            if (is_cte)
            {
                return lookup_result;
            }
            else if (resolved_identifier->as<ConstantNode>())
            {
                lookup_result.resolved_identifier = resolved_identifier;
                return lookup_result;
            }

            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Resolve identifier '{}' from parent scope only supported for constants and CTE. Actual {} node type {}. In scope {}",
                identifier_lookup.identifier.getFullName(),
                resolved_identifier->formatASTForErrorMessage(),
                resolved_identifier->getNodeTypeName(),
                scope.scope_node->formatASTForErrorMessage());
        }
    }

    return {};
}

/** Resolve identifier in scope.
  *
  * If identifier was resolved resolve identified lookup status will be updated.
  *
  * Steps:
  * 1. Register identifier lookup in scope identifier lookup to resolve status table.
  * If entry is already registered and is not resolved, that means that we have cyclic aliases for identifier.
  * Example: SELECT a AS b, b AS a;
  * Try resolve identifier in current scope:
  * 3. Try resolve identifier from expression arguments.
  *
  * If prefer_column_name_to_alias = true.
  * 4. Try to resolve identifier from join tree.
  * 5. Try to resolve identifier from aliases.
  * Otherwise.
  * 4. Try to resolve identifier from aliases.
  * 5. Try to resolve identifier from join tree.
  *
  * 6. If it is table identifier lookup try to lookup identifier in current scope CTEs.
  *
  * 7. If identifier is not resolved in current scope, try to resolve it in parent scopes.
  * 8. If identifier is not resolved from parent scopes and it is table identifier lookup try to lookup identifier
  * in database catalog.
  *
  * Same is not done for functions because function resolution is more complex, and in case of aggregate functions requires not only name
  * but also argument types, it is responsibility of resolve function method to handle resolution of function name.
  *
  * 9. If identifier was not resolved, or identifier caching was disabled remove it from identifier lookup to resolve status table.
  *
  * It is okay for identifier to be not resolved, in case we want first try to lookup identifier in one context,
  * then if there is no identifier in this context, try to lookup in another context.
  * Example: Try to lookup identifier as expression, if it is not found, lookup as function.
  * Example: Try to lookup identifier as expression, if it is not found, lookup as table.
  */
IdentifierResolveResult QueryAnalyzer::tryResolveIdentifier(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope, IdentifierResolveSettings identifier_resolve_settings)
{
    auto it = scope.identifier_lookup_to_result.find(identifier_lookup);
    if (it != scope.identifier_lookup_to_result.end())
    {
        if (!it->second.resolved_identifier)
            throw Exception(ErrorCodes::CYCLIC_ALIASES,
                "Cyclic aliases for identifier '{}'. In scope {}",
                identifier_lookup.identifier.getFullName(),
                scope.scope_node->formatASTForErrorMessage());

        if (scope.use_identifier_lookup_to_result_cache && !scope.non_cached_identifier_lookups_during_expression_resolve.contains(identifier_lookup))
            return it->second;
    }

    auto [insert_it, _] = scope.identifier_lookup_to_result.insert({identifier_lookup, IdentifierResolveResult()});
    it = insert_it;

    /// Resolve identifier from current scope

    IdentifierResolveResult resolve_result;
    resolve_result.resolved_identifier = tryResolveIdentifierFromExpressionArguments(identifier_lookup, scope);
    if (resolve_result.resolved_identifier)
        resolve_result.resolve_place = IdentifierResolvePlace::EXPRESSION_ARGUMENTS;

    if (!resolve_result.resolved_identifier)
    {
        bool prefer_column_name_to_alias = scope.context->getSettingsRef().prefer_column_name_to_alias;

        if (unlikely(prefer_column_name_to_alias))
        {
            if (identifier_resolve_settings.allow_to_check_join_tree)
            {
                resolve_result.resolved_identifier = tryResolveIdentifierFromJoinTree(identifier_lookup, scope);

                if (resolve_result.resolved_identifier)
                    resolve_result.resolve_place = IdentifierResolvePlace::JOIN_TREE;
            }

            if (!resolve_result.resolved_identifier)
            {
                resolve_result.resolved_identifier = tryResolveIdentifierFromAliases(identifier_lookup, scope, identifier_resolve_settings);

                if (resolve_result.resolved_identifier)
                    resolve_result.resolve_place = IdentifierResolvePlace::ALIASES;
            }
        }
        else
        {
            resolve_result.resolved_identifier = tryResolveIdentifierFromAliases(identifier_lookup, scope, identifier_resolve_settings);

            if (resolve_result.resolved_identifier)
            {
                resolve_result.resolve_place = IdentifierResolvePlace::ALIASES;
            }
            else if (identifier_resolve_settings.allow_to_check_join_tree)
            {
                resolve_result.resolved_identifier = tryResolveIdentifierFromJoinTree(identifier_lookup, scope);

                if (resolve_result.resolved_identifier)
                    resolve_result.resolve_place = IdentifierResolvePlace::JOIN_TREE;
            }
        }
    }

    if (!resolve_result.resolved_identifier && identifier_lookup.isTableExpressionLookup())
    {
        auto cte_query_node_it = scope.cte_name_to_query_node.find(identifier_lookup.identifier.getFullName());
        if (cte_query_node_it != scope.cte_name_to_query_node.end())
        {
            resolve_result.resolved_identifier = cte_query_node_it->second;
            resolve_result.resolve_place = IdentifierResolvePlace::CTE;
        }
    }

    /// Try to resolve identifier from parent scopes

    if (!resolve_result.resolved_identifier && identifier_resolve_settings.allow_to_check_parent_scopes)
    {
        resolve_result = tryResolveIdentifierInParentScopes(identifier_lookup, scope);

        if (resolve_result.resolved_identifier)
            resolve_result.resolved_from_parent_scopes = true;
    }

    /// Try to resolve table identifier from database catalog

    if (!resolve_result.resolved_identifier && identifier_resolve_settings.allow_to_check_database_catalog && identifier_lookup.isTableExpressionLookup())
    {
        resolve_result.resolved_identifier = tryResolveTableIdentifierFromDatabaseCatalog(identifier_lookup.identifier, scope.context);

        if (resolve_result.resolved_identifier)
            resolve_result.resolve_place = IdentifierResolvePlace::DATABASE_CATALOG;
    }

    it->second = resolve_result;

    /** If identifier was not resolved, or during expression resolution identifier was explicitly added into non cached set,
      * or identifier caching was disabled in resolve scope we remove identifier lookup result from identifier lookup to result table.
      */
    if (!resolve_result.resolved_identifier ||
        scope.non_cached_identifier_lookups_during_expression_resolve.contains(identifier_lookup) ||
        !scope.use_identifier_lookup_to_result_cache)
        scope.identifier_lookup_to_result.erase(it);

    return resolve_result;
}

/// Resolve query tree nodes functions implementation

/** Qualify matched columns projection names for unqualified matcher or qualified matcher resolved nodes
  *
  * Example: SELECT * FROM test_table AS t1, test_table AS t2;
  */
void QueryAnalyzer::qualifyMatchedColumnsProjectionNamesIfNeeded(QueryTreeNodesWithNames & matched_nodes_with_column_names,
    const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope)
{
    /// Build additional column qualification parts array
    std::vector<std::string> additional_column_qualification_parts;

    if (table_expression_node->hasAlias())
        additional_column_qualification_parts = {table_expression_node->getAlias()};
    else if (auto * table_node = table_expression_node->as<TableNode>())
        additional_column_qualification_parts = {table_node->getStorageID().getDatabaseName(), table_node->getStorageID().getTableName()};

    size_t additional_column_qualification_parts_size = additional_column_qualification_parts.size();

    /** For each matched column node iterate over additional column qualifications and apply them if column needs to be qualified.
      * To check if column needs to be qualified we check if column name can bind to any other table expression in scope or to scope aliases.
      */
    std::vector<std::string> column_qualified_identifier_parts;

    for (auto & [column_node, column_name] : matched_nodes_with_column_names)
    {
        column_qualified_identifier_parts = Identifier(column_name).getParts();

        /// Iterate over additional column qualifications and apply them if needed
        for (size_t i = 0; i < additional_column_qualification_parts_size; ++i)
        {
            bool need_to_qualify = false;
            auto identifier_to_check = Identifier(column_qualified_identifier_parts);
            IdentifierLookup lookup{identifier_to_check, IdentifierLookupContext::EXPRESSION};

            for (auto & table_expression_data : scope.table_expression_node_to_data)
            {
                if (table_expression_data.first.get() == table_expression_node.get())
                    continue;

                if (tryBindIdentifierToTableExpression(lookup, table_expression_data.first, scope))
                {
                    need_to_qualify = true;
                    break;
                }
            }

            if (tryBindIdentifierToAliases(lookup, scope))
                need_to_qualify = true;

            if (need_to_qualify)
            {
                /** Add last qualification part that was not used into column qualified identifier.
                  * If additional column qualification parts consists from [database_name, table_name].
                  * On first iteration if column is needed to be qualified to qualify it with table_name.
                  * On second iteration if column is needed to be qualified to qualify it with database_name.
                  */
                size_t part_index_to_use_for_qualification = additional_column_qualification_parts_size - i - 1;
                const auto & part_to_use = additional_column_qualification_parts[part_index_to_use_for_qualification];
                column_qualified_identifier_parts.insert(column_qualified_identifier_parts.begin(), part_to_use);
            }
            else
            {
                break;
            }
        }

        node_to_projection_name.emplace(column_node, Identifier(column_qualified_identifier_parts).getFullName());
    }
}

/** Resolve qualified tree matcher.
  *
  * First try to match qualified identifier to expression. If qualified identifier matched expression node then
  * if expression is compound match it column names using matcher `isMatchingColumn` method, if expression is not compound, throw exception.
  * If qualified identifier did not match expression in query tree, try to lookup qualified identifier in table context.
  */
QueryAnalyzer::QueryTreeNodesWithNames QueryAnalyzer::resolveQualifiedMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope)
{
    auto & matcher_node_typed = matcher_node->as<MatcherNode &>();
    assert(matcher_node_typed.isQualified());

    QueryTreeNodesWithNames matched_expression_nodes_with_column_names;

    auto expression_identifier_lookup = IdentifierLookup{matcher_node_typed.getQualifiedIdentifier(), IdentifierLookupContext::EXPRESSION};
    auto expression_identifier_resolve_result = tryResolveIdentifier(expression_identifier_lookup, scope);
    auto expression_query_tree_node = expression_identifier_resolve_result.resolved_identifier;

    /// Try to resolve unqualified matcher for query expression

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

        auto qualified_matcher_element_identifier = matcher_node_typed.getQualifiedIdentifier();
        for (const auto & element_name : element_names)
        {
            if (!matcher_node_typed.isMatchingColumn(element_name))
                continue;

            auto tuple_element_function = std::make_shared<FunctionNode>("tupleElement");
            tuple_element_function->getArguments().getNodes().push_back(expression_query_tree_node);
            tuple_element_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(element_name));

            QueryTreeNodePtr function_query_node = tuple_element_function;
            resolveFunction(function_query_node, scope);

            qualified_matcher_element_identifier.push_back(element_name);
            node_to_projection_name.emplace(function_query_node, qualified_matcher_element_identifier.getFullName());
            qualified_matcher_element_identifier.pop_back();

            matched_expression_nodes_with_column_names.emplace_back(std::move(function_query_node), element_name);
        }

        return matched_expression_nodes_with_column_names;
    }

    /// Try to resolve qualified matcher for table expression

    IdentifierResolveSettings identifier_resolve_settings;
    identifier_resolve_settings.allow_to_check_cte = false;
    identifier_resolve_settings.allow_to_check_database_catalog = false;

    auto table_identifier_lookup = IdentifierLookup{matcher_node_typed.getQualifiedIdentifier(), IdentifierLookupContext::TABLE_EXPRESSION};
    auto table_identifier_resolve_result = tryResolveIdentifier(table_identifier_lookup, scope, identifier_resolve_settings);
    auto table_expression_node = table_identifier_resolve_result.resolved_identifier;

    if (!table_expression_node)
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Qualified matcher {} does not find table. In scope {}",
            matcher_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());
    }

    NamesAndTypes initial_matcher_columns;

    auto * table_expression_query_node = table_expression_node->as<QueryNode>();
    auto * table_expression_union_node = table_expression_node->as<UnionNode>();
    auto * table_expression_table_node = table_expression_node->as<TableNode>();
    auto * table_expression_table_function_node = table_expression_node->as<TableFunctionNode>();

    if (table_expression_query_node || table_expression_union_node)
    {
        initial_matcher_columns = table_expression_query_node ? table_expression_query_node->getProjectionColumns()
                                                              : table_expression_union_node->computeProjectionColumns();
    }
    else if (table_expression_table_node || table_expression_table_function_node)
    {
        const auto & storage_snapshot = table_expression_table_node ? table_expression_table_node->getStorageSnapshot()
                                                                    : table_expression_table_function_node->getStorageSnapshot();
        auto storage_columns_list = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::All));
        initial_matcher_columns = NamesAndTypes(storage_columns_list.begin(), storage_columns_list.end());
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Invalid table expression node {}. In scope {}",
            table_expression_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());
    }

    for (auto & column : initial_matcher_columns)
    {
        const auto & column_name = column.name;
        if (matcher_node_typed.isMatchingColumn(column_name))
            matched_expression_nodes_with_column_names.emplace_back(std::make_shared<ColumnNode>(column, table_expression_node), column_name);
    }

    qualifyMatchedColumnsProjectionNamesIfNeeded(matched_expression_nodes_with_column_names, table_expression_node, scope);

    return matched_expression_nodes_with_column_names;
}


/// Resolve non qualified matcher, using scope join tree node.
QueryAnalyzer::QueryTreeNodesWithNames QueryAnalyzer::resolveUnqualifiedMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope)
{
    auto & matcher_node_typed = matcher_node->as<MatcherNode &>();
    assert(matcher_node_typed.isUnqualified());

    /** There can be edge case if matcher is inside lambda expression.
      * Try to find parent query expression using parent scopes.
      */
    auto * nearest_query_scope = scope.getNearestQueryScope();
    auto * nearest_query_scope_query_node = nearest_query_scope ? nearest_query_scope->scope_node->as<QueryNode>() : nullptr;

    /// If there are no parent query scope or query scope does not have join tree
    if (!nearest_query_scope_query_node || !nearest_query_scope_query_node->getJoinTree())
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Unqualified matcher {} cannot be resolved. There are no table sources. In scope {}",
            matcher_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());
    }

    /** For unqualifited matcher resolve we build table expressions stack from JOIN tree and then process it.
      * For table, table function, query, union table expressions add matched columns into table expressions columns stack.
      * For array join continue processing.
      * For join node combine last left and right table expressions columns on stack together. It is important that if JOIN has USING
      * we must add USING columns before combining left and right table expressions columns. Columns from left and right table
      * expressions that have same names as columns in USING clause must be skipped.
      */

    auto table_expressions_stack = buildTableExpressionsStack(nearest_query_scope_query_node->getJoinTree());
    std::vector<QueryTreeNodesWithNames> table_expressions_column_nodes_with_names_stack;

    std::unordered_set<std::string> left_table_expression_column_names_to_skip;
    std::unordered_set<std::string> right_table_expression_column_names_to_skip;

    for (auto & table_expression : table_expressions_stack)
    {
        QueryTreeNodesWithNames matched_expression_nodes_with_column_names;

        if (auto * array_join_node = table_expression->as<ArrayJoinNode>())
        {
            if (table_expressions_column_nodes_with_names_stack.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expected at least 1 table expressions on stack before ARRAY JOIN processing");

            auto & table_expression_column_nodes_with_names = table_expressions_column_nodes_with_names_stack.back();

            const auto & array_join_column_list = array_join_node->getJoinExpressions();
            const auto & array_join_column_nodes = array_join_column_list.getNodes();

            /** Special case with ARRAY JOIN column without alias.
              *
              * CREATE TABLE test_table (id UInt64, value String, value_array Array(UInt8)) ENGINE=TinyLog;
              * SELECT * FROM test_table ARRAY JOIN value_array;
              *
              * In matched columns `value_array` must be resolved into array join column.
              */
            for (const auto & array_join_column_node : array_join_column_nodes)
            {
                if (array_join_column_node->hasAlias())
                    continue;

                auto array_join_column_inner_expression = array_join_column_node->as<ColumnNode &>().getExpressionOrThrow();
                if (array_join_column_inner_expression->getNodeType() != QueryTreeNodeType::COLUMN)
                    continue;

                for (auto & table_expressions_column_node_with_name : table_expression_column_nodes_with_names)
                {
                    auto & table_expression_column_node = table_expressions_column_node_with_name.first;

                    if (table_expression_column_node.get() == array_join_column_inner_expression.get() ||
                        table_expression_column_node->isEqual(*array_join_column_inner_expression))
                    {
                        table_expression_column_node = array_join_column_node;
                    }
                }
            }

            continue;
        }

        bool table_expression_in_resolve_process = scope.table_expressions_in_resolve_process.contains(table_expression.get());

        auto * join_node = table_expression->as<JoinNode>();

        if (join_node)
        {
            size_t table_expressions_column_nodes_with_names_stack_size = table_expressions_column_nodes_with_names_stack.size();
            if (table_expressions_column_nodes_with_names_stack_size < 2)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expected at least 2 table expressions on stack before JOIN processing. Actual {}",
                    table_expressions_column_nodes_with_names_stack_size);

            auto right_table_expression_columns = std::move(table_expressions_column_nodes_with_names_stack.back());
            table_expressions_column_nodes_with_names_stack.pop_back();

            auto left_table_expression_columns = std::move(table_expressions_column_nodes_with_names_stack.back());
            table_expressions_column_nodes_with_names_stack.pop_back();

            left_table_expression_column_names_to_skip.clear();
            right_table_expression_column_names_to_skip.clear();

            /** If there is JOIN with USING we need to match only single USING column and do not use left table expression
              * and right table expression column with same name.
              *
              * Example: SELECT id FROM test_table_1 AS t1 INNER JOIN test_table_2 AS t2 USING (id);
              */
            if (!table_expression_in_resolve_process && join_node->isUsingJoinExpression())
            {
                auto & join_using_list = join_node->getJoinExpression()->as<ListNode &>();

                for (auto & join_using_node : join_using_list.getNodes())
                {
                    auto & column_node = join_using_node->as<ColumnNode &>();
                    const auto & using_column_name = column_node.getColumnName();

                    if (!matcher_node_typed.isMatchingColumn(using_column_name))
                        continue;

                    const auto & join_using_column_nodes_list = column_node.getExpressionOrThrow()->as<ListNode &>();
                    const auto & join_using_column_nodes = join_using_column_nodes_list.getNodes();

                    QueryTreeNodePtr matched_column_node;

                    if (isRight(join_node->getKind()))
                        matched_column_node = join_using_column_nodes.at(1);
                    else
                        matched_column_node = join_using_column_nodes.at(0);

                    /** It is possible that in USING there is JOIN with array joined column.
                      * SELECT * FROM (SELECT [0] AS value) AS t1 ARRAY JOIN value AS id INNER JOIN test_table USING (id);
                      * In such example match `value` column from t1, and all columns from test_table except `id`.
                      *
                      * SELECT * FROM (SELECT [0] AS id) AS t1 ARRAY JOIN id INNER JOIN test_table USING (id);
                      * In such example, match `id` column from ARRAY JOIN, and all columns from test_table except `id`.
                      *
                      * SELECT * FROM (SELECT [0] AS id) AS t1 ARRAY JOIN id AS id INNER JOIN test_table USING (id);
                      * In such example match `id` column from t1, and all columns from test_table except `id`.
                      *
                      * SELECT * FROM (SELECT [0] AS id) AS t1 ARRAY JOIN [1] AS id INNER JOIN test_table USING (id);
                      * In such example match `id` column from t1, and all columns from test_table except `id`.
                      */
                    auto matched_column_source = matched_column_node->as<ColumnNode &>().getColumnSource();

                    if (matched_column_source->getNodeType() == QueryTreeNodeType::ARRAY_JOIN && matched_column_node->hasAlias())
                    {
                        if (isRight(join_node->getKind()))
                            left_table_expression_column_names_to_skip.insert(using_column_name);
                        else
                            right_table_expression_column_names_to_skip.insert(using_column_name);
                    }
                    else
                    {
                        left_table_expression_column_names_to_skip.insert(using_column_name);
                        right_table_expression_column_names_to_skip.insert(using_column_name);
                        matched_expression_nodes_with_column_names.emplace_back(std::move(matched_column_node), using_column_name);
                    }
                }
            }

            for (auto && left_table_column : left_table_expression_columns)
            {
                if (left_table_expression_column_names_to_skip.contains(left_table_column.second))
                    continue;

                matched_expression_nodes_with_column_names.push_back(std::move(left_table_column));
            }

            for (auto && right_table_column : right_table_expression_columns)
            {
                if (right_table_expression_column_names_to_skip.contains(right_table_column.second))
                    continue;

                matched_expression_nodes_with_column_names.push_back(std::move(right_table_column));
            }

            table_expressions_column_nodes_with_names_stack.push_back(std::move(matched_expression_nodes_with_column_names));
            continue;
        }

        auto * table_node = table_expression->as<TableNode>();
        auto * table_function_node = table_expression->as<TableFunctionNode>();
        auto * query_node = table_expression->as<QueryNode>();
        auto * union_node = table_expression->as<UnionNode>();

        if (table_expression_in_resolve_process)
        {
            table_expressions_column_nodes_with_names_stack.emplace_back();
            continue;
        }

        NamesAndTypes table_expression_columns;

        if (query_node || union_node)
        {
            table_expression_columns = query_node ? query_node->getProjectionColumns() : union_node->computeProjectionColumns();
        }
        else if (table_node || table_function_node)
        {
            const auto & storage_snapshot
                = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();

            UInt8 get_column_options_kind = 0;

            if (matcher_node_typed.isAsteriskMatcher())
            {
                get_column_options_kind = GetColumnsOptions::Ordinary;
                const auto & settings = scope.context->getSettingsRef();

                if (settings.asterisk_include_alias_columns)
                    get_column_options_kind |= GetColumnsOptions::Kind::Aliases;

                if (settings.asterisk_include_materialized_columns)
                    get_column_options_kind |= GetColumnsOptions::Kind::Materialized;
            }
            else
            {
                /// TODO: Check if COLUMNS select aliases column by default
                get_column_options_kind = GetColumnsOptions::All;
            }

            auto get_columns_options = GetColumnsOptions(static_cast<GetColumnsOptions::Kind>(get_column_options_kind));
            auto storage_columns_list = storage_snapshot->getColumns(get_columns_options);
            table_expression_columns = NamesAndTypes(storage_columns_list.begin(), storage_columns_list.end());
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Unqualified matcher {} resolve unexpected table expression. In scope {}",
                matcher_node_typed.formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }

        for (auto & table_expression_column : table_expression_columns)
        {
            if (!matcher_node_typed.isMatchingColumn(table_expression_column.name))
                continue;

            auto matched_column_node = std::make_shared<ColumnNode>(table_expression_column, table_expression);
            matched_expression_nodes_with_column_names.emplace_back(std::move(matched_column_node), table_expression_column.name);
        }

        qualifyMatchedColumnsProjectionNamesIfNeeded(matched_expression_nodes_with_column_names, table_expression, scope);

        for (auto & [matched_node, column_name] : matched_expression_nodes_with_column_names)
        {
            auto node_projection_name_it = node_to_projection_name.find(matcher_node);
            if (node_projection_name_it != node_to_projection_name.end())
                column_name = node_projection_name_it->second;
        }

        table_expressions_column_nodes_with_names_stack.push_back(std::move(matched_expression_nodes_with_column_names));
    }

    QueryTreeNodesWithNames result;

    for (auto & table_expression_column_nodes_with_names : table_expressions_column_nodes_with_names_stack)
    {
        for (auto && table_expression_column_node_with_name : table_expression_column_nodes_with_names)
            result.push_back(std::move(table_expression_column_node_with_name));
    }

    return result;
}


/** Resolve query tree matcher. Check MatcherNode.h for detailed matcher description. Check ColumnTransformers.h for detailed transformers description.
  *
  * 1. Populate matched expression nodes resolving qualified or unqualified matcher.
  * 2. Apply column transformers to matched expression nodes. For strict column transformers save used column names.
  * 3. Validate strict column transformers.
  */
ProjectionNames QueryAnalyzer::resolveMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope)
{
    auto & matcher_node_typed = matcher_node->as<MatcherNode &>();

    QueryTreeNodesWithNames matched_expression_nodes_with_names;

    if (matcher_node_typed.isQualified())
        matched_expression_nodes_with_names = resolveQualifiedMatcher(matcher_node, scope);
    else
        matched_expression_nodes_with_names = resolveUnqualifiedMatcher(matcher_node, scope);

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
    ProjectionNames result_projection_names;
    ProjectionNames node_projection_names;

    for (auto & [node, column_name] : matched_expression_nodes_with_names)
    {
        bool apply_transformer_was_used = false;
        bool replace_transformer_was_used = false;
        bool execute_apply_transformer = false;
        bool execute_replace_transformer = false;

        auto projection_name_it = node_to_projection_name.find(node);
        if (projection_name_it != node_to_projection_name.end())
            result_projection_names.push_back(projection_name_it->second);
        else
            result_projection_names.push_back(column_name);

        for (const auto & transformer : matcher_node_typed.getColumnTransformers().getNodes())
        {
            if (auto * apply_transformer = transformer->as<ApplyColumnTransformerNode>())
            {
                const auto & expression_node = apply_transformer->getExpressionNode();
                apply_transformer_was_used = true;

                if (apply_transformer->getApplyTransformerType() == ApplyColumnTransformerType::LAMBDA)
                {
                    auto lambda_expression_to_resolve = expression_node->clone();
                    IdentifierResolveScope lambda_scope(expression_node, &scope /*parent_scope*/);
                    node_projection_names = resolveLambda(expression_node, lambda_expression_to_resolve, {node}, lambda_scope);
                    auto & lambda_expression_to_resolve_typed = lambda_expression_to_resolve->as<LambdaNode &>();
                    node = lambda_expression_to_resolve_typed.getExpression();
                }
                else if (apply_transformer->getApplyTransformerType() == ApplyColumnTransformerType::FUNCTION)
                {
                    auto function_to_resolve_untyped = expression_node->clone();
                    auto & function_to_resolve_typed = function_to_resolve_untyped->as<FunctionNode &>();
                    function_to_resolve_typed.getArguments().getNodes().push_back(node);
                    node_projection_names = resolveFunction(function_to_resolve_untyped, scope);
                    node = function_to_resolve_untyped;
                }
                else
                {
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                        "Unsupported apply matcher expression type. Expected lambda or function apply transformer. Actual {}. In scope {}",
                        transformer->formatASTForErrorMessage(),
                        scope.scope_node->formatASTForErrorMessage());
                }

                execute_apply_transformer = true;
            }
            else if (auto * except_transformer = transformer->as<ExceptColumnTransformerNode>())
            {
                if (apply_transformer_was_used || replace_transformer_was_used)
                    break;

                if (except_transformer->isColumnMatching(column_name))
                {
                    if (except_transformer->isStrict())
                        add_strict_transformer_column_name(except_transformer, column_name);

                    node = {};
                    break;
                }
            }
            else if (auto * replace_transformer = transformer->as<ReplaceColumnTransformerNode>())
            {
                if (apply_transformer_was_used || replace_transformer_was_used)
                    break;

                replace_transformer_was_used = true;

                auto replace_expression = replace_transformer->findReplacementExpression(column_name);
                if (!replace_expression)
                    continue;

                if (replace_transformer->isStrict())
                    add_strict_transformer_column_name(replace_transformer, column_name);

                node = replace_expression->clone();
                node_projection_names = resolveExpressionNode(node, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
                execute_replace_transformer = true;
            }

            if (execute_apply_transformer || execute_replace_transformer)
            {
                if (auto * node_list = node->as<ListNode>())
                {
                    auto & node_list_nodes = node_list->getNodes();
                    size_t node_list_nodes_size = node_list_nodes.size();

                    if (node_list_nodes_size != 1)
                        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                            "{} transformer {} resolved as list node with size {}. Expected 1. In scope {}",
                            execute_apply_transformer ? "APPLY" : "REPLACE",
                            transformer->formatASTForErrorMessage(),
                            node_list_nodes_size,
                            scope.scope_node->formatASTForErrorMessage());

                    node = node_list_nodes[0];
                }

                if (node_projection_names.size() != 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Matcher node expected 1 projection name. Actual {}", node_projection_names.size());

                result_projection_names.back() = std::move(node_projection_names[0]);
                node_to_projection_name.emplace(node, result_projection_names.back());
                node_projection_names.clear();
            }
        }

        if (node)
            list->getNodes().push_back(node);
        else
            result_projection_names.pop_back();
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

    matcher_node = std::move(list);

    return result_projection_names;
}

/** Resolve window function window node.
  *
  * Node can be identifier or window node.
  * Example: SELECT count(*) OVER w FROM test_table WINDOW w AS (PARTITION BY id);
  * Example: SELECT count(*) OVER (PARTITION BY id);
  *
  * If node has parent window name specified, then parent window definition is searched in nearest query scope WINDOW section.
  * If node is identifier, than node is replaced with window definition.
  * If node is window, that window node is merged with parent window node.
  *
  * Window node PARTITION BY and ORDER BY parts are resolved.
  * If window node has frame begin OFFSET or frame end OFFSET specified, they are resolved, and window node frame constants are updated.
  * Window node frame is validated.
  */
ProjectionName QueryAnalyzer::resolveWindow(QueryTreeNodePtr & node, IdentifierResolveScope & scope)
{
    std::string parent_window_name;
    auto * identifier_node = node->as<IdentifierNode>();

    ProjectionName result_projection_name;
    QueryTreeNodePtr parent_window_node;

    if (identifier_node)
        parent_window_name = identifier_node->getIdentifier().getFullName();
    else if (auto * window_node = node->as<WindowNode>())
        parent_window_name = window_node->getParentWindowName();

    if (!parent_window_name.empty())
    {
        auto * nearest_query_scope = scope.getNearestQueryScope();

        if (!nearest_query_scope)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window '{}' does not exists.", parent_window_name);

        auto & scope_window_name_to_window_node = nearest_query_scope->window_name_to_window_node;

        auto window_node_it = scope_window_name_to_window_node.find(parent_window_name);
        if (window_node_it == scope_window_name_to_window_node.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Window '{}' does not exists. In scope {}",
                parent_window_name,
                nearest_query_scope->scope_node->formatASTForErrorMessage());

        parent_window_node = window_node_it->second;

        if (identifier_node)
        {
            node = parent_window_node->clone();
            result_projection_name = parent_window_name;
        }
        else
        {
            mergeWindowWithParentWindow(node, parent_window_node, scope);
        }
    }

    auto & window_node = node->as<WindowNode &>();
    window_node.setParentWindowName({});

    ProjectionNames partition_by_projection_names = resolveExpressionNodeList(window_node.getPartitionByNode(),
        scope,
        false /*allow_lambda_expression*/,
        false /*allow_table_expression*/);

    ProjectionNames order_by_projection_names = resolveSortNodeList(window_node.getOrderByNode(), scope);

    ProjectionNames frame_begin_offset_projection_names;
    ProjectionNames frame_end_offset_projection_names;

    if (window_node.hasFrameBeginOffset())
    {
        frame_begin_offset_projection_names = resolveExpressionNode(window_node.getFrameBeginOffsetNode(),
            scope,
            false /*allow_lambda_expression*/,
            false /*allow_table_expression*/);

        const auto * window_frame_begin_constant_node = window_node.getFrameBeginOffsetNode()->as<ConstantNode>();
        if (!window_frame_begin_constant_node || !isNativeNumber(removeNullable(window_frame_begin_constant_node->getResultType())))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Window frame begin OFFSET expression must be constant with numeric type. Actual {}. In scope {}",
                window_node.getFrameBeginOffsetNode()->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());

        window_node.getWindowFrame().begin_offset = window_frame_begin_constant_node->getValue();
        if (frame_begin_offset_projection_names.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Window FRAME begin offset expected 1 projection name. Actual {}",
                frame_begin_offset_projection_names.size());
    }

    if (window_node.hasFrameEndOffset())
    {
        frame_end_offset_projection_names = resolveExpressionNode(window_node.getFrameEndOffsetNode(),
            scope,
            false /*allow_lambda_expression*/,
            false /*allow_table_expression*/);

        const auto * window_frame_end_constant_node = window_node.getFrameEndOffsetNode()->as<ConstantNode>();
        if (!window_frame_end_constant_node || !isNativeNumber(removeNullable(window_frame_end_constant_node->getResultType())))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Window frame begin OFFSET expression must be constant with numeric type. Actual {}. In scope {}",
                window_node.getFrameEndOffsetNode()->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());

        window_node.getWindowFrame().end_offset = window_frame_end_constant_node->getValue();
        if (frame_end_offset_projection_names.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Window FRAME begin offset expected 1 projection name. Actual {}",
                frame_end_offset_projection_names.size());
    }

    window_node.getWindowFrame().checkValid();

    if (result_projection_name.empty())
    {
        result_projection_name = calculateWindowProjectionName(node,
            parent_window_node,
            parent_window_name,
            partition_by_projection_names,
            order_by_projection_names,
            frame_begin_offset_projection_names.empty() ? "" : frame_begin_offset_projection_names.front(),
            frame_end_offset_projection_names.empty() ? "" : frame_end_offset_projection_names.front());
    }

    return result_projection_name;
}

/** Resolve lambda function.
  * This function modified lambda_node during resolve. It is caller responsibility to clone lambda before resolve
  * if it is needed for later use.
  *
  * Lambda body expression result projection names is used as lambda projection names.
  *
  * Lambda expression can be resolved into list node. It is caller responsibility to handle it properly.
  *
  * lambda_node - node that must have LambdaNode type.
  * lambda_node_to_resolve - lambda node to resolve that must have LambdaNode type.
  * arguments - lambda arguments.
  * scope - lambda scope. It is client responsibility to create it.
  *
  * Resolve steps:
  * 1. Validate arguments.
  * 2. Register lambda node in lambdas in resolve process. This is necessary to prevent recursive lambda resolving.
  * 3. Initialize scope with lambda aliases.
  * 4. Validate lambda argument names, and scope expressions.
  * 5. Resolve lambda body expression.
  * 6. Deregister lambda node from lambdas in resolve process.
  */
ProjectionNames QueryAnalyzer::resolveLambda(const QueryTreeNodePtr & lambda_node,
    const QueryTreeNodePtr & lambda_node_to_resolve,
    const QueryTreeNodes & lambda_arguments,
    IdentifierResolveScope & scope)
{
    auto & lambda_to_resolve = lambda_node_to_resolve->as<LambdaNode &>();
    auto & lambda_arguments_nodes = lambda_to_resolve.getArguments().getNodes();
    size_t lambda_arguments_nodes_size = lambda_arguments_nodes.size();

    /** Register lambda as being resolved, to prevent recursive lambdas resolution.
      * Example: WITH (x -> x + lambda_2(x)) AS lambda_1, (x -> x + lambda_1(x)) AS lambda_2 SELECT 1;
      */
    auto it = lambdas_in_resolve_process.find(lambda_node.get());
    if (it != lambdas_in_resolve_process.end())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Recursive lambda {}. In scope {}",
            lambda_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());
    lambdas_in_resolve_process.emplace(lambda_node.get());

    size_t arguments_size = lambda_arguments.size();
    if (lambda_arguments_nodes_size != arguments_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Lambda {} expect {} arguments. Actual {}. In scope {}",
            lambda_to_resolve.formatASTForErrorMessage(),
            arguments_size,
            lambda_arguments_nodes_size,
            scope.scope_node->formatASTForErrorMessage());

    /// Initialize aliases in lambda scope
    QueryExpressionsAliasVisitor visitor(scope);
    visitor.visit(lambda_to_resolve.getExpression());

    /** Replace lambda arguments with new arguments.
      * Additionally validate that there are no aliases with same name as lambda arguments.
      * Arguments are registered in current scope expression_argument_name_to_node map.
      */
    QueryTreeNodes lambda_new_arguments_nodes;
    lambda_new_arguments_nodes.reserve(lambda_arguments_nodes_size);

    for (size_t i = 0; i < lambda_arguments_nodes_size; ++i)
    {
        auto & lambda_argument_node = lambda_arguments_nodes[i];
        auto & lambda_argument_node_typed = lambda_argument_node->as<IdentifierNode &>();
        const auto & lambda_argument_name = lambda_argument_node_typed.getIdentifier().getFullName();

        bool has_expression_node = scope.alias_name_to_expression_node.contains(lambda_argument_name);
        bool has_alias_node = scope.alias_name_to_lambda_node.contains(lambda_argument_name);

        if (has_expression_node || has_alias_node)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Alias name '{}' inside lambda {} cannot have same name as lambda argument. In scope {}",
                lambda_argument_name,
                lambda_argument_node_typed.formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }

        scope.expression_argument_name_to_node.emplace(lambda_argument_name, lambda_arguments[i]);
        lambda_new_arguments_nodes.push_back(lambda_arguments[i]);
    }

    lambda_to_resolve.getArguments().getNodes() = std::move(lambda_new_arguments_nodes);

    /// Lambda body expression is resolved as standard query expression node.
    auto result_projection_names = resolveExpressionNode(lambda_to_resolve.getExpression(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

    lambdas_in_resolve_process.erase(lambda_node.get());

    return result_projection_names;
}

/** Resolve function node in scope.
  * During function node resolve, function node can be replaced with another expression (if it match lambda or sql user defined function),
  * with constant (if it allow constant folding), or with expression list. It is caller responsibility to handle such cases appropriately.
  *
  * Steps:
  * 1. Resolve function parameters. Validate that each function parameter must be constant node.
  * 2. Try to lookup function as lambda in current scope. If it is lambda we can skip `in` and `count` special handling.
  * 3. If function is count function, that take unqualified ASTERISK matcher, remove it from its arguments. Example: SELECT count(*) FROM test_table;
  * 4. If function is `IN` function, then right part of `IN` function is replaced as subquery.
  * 5. Resolve function arguments list, lambda expressions are allowed as function arguments.
  * For `IN` function table expressions are allowed as function arguments.
  * 6. Initialize argument_columns, argument_types, function_lambda_arguments_indexes arrays from function arguments.
  * 7. If function name identifier was not resolved as function in current scope, try to lookup lambda from sql user defined functions factory.
  * 8. If function was resolve as lambda from step 2 or 7, then resolve lambda using function arguments and replace function node with lambda result.
  * After than function node is resolved.
  * 9. If function was not resolved during step 6 as lambda, then try to resolve function as window function or executable user defined function
  * or ordinary function or aggregate function.
  *
  * If function is resolved as window function or executable user defined function or aggregate function, function node is resolved
  * no additional special handling is required.
  *
  * 8. If function was resolved as non aggregate function. Then if some of function arguments are lambda expressions, their result types need to be initialized and
  * they must be resolved.
  * 9. If function is suitable for constant folding, try to perform constant folding for function node.
  */
ProjectionNames QueryAnalyzer::resolveFunction(QueryTreeNodePtr & node, IdentifierResolveScope & scope)
{
    FunctionNodePtr function_node_ptr = std::static_pointer_cast<FunctionNode>(node);
    auto function_name = function_node_ptr->getFunctionName();

    /// Resolve function parameters

    auto parameters_projection_names = resolveExpressionNodeList(function_node_ptr->getParametersNode(),
        scope,
        false /*allow_lambda_expression*/,
        false /*allow_table_expression*/);

    /// Convert function parameters into constant parameters array

    Array parameters;

    auto & parameters_nodes = function_node_ptr->getParameters().getNodes();
    parameters.reserve(parameters_nodes.size());

    for (auto & parameter_node : parameters_nodes)
    {
        const auto * constant_node = parameter_node->as<ConstantNode>();
        if (!constant_node)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Parameter for function {} expected to have constant value. Actual {}. In scope {}",
            function_name,
            parameter_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());

        parameters.push_back(constant_node->getValue());
    }

    //// If function node is not window function try to lookup function node name as lambda identifier.
    QueryTreeNodePtr lambda_expression_untyped;
    if (!function_node_ptr->isWindowFunction())
    {
        auto function_lookup_result = tryResolveIdentifier({Identifier{function_name}, IdentifierLookupContext::FUNCTION}, scope);
        lambda_expression_untyped = function_lookup_result.resolved_identifier;
    }

    bool is_special_function_in = false;
    bool is_special_function_dict_get_or_join_get = false;
    bool is_special_function_exists = false;

    if (!lambda_expression_untyped)
    {
        is_special_function_in = isNameOfInFunction(function_name);
        is_special_function_dict_get_or_join_get = functionIsJoinGet(function_name) || functionIsDictGet(function_name);
        is_special_function_exists = function_name == "exists";

        /// Handle SELECT count(*) FROM test_table
        if (function_name == "count" && function_node_ptr->getArguments().getNodes().size() == 1)
        {
            auto * matcher_node = function_node_ptr->getArguments().getNodes().front()->as<MatcherNode>();
            if (matcher_node && matcher_node->isUnqualified())
                function_node_ptr->getArguments().getNodes().clear();
        }
    }

    /** Special functions dictGet and its variations and joinGet can be executed when first argument is identifier.
      * Example: SELECT dictGet(identifier, 'value', toUInt64(0));
      *
      * Try to resolve identifier as expression identifier and if it is resolved use it.
      * Example: WITH 'dict_name' AS identifier SELECT dictGet(identifier, 'value', toUInt64(0));
      *
      * Otherwise replace identifier with identifier full name constant.
      * Validation that dictionary exists or table exists will be performed during function `getReturnType` method call.
      */
    if (is_special_function_dict_get_or_join_get &&
        !function_node_ptr->getArguments().getNodes().empty() &&
        function_node_ptr->getArguments().getNodes()[0]->getNodeType() == QueryTreeNodeType::IDENTIFIER)
    {
        auto & first_argument = function_node_ptr->getArguments().getNodes()[0];
        auto & identifier_node = first_argument->as<IdentifierNode &>();
        IdentifierLookup identifier_lookup{identifier_node.getIdentifier(), IdentifierLookupContext::EXPRESSION};
        auto resolve_result = tryResolveIdentifier(identifier_lookup, scope);

        if (resolve_result.isResolved())
            first_argument = std::move(resolve_result.resolved_identifier);
        else
            first_argument = std::make_shared<ConstantNode>(identifier_node.getIdentifier().getFullName());
    }

    /// Resolve function arguments

    bool allow_table_expressions = is_special_function_in || is_special_function_exists;
    auto arguments_projection_names = resolveExpressionNodeList(function_node_ptr->getArgumentsNode(),
        scope,
        true /*allow_lambda_expression*/,
        allow_table_expressions /*allow_table_expression*/);

    if (is_special_function_exists)
    {
        /// Rewrite EXISTS (subquery) into 1 IN (SELECT 1 FROM (subquery) LIMIT 1).
        auto & exists_subquery_argument = function_node_ptr->getArguments().getNodes().at(0);

        auto constant_data_type = std::make_shared<DataTypeUInt64>();

        auto in_subquery = std::make_shared<QueryNode>();
        in_subquery->getProjection().getNodes().push_back(std::make_shared<ConstantNode>(1UL, constant_data_type));
        in_subquery->getJoinTree() = exists_subquery_argument;
        in_subquery->getLimit() = std::make_shared<ConstantNode>(1UL, constant_data_type);
        in_subquery->resolveProjectionColumns({NameAndTypePair("1", constant_data_type)});
        in_subquery->setIsSubquery(true);

        function_node_ptr = std::make_shared<FunctionNode>("in");
        function_node_ptr->getArguments().getNodes() = {std::make_shared<ConstantNode>(1UL, constant_data_type), in_subquery};
        node = function_node_ptr;
        function_name = "in";

        is_special_function_in = true;
    }

    auto & function_node = *function_node_ptr;

    /// Replace right IN function argument if it is table or table function with subquery that read ordinary columns
    if (is_special_function_in)
    {
        auto & function_in_arguments_nodes = function_node.getArguments().getNodes();
        if (function_in_arguments_nodes.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} expects 2 arguments", function_name);

        auto & in_second_argument = function_in_arguments_nodes[1];
        auto * table_node = in_second_argument->as<TableNode>();
        auto * table_function_node = in_second_argument->as<TableFunctionNode>();
        auto * query_node = in_second_argument->as<QueryNode>();
        auto * union_node = in_second_argument->as<UnionNode>();

        if (table_node && dynamic_cast<StorageSet *>(table_node->getStorage().get()) != nullptr)
        {
            /// If table is already prepared set, we do not replace it with subquery
        }
        else if (table_node || table_function_node)
        {
            const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();
            auto columns_to_select = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::Ordinary));

            size_t columns_to_select_size = columns_to_select.size();

            auto column_nodes_to_select = std::make_shared<ListNode>();
            column_nodes_to_select->getNodes().reserve(columns_to_select_size);

            NamesAndTypes projection_columns;
            projection_columns.reserve(columns_to_select_size);

            for (auto & column : columns_to_select)
            {
                column_nodes_to_select->getNodes().emplace_back(std::make_shared<ColumnNode>(column, in_second_argument));
                projection_columns.emplace_back(column.name, column.type);
            }

            auto in_second_argument_query_node = std::make_shared<QueryNode>();
            in_second_argument_query_node->setIsSubquery(true);
            in_second_argument_query_node->getProjectionNode() = std::move(column_nodes_to_select);
            in_second_argument_query_node->getJoinTree() = std::move(in_second_argument);
            in_second_argument_query_node->resolveProjectionColumns(std::move(projection_columns));

            in_second_argument = std::move(in_second_argument_query_node);
        }
        else if (query_node || union_node)
        {
            IdentifierResolveScope subquery_scope(in_second_argument, &scope /*parent_scope*/);
            subquery_scope.subquery_depth = scope.subquery_depth + 1;

            if (query_node)
                resolveQuery(in_second_argument, subquery_scope);
            else if (union_node)
                resolveUnion(in_second_argument, subquery_scope);
        }
    }

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

        /** If function argument is lambda, save lambda argument index and initialize argument type as DataTypeFunction
          * where function argument types are initialized with empty array of lambda arguments size.
          */
        if (const auto * lambda_node = function_argument->as<const LambdaNode>())
        {
            size_t lambda_arguments_size = lambda_node->getArguments().getNodes().size();
            argument_column.type = std::make_shared<DataTypeFunction>(DataTypes(lambda_arguments_size, nullptr), nullptr);
            function_lambda_arguments_indexes.push_back(function_argument_index);
        }
        else if (is_special_function_in &&
            (function_argument->getNodeType() == QueryTreeNodeType::TABLE ||
            function_argument->getNodeType() == QueryTreeNodeType::QUERY ||
            function_argument->getNodeType() == QueryTreeNodeType::UNION))
        {
            argument_column.type = std::make_shared<DataTypeSet>();
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

        const auto * constant_node = function_argument->as<ConstantNode>();
        if (constant_node)
        {
            argument_column.column = constant_node->getResultType()->createColumnConst(1, constant_node->getValue());
            argument_column.type = constant_node->getResultType();
        }
        else
        {
            all_arguments_constants = false;
        }

        argument_types.push_back(argument_column.type);
        argument_columns.emplace_back(std::move(argument_column));
    }

    /// Calculate function projection name
    ProjectionNames result_projection_names = {calculateFunctionProjectionName(node, parameters_projection_names, arguments_projection_names)};

    /** Try to resolve function as
      * 1. Lambda function in current scope. Example: WITH (x -> x + 1) AS lambda SELECT lambda(1);
      * 2. Lambda function from sql user defined functions.
      * 3. Special `untuple` function.
      * 4. Special `grouping` function.
      * 5. Window function.
      * 6. Executable user defined function.
      * 7. Ordinary function.
      * 8. Aggregate function.
      *
      * TODO: Provide better error hints.
      */
    if (!function_node.isWindowFunction())
    {
        if (!lambda_expression_untyped)
            lambda_expression_untyped = tryGetLambdaFromSQLUserDefinedFunctions(function_node.getFunctionName(), scope.context);

        /** If function is resolved as lambda.
          * Clone lambda before resolve.
          * Initialize lambda arguments as function arguments.
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
            ProjectionNames lambda_projection_names = resolveLambda(lambda_expression_untyped, lambda_expression_clone, function_arguments, lambda_scope);

            auto & resolved_lambda = lambda_expression_clone->as<LambdaNode &>();
            node = resolved_lambda.getExpression();

            if (node->getNodeType() == QueryTreeNodeType::LIST)
                result_projection_names = std::move(lambda_projection_names);

            return result_projection_names;
        }

        if (function_name == "untuple")
        {
            /// Special handling of `untuple` function

            if (function_arguments.size() != 1)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Function 'untuple' must have 1 argument. In scope {}",
                    scope.scope_node->formatASTForErrorMessage());

            const auto & untuple_argument = function_arguments[0];
            auto result_type = untuple_argument->getResultType();
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
                tuple_element_function->getArguments().getNodes().push_back(untuple_argument);
                tuple_element_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(element_name));

                QueryTreeNodePtr function_query_node = tuple_element_function;
                resolveFunction(function_query_node, scope);

                result_list->getNodes().push_back(std::move(function_query_node));
            }

            auto untuple_argument_projection_name = arguments_projection_names.at(0);
            result_projection_names.clear();

            for (const auto & element_name : element_names)
            {
                if (node->hasAlias())
                    result_projection_names.push_back(node->getAlias() + '.' + element_name);
                else
                    result_projection_names.push_back(fmt::format("tupleElement({}, '{}')", untuple_argument_projection_name, element_name));
            }

            node = std::move(result_list);
            return result_projection_names;
        }
        else if (function_name == "grouping")
        {
            /// It is responsibility of planner to perform additional handling of grouping function
            if (function_arguments_size == 0)
                throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                    "Function GROUPING expects at least one argument");
            else if (function_arguments_size > 64)
                throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                    "Function GROUPING can have up to 64 arguments, but {} provided",
                    function_arguments_size);

            bool force_grouping_standard_compatibility = scope.context->getSettingsRef().force_grouping_standard_compatibility;
            auto grouping_function = std::make_shared<FunctionGrouping>(force_grouping_standard_compatibility);
            auto grouping_function_adaptor = std::make_shared<FunctionToOverloadResolverAdaptor>(std::move(grouping_function));
            function_node.resolveAsFunction(std::move(grouping_function_adaptor), std::make_shared<DataTypeUInt64>());
            return result_projection_names;
        }
    }

    if (function_node.isWindowFunction())
    {
        if (!AggregateFunctionFactory::instance().isAggregateFunctionName(function_name))
        {
            std::string error_message = fmt::format("Aggregate function with name {} does not exists. In scope {}",
               function_name,
               scope.scope_node->formatASTForErrorMessage());

            AggregateFunctionFactory::instance().appendHintsMessage(error_message, function_name);
            throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION, error_message);
        }

        AggregateFunctionProperties properties;
        auto aggregate_function = AggregateFunctionFactory::instance().get(function_name, argument_types, parameters, properties);

        function_node.resolveAsWindowFunction(aggregate_function, aggregate_function->getReturnType());

        bool window_node_is_identifier = function_node.getWindowNode()->getNodeType() == QueryTreeNodeType::IDENTIFIER;
        ProjectionName window_projection_name = resolveWindow(function_node.getWindowNode(), scope);

        if (window_node_is_identifier)
            result_projection_names[0] += " OVER " + window_projection_name;
        else
            result_projection_names[0] += " OVER (" + window_projection_name + ')';

        return result_projection_names;
    }

    FunctionOverloadResolverPtr function = UserDefinedExecutableFunctionFactory::instance().tryGet(function_name, scope.context, parameters);

    if (!function)
        function = FunctionFactory::instance().tryGet(function_name, scope.context);

    if (!function)
    {
        if (!AggregateFunctionFactory::instance().isAggregateFunctionName(function_name))
        {
            std::vector<std::string> possible_function_names;

            auto function_names = UserDefinedExecutableFunctionFactory::instance().getRegisteredNames(scope.context);
            possible_function_names.insert(possible_function_names.end(), function_names.begin(), function_names.end());

            function_names = UserDefinedSQLFunctionFactory::instance().getAllRegisteredNames();
            possible_function_names.insert(possible_function_names.end(), function_names.begin(), function_names.end());

            function_names = FunctionFactory::instance().getAllRegisteredNames();
            possible_function_names.insert(possible_function_names.end(), function_names.begin(), function_names.end());

            function_names = AggregateFunctionFactory::instance().getAllRegisteredNames();
            possible_function_names.insert(possible_function_names.end(), function_names.begin(), function_names.end());

            for (auto & [name, lambda_node] : scope.alias_name_to_lambda_node)
            {
                if (lambda_node->getNodeType() == QueryTreeNodeType::LAMBDA)
                    possible_function_names.push_back(name);
            }

            NamePrompter<2> name_prompter;
            auto hints = name_prompter.getHints(function_name, possible_function_names);

            throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
                "Function with name {} does not exists. In scope {}{}",
                function_name,
                scope.scope_node->formatASTForErrorMessage(),
                getHintsErrorMessageSuffix(hints));
        }

        AggregateFunctionProperties properties;
        auto aggregate_function = AggregateFunctionFactory::instance().get(function_name, argument_types, parameters, properties);
        function_node.resolveAsAggregateFunction(aggregate_function, aggregate_function->getReturnType());
        return result_projection_names;
    }

    /** For lambda arguments we need to initialize lambda argument types DataTypeFunction using `getLambdaArgumentTypes` function.
      * Then each lambda arguments are initialized with columns, where column source is lambda.
      * This information is important for later steps of query processing.
      * Example: SELECT arrayMap(x -> x + 1, [1, 2, 3]).
      * lambda node x -> x + 1 identifier x is resolved as column where source is lambda node.
      */
    bool has_lambda_arguments = !function_lambda_arguments_indexes.empty();
    if (has_lambda_arguments)
    {
        function->getLambdaArgumentTypes(argument_types);

        ProjectionNames lambda_projection_names;
        for (auto & function_lambda_argument_index : function_lambda_arguments_indexes)
        {
            auto & lambda_argument = function_arguments[function_lambda_argument_index];
            auto lambda_to_resolve = lambda_argument->clone();
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
                    "Function {} function data type for lambda argument with index {} arguments size mismatch. Actual {}. Expected {}. In scope {}",
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
            lambda_projection_names = resolveLambda(lambda_argument, lambda_to_resolve, lambda_arguments, lambda_scope);

            if (auto * lambda_list_node_result = lambda_to_resolve_typed.getExpression()->as<ListNode>())
            {
                size_t lambda_list_node_result_nodes_size = lambda_list_node_result->getNodes().size();

                if (lambda_list_node_result_nodes_size != 1)
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                        "Lambda as function argument resolved as list node with size {}. Expected 1. In scope {}",
                        lambda_list_node_result_nodes_size,
                        lambda_to_resolve->formatASTForErrorMessage());

                lambda_to_resolve_typed.getExpression() = lambda_list_node_result->getNodes().front();
            }

            if (arguments_projection_names.at(function_lambda_argument_index) == PROJECTION_NAME_PLACEHOLDER)
            {
                size_t lambda_projection_names_size =lambda_projection_names.size();
                if (lambda_projection_names_size != 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Lambda argument inside function expected to have 1 projection name. Actual {}",
                        lambda_projection_names_size);

                WriteBufferFromOwnString lambda_argument_projection_name_buffer;
                lambda_argument_projection_name_buffer << "lambda(";
                lambda_argument_projection_name_buffer << "tuple(";

                size_t lambda_argument_names_size = lambda_argument_names.size();

                for (size_t i = 0; i < lambda_argument_names_size; ++i)
                {
                    const auto & lambda_argument_name = lambda_argument_names[i];
                    lambda_argument_projection_name_buffer << lambda_argument_name;

                    if (i + 1 != lambda_argument_names_size)
                        lambda_argument_projection_name_buffer << ", ";
                }

                lambda_argument_projection_name_buffer << "), ";
                lambda_argument_projection_name_buffer << lambda_projection_names[0];
                lambda_argument_projection_name_buffer << ")";

                lambda_projection_names.clear();

                arguments_projection_names[function_lambda_argument_index] = lambda_argument_projection_name_buffer.str();
            }

            argument_types[function_lambda_argument_index] = std::make_shared<DataTypeFunction>(function_data_type_argument_types, lambda_to_resolve->getResultType());
            argument_columns[function_lambda_argument_index].type = argument_types[function_lambda_argument_index];
            function_arguments[function_lambda_argument_index] = std::move(lambda_to_resolve);
        }

        /// Recalculate function projection name after lambda resolution
        result_projection_names = {calculateFunctionProjectionName(node, parameters_projection_names, arguments_projection_names)};
    }

    /** Create SET column for special function IN to allow constant folding
      * if left and right arguments are constants.
      *
      * Example: SELECT * FROM test_table LIMIT 1 IN 1;
      */
    if (is_special_function_in)
    {
        const auto * first_argument_constant_node = function_arguments[0]->as<ConstantNode>();
        const auto * second_argument_constant_node = function_arguments[1]->as<ConstantNode>();

        if (first_argument_constant_node && second_argument_constant_node)
        {
            const auto & first_argument_constant_type = first_argument_constant_node->getResultType();
            const auto & second_argument_constant_literal = second_argument_constant_node->getValue();
            const auto & second_argument_constant_type = second_argument_constant_node->getResultType();

            auto set = makeSetForConstantValue(first_argument_constant_type,
                second_argument_constant_literal,
                second_argument_constant_type,
                scope.context->getSettingsRef());

            /// Create constant set column for constant folding

            auto column_set = ColumnSet::create(1, std::move(set));
            argument_columns[1].column = ColumnConst::create(std::move(column_set), 1);
        }
    }

    std::shared_ptr<ConstantValue> constant_value;

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
                Field column_constant_value;
                column->get(0, column_constant_value);
                constant_value = std::make_shared<ConstantValue>(std::move(column_constant_value), result_type);
            }
        }
    }
    catch (Exception & e)
    {
        e.addMessage("In scope {}", scope.scope_node->formatASTForErrorMessage());
        throw;
    }

    function_node.resolveAsFunction(std::move(function), std::move(result_type));

    if (constant_value)
        node = std::make_shared<ConstantNode>(std::move(constant_value), node);

    return result_projection_names;
}

/** Resolve expression node.
  * Argument node can be replaced with different node, or even with list node in case of matcher resolution.
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
  *
  * If allow_table_expression = true and node is query node, then it is not evaluated as scalar subquery.
  * Although if node is identifier that is resolved into query node that query is evaluated as scalar subquery.
  * SELECT id, (SELECT 1) AS c FROM test_table WHERE a IN c;
  * SELECT id, FROM test_table WHERE a IN (SELECT 1);
  *
  * 3. Special case identifier node.
  * Try resolve it as expression identifier.
  * Then if allow_lambda_expression = true try to resolve it as function.
  * Then if allow_table_expression = true try to resolve it as table expression.
  *
  * 4. If node has alias, update its value in scope alias map. Deregister alias from expression_aliases_in_resolve_process.
  */
ProjectionNames QueryAnalyzer::resolveExpressionNode(QueryTreeNodePtr & node, IdentifierResolveScope & scope, bool allow_lambda_expression, bool allow_table_expression)
{
    auto resolved_expression_it = resolved_expressions.find(node);
    if (resolved_expression_it != resolved_expressions.end())
    {
        /** There can be edge case, when subquery for IN function is resolved multiple times in different context.
          * SELECT id IN (subquery AS value), value FROM test_table;
          * When we start to resolve `value` identifier, subquery is already resolved but constant folding is not performed.
          */
        auto node_type = node->getNodeType();
        if (!allow_table_expression && (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION))
        {
            IdentifierResolveScope subquery_scope(node, &scope /*parent_scope*/);
            subquery_scope.subquery_depth = scope.subquery_depth + 1;

            evaluateScalarSubqueryIfNeeded(node, subquery_scope.subquery_depth, subquery_scope.context);
        }

        return resolved_expression_it->second;
    }

    String node_alias = node->getAlias();
    ProjectionNames result_projection_names;

    if (node_alias.empty())
    {
        auto projection_name_it = node_to_projection_name.find(node);
        if (projection_name_it != node_to_projection_name.end())
            result_projection_names.push_back(projection_name_it->second);
    }
    else
    {
        result_projection_names.push_back(node_alias);
    }

    /** Do not use alias table if node has alias same as some other node.
      * Example: WITH x -> x + 1 AS lambda SELECT 1 AS lambda;
      * During 1 AS lambda resolve if we use alias table we replace node with x -> x + 1 AS lambda.
      *
      * Do not use alias table if allow_table_expression = true and we resolve query node directly.
      * Example: SELECT a FROM test_table WHERE id IN (SELECT 1) AS a;
      * To support both (SELECT 1) AS expression in projection and (SELECT 1) as subquery in IN, do not use
      * alias table because in alias table subquery could be evaluated as scalar.
      */
    bool use_alias_table = true;
    if (scope.nodes_with_duplicated_aliases.contains(node) || (allow_table_expression && node->getNodeType() == QueryTreeNodeType::QUERY))
        use_alias_table = false;

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

    auto node_type = node->getNodeType();

    switch (node_type)
    {
        case QueryTreeNodeType::IDENTIFIER:
        {
            auto & identifier_node = node->as<IdentifierNode &>();
            auto unresolved_identifier = identifier_node.getIdentifier();
            auto resolve_identifier_expression_result = tryResolveIdentifier({unresolved_identifier, IdentifierLookupContext::EXPRESSION}, scope);
            node = resolve_identifier_expression_result.resolved_identifier;

            if (node && result_projection_names.empty() &&
                (resolve_identifier_expression_result.isResolvedFromJoinTree() || resolve_identifier_expression_result.isResolvedFromExpressionArguments()))
            {
                auto projection_name_it = node_to_projection_name.find(node);
                if (projection_name_it != node_to_projection_name.end())
                    result_projection_names.push_back(projection_name_it->second);
            }

            if (node && !node_alias.empty())
                scope.alias_name_to_lambda_node.erase(node_alias);

            if (!node && allow_lambda_expression)
            {
                node = tryResolveIdentifier({unresolved_identifier, IdentifierLookupContext::FUNCTION}, scope).resolved_identifier;

                if (node && !node_alias.empty())
                    scope.alias_name_to_expression_node.erase(node_alias);
            }

            if (!node && allow_table_expression)
            {
                node = tryResolveIdentifier({unresolved_identifier, IdentifierLookupContext::TABLE_EXPRESSION}, scope).resolved_identifier;

                /// If table identifier is resolved as CTE clone it
                bool resolved_as_cte = node && node->as<QueryNode>() && node->as<QueryNode>()->isCTE();

                if (resolved_as_cte)
                {
                    node = node->clone();
                    node->as<QueryNode &>().setIsCTE(false);
                }
            }

            if (!node)
            {
                std::string message_clarification;
                if (allow_lambda_expression)
                    message_clarification = std::string(" or ") + toStringLowercase(IdentifierLookupContext::FUNCTION);

                if (allow_table_expression)
                    message_clarification = std::string(" or ") + toStringLowercase(IdentifierLookupContext::TABLE_EXPRESSION);

                std::unordered_set<Identifier> valid_identifiers;
                collectScopeWithParentScopesValidIdentifiersForTypoCorrection(unresolved_identifier,
                    scope,
                    true,
                    allow_lambda_expression,
                    allow_table_expression,
                    valid_identifiers);

                auto hints = collectIdentifierTypoHints(unresolved_identifier, valid_identifiers);

                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown {}{} identifier '{}' in scope {}{}",
                    toStringLowercase(IdentifierLookupContext::EXPRESSION),
                    message_clarification,
                    unresolved_identifier.getFullName(),
                    scope.scope_node->formatASTForErrorMessage(),
                    getHintsErrorMessageSuffix(hints));
            }

            if (node->getNodeType() == QueryTreeNodeType::LIST)
            {
                result_projection_names.clear();
                resolved_expression_it = resolved_expressions.find(node);
                if (resolved_expression_it != resolved_expressions.end())
                    return resolved_expression_it->second;
                else
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Identifier '{}' resolve into list node and list node projection names are not initialized. In scope {}",
                        unresolved_identifier.getFullName(),
                        scope.scope_node->formatASTForErrorMessage());
            }

            if (result_projection_names.empty())
                result_projection_names.push_back(unresolved_identifier.getFullName());

            break;
        }
        case QueryTreeNodeType::MATCHER:
        {
            result_projection_names = resolveMatcher(node, scope);
            break;
        }
        case QueryTreeNodeType::LIST:
        {
            /** Edge case if list expression has alias.
              * Matchers cannot have aliases, but `untuple` function can.
              * Example: SELECT a, untuple(CAST(('hello', 1) AS Tuple(name String, count UInt32))) AS a;
              * During resolveFunction `untuple` function is replaced by list of 2 constants 'hello', 1.
              */
            result_projection_names = resolveExpressionNodeList(node, scope, allow_lambda_expression, allow_lambda_expression);
            break;
        }
        case QueryTreeNodeType::CONSTANT:
        {
            if (result_projection_names.empty())
            {
                const auto & constant_node = node->as<ConstantNode &>();
                result_projection_names.push_back(constant_node.getValueStringRepresentation());
            }

            /// Already resolved
            break;
        }
        case QueryTreeNodeType::COLUMN:
        {
            auto & column_node = node->as<ColumnNode &>();
            if (column_node.hasExpression())
                resolveExpressionNode(column_node.getExpression(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

            if (result_projection_names.empty())
                result_projection_names.push_back(column_node.getColumnName());

            break;
        }
        case QueryTreeNodeType::FUNCTION:
        {
            auto function_projection_names = resolveFunction(node, scope);

            if (result_projection_names.empty() || node->getNodeType() == QueryTreeNodeType::LIST)
                result_projection_names = std::move(function_projection_names);

            break;
        }
        case QueryTreeNodeType::LAMBDA:
        {
            if (!allow_lambda_expression)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Lambda {} is not allowed in expression context. In scope {}",
                    node->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            if (result_projection_names.empty())
                result_projection_names.push_back(PROJECTION_NAME_PLACEHOLDER);

            /// Lambda must be resolved by caller
            break;
        }
        case QueryTreeNodeType::QUERY:
            [[fallthrough]];
        case QueryTreeNodeType::UNION:
        {
            IdentifierResolveScope subquery_scope(node, &scope /*parent_scope*/);
            subquery_scope.subquery_depth = scope.subquery_depth + 1;

            if (node_type == QueryTreeNodeType::QUERY)
                resolveQuery(node, subquery_scope);
            else
                resolveUnion(node, subquery_scope);

            if (!allow_table_expression)
                evaluateScalarSubqueryIfNeeded(node, subquery_scope.subquery_depth, subquery_scope.context);

            ++subquery_counter;
            if (result_projection_names.empty())
                result_projection_names.push_back("_subquery_" + std::to_string(subquery_counter));

            break;
        }
        case QueryTreeNodeType::TABLE:
        {
            if (!allow_table_expression)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Table {} is not allowed in expression context. In scope {}",
                    node->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            auto & table_node = node->as<TableNode &>();
            result_projection_names.push_back(table_node.getStorageID().getFullNameNotQuoted());

            break;
        }
        case QueryTreeNodeType::TRANSFORMER:
            [[fallthrough]];
        case QueryTreeNodeType::SORT:
            [[fallthrough]];
        case QueryTreeNodeType::INTERPOLATE:
            [[fallthrough]];
        case QueryTreeNodeType::WINDOW:
            [[fallthrough]];
        case QueryTreeNodeType::TABLE_FUNCTION:
            [[fallthrough]];
        case QueryTreeNodeType::ARRAY_JOIN:
            [[fallthrough]];
        case QueryTreeNodeType::JOIN:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "{} {} is not allowed in expression context. In scope {}",
                node->getNodeType(),
                node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }
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

    resolved_expressions.emplace(node, result_projection_names);

    scope.expressions_in_resolve_process_stack.popNode();
    bool expression_was_root = scope.expressions_in_resolve_process_stack.empty();
    if (expression_was_root)
        scope.non_cached_identifier_lookups_during_expression_resolve.clear();

    return result_projection_names;
}

/** Resolve expression node list.
  * If expression is CTE subquery node it is skipped.
  * If expression is resolved in list, it is flattened into initial node list.
  *
  * Such examples must work:
  * Example: CREATE TABLE test_table (id UInt64, value UInt64) ENGINE=TinyLog; SELECT plus(*) FROM test_table;
  * Example: SELECT *** FROM system.one;
  */
ProjectionNames QueryAnalyzer::resolveExpressionNodeList(QueryTreeNodePtr & node_list, IdentifierResolveScope & scope, bool allow_lambda_expression, bool allow_table_expression)
{
    auto & node_list_typed = node_list->as<ListNode &>();
    size_t node_list_size = node_list_typed.getNodes().size();

    QueryTreeNodes result_nodes;
    result_nodes.reserve(node_list_size);

    ProjectionNames result_projection_names;

    for (auto & node : node_list_typed.getNodes())
    {
        auto node_to_resolve = node;
        auto expression_node_projection_names = resolveExpressionNode(node_to_resolve, scope, allow_lambda_expression, allow_table_expression);

        size_t expected_projection_names_size = 1;
        if (auto * expression_list = node_to_resolve->as<ListNode>())
        {
            expected_projection_names_size = expression_list->getNodes().size();
            for (auto & expression_list_node : expression_list->getNodes())
                result_nodes.push_back(expression_list_node);
        }
        else
        {
            result_nodes.push_back(std::move(node_to_resolve));
        }

        if (expression_node_projection_names.size() != expected_projection_names_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expression nodes list expected {} projection names. Actual {}",
                expected_projection_names_size,
                expression_node_projection_names.size());

        result_projection_names.insert(result_projection_names.end(), expression_node_projection_names.begin(), expression_node_projection_names.end());
        expression_node_projection_names.clear();
    }

    node_list_typed.getNodes() = std::move(result_nodes);

    return result_projection_names;
}

/** Resolve sort columns nodes list.
  */
ProjectionNames QueryAnalyzer::resolveSortNodeList(QueryTreeNodePtr & sort_node_list, IdentifierResolveScope & scope)
{
    ProjectionNames result_projection_names;
    ProjectionNames sort_expression_projection_names;
    ProjectionNames fill_from_expression_projection_names;
    ProjectionNames fill_to_expression_projection_names;
    ProjectionNames fill_step_expression_projection_names;

    auto & sort_node_list_typed = sort_node_list->as<ListNode &>();
    for (auto & node : sort_node_list_typed.getNodes())
    {
        auto & sort_node = node->as<SortNode &>();
        sort_expression_projection_names = resolveExpressionNode(sort_node.getExpression(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

        if (auto * sort_column_list_node = sort_node.getExpression()->as<ListNode>())
        {
            size_t sort_column_list_node_size = sort_column_list_node->getNodes().size();
            if (sort_column_list_node_size != 1)
            {
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Sort column node expression resolved into list with size {}. Expected 1. In scope {}",
                    sort_column_list_node_size,
                    scope.scope_node->formatASTForErrorMessage());
            }

            sort_node.getExpression() = sort_column_list_node->getNodes().front();
        }

        size_t sort_expression_projection_names_size = sort_expression_projection_names.size();
        if (sort_expression_projection_names_size != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Sort expression expected 1 projection name. Actual {}",
                sort_expression_projection_names_size);

        if (sort_node.hasFillFrom())
        {
            fill_from_expression_projection_names = resolveExpressionNode(sort_node.getFillFrom(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

            const auto * constant_node = sort_node.getFillFrom()->as<ConstantNode>();
            if (!constant_node || !isColumnedAsNumber(constant_node->getResultType()))
                throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "Sort FILL FROM expression must be constant with numeric type. Actual {}. In scope {}",
                    sort_node.getFillFrom()->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            size_t fill_from_expression_projection_names_size = fill_from_expression_projection_names.size();
            if (fill_from_expression_projection_names_size != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Sort node FILL FROM expression expected 1 projection name. Actual {}",
                    fill_from_expression_projection_names_size);
        }

        if (sort_node.hasFillTo())
        {
            fill_to_expression_projection_names = resolveExpressionNode(sort_node.getFillTo(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

            const auto * constant_node = sort_node.getFillTo()->as<ConstantNode>();
            if (!constant_node || !isColumnedAsNumber(constant_node->getResultType()))
                throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "Sort FILL TO expression must be constant with numeric type. Actual {}. In scope {}",
                    sort_node.getFillFrom()->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            size_t fill_to_expression_projection_names_size = fill_to_expression_projection_names.size();
            if (fill_to_expression_projection_names_size != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Sort node FILL TO expression expected 1 projection name. Actual {}",
                    fill_to_expression_projection_names_size);
        }

        if (sort_node.hasFillStep())
        {
            fill_step_expression_projection_names = resolveExpressionNode(sort_node.getFillStep(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

            const auto * constant_node = sort_node.getFillStep()->as<ConstantNode>();
            if (!constant_node)
                throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "Sort FILL STEP expression must be constant with numeric or interval type. Actual {}. In scope {}",
                    sort_node.getFillStep()->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            bool is_number = isColumnedAsNumber(constant_node->getResultType());
            bool is_interval = WhichDataType(constant_node->getResultType()).isInterval();
            if (!is_number && !is_interval)
                throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "Sort FILL STEP expression must be constant with numeric or interval type. Actual {}. In scope {}",
                    sort_node.getFillStep()->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            size_t fill_step_expression_projection_names_size = fill_step_expression_projection_names.size();
            if (fill_step_expression_projection_names_size != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Sort FILL STEP expression expected 1 projection name. Actual {}",
                    fill_step_expression_projection_names_size);
        }

        auto sort_column_projection_name = calculateSortColumnProjectionName(node,
            sort_expression_projection_names[0],
            fill_from_expression_projection_names.empty() ? "" : fill_from_expression_projection_names.front(),
            fill_to_expression_projection_names.empty() ? "" : fill_to_expression_projection_names.front(),
            fill_step_expression_projection_names.empty() ? "" : fill_step_expression_projection_names.front());

        result_projection_names.push_back(std::move(sort_column_projection_name));

        sort_expression_projection_names.clear();
        fill_from_expression_projection_names.clear();
        fill_to_expression_projection_names.clear();
        fill_step_expression_projection_names.clear();
    }

    return result_projection_names;
}

/** Resolve interpolate columns nodes list.
  */
void QueryAnalyzer::resolveInterpolateColumnsNodeList(QueryTreeNodePtr & interpolate_node_list, IdentifierResolveScope & scope)
{
    auto & interpolate_node_list_typed = interpolate_node_list->as<ListNode &>();

    for (auto & interpolate_node : interpolate_node_list_typed.getNodes())
    {
        auto & interpolate_node_typed = interpolate_node->as<InterpolateNode &>();

        resolveExpressionNode(interpolate_node_typed.getExpression(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
        resolveExpressionNode(interpolate_node_typed.getInterpolateExpression(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
    }
}

/** Resolve window nodes list.
  */
void QueryAnalyzer::resolveWindowNodeList(QueryTreeNodePtr & window_node_list, IdentifierResolveScope & scope)
{
    auto & window_node_list_typed = window_node_list->as<ListNode &>();
    for (auto & node : window_node_list_typed.getNodes())
        resolveWindow(node, scope);
}

NamesAndTypes QueryAnalyzer::resolveProjectionExpressionNodeList(QueryTreeNodePtr & projection_node_list, IdentifierResolveScope & scope)
{
    ProjectionNames projection_names = resolveExpressionNodeList(projection_node_list, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

    auto projection_nodes = projection_node_list->as<ListNode &>().getNodes();
    size_t projection_nodes_size = projection_nodes.size();

    NamesAndTypes projection_columns;
    projection_columns.reserve(projection_nodes_size);

    for (size_t i = 0; i < projection_nodes_size; ++i)
    {
        auto projection_node = projection_nodes[i];

        if (!isExpressionNodeType(projection_node->getNodeType()))
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Projection node must be constant, function, column, query or union");

        projection_columns.emplace_back(projection_names[i], projection_node->getResultType());
    }

    return projection_columns;
}

/** Initialize query join tree node.
  *
  * 1. Resolve identifiers.
  * 2. Register table, table function, query, union, join, array join nodes in scope table expressions in resolve process.
  */
void QueryAnalyzer::initializeQueryJoinTreeNode(QueryTreeNodePtr & join_tree_node, IdentifierResolveScope & scope)
{
    std::deque<QueryTreeNodePtr *> join_tree_node_ptrs_to_process_queue;
    join_tree_node_ptrs_to_process_queue.push_back(&join_tree_node);

    while (!join_tree_node_ptrs_to_process_queue.empty())
    {
        auto * current_join_tree_node_ptr = join_tree_node_ptrs_to_process_queue.front();
        join_tree_node_ptrs_to_process_queue.pop_front();

        auto & current_join_tree_node = *current_join_tree_node_ptr;
        auto current_join_tree_node_type = current_join_tree_node->getNodeType();

        switch (current_join_tree_node_type)
        {
            case QueryTreeNodeType::IDENTIFIER:
            {
                auto & from_table_identifier = current_join_tree_node->as<IdentifierNode &>();
                auto table_identifier_lookup = IdentifierLookup{from_table_identifier.getIdentifier(), IdentifierLookupContext::TABLE_EXPRESSION};

                auto from_table_identifier_alias = from_table_identifier.getAlias();

                IdentifierResolveSettings resolve_settings;
                /// In join tree initialization ignore join tree as identifier lookup source
                resolve_settings.allow_to_check_join_tree = false;
                /** Disable resolve of subquery during identifier resolution.
                  * Example: SELECT * FROM (SELECT 1) AS t1, t1;
                  * During `t1` identifier resolution we resolve it into subquery SELECT 1, but we want to disable
                  * subquery resolution at this stage, because JOIN TREE of parent query is not resolved.
                  */
                resolve_settings.allow_to_resolve_subquery_during_identifier_resolution = false;

                scope.expressions_in_resolve_process_stack.pushNode(current_join_tree_node);

                auto table_identifier_resolve_result = tryResolveIdentifier(table_identifier_lookup, scope, resolve_settings);

                scope.expressions_in_resolve_process_stack.popNode();
                bool expression_was_root = scope.expressions_in_resolve_process_stack.empty();
                if (expression_was_root)
                    scope.non_cached_identifier_lookups_during_expression_resolve.clear();

                auto resolved_identifier = table_identifier_resolve_result.resolved_identifier;

                if (!resolved_identifier)
                    throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                        "Unknown table expression identifier '{}' in scope {}",
                        from_table_identifier.getIdentifier().getFullName(),
                        scope.scope_node->formatASTForErrorMessage());

                resolved_identifier = resolved_identifier->clone();

                /// Update alias name to table expression map
                auto table_expression_it = scope.alias_name_to_table_expression_node.find(from_table_identifier_alias);
                if (table_expression_it != scope.alias_name_to_table_expression_node.end())
                    table_expression_it->second = resolved_identifier;

                auto table_expression_modifiers = from_table_identifier.getTableExpressionModifiers();

                auto * resolved_identifier_query_node = resolved_identifier->as<QueryNode>();
                auto * resolved_identifier_union_node = resolved_identifier->as<UnionNode>();

                if (resolved_identifier_query_node || resolved_identifier_union_node)
                {
                    if (resolved_identifier_query_node)
                        resolved_identifier_query_node->setIsCTE(false);
                    else
                        resolved_identifier_union_node->setIsCTE(false);

                    if (table_expression_modifiers.has_value())
                    {
                        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                            "Table expression modifiers {} are not supported for subquery {}",
                            table_expression_modifiers->formatForErrorMessage(),
                            resolved_identifier->formatASTForErrorMessage());
                    }
                }
                else if (auto * resolved_identifier_table_node = resolved_identifier->as<TableNode>())
                {
                    if (table_expression_modifiers.has_value())
                        resolved_identifier_table_node->setTableExpressionModifiers(*table_expression_modifiers);
                }
                else if (auto * resolved_identifier_table_function_node = resolved_identifier->as<TableFunctionNode>())
                {
                    if (table_expression_modifiers.has_value())
                        resolved_identifier_table_function_node->setTableExpressionModifiers(*table_expression_modifiers);
                }
                else
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Identifier in JOIN TREE '{}' resolved into unexpected table expression. In scope {}",
                        from_table_identifier.getIdentifier().getFullName(),
                        scope.scope_node->formatASTForErrorMessage());
                }

                auto current_join_tree_node_alias = current_join_tree_node->getAlias();
                resolved_identifier->setAlias(current_join_tree_node_alias);
                current_join_tree_node = resolved_identifier;

                scope.table_expressions_in_resolve_process.insert(current_join_tree_node.get());
                break;
            }
            case QueryTreeNodeType::QUERY:
            {
                scope.table_expressions_in_resolve_process.insert(current_join_tree_node.get());
                break;
            }
            case QueryTreeNodeType::UNION:
            {
                scope.table_expressions_in_resolve_process.insert(current_join_tree_node.get());
                break;
            }
            case QueryTreeNodeType::TABLE_FUNCTION:
            {
                scope.table_expressions_in_resolve_process.insert(current_join_tree_node.get());
                break;
            }
            case QueryTreeNodeType::TABLE:
            {
                scope.table_expressions_in_resolve_process.insert(current_join_tree_node.get());
                break;
            }
            case QueryTreeNodeType::ARRAY_JOIN:
            {
                auto & array_join = current_join_tree_node->as<ArrayJoinNode &>();
                join_tree_node_ptrs_to_process_queue.push_back(&array_join.getTableExpression());
                scope.table_expressions_in_resolve_process.insert(current_join_tree_node.get());
                break;
            }
            case QueryTreeNodeType::JOIN:
            {
                auto & join = current_join_tree_node->as<JoinNode &>();
                join_tree_node_ptrs_to_process_queue.push_back(&join.getLeftTableExpression());
                join_tree_node_ptrs_to_process_queue.push_back(&join.getRightTableExpression());
                scope.table_expressions_in_resolve_process.insert(current_join_tree_node.get());
                break;
            }
            default:
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Query FROM section expected table, table function, query, UNION, ARRAY JOIN or JOIN. Actual {} {}. In scope {}",
                    current_join_tree_node->getNodeTypeName(),
                    current_join_tree_node->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());
            }
        }
    }
}

/// Initialize table expression columns for table expression node
void QueryAnalyzer::initializeTableExpressionColumns(const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope)
{
    auto * table_node = table_expression_node->as<TableNode>();
    auto * query_node = table_expression_node->as<QueryNode>();
    auto * union_node = table_expression_node->as<UnionNode>();
    auto * table_function_node = table_expression_node->as<TableFunctionNode>();

    if (!table_node && !table_function_node && !query_node && !union_node)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
        "Unexpected table expression. Expected table, table function, query or union node. Actual {}. In scope {}",
        table_expression_node->formatASTForErrorMessage(),
        scope.scope_node->formatASTForErrorMessage());

    auto table_expression_data_it = scope.table_expression_node_to_data.find(table_expression_node);
    if (table_expression_data_it != scope.table_expression_node_to_data.end())
        return;

    TableExpressionData table_expression_data;

    if (table_node)
    {
        const auto & table_storage_id = table_node->getStorageID();
        table_expression_data.table_name = table_storage_id.table_name;
        table_expression_data.database_name = table_storage_id.database_name;
        table_expression_data.table_expression_name = table_storage_id.getFullNameNotQuoted();
        table_expression_data.table_expression_description = "table";
    }
    else if (query_node || union_node)
    {
        table_expression_data.table_name = query_node ? query_node->getCTEName() : union_node->getCTEName();
        table_expression_data.table_expression_description = "subquery";
    }
    else if (table_function_node)
    {
        table_expression_data.table_expression_description = "table_function";
    }

    if (table_expression_node->hasAlias())
        table_expression_data.table_expression_name = table_expression_node->getAlias();

    if (table_node || table_function_node)
    {
        const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();

        auto column_names_and_types = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns().withVirtuals());
        const auto & columns_description = storage_snapshot->metadata->getColumns();

        std::vector<std::pair<std::string, ColumnNodePtr>> alias_columns_to_resolve;
        ColumnNameToColumnNodeMap column_name_to_column_node;
        column_name_to_column_node.reserve(column_names_and_types.size());

        /** For ALIAS columns in table we must additionally analyze ALIAS expressions.
          * Example: CREATE TABLE test_table (id UInt64, alias_value_1 ALIAS id + 5);
          *
          * To do that we collect alias columns and build table column name to column node map.
          * For each alias column we build identifier resolve scope, initialize it with table column name to node map
          * and resolve alias column.
          */
        for (const auto & column_name_and_type : column_names_and_types)
        {
            const auto & column_default = columns_description.getDefault(column_name_and_type.name);

            if (column_default && column_default->kind == ColumnDefaultKind::Alias)
            {
                auto column_node = std::make_shared<ColumnNode>(column_name_and_type, buildQueryTree(column_default->expression, scope.context), table_expression_node);
                column_name_to_column_node.emplace(column_name_and_type.name, column_node);
                alias_columns_to_resolve.emplace_back(column_name_and_type.name, column_node);
            }
            else
            {
                auto column_node = std::make_shared<ColumnNode>(column_name_and_type, table_expression_node);
                column_name_to_column_node.emplace(column_name_and_type.name, column_node);
            }
        }

        for (auto & [alias_column_to_resolve_name, alias_column_to_resolve] : alias_columns_to_resolve)
        {
            /** Alias column could be potentially resolved during resolve of other ALIAS column.
              * Example: CREATE TABLE test_table (id UInt64, alias_value_1 ALIAS id + alias_value_2, alias_value_2 ALIAS id + 5) ENGINE=TinyLog;
              *
              * During resolve of alias_value_1, alias_value_2 column will be resolved.
              */
            alias_column_to_resolve = column_name_to_column_node[alias_column_to_resolve_name];

            IdentifierResolveScope alias_column_resolve_scope(alias_column_to_resolve, nullptr /*parent_scope*/);
            alias_column_resolve_scope.column_name_to_column_node = std::move(column_name_to_column_node);
            alias_column_resolve_scope.context = scope.context;

            /// Initialize aliases in alias column scope
            QueryExpressionsAliasVisitor visitor(alias_column_resolve_scope);
            visitor.visit(alias_column_to_resolve->getExpression());

            resolveExpressionNode(alias_column_resolve_scope.scope_node,
                alias_column_resolve_scope,
                false /*allow_lambda_expression*/,
                false /*allow_table_expression*/);

            column_name_to_column_node = std::move(alias_column_resolve_scope.column_name_to_column_node);
            column_name_to_column_node[alias_column_to_resolve_name] = alias_column_to_resolve;
        }

        table_expression_data.column_name_to_column_node = std::move(column_name_to_column_node);
    }
    else if (query_node || union_node)
    {
        auto column_names_and_types = query_node ? query_node->getProjectionColumns() : union_node->computeProjectionColumns();
        table_expression_data.column_name_to_column_node.reserve(column_names_and_types.size());

        for (const auto & column_name_and_type : column_names_and_types)
        {
            auto column_node = std::make_shared<ColumnNode>(column_name_and_type, table_expression_node);
            table_expression_data.column_name_to_column_node.emplace(column_name_and_type.name, column_node);
        }
    }

    table_expression_data.column_identifier_first_parts.reserve(table_expression_data.column_name_to_column_node.size());

    for (auto & [column_name, _] : table_expression_data.column_name_to_column_node)
    {
        Identifier column_name_identifier(column_name);
        table_expression_data.column_identifier_first_parts.insert(column_name_identifier.at(0));
    }

    scope.table_expression_node_to_data.emplace(table_expression_node, std::move(table_expression_data));
}

/** Resolve query join tree.
  *
  * Query join tree must be initialized before calling this function.
  */
void QueryAnalyzer::resolveQueryJoinTreeNode(QueryTreeNodePtr & join_tree_node, IdentifierResolveScope & scope, QueryExpressionsAliasVisitor & expressions_visitor)
{
    auto add_table_expression_alias_into_scope = [&](const QueryTreeNodePtr & table_expression_node)
    {
        const auto & alias_name = table_expression_node->getAlias();
        if (alias_name.empty())
            return;

        auto [it, inserted] = scope.alias_name_to_table_expression_node.emplace(alias_name, table_expression_node);
        if (!inserted)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Duplicate aliases {} for table expressions in FROM section are not allowed. Try to register {}. Already registered {}.",
                alias_name,
                table_expression_node->formatASTForErrorMessage(),
                it->second->formatASTForErrorMessage());
    };

    auto from_node_type = join_tree_node->getNodeType();

    switch (from_node_type)
    {
        case QueryTreeNodeType::QUERY:
            [[fallthrough]];
        case QueryTreeNodeType::UNION:
        {
            IdentifierResolveScope subquery_scope(join_tree_node, &scope);
            subquery_scope.subquery_depth = scope.subquery_depth + 1;

            if (from_node_type == QueryTreeNodeType::QUERY)
                resolveQuery(join_tree_node, subquery_scope);
            else if (from_node_type == QueryTreeNodeType::UNION)
                resolveUnion(join_tree_node, subquery_scope);

            break;
        }
        case QueryTreeNodeType::TABLE_FUNCTION:
        {
            auto & table_function_node = join_tree_node->as<TableFunctionNode &>();
            expressions_visitor.visit(table_function_node.getArgumentsNode());

            const auto & table_function_factory = TableFunctionFactory::instance();
            const auto & table_function_name = table_function_node.getTableFunctionName();

            auto & scope_context = scope.context;

            TableFunctionPtr table_function_ptr = table_function_factory.tryGet(table_function_name, scope_context);
            if (!table_function_ptr)
            {
                auto hints = TableFunctionFactory::instance().getHints(table_function_name);
                if (!hints.empty())
                    throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
                        "Unknown table function {}. Maybe you meant: {}",
                        table_function_name,
                        DB::toString(hints));
                else
                    throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown table function {}", table_function_name);
            }

            if (scope_context->getSettingsRef().use_structure_from_insertion_table_in_table_functions && table_function_ptr->needStructureHint())
            {
                const auto & insertion_table = scope_context->getInsertionTable();
                if (!insertion_table.empty())
                {
                    const auto & structure_hint
                        = DatabaseCatalog::instance().getTable(insertion_table, scope_context)->getInMemoryMetadataPtr()->columns;
                    table_function_ptr->setStructureHint(structure_hint);
                }
            }

            resolveExpressionNodeList(table_function_node.getArgumentsNode(), scope, false /*allow_lambda_expression*/, true /*allow_table_expression*/);

            auto table_function_ast = table_function_node.toAST();
            table_function_ptr->parseArguments(table_function_ast, scope_context);

            auto table_function_storage = table_function_ptr->execute(table_function_ast, scope_context, table_function_ptr->getName());
            table_function_node.resolve(std::move(table_function_ptr), std::move(table_function_storage), scope_context);

            break;
        }
        case QueryTreeNodeType::TABLE:
        {
            break;
        }
        case QueryTreeNodeType::ARRAY_JOIN:
        {
            auto & array_join_node = join_tree_node->as<ArrayJoinNode &>();
            resolveQueryJoinTreeNode(array_join_node.getTableExpression(), scope, expressions_visitor);
            validateJoinTableExpressionWithoutAlias(join_tree_node, array_join_node.getTableExpression(), scope);

            std::unordered_set<String> array_join_column_names;

            /// Wrap array join expressions into column nodes, where array join expression is inner expression.

            for (auto & array_join_expression : array_join_node.getJoinExpressions().getNodes())
            {
                auto array_join_expression_alias = array_join_expression->getAlias();
                if (!array_join_expression_alias.empty() && scope.alias_name_to_expression_node.contains(array_join_expression_alias))
                    throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
                        "ARRAY JOIN expression {} with duplicate alias {}. In scope {}",
                        array_join_expression->formatASTForErrorMessage(),
                        array_join_expression_alias,
                        scope.scope_node->formatASTForErrorMessage());

                /// Add array join expression into scope
                expressions_visitor.visit(array_join_expression);

                resolveExpressionNode(array_join_expression, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

                auto result_type = array_join_expression->getResultType();

                if (!isArray(result_type))
                    throw Exception(ErrorCodes::TYPE_MISMATCH,
                        "ARRAY JOIN {} requires expression with Array type. Actual {}. In scope {}",
                        array_join_node.formatASTForErrorMessage(),
                        result_type->getName(),
                        scope.scope_node->formatASTForErrorMessage());

                result_type = assert_cast<const DataTypeArray &>(*result_type).getNestedType();

                String array_join_column_name;

                if (!array_join_expression_alias.empty())
                {
                    array_join_column_name = array_join_expression_alias;
                }
                else if (auto * array_join_expression_inner_column = array_join_expression->as<ColumnNode>())
                {
                    array_join_column_name = array_join_expression_inner_column->getColumnName();
                }
                else
                {
                    array_join_column_name = "__array_join_expression_" + std::to_string(array_join_expressions_counter);
                    ++array_join_expressions_counter;
                }

                if (array_join_column_names.contains(array_join_column_name))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "ARRAY JOIN {} multiple columns with name {}. In scope {}",
                        array_join_node.formatASTForErrorMessage(),
                        array_join_column_name,
                        scope.scope_node->formatASTForErrorMessage());
                array_join_column_names.emplace(array_join_column_name);

                auto array_join_column = std::make_shared<ColumnNode>(NameAndTypePair{array_join_column_name, result_type}, array_join_expression, join_tree_node);
                array_join_expression = std::move(array_join_column);
                array_join_expression->setAlias(array_join_expression_alias);

                auto it = scope.alias_name_to_expression_node.find(array_join_expression_alias);
                if (it != scope.alias_name_to_expression_node.end())
                    it->second = array_join_expression;
            }

            break;
        }
        case QueryTreeNodeType::JOIN:
        {
            auto & join_node = join_tree_node->as<JoinNode &>();

            resolveQueryJoinTreeNode(join_node.getLeftTableExpression(), scope, expressions_visitor);
            validateJoinTableExpressionWithoutAlias(join_tree_node, join_node.getLeftTableExpression(), scope);

            resolveQueryJoinTreeNode(join_node.getRightTableExpression(), scope, expressions_visitor);
            validateJoinTableExpressionWithoutAlias(join_tree_node, join_node.getRightTableExpression(), scope);

            if (join_node.isUsingJoinExpression())
            {
                auto & join_using_list = join_node.getJoinExpression()->as<ListNode &>();
                std::unordered_set<std::string> join_using_identifiers;

                for (auto & join_using_node : join_using_list.getNodes())
                {
                    auto * identifier_node = join_using_node->as<IdentifierNode>();
                    if (!identifier_node)
                        continue;

                    const auto & identifier_full_name = identifier_node->getIdentifier().getFullName();

                    if (join_using_identifiers.contains(identifier_full_name))
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "JOIN {} identifier '{}' appears more than once in USING clause",
                            join_node.formatASTForErrorMessage(),
                            identifier_full_name);

                    join_using_identifiers.insert(identifier_full_name);

                    IdentifierLookup identifier_lookup {identifier_node->getIdentifier(), IdentifierLookupContext::EXPRESSION};
                    auto result_left_table_expression = tryResolveIdentifierFromJoinTreeNode(identifier_lookup, join_node.getLeftTableExpression(), scope);
                    if (!result_left_table_expression)
                        throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "JOIN {} using identifier '{}' cannot be resolved from left table expression. In scope {}",
                            join_node.formatASTForErrorMessage(),
                            identifier_full_name,
                            scope.scope_node->formatASTForErrorMessage());

                    auto result_right_table_expression = tryResolveIdentifierFromJoinTreeNode(identifier_lookup, join_node.getRightTableExpression(), scope);
                    if (!result_right_table_expression)
                        throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "JOIN {} using identifier '{}' cannot be resolved from right table expression. In scope {}",
                            join_node.formatASTForErrorMessage(),
                            identifier_full_name,
                            scope.scope_node->formatASTForErrorMessage());

                    DataTypePtr common_type = tryGetLeastSupertype(DataTypes{result_left_table_expression->getResultType(), result_right_table_expression->getResultType()});

                    if (!common_type)
                        throw Exception(ErrorCodes::NO_COMMON_TYPE,
                            "JOIN {} cannot infer common type for {} and {} in USING for identifier '{}'. In scope {}",
                            join_node.formatASTForErrorMessage(),
                            result_left_table_expression->getResultType()->getName(),
                            result_right_table_expression->getResultType()->getName(),
                            identifier_full_name,
                            scope.scope_node->formatASTForErrorMessage());

                    NameAndTypePair join_using_columns_common_name_and_type(identifier_full_name, common_type);
                    ListNodePtr join_using_expression = std::make_shared<ListNode>(QueryTreeNodes{result_left_table_expression, result_right_table_expression});
                    auto join_using_column = std::make_shared<ColumnNode>(join_using_columns_common_name_and_type, std::move(join_using_expression), join_tree_node);

                    join_using_node = std::move(join_using_column);
                }
            }
            else if (join_node.getJoinExpression())
            {
                expressions_visitor.visit(join_node.getJoinExpression());
                auto join_expression = join_node.getJoinExpression();
                resolveExpressionNode(join_expression, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
                join_node.getJoinExpression() = std::move(join_expression);
            }

            break;
        }
        case QueryTreeNodeType::IDENTIFIER:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Identifiers in FROM section must be already resolved. In scope {}",
                join_tree_node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }
        default:
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Query FROM section expected table, table function, query, ARRAY JOIN or JOIN. Actual {}. In scope {}",
                join_tree_node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }
    }

    auto join_tree_node_type = join_tree_node->getNodeType();
    if (isTableExpressionNodeType(join_tree_node_type))
    {
        validateTableExpressionModifiers(join_tree_node, scope);
        initializeTableExpressionColumns(join_tree_node, scope);
    }

    add_table_expression_alias_into_scope(join_tree_node);
    scope.table_expressions_in_resolve_process.erase(join_tree_node.get());
}

class ValidateGroupByColumnsVisitor : public ConstInDepthQueryTreeVisitor<ValidateGroupByColumnsVisitor>
{
public:
    ValidateGroupByColumnsVisitor(const QueryTreeNodes & group_by_keys_nodes_, const IdentifierResolveScope & scope_)
        : group_by_keys_nodes(group_by_keys_nodes_)
        , scope(scope_)
    {}

    void visitImpl(const QueryTreeNodePtr & node)
    {
        auto query_tree_node_type = node->getNodeType();
        if (query_tree_node_type == QueryTreeNodeType::CONSTANT ||
            query_tree_node_type == QueryTreeNodeType::SORT ||
            query_tree_node_type == QueryTreeNodeType::INTERPOLATE)
            return;

        auto * function_node = node->as<FunctionNode>();
        if (function_node && function_node->getFunctionName() == "grouping")
        {
            auto & grouping_function_arguments_nodes = function_node->getArguments().getNodes();
            for (auto & grouping_function_arguments_node : grouping_function_arguments_nodes)
            {
                bool found_argument_in_group_by_keys = false;

                for (const auto & group_by_key_node : group_by_keys_nodes)
                {
                    if (grouping_function_arguments_node->isEqual(*group_by_key_node))
                    {
                        found_argument_in_group_by_keys = true;
                        break;
                    }
                }

                if (!found_argument_in_group_by_keys)
                    throw Exception(ErrorCodes::NOT_AN_AGGREGATE,
                        "GROUPING function argument {} is not in GROUP BY. In scope {}",
                        grouping_function_arguments_node->formatASTForErrorMessage(),
                        scope.scope_node->formatASTForErrorMessage());
            }

            return;
        }

        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        auto column_node_source = column_node->getColumnSource();
        if (column_node_source->getNodeType() == QueryTreeNodeType::LAMBDA)
            return;

        for (const auto & group_by_key_node : group_by_keys_nodes)
        {
            if (node->isEqual(*group_by_key_node))
                return;
        }

        std::string column_name;

        if (column_node_source->hasAlias())
            column_name = column_node_source->getAlias();
        else if (auto * table_node = column_node_source->as<TableNode>())
            column_name = table_node->getStorageID().getFullTableName();

        column_name += '.' + column_node->getColumnName();

        throw Exception(ErrorCodes::NOT_AN_AGGREGATE,
            "Column {} is not under aggregate function and not in GROUP BY. In scope {}",
            column_name,
            scope.scope_node->formatASTForErrorMessage());
    }

    bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        auto * child_function_node = child_node->as<FunctionNode>();
        if (child_function_node)
        {
            if (child_function_node->isAggregateFunction())
                return false;

            for (const auto & group_by_key_node : group_by_keys_nodes)
            {
                if (child_node->isEqual(*group_by_key_node))
                    return false;
            }
        }

        return !(child_node->getNodeType() == QueryTreeNodeType::QUERY || child_node->getNodeType() == QueryTreeNodeType::UNION);
    }

private:
    const QueryTreeNodes & group_by_keys_nodes;
    const IdentifierResolveScope & scope;
};

/** Resolve query.
  * This function modifies query node during resolve. It is caller responsibility to clone query node before resolve
  * if it is needed for later use.
  *
  * query_node - query_tree_node that must have QueryNode type.
  * scope - query scope. It is caller responsibility to create it.
  *
  * Resolve steps:
  * 1. Validate subqueries depth, perform GROUP BY validation that does not depend on information about aggregate functions.
  * 2. Initialize query scope with aliases.
  * 3. Register CTE subqueries from WITH section in scope and remove them from WITH section.
  * 4. Resolve JOIN TREE.
  * 5. Resolve projection columns.
  * 6. Resolve expressions in other query parts.
  * 7. Validate nodes with duplicate aliases.
  * 8. Validate aggregate functions, GROUPING function, window functions.
  * 9. Remove WITH and WINDOW sections from query.
  * 10. Remove aliases from expression and lambda nodes.
  * 11. Resolve query tree node with projection columns.
  */
void QueryAnalyzer::resolveQuery(const QueryTreeNodePtr & query_node, IdentifierResolveScope & scope)
{
    size_t max_subquery_depth = scope.context->getSettingsRef().max_subquery_depth;
    if (max_subquery_depth && scope.subquery_depth > max_subquery_depth)
        throw Exception(ErrorCodes::TOO_DEEP_SUBQUERIES,
            "Too deep subqueries. Maximum: {}",
            max_subquery_depth);

    auto & query_node_typed = query_node->as<QueryNode &>();

    if (query_node_typed.hasSettingsChanges())
    {
        auto updated_scope_context = Context::createCopy(scope.context);
        updated_scope_context->applySettingsChanges(query_node_typed.getSettingsChanges());
        scope.context = std::move(updated_scope_context);
    }

    const auto & settings = scope.context->getSettingsRef();

    if (settings.group_by_use_nulls)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "GROUP BY use nulls is not supported");

    bool is_rollup_or_cube = query_node_typed.isGroupByWithRollup() || query_node_typed.isGroupByWithCube();

    if (query_node_typed.isGroupByWithGroupingSets() && query_node_typed.isGroupByWithTotals())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "WITH TOTALS and GROUPING SETS are not supported together");

    if (query_node_typed.isGroupByWithGroupingSets() && is_rollup_or_cube)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "GROUPING SETS are not supported together with ROLLUP and CUBE");

    if (query_node_typed.isGroupByWithRollup() && (query_node_typed.isGroupByWithGroupingSets() || query_node_typed.isGroupByWithCube()))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ROLLUP is not supported together with GROUPING SETS and CUBE");

    if (query_node_typed.isGroupByWithCube() && (query_node_typed.isGroupByWithGroupingSets() || query_node_typed.isGroupByWithRollup()))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CUBE is not supported together with GROUPING SETS and ROLLUP");

    if (query_node_typed.hasHaving() && query_node_typed.isGroupByWithTotals() && is_rollup_or_cube)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "WITH TOTALS and WITH ROLLUP or CUBE are not supported together in presence of HAVING");

    /// Initialize aliases in query node scope
    QueryExpressionsAliasVisitor visitor(scope);

    if (query_node_typed.hasWith())
        visitor.visit(query_node_typed.getWithNode());

    if (!query_node_typed.getProjection().getNodes().empty())
        visitor.visit(query_node_typed.getProjectionNode());

    if (query_node_typed.getPrewhere())
        visitor.visit(query_node_typed.getPrewhere());

    if (query_node_typed.getWhere())
        visitor.visit(query_node_typed.getWhere());

    if (query_node_typed.hasGroupBy())
        visitor.visit(query_node_typed.getGroupByNode());

    if (query_node_typed.hasHaving())
        visitor.visit(query_node_typed.getHaving());

    if (query_node_typed.hasWindow())
        visitor.visit(query_node_typed.getWindowNode());

    if (query_node_typed.hasOrderBy())
        visitor.visit(query_node_typed.getOrderByNode());

    if (query_node_typed.hasInterpolate())
        visitor.visit(query_node_typed.getInterpolate());

    if (query_node_typed.hasLimitByLimit())
        visitor.visit(query_node_typed.getLimitByLimit());

    if (query_node_typed.hasLimitByOffset())
        visitor.visit(query_node_typed.getLimitByOffset());

    if (query_node_typed.hasLimitBy())
        visitor.visit(query_node_typed.getLimitByNode());

    if (query_node_typed.hasLimit())
        visitor.visit(query_node_typed.getLimit());

    if (query_node_typed.hasOffset())
        visitor.visit(query_node_typed.getOffset());

    /// Register CTE subqueries and remove them from WITH section

    auto & with_nodes = query_node_typed.getWith().getNodes();

    for (auto & node : with_nodes)
    {
        auto * subquery_node = node->as<QueryNode>();
        auto * union_node = node->as<UnionNode>();

        bool subquery_is_cte = (subquery_node && subquery_node->isCTE()) || (union_node && union_node->isCTE());

        if (!subquery_is_cte)
            continue;

        const auto & cte_name = subquery_node ? subquery_node->getCTEName() : union_node->getCTEName();

        auto [_, inserted] = scope.cte_name_to_query_node.emplace(cte_name, node);
        if (!inserted)
            throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
                "CTE with name {} already exists. In scope {}",
                cte_name,
                scope.scope_node->formatASTForErrorMessage());
    }

    std::erase_if(with_nodes, [](const QueryTreeNodePtr & node)
    {
        auto * subquery_node = node->as<QueryNode>();
        auto * union_node = node->as<UnionNode>();

        return (subquery_node && subquery_node->isCTE()) || (union_node && union_node->isCTE());
    });

    for (auto & window_node : query_node_typed.getWindow().getNodes())
    {
        auto & window_node_typed = window_node->as<WindowNode &>();
        auto parent_window_name = window_node_typed.getParentWindowName();
        if (!parent_window_name.empty())
        {
            auto window_node_it = scope.window_name_to_window_node.find(parent_window_name);
            if (window_node_it == scope.window_name_to_window_node.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Window '{}' does not exists. In scope {}",
                    parent_window_name,
                    scope.scope_node->formatASTForErrorMessage());

            mergeWindowWithParentWindow(window_node, window_node_it->second, scope);
            window_node_typed.setParentWindowName({});
        }

        scope.window_name_to_window_node.emplace(window_node_typed.getAlias(), window_node);
    }

    /** Disable identifier cache during JOIN TREE resolve.
      * Depending on JOIN expression section, identifier with same name
      * can be resolved in different columns.
      *
      * Example: SELECT id FROM test_table AS t1 INNER JOIN test_table AS t2 ON t1.id = t2.id INNER JOIN test_table AS t3 ON t1.id = t3.id
      * In first join expression ON t1.id = t2.id t1.id is resolved into test_table.id column.
      * In second join expression ON t1.id = t3.id t1.id must be resolved into test_table.id column after first JOIN.
      */
    scope.use_identifier_lookup_to_result_cache = false;

    if (query_node_typed.getJoinTree())
    {
        TableExpressionsAliasVisitor table_expressions_visitor(scope);
        table_expressions_visitor.visit(query_node_typed.getJoinTree());

        initializeQueryJoinTreeNode(query_node_typed.getJoinTree(), scope);
        scope.alias_name_to_table_expression_node.clear();

        resolveQueryJoinTreeNode(query_node_typed.getJoinTree(), scope, visitor);
    }

    scope.use_identifier_lookup_to_result_cache = true;

    /// Resolve query node sections.

    auto projection_columns = resolveProjectionExpressionNodeList(query_node_typed.getProjectionNode(), scope);
    if (query_node_typed.getProjection().getNodes().empty())
        throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED,
            "Empty list of columns in projection. In scope {}",
            scope.scope_node->formatASTForErrorMessage());

    if (query_node_typed.hasWith())
        resolveExpressionNodeList(query_node_typed.getWithNode(), scope, true /*allow_lambda_expression*/, false /*allow_table_expression*/);

    if (query_node_typed.getPrewhere())
        resolveExpressionNode(query_node_typed.getPrewhere(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

    if (query_node_typed.getWhere())
        resolveExpressionNode(query_node_typed.getWhere(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

    if (query_node_typed.hasGroupBy())
    {
        if (query_node_typed.isGroupByWithGroupingSets())
        {
            for (auto & grouping_sets_keys_list_node : query_node_typed.getGroupBy().getNodes())
            {
                if (settings.enable_positional_arguments)
                    replaceNodesWithPositionalArguments(grouping_sets_keys_list_node, query_node_typed.getProjection().getNodes(), scope);

                resolveExpressionNodeList(grouping_sets_keys_list_node, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
            }
        }
        else
        {
            if (settings.enable_positional_arguments)
                replaceNodesWithPositionalArguments(query_node_typed.getGroupByNode(), query_node_typed.getProjection().getNodes(), scope);

            resolveExpressionNodeList(query_node_typed.getGroupByNode(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
        }
    }

    if (query_node_typed.hasHaving())
        resolveExpressionNode(query_node_typed.getHaving(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

    if (query_node_typed.hasWindow())
        resolveWindowNodeList(query_node_typed.getWindowNode(), scope);

    if (query_node_typed.hasOrderBy())
    {
        if (settings.enable_positional_arguments)
            replaceNodesWithPositionalArguments(query_node_typed.getOrderByNode(), query_node_typed.getProjection().getNodes(), scope);

        resolveSortNodeList(query_node_typed.getOrderByNode(), scope);
    }

    if (query_node_typed.hasInterpolate())
        resolveInterpolateColumnsNodeList(query_node_typed.getInterpolate(), scope);

    if (query_node_typed.hasLimitByLimit())
    {
        resolveExpressionNode(query_node_typed.getLimitByLimit(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
        validateLimitOffsetExpression(query_node_typed.getLimitByLimit(), "LIMIT BY LIMIT", scope);
    }

    if (query_node_typed.hasLimitByOffset())
    {
        resolveExpressionNode(query_node_typed.getLimitByOffset(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
        validateLimitOffsetExpression(query_node_typed.getLimitByOffset(), "LIMIT BY OFFSET", scope);
    }

    if (query_node_typed.hasLimitBy())
    {
        if (settings.enable_positional_arguments)
            replaceNodesWithPositionalArguments(query_node_typed.getLimitByNode(), query_node_typed.getProjection().getNodes(), scope);

        resolveExpressionNodeList(query_node_typed.getLimitByNode(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
    }

    if (query_node_typed.hasLimit())
    {
        resolveExpressionNode(query_node_typed.getLimit(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
        validateLimitOffsetExpression(query_node_typed.getLimit(), "LIMIT", scope);
    }

    if (query_node_typed.hasOffset())
    {
        resolveExpressionNode(query_node_typed.getOffset(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
        validateLimitOffsetExpression(query_node_typed.getOffset(), "OFFSET", scope);
    }

    /** Resolve nodes with duplicate aliases.
      * Table expressions cannot have duplicate aliases.
      *
      * Such nodes during scope aliases collection are placed into duplicated array.
      * After scope nodes are resolved, we can compare node with duplicate alias with
      * node from scope alias table.
      */
    for (const auto & node_with_duplicated_alias : scope.nodes_with_duplicated_aliases)
    {
        auto node = node_with_duplicated_alias;
        auto node_alias = node->getAlias();
        resolveExpressionNode(node, scope, true /*allow_lambda_expression*/, false /*allow_table_expression*/);

        bool has_node_in_alias_table = false;

        auto it = scope.alias_name_to_expression_node.find(node_alias);
        if (it != scope.alias_name_to_expression_node.end())
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

        node->removeAlias();
    }

    if (query_node_typed.isGroupByAll())
        expandGroupByAll(query_node_typed);

    /** Validate aggregates
      *
      * 1. Check that there are no aggregate functions and GROUPING function in JOIN TREE, WHERE, PREWHERE, in another aggregate functions.
      * 2. Check that there are no window functions in JOIN TREE, WHERE, PREWHERE, HAVING, WINDOW, inside another aggregate function,
      * inside window function arguments, inside window function window definition.
      * 3. Check that there are no columns that are not specified in GROUP BY keys.
      * 4. Validate GROUP BY modifiers.
      */
    auto join_tree_node_type = query_node_typed.getJoinTree()->getNodeType();
    bool join_tree_is_subquery = join_tree_node_type == QueryTreeNodeType::QUERY || join_tree_node_type == QueryTreeNodeType::UNION;

    if (!join_tree_is_subquery)
    {
        assertNoAggregateFunctionNodes(query_node_typed.getJoinTree(), "in JOIN TREE");
        assertNoGroupingFunction(query_node_typed.getJoinTree(), "in JOIN TREE");
        assertNoWindowFunctionNodes(query_node_typed.getJoinTree(), "in JOIN TREE");
    }

    if (query_node_typed.hasWhere())
    {
        assertNoAggregateFunctionNodes(query_node_typed.getWhere(), "in WHERE");
        assertNoGroupingFunction(query_node_typed.getWhere(), "in WHERE");
        assertNoWindowFunctionNodes(query_node_typed.getWhere(), "in WHERE");
    }

    if (query_node_typed.hasPrewhere())
    {
        assertNoAggregateFunctionNodes(query_node_typed.getPrewhere(), "in PREWHERE");
        assertNoGroupingFunction(query_node_typed.getPrewhere(), "in PREWHERE");
        assertNoWindowFunctionNodes(query_node_typed.getPrewhere(), "in PREWHERE");
    }

    if (query_node_typed.hasHaving())
        assertNoWindowFunctionNodes(query_node_typed.getHaving(), "in HAVING");

    if (query_node_typed.hasWindow())
        assertNoWindowFunctionNodes(query_node_typed.getWindowNode(), "in WINDOW");

    QueryTreeNodes aggregate_function_nodes;
    QueryTreeNodes window_function_nodes;

    collectAggregateFunctionNodes(query_node, aggregate_function_nodes);
    collectWindowFunctionNodes(query_node, window_function_nodes);

    if (query_node_typed.hasGroupBy())
        assertNoAggregateFunctionNodes(query_node_typed.getGroupByNode(), "in GROUP BY");

    for (auto & aggregate_function_node : aggregate_function_nodes)
    {
        auto & aggregate_function_node_typed = aggregate_function_node->as<FunctionNode &>();

        assertNoAggregateFunctionNodes(aggregate_function_node_typed.getArgumentsNode(), "inside another aggregate function");
        assertNoGroupingFunction(aggregate_function_node_typed.getArgumentsNode(), "inside another aggregate function");
        assertNoWindowFunctionNodes(aggregate_function_node_typed.getArgumentsNode(), "inside an aggregate function");
    }

    for (auto & window_function_node : window_function_nodes)
    {
        auto & window_function_node_typed = window_function_node->as<FunctionNode &>();
        assertNoWindowFunctionNodes(window_function_node_typed.getArgumentsNode(), "inside another window function");

        if (query_node_typed.hasWindow())
            assertNoWindowFunctionNodes(window_function_node_typed.getWindowNode(), "inside window definition");
    }

    QueryTreeNodes group_by_keys_nodes;
    group_by_keys_nodes.reserve(query_node_typed.getGroupBy().getNodes().size());

    for (auto & node : query_node_typed.getGroupBy().getNodes())
    {
        if (query_node_typed.isGroupByWithGroupingSets())
        {
            auto & grouping_set_keys = node->as<ListNode &>();
            for (auto & grouping_set_key : grouping_set_keys.getNodes())
            {
                if (grouping_set_key->as<ConstantNode>())
                    continue;

                group_by_keys_nodes.push_back(grouping_set_key);
            }
        }
        else
        {
            if (node->as<ConstantNode>())
                continue;

            group_by_keys_nodes.push_back(node);
        }
    }

    if (query_node_typed.getGroupBy().getNodes().empty())
    {
        if (query_node_typed.hasHaving())
            assertNoGroupingFunction(query_node_typed.getHaving(), "in HAVING without GROUP BY");

        if (query_node_typed.hasOrderBy())
            assertNoGroupingFunction(query_node_typed.getOrderByNode(), "in ORDER BY without GROUP BY");

        assertNoGroupingFunction(query_node_typed.getProjectionNode(), "in SELECT without GROUP BY");
    }

    bool has_aggregation = !query_node_typed.getGroupBy().getNodes().empty() || !aggregate_function_nodes.empty();

    if (has_aggregation)
    {
        ValidateGroupByColumnsVisitor validate_group_by_columns_visitor(group_by_keys_nodes, scope);

        if (query_node_typed.hasHaving())
            validate_group_by_columns_visitor.visit(query_node_typed.getHaving());

        if (query_node_typed.hasOrderBy())
            validate_group_by_columns_visitor.visit(query_node_typed.getOrderByNode());

        validate_group_by_columns_visitor.visit(query_node_typed.getProjectionNode());
    }

    if (!has_aggregation && (query_node_typed.isGroupByWithGroupingSets() || is_rollup_or_cube))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "WITH TOTALS, ROLLUP, CUBE or GROUPING SETS are not supported without aggregation");

    /** WITH section can be safely removed, because WITH section only can provide aliases to query expressions
      * and CTE for other sections to use.
      *
      * Example: WITH 1 AS constant, (x -> x + 1) AS lambda, a AS (SELECT * FROM test_table);
      */
    query_node_typed.getWith().getNodes().clear();

    /** WINDOW section can be safely removed, because WINDOW section can only provide window definition to window functions.
      *
      * Example: SELECT count(*) OVER w FROM test_table WINDOW w AS (PARTITION BY id);
      */
    query_node_typed.getWindow().getNodes().clear();

    /// Remove aliases from expression and lambda nodes

    for (auto & [_, node] : scope.alias_name_to_expression_node)
        node->removeAlias();

    for (auto & [_, node] : scope.alias_name_to_lambda_node)
        node->removeAlias();

    query_node_typed.resolveProjectionColumns(std::move(projection_columns));
}

void QueryAnalyzer::resolveUnion(const QueryTreeNodePtr & union_node, IdentifierResolveScope & scope)
{
    auto & union_node_typed = union_node->as<UnionNode &>();
    auto & queries_nodes = union_node_typed.getQueries().getNodes();

    for (auto & query_node : queries_nodes)
    {
        IdentifierResolveScope subquery_scope(query_node, &scope /*parent_scope*/);
        auto query_node_type = query_node->getNodeType();

        if (query_node_type == QueryTreeNodeType::QUERY)
        {
            resolveQuery(query_node, subquery_scope);
        }
        else if (query_node_type == QueryTreeNodeType::UNION)
        {
            resolveUnion(query_node, subquery_scope);
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "UNION unsupported node {}. In scope {}",
                query_node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }
    }
}

}

QueryAnalysisPass::QueryAnalysisPass(QueryTreeNodePtr table_expression_)
    : table_expression(std::move(table_expression_))
{}

void QueryAnalysisPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    QueryAnalyzer analyzer;
    analyzer.resolve(query_tree_node, table_expression, context);
}

}
