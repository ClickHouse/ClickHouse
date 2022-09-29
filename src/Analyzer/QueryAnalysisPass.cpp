#include <Analyzer/QueryAnalysisPass.h>

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
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/getLeastSupertype.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>

#include <Analyzer/Utils.h>
#include <Analyzer/SetUtils.h>
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
#include <Analyzer/CollectAggregateFunctionNodes.h>
#include <Analyzer/CollectWindowFunctionNodes.h>

#include <Databases/IDatabase.h>

#include <Storages/IStorage.h>
#include <Storages/StorageSet.h>

#include <Interpreters/convertFieldToType.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/Context.h>
#include <Interpreters/UserDefinedExecutableFunctionFactory.h>
#include <Interpreters/UserDefinedSQLFunctionFactory.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Set.h>
#include <Interpreters/misc.h>

#include <Functions/FunctionFactory.h>
#include <Functions/grouping.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>


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
    extern const int ILLEGAL_AGGREGATION;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_FINAL;
    extern const int SAMPLING_NOT_SUPPORTED;
    extern const int NO_COMMON_TYPE;
    extern const int NOT_IMPLEMENTED;
    extern const int ALIAS_REQUIRED;
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
  * TODO: Add expression name into query tree node. Example: SELECT plus(1, 1). Result: SELECT 2. Expression name of constant node should be 2.
  * TODO: Update exception messages
  * TODO: JOIN TREE subquery constant columns
  * TODO: Column name qualifications
  * TODO: Table identifiers with optional UUID.
  * TODO: Lookup functions arrayReduce(sum, [1, 2, 3]);
  * TODO: SELECT (compound_expression).*, (compound_expression).COLUMNS are not supported on parser level.
  * TODO: SELECT a.b.c.*, a.b.c.COLUMNS. Qualified matcher where identifier size is greater than 2 are not supported on parser level.
  * TODO: Support function identifier resolve from parent query scope, if lambda in parent scope does not capture any columns.
  * TODO: Support group_by_use_nulls
  */

namespace
{

/// Identifier lookup context
enum class IdentifierLookupContext : uint8_t
{
    EXPRESSION = 0,
    FUNCTION,
    TABLE,
};

const char * toString(IdentifierLookupContext identifier_lookup_context)
{
    switch (identifier_lookup_context)
    {
        case IdentifierLookupContext::EXPRESSION: return "EXPRESSION";
        case IdentifierLookupContext::FUNCTION: return "FUNCTION";
        case IdentifierLookupContext::TABLE: return "TABLE";
    }
}

const char * toStringLowercase(IdentifierLookupContext identifier_lookup_context)
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
    /// Allow to check parent scopes during identifier resolution
    bool allow_to_check_parent_scopes = true;

    /// Allow to check join tree during identifier resolution
    bool allow_to_check_join_tree = true;

    /// Allow to check CTEs during table identifier resolution
    bool allow_to_check_cte = true;

    /// Allow to check database catalog during table identifier resolution
    bool allow_to_check_database_catalog = true;
};

struct TableExpressionData
{
    std::string table_expression_name;
    std::string table_expression_description;
    std::string table_name;
    std::string database_name;
    std::unordered_map<std::string, ColumnNodePtr> column_name_to_column_node;
    std::unordered_set<std::string> column_identifier_first_parts;

    bool hasFullIdentifierName(IdentifierView identifier) const
    {
        return column_name_to_column_node.contains(std::string(identifier.getFullName()));
    }

    bool canBindIdentifier(IdentifierView identifier) const
    {
        return column_identifier_first_parts.contains(std::string(identifier.at(0)));
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

        for (const auto & [expression, alias] : expressions)
        {
            buffer << "Expression ";
            buffer << expression->formatASTForErrorMessage();

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
    std::unordered_map<std::string, ColumnNodePtr> column_name_to_column_node;

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

    /// Allow to check parent scopes if identifier cannot be resolved in current scope
    bool allow_to_check_parent_scopes = true;

    /// Use identifier lookup to result cache
    bool use_identifier_lookup_to_result_cache = true;

    /// Table expression node to data cache
    std::unordered_map<QueryTreeNodePtr, TableExpressionData> table_expression_node_to_data;

    /// Stage when names for projection are calculated
    bool projection_names_calculation_stage = false;

    /// Node to projection name
    std::unordered_map<QueryTreeNodePtr, std::string> node_to_projection_name;

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

        buffer << "Nodes with duplicated aliases size " << nodes_with_duplicated_aliases.size() << '\n';
        for (const auto & node : nodes_with_duplicated_aliases)
            buffer << "Alias name " << node->getAlias() << " node " << node->formatASTForErrorMessage() << '\n';

        buffer << "Expression resolve process stack " << '\n';
        expressions_in_resolve_process_stack.dump(buffer);

        buffer << "Allow to check parent scopes " << allow_to_check_parent_scopes << '\n';
        // buffer << "Parent scope " << parent_scope << '\n';
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
  *
  * It is client responsibility after resolving identifier node with alias, make following actions:
  * 1. If identifier node was resolved in function scope, remove alias from scope expression map.
  * 2. If identifier node was resolved in expression scope, remove alias from scope function map.
  *
  * That way we separate alias map initialization and expressions resolution.
  */
class QueryExpressionsAliasVisitorMatcher
{
public:
    using Visitor = InDepthQueryTreeVisitor<QueryExpressionsAliasVisitorMatcher, true, true>;

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
            if (query_tree_node->isCTE())
                return false;

            updateAliasesIfNeeded(data, child, false);
            return false;
        }
        else if (auto * union_node = child->as<UnionNode>())
        {
            if (union_node->isCTE())
                return false;

            updateAliasesIfNeeded(data, child, false);
            return false;
        }

        return true;
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

using QueryExpressionsAliasVisitor = QueryExpressionsAliasVisitorMatcher::Visitor;

class TableExpressionsAliasVisitorMatcher
{
public:
    using Visitor = InDepthQueryTreeVisitor<TableExpressionsAliasVisitorMatcher, true, false>;

    struct Data
    {
        IdentifierResolveScope & scope;
    };

    static void visit(QueryTreeNodePtr & node, Data & data)
    {
        updateAliasesIfNeeded(data, node);
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
    static void updateAliasesIfNeeded(Data & data, const QueryTreeNodePtr & node)
    {
        if (!node->hasAlias())
            return;

        const auto & node_alias = node->getAlias();
        auto [_, inserted] = data.scope.alias_name_to_table_expression_node.emplace(node_alias, node);
        if (!inserted)
            throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
                "Multiple table expressions with same alias {}. In scope {}",
                node_alias,
                data.scope.scope_node->formatASTForErrorMessage());
    }
};

using TableExpressionsAliasVisitor = TableExpressionsAliasVisitorMatcher::Visitor;

class QueryAnalyzer
{
public:
    explicit QueryAnalyzer(ContextPtr context_)
        : context(std::move(context_))
    {}

    void resolve(QueryTreeNodePtr node, const QueryTreeNodePtr & table_expression)
    {
        IdentifierResolveScope scope(node, nullptr /*parent_scope*/);

        auto node_type = node->getNodeType();

        if (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION)
        {
            if (table_expression)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "For query or union analysis table expression must be empty");

            if (node_type == QueryTreeNodeType::QUERY)
                resolveQuery(node, scope);
            else
                resolveUnion(node, scope);
        }
        else if (node_type == QueryTreeNodeType::CONSTANT || node_type == QueryTreeNodeType::IDENTIFIER || node_type == QueryTreeNodeType::COLUMN ||
            node_type == QueryTreeNodeType::FUNCTION || node_type == QueryTreeNodeType::LIST)
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
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Node {} with type {} is not supported by query analyzer. Supported nodes are union, query, constant, identifier, column, function, list.",
                node->formatASTForErrorMessage(),
                node->getNodeTypeName());
        }
    }

private:
    /// Utility functions

    static QueryTreeNodePtr wrapExpressionNodeInTupleElement(QueryTreeNodePtr expression_node, IdentifierView nested_path);

    QueryTreeNodePtr tryGetLambdaFromSQLUserDefinedFunction(const std::string & function_name);

    void evaluateScalarSubquery(QueryTreeNodePtr & query_tree_node, size_t subquery_depth);

    void validateJoinTableExpressionWithoutAlias(const QueryTreeNodePtr & join_node, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    static void validateLimitOffsetExpression(QueryTreeNodePtr & expression_node, const String & expression_description, IdentifierResolveScope & scope);

    static void validateTableExpressionModifiers(const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    static void mergeWindowWithParentWindow(const QueryTreeNodePtr & window_node, const QueryTreeNodePtr & parent_window_node, IdentifierResolveScope & scope);

    static void replaceNodesWithPositionalArguments(QueryTreeNodePtr & node_list, const QueryTreeNodes & projection_nodes, IdentifierResolveScope & scope);

    /// Resolve identifier functions

    QueryTreeNodePtr tryResolveTableIdentifierFromDatabaseCatalog(const Identifier & table_identifier);

    QueryTreeNodePtr tryResolveIdentifierFromExpressionArguments(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromAliases(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope, IdentifierResolveSettings identifier_resolve_settings = {});

    QueryTreeNodePtr tryResolveIdentifierFromTableColumns(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    bool tryBindIdentifierToTableExpression(const IdentifierLookup & identifier_lookup, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromTableExpression(const IdentifierLookup & identifier_lookup, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromJoin(const IdentifierLookup & identifier_lookup, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromArrayJoin(const IdentifierLookup & identifier_lookup, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromJoinTreeNode(const IdentifierLookup & identifier_lookup, const QueryTreeNodePtr & join_tree_node, IdentifierResolveScope & scope);

    QueryTreeNodePtr tryResolveIdentifierFromJoinTree(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    IdentifierResolveResult tryResolveIdentifierInParentScopes(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope);

    IdentifierResolveResult tryResolveIdentifier(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope, IdentifierResolveSettings identifier_resolve_settings = {});

    /// Resolve query tree nodes functions

    using QueryTreeNodesWithNames = std::vector<std::pair<QueryTreeNodePtr, std::string>>;

    void matcherQualifyColumnsForProjectionNamesIfNeeded(QueryTreeNodesWithNames & matched_nodes_with_column_names, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    QueryTreeNodesWithNames resolveQualifiedMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope);

    QueryTreeNodesWithNames resolveUnqualifiedMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope);

    QueryTreeNodePtr resolveMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope);

    void resolveWindow(QueryTreeNodePtr & window_node, IdentifierResolveScope & scope);

    void resolveLambda(const QueryTreeNodePtr & lambda_node, const QueryTreeNodes & lambda_arguments, IdentifierResolveScope & scope);

    void resolveFunction(QueryTreeNodePtr & function_node, IdentifierResolveScope & scope);

    void resolveExpressionNode(QueryTreeNodePtr & node, IdentifierResolveScope & scope, bool allow_lambda_expression, bool allow_table_expression);

    void resolveExpressionNodeList(QueryTreeNodePtr & node_list, IdentifierResolveScope & scope, bool allow_lambda_expression, bool allow_table_expression);

    void resolveSortColumnsNodeList(QueryTreeNodePtr & sort_node_list, IdentifierResolveScope & scope);

    void resolveInterpolateColumnsNodeList(QueryTreeNodePtr & interpolate_node_list, IdentifierResolveScope & scope);

    void resolveWindowNodeList(QueryTreeNodePtr & window_node_list, IdentifierResolveScope & scope);

    String calculateProjectionNodeDisplayName(QueryTreeNodePtr & node, IdentifierResolveScope & scope);

    NamesAndTypes resolveProjectionExpressionNodeList(QueryTreeNodePtr & projection_node_list, IdentifierResolveScope & scope);

    void initializeQueryJoinTreeNode(QueryTreeNodePtr & join_tree_node, IdentifierResolveScope & scope);

    void initializeTableExpressionColumns(const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope);

    void resolveQueryJoinTreeNode(QueryTreeNodePtr & join_tree_node, IdentifierResolveScope & scope, QueryExpressionsAliasVisitor & expressions_visitor);

    void resolveQuery(const QueryTreeNodePtr & query_node, IdentifierResolveScope & scope);

    void resolveUnion(const QueryTreeNodePtr & union_node, IdentifierResolveScope & scope);

    /// Query analyzer context
    ContextPtr context;

    /// Lambdas that are currently in resolve process
    std::unordered_set<IQueryTreeNode *> lambdas_in_resolve_process;

    /// Array join expressions counter
    size_t array_join_expressions_counter = 0;
};

/// Utility functions implementation

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
QueryTreeNodePtr QueryAnalyzer::tryGetLambdaFromSQLUserDefinedFunction(const std::string & function_name)
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

/// Evaluate scalar subquery and perform constant folding.
void QueryAnalyzer::evaluateScalarSubquery(QueryTreeNodePtr & node, size_t subquery_depth)
{
    auto * query_node = node->as<QueryNode>();
    auto * union_node = node->as<UnionNode>();
    if (!query_node && !union_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Node must have query or union type. Actual {} {}",
            node->getNodeTypeName(),
            node->formatASTForErrorMessage());

    if ((query_node && query_node->hasConstantValue()) ||
        (union_node && union_node->hasConstantValue()))
        return;

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

        if (query_node)
            query_node->performConstantFolding(std::move(constant_value));
        else if (union_node)
            query_node->performConstantFolding(std::move(constant_value));

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
    if (query_node)
        query_node->performConstantFolding(std::move(constant_value));
    else if (union_node)
        union_node->performConstantFolding(std::move(constant_value));
}

void QueryAnalyzer::validateJoinTableExpressionWithoutAlias(const QueryTreeNodePtr & join_node, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope)
{
    if (!context->getSettingsRef().joined_subquery_requires_alias)
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

void QueryAnalyzer::validateLimitOffsetExpression(QueryTreeNodePtr & expression_node, const String & expression_description, IdentifierResolveScope & scope)
{
    const auto limit_offset_constant_value = expression_node->getConstantValueOrNull();
    if (!limit_offset_constant_value || !isNativeNumber(removeNullable(limit_offset_constant_value->getType())))
        throw Exception(ErrorCodes::INVALID_LIMIT_EXPRESSION,
            "{} expression must be constant with numeric type. Actual {}. In scope {}",
            expression_description,
            expression_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());

    Field converted = convertFieldToType(limit_offset_constant_value->getValue(), DataTypeUInt64());
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

    if (query_node || union_node)
    {
        auto table_expression_modifiers = query_node ? query_node->getTableExpressionModifiers() : union_node->getTableExpressionModifiers();

        if (table_expression_modifiers.has_value())
        {
            String table_expression_modifiers_error_message;

            if (table_expression_modifiers->hasFinal())
            {
                table_expression_modifiers_error_message += "FINAL";

                if (table_expression_modifiers->hasSampleSizeRatio())
                    table_expression_modifiers_error_message += ", SAMPLE";
            }
            else if (table_expression_modifiers->hasSampleSizeRatio())
            {
                table_expression_modifiers_error_message += "SAMPLE";
            }

            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Table expression modifiers {} are not supported for subquery {}. In scope {}",
                table_expression_modifiers_error_message,
                table_expression_node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }
    }
    else if (table_node || table_function_node)
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

void QueryAnalyzer::mergeWindowWithParentWindow(const QueryTreeNodePtr & window_node, const QueryTreeNodePtr & parent_window_node, IdentifierResolveScope & scope)
{
    auto & window_node_typed = window_node->as<WindowNode &>();
    auto parent_window_name = window_node_typed.getParentWindowName();

    auto & parent_window_node_typed = parent_window_node->as<WindowNode &>();

    // If an existing_window_name is specified it must refer to an earlier
    // entry in the WINDOW list; the new window copies its partitioning clause
    // from that entry, as well as its ordering clause if any. In this case
    // the new window cannot specify its own PARTITION BY clause, and it can
    // specify ORDER BY only if the copied window does not have one. The new
    // window always uses its own frame clause; the copied window must not
    // specify a frame clause.
    // -- https://www.postgresql.org/docs/current/sql-select.html
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

/// Resolve identifier functions implementation

/// Try resolve table identifier from database catalog
QueryTreeNodePtr QueryAnalyzer::tryResolveTableIdentifierFromDatabaseCatalog(const Identifier & table_identifier)
{
    size_t parts_size = table_identifier.getPartsSize();
    if (parts_size < 1 || parts_size > 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table identifier should consist of 1 or 2 parts. Actual {}",
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
    if (identifier_lookup.isExpressionLookup() && node_type != QueryTreeNodeType::COLUMN && node_type != QueryTreeNodeType::CONSTANT
        && node_type != QueryTreeNodeType::FUNCTION && node_type != QueryTreeNodeType::QUERY && node_type != QueryTreeNodeType::UNION)
        return {};
    else if (identifier_lookup.isTableLookup() && node_type != QueryTreeNodeType::TABLE && node_type != QueryTreeNodeType::TABLE_FUNCTION &&
        node_type != QueryTreeNodeType::QUERY && node_type != QueryTreeNodeType::UNION)
        return {};
    else if (identifier_lookup.isFunctionLookup() && node_type != QueryTreeNodeType::LAMBDA)
        return {};

    if (!resolve_full_identifier && identifier_lookup.identifier.isCompound() && identifier_lookup.isExpressionLookup())
    {
        auto nested_path = IdentifierView(identifier_lookup.identifier);
        nested_path.popFirst();

        auto tuple_element_result = wrapExpressionNodeInTupleElement(it->second, nested_path);
        resolveFunction(tuple_element_result, scope);

        return tuple_element_result;
    }

    return it->second;
}

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
  * Check QueryExpressionsAliasVisitor documentation.
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

    if (scope.expressions_in_resolve_process_stack.hasExpressionWithAlias(identifier_bind_part))
    {
        const auto * root_expression = scope.expressions_in_resolve_process_stack.getRoot();
        const auto * top_expression = scope.expressions_in_resolve_process_stack.getTop();

        if (!isNodePartOfTree(top_expression, root_expression))
            throw Exception(ErrorCodes::CYCLIC_ALIASES,
                "Cyclic aliases for identifier {}. In scope {}",
                identifier_lookup.identifier.getFullName(),
                scope.scope_node->formatASTForErrorMessage());

        scope.non_cached_identifier_lookups_during_expression_resolve.insert(identifier_lookup);
        return {};
    }

    scope.expressions_in_resolve_process_stack.pushNode(it->second);

    auto node_type = it->second->getNodeType();

    /// Resolve expression if necessary
    if (node_type == QueryTreeNodeType::IDENTIFIER)
    {
        auto & alias_identifier_node = it->second->as<IdentifierNode &>();
        auto identifier = alias_identifier_node.getIdentifier();
        auto lookup_result = tryResolveIdentifier(IdentifierLookup{identifier, identifier_lookup.lookup_context}, scope, identifier_resolve_settings);
        it->second = lookup_result.resolved_identifier;

        /** During collection of aliases if node is identifier and has alias, we cannot say if it is
          * column or function node. Check QueryExpressionsAliasVisitor documentation for clarification.
          *
          * If we resolved identifier node as expression, we must remove identifier node alias from
          * function alias map.
          * If we resolved identifier node as function, we must remove identifier node alias from
          * expression alias map.
          */
        if (identifier_lookup.isExpressionLookup() && it->second)
            scope.alias_name_to_lambda_node.erase(identifier_bind_part);
        else if (identifier_lookup.isFunctionLookup() && it->second)
            scope.alias_name_to_expression_node.erase(identifier_bind_part);
    }
    else if (node_type == QueryTreeNodeType::FUNCTION)
    {
        resolveFunction(it->second, scope);
    }
    else if (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION)
    {
        IdentifierResolveScope subquery_scope(it->second, &scope /*parent_scope*/);
        subquery_scope.subquery_depth = scope.subquery_depth + 1;

        if (node_type == QueryTreeNodeType::QUERY)
            resolveQuery(it->second, subquery_scope);
        else if (node_type == QueryTreeNodeType::UNION)
            resolveUnion(it->second, subquery_scope);

        if (identifier_lookup.isExpressionLookup())
            evaluateScalarSubquery(it->second, subquery_scope.subquery_depth);
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

    return result;
}

/** Resolve identifier from table columns.
  *
  * 1. If table column nodes are empty or identifier is not expression lookup return nullptr.
  * 2. If identifier full name match table column use column. Save information that we resolve identifier using full name.
  * 3. Else if identifier binds to table column, use column.
  * 4. Try to resolve column ALIAS expression if it exists.
  * 5. If identifier was compound and was not resolved using full name during step 1 pop first part from identifier lookup and wrap column node
  * using nested parts of identifier using `wrapExpressionNodeInTupleElement` function.
  * This can be the case with compound ALIAS columns.
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
    {
        auto nested_path = IdentifierView(identifier_lookup.identifier);
        nested_path.popFirst();

        auto tuple_element_result = wrapExpressionNodeInTupleElement(it->second, nested_path);
        resolveFunction(tuple_element_result, scope);

        result = tuple_element_result;
    }

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

    if (identifier_lookup.isTableLookup())
    {
        size_t parts_size = identifier_lookup.identifier.getPartsSize();
        if (parts_size != 1 && parts_size != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Expected identifier {} to contain 1 or 2 parts size to be resolved as table. In scope {}",
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

    if (identifier_lookup.isTableLookup())
    {
        size_t parts_size = identifier_lookup.identifier.getPartsSize();
        if (parts_size != 1 && parts_size != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Expected identifier {} to contain 1 or 2 parts size to be resolved as table. In scope {}",
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

        if (!result_column || (!match_full_identifier && !compound_identifier))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Identifier {} cannot be resolved from {}{}. In scope {}",
                identifier.getFullName(),
                table_expression_data.table_expression_description,
                table_expression_data.table_expression_name.empty() ? "" : " with name " + table_expression_data.table_expression_name,
                scope.scope_node->formatASTForErrorMessage());

        QueryTreeNodePtr result_expression = result_column;
        bool projection_name_clone_is_needed = true;

        if (!match_full_identifier && compound_identifier)
        {
            IdentifierView nested_path(identifier_view);
            nested_path.popFirst();
            auto tuple_element_result = wrapExpressionNodeInTupleElement(result_expression, identifier_view);
            resolveFunction(tuple_element_result, scope);
            result_expression = std::move(tuple_element_result);
            projection_name_clone_is_needed = false;
        }

        if (scope.projection_names_calculation_stage && scope.table_expression_node_to_data.size() > 1)
        {
            if (projection_name_clone_is_needed)
                result_expression = result_expression->clone();

            auto qualified_identifier = identifier;
            for (size_t i = 0; i < identifier_column_qualifier_parts; ++i)
            {
                auto qualified_identifier_with_removed_part = qualified_identifier;
                qualified_identifier_with_removed_part.popFirst();

                if (qualified_identifier_with_removed_part.empty())
                    break;

                if (context->getSettingsRef().prefer_column_name_to_alias
                    && scope.alias_name_to_expression_node.contains(qualified_identifier_with_removed_part[0]))
                    break;

                bool can_remove_qualificator = true;

                for (auto & table_expression_to_check_data : scope.table_expression_node_to_data)
                {
                    const auto & table_expression_to_check = table_expression_to_check_data.first;
                    if (table_expression_to_check.get() == table_expression_node.get())
                        continue;

                    IdentifierLookup column_identifier_lookup {qualified_identifier_with_removed_part, IdentifierLookupContext::EXPRESSION};
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
            scope.node_to_projection_name.emplace(result_expression, std::move(qualified_identifier_full_name));
        }

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
            join_using_column_name_to_column_node.emplace(column_node.getName(), std::static_pointer_cast<ColumnNode>(join_using_node));
        }
    }

    std::optional<JoinTableSide> resolved_side;
    QueryTreeNodePtr resolved_identifier;

    auto resolve_from_using_column = [&](const QueryTreeNodePtr & using_column, JoinTableSide expression_side)
    {
        auto & using_column_node = using_column->as<ColumnNode &>();
        auto & using_expression_list = using_column_node.getExpression()->as<ListNode &>();

        size_t inner_column_node_index = expression_side == JoinTableSide::Left ? 0 : 1;
        const auto & inner_column_node = using_expression_list.getNodes().at(inner_column_node_index);

        auto result_column_node = inner_column_node->clone();
        auto & result_column = result_column_node->as<ColumnNode &>();
        result_column.setColumnType(using_column_node.getColumnType());

        return result_column_node;
    };

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
            resolved_identifier = resolve_from_using_column(using_column_node_it->second, using_column_inner_column_table_side);
        }
        else
        {
            throw Exception(ErrorCodes::AMBIGUOUS_IDENTIFIER,
                "JOIN {} ambiguous identifier {}. In scope {}",
                table_expression_node->formatASTForErrorMessage(),
                identifier_lookup.dump(),
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

    bool join_use_nulls = context->getSettingsRef().join_use_nulls;

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

    /** Special case when qualified or unqualified identifier point to array join expression without alias.
      *
      * CREATE TABLE test_table (id UInt64, value String, value_array Array(UInt8)) ENGINE=TinyLog;
      * SELECT id, value, value_array, test_table.value_array, default.test_table.value_array FROM test_table ARRAY JOIN value_array;
      *
      * value_array, test_table.value_array, default.test_table.value_array must be resolved into array join expression.
      */
    if (!scope.table_expressions_in_resolve_process.contains(table_expression_node.get()) && resolved_identifier)
    {
        for (const auto & array_join_expression : from_array_join_node.getJoinExpressions().getNodes())
        {
            auto & array_join_column_expression = array_join_expression->as<ColumnNode &>();
            if (array_join_column_expression.hasAlias())
                continue;

            auto & array_join_column_inner_expression = array_join_column_expression.getExpressionOrThrow();
            if (array_join_column_inner_expression.get() == resolved_identifier.get() ||
                array_join_column_inner_expression->isEqual(*resolved_identifier))
            {
                auto array_join_column = array_join_column_expression.getColumn();
                auto result = std::make_shared<ColumnNode>(array_join_column, table_expression_node);

                return result;
            }
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
    if (!scope.allow_to_check_parent_scopes)
        return {};

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
            if (identifier_lookup.isTableLookup() && !is_cte)
                continue;

            if (is_cte)
            {
                return lookup_result;
            }
            else if (const auto constant_value = resolved_identifier->getConstantValueOrNull())
            {
                lookup_result.resolved_identifier = std::make_shared<ConstantNode>(constant_value);
                return lookup_result;
            }

            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Resolve identifier {} from parent scope only supported for constants and CTE. Actual {} node type {}. In scope {}",
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
  * 1. Register identifier lookup in scope identifier_lookup_to_resolve_status table.
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
  * 9. If identifier was not resolved remove it from identifier_lookup_to_resolve_status table.
  *
  * It is okay for identifier to be not resolved, in case we want first try to lookup identifier in one context,
  * then if there is no identifier in this context, try to lookup in another context.
  * Example: Try to lookup identifier as function, if it is not found lookup as expression.
  * Example: Try to lookup identifier as expression, if it is not found lookup as table.
  */
IdentifierResolveResult QueryAnalyzer::tryResolveIdentifier(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope, IdentifierResolveSettings identifier_resolve_settings)
{
    auto it = scope.identifier_lookup_to_result.find(identifier_lookup);
    if (it != scope.identifier_lookup_to_result.end())
    {
        if (!it->second.resolved_identifier)
            throw Exception(ErrorCodes::CYCLIC_ALIASES,
                "Cyclic aliases for identifier {}. In scope {}",
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
        bool prefer_column_name_to_alias = context->getSettingsRef().prefer_column_name_to_alias;

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

    if (!resolve_result.resolved_identifier && identifier_lookup.isTableLookup())
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

    if (!resolve_result.resolved_identifier && identifier_resolve_settings.allow_to_check_database_catalog && identifier_lookup.isTableLookup())
    {
        resolve_result.resolved_identifier = tryResolveTableIdentifierFromDatabaseCatalog(identifier_lookup.identifier);

        if (resolve_result.resolved_identifier)
            resolve_result.resolve_place = IdentifierResolvePlace::DATABASE_CATALOG;
    }

    it->second = resolve_result;

    if (!resolve_result.resolved_identifier ||
        scope.non_cached_identifier_lookups_during_expression_resolve.contains(identifier_lookup) ||
        !scope.use_identifier_lookup_to_result_cache)
        scope.identifier_lookup_to_result.erase(it);

    return resolve_result;
}

/// Resolve query tree nodes functions implementation


void QueryAnalyzer::matcherQualifyColumnsForProjectionNamesIfNeeded(QueryTreeNodesWithNames & matched_nodes_with_column_names,
    const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope)
{
    if (!scope.projection_names_calculation_stage || scope.table_expression_node_to_data.size() < 2)
        return;

    std::vector<std::string> qualify_identifier_parts;

    if (table_expression_node->hasAlias())
        qualify_identifier_parts = {table_expression_node->getAlias()};
    else if (auto * table_node = table_expression_node->as<TableNode>())
        qualify_identifier_parts = {table_node->getStorageID().getDatabaseName(), table_node->getStorageID().getTableName()};

    size_t qualify_identifier_parts_size = qualify_identifier_parts.size();

    for (auto & [column_node, column_name] : matched_nodes_with_column_names)
    {
        std::vector<std::string> column_qualified_identifier_parts = Identifier(column_name).getParts();

        for (size_t i = 0; i < qualify_identifier_parts_size; ++i)
        {
            bool need_to_qualify = false;
            auto identifier_to_check = Identifier(column_qualified_identifier_parts);

            for (auto & table_expression_data : scope.table_expression_node_to_data)
            {
                if (table_expression_data.first.get() == table_expression_node.get())
                    continue;

                IdentifierLookup lookup{identifier_to_check, IdentifierLookupContext::EXPRESSION};
                if (tryBindIdentifierToTableExpression(lookup, table_expression_data.first, scope))
                {
                    need_to_qualify = true;
                    break;
                }
            }

            if (need_to_qualify)
            {
                size_t part_index_to_use_for_qualification = qualify_identifier_parts_size - i - 1;
                const auto & part_to_use = qualify_identifier_parts[part_index_to_use_for_qualification];
                column_qualified_identifier_parts.insert(column_qualified_identifier_parts.begin(), part_to_use);
            }
            else
            {
                break;
            }
        }

        scope.node_to_projection_name.emplace(column_node, Identifier(column_qualified_identifier_parts).getFullName());
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

        auto matcher_qualified_identifier_copy = matcher_node_typed.getQualifiedIdentifier();
        for (const auto & element_name : element_names)
        {
            if (!matcher_node_typed.isMatchingColumn(element_name))
                continue;

            auto tuple_element_function = std::make_shared<FunctionNode>("tupleElement");
            tuple_element_function->getArguments().getNodes().push_back(expression_query_tree_node);
            tuple_element_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(element_name));

            QueryTreeNodePtr function_query_node = tuple_element_function;
            resolveFunction(function_query_node, scope);

            if (scope.projection_names_calculation_stage)
            {
                matcher_qualified_identifier_copy.push_back(element_name);
                scope.node_to_projection_name.emplace(function_query_node, matcher_qualified_identifier_copy.getFullName());
                matcher_qualified_identifier_copy.pop_back();
            }

            matched_expression_nodes_with_column_names.emplace_back(std::move(function_query_node), element_name);
        }

        return matched_expression_nodes_with_column_names;
    }

    /// Try to resolve unqualified matcher for table expression

    IdentifierResolveSettings identifier_resolve_settings;
    identifier_resolve_settings.allow_to_check_cte = false;
    identifier_resolve_settings.allow_to_check_database_catalog = false;

    auto table_identifier_lookup = IdentifierLookup{matcher_node_typed.getQualifiedIdentifier(), IdentifierLookupContext::TABLE};
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

    matcherQualifyColumnsForProjectionNamesIfNeeded(matched_expression_nodes_with_column_names, table_expression_node, scope);

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
    IQueryTreeNode * scope_node = scope.scope_node.get();
    auto * scope_query_node = scope_node->as<QueryNode>();

    while (!scope_query_node)
    {
        if (!scope.parent_scope)
            break;

        scope_node = scope.parent_scope->scope_node.get();
        scope_query_node = scope_node->as<QueryNode>();
    }

    /// If there are no parent query scope or query scope does not have join tree
    if (!scope_query_node || !scope_query_node->getJoinTree())
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

    auto table_expressions_stack = buildTableExpressionsStack(scope_query_node->getJoinTree());
    std::vector<QueryTreeNodesWithNames> table_expressions_column_nodes_with_names_stack;

    for (auto & table_expression : table_expressions_stack)
    {
        QueryTreeNodesWithNames matched_expression_nodes_with_column_names;

        if (auto * array_join_node = table_expression->as<ArrayJoinNode>())
            continue;

        bool table_expression_in_resolve_process = scope.table_expressions_in_resolve_process.contains(table_expression.get());

        auto * join_node = table_expression->as<JoinNode>();

        if (join_node)
        {
            size_t table_expressions_column_nodes_with_names_stack_size = table_expressions_column_nodes_with_names_stack.size();
            if (table_expressions_column_nodes_with_names_stack_size != 2)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expected 2 table expressions on stack before JOIN processing. Actual {}",
                    table_expressions_column_nodes_with_names_stack_size);

            auto right_table_expression_columns = std::move(table_expressions_column_nodes_with_names_stack.back());
            table_expressions_column_nodes_with_names_stack.pop_back();

            auto left_table_expression_columns = std::move(table_expressions_column_nodes_with_names_stack.back());
            table_expressions_column_nodes_with_names_stack.pop_back();

            std::unordered_set<std::string> column_names_to_skip;

            if (!table_expression_in_resolve_process && join_node->isUsingJoinExpression())
            {
                auto & join_using_list = join_node->getJoinExpression()->as<ListNode &>();

                for (auto & join_using_node : join_using_list.getNodes())
                {
                    auto & column_node = join_using_node->as<ColumnNode &>();
                    const auto & column_name = column_node.getColumnName();

                    if (!matcher_node_typed.isMatchingColumn(column_name))
                        continue;

                    column_names_to_skip.insert(column_name);

                    QueryTreeNodePtr column_source = getColumnSourceForJoinNodeWithUsing(table_expression);
                    auto matched_column_node = std::make_shared<ColumnNode>(column_node.getColumn(), column_source);
                    matched_expression_nodes_with_column_names.emplace_back(std::move(matched_column_node), column_name);
                }
            }

            for (auto && left_table_column : left_table_expression_columns)
            {
                if (column_names_to_skip.contains(left_table_column.second))
                    continue;

                matched_expression_nodes_with_column_names.push_back(std::move(left_table_column));
            }

            for (auto && right_table_column : right_table_expression_columns)
            {
                if (column_names_to_skip.contains(right_table_column.second))
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

        matcherQualifyColumnsForProjectionNamesIfNeeded(matched_expression_nodes_with_column_names, table_expression, scope);
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
QueryTreeNodePtr QueryAnalyzer::resolveMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope)
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

    for (auto & [node, column_name] : matched_expression_nodes_with_names)
    {
        bool apply_transformer_was_used = false;
        bool replace_transformer_was_used = false;

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
                    resolveLambda(lambda_expression_to_resolve, {node}, lambda_scope);
                    auto & lambda_expression_to_resolve_typed = lambda_expression_to_resolve->as<LambdaNode &>();

                    if (auto * lambda_list_node_result = lambda_expression_to_resolve_typed.getExpression()->as<ListNode>())
                    {
                        auto & lambda_list_node_result_nodes = lambda_list_node_result->getNodes();
                        size_t lambda_list_node_result_nodes_size = lambda_list_node_result->getNodes().size();

                        if (lambda_list_node_result_nodes_size != 1)
                            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                            "Lambda in APPLY transformer {} resolved as list node with size {}. Expected 1. In scope {}",
                            apply_transformer->formatASTForErrorMessage(),
                            lambda_list_node_result_nodes_size,
                            scope.scope_node->formatASTForErrorMessage());

                        lambda_expression_to_resolve_typed.getExpression() = lambda_list_node_result_nodes[0];
                    }

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
                resolveExpressionNode(node, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
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
void QueryAnalyzer::resolveWindow(QueryTreeNodePtr & node, IdentifierResolveScope & scope)
{
    std::string parent_window_name;
    auto * identifier_node = node->as<IdentifierNode>();

    if (identifier_node)
    {
        parent_window_name = identifier_node->getIdentifier().getFullName();
    }
    else if (auto * window_node = node->as<WindowNode>())
    {
        parent_window_name = window_node->getParentWindowName();
    }

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

        if (identifier_node)
            node = window_node_it->second->clone();
        else
            mergeWindowWithParentWindow(node, window_node_it->second, scope);
    }

    auto & window_node = node->as<WindowNode &>();

    window_node.setParentWindowName({});
    resolveExpressionNodeList(window_node.getPartitionByNode(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
    resolveSortColumnsNodeList(window_node.getOrderByNode(), scope);

    if (window_node.hasFrameBeginOffset())
    {
        resolveExpressionNode(window_node.getFrameBeginOffsetNode(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

        const auto window_frame_begin_constant_value = window_node.getFrameBeginOffsetNode()->getConstantValueOrNull();
        if (!window_frame_begin_constant_value || !isNativeNumber(removeNullable(window_frame_begin_constant_value->getType())))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Window frame begin OFFSET expression must be constant with numeric type. Actual {}. In scope {}",
                window_node.getFrameBeginOffsetNode()->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());

        window_node.getWindowFrame().begin_offset = window_frame_begin_constant_value->getValue();
    }

    if (window_node.hasFrameEndOffset())
    {
        resolveExpressionNode(window_node.getFrameEndOffsetNode(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

        const auto window_frame_end_constant_value = window_node.getFrameEndOffsetNode()->getConstantValueOrNull();
        if (!window_frame_end_constant_value || !isNativeNumber(removeNullable(window_frame_end_constant_value->getType())))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Window frame begin OFFSET expression must be constant with numeric type. Actual {}. In scope {}",
                window_node.getFrameEndOffsetNode()->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());

        window_node.getWindowFrame().end_offset = window_frame_end_constant_value->getValue();
    }

    window_node.getWindowFrame().checkValid();
}

/** Resolve lambda function.
  * This function modified lambda_node during resolve. It is caller responsibility to clone lambda before resolve
  * if it is needed for later use.
  *
  * Lambda expression can be resolved into list node. It is caller responsibility to handle it properly.
  *
  * lambda_node - node that must have LambdaNode type.
  * arguments - lambda arguments.
  * scope - lambda scope. It is client responsibility to create it.
  *
  * Resolve steps:
  * 1. Validate arguments.
  * 2. Register lambda in lambdas in resolve process. This is necessary to prevent recursive lambda resolving.
  * 3. Initialize scope with lambda aliases.
  * 4. Validate lambda argument names, and scope expressions.
  * 5. Resolve lambda body expression.
  * 6. Deregister lambda from lambdas in resolve process.
  */
void QueryAnalyzer::resolveLambda(const QueryTreeNodePtr & lambda_node, const QueryTreeNodes & lambda_arguments, IdentifierResolveScope & scope)
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
    QueryExpressionsAliasVisitor::Data data{scope};
    QueryExpressionsAliasVisitor visitor(data);
    visitor.visit(lambda.getExpression());

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

        bool has_expression_node = data.scope.alias_name_to_expression_node.contains(lambda_argument_name);
        bool has_alias_node = data.scope.alias_name_to_lambda_node.contains(lambda_argument_name);

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

    /// Lambda body expression is resolved as standard query expression node.
    resolveExpressionNode(lambda.getExpression(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

    lambdas_in_resolve_process.erase(lambda_node.get());
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
void QueryAnalyzer::resolveFunction(QueryTreeNodePtr & node, IdentifierResolveScope & scope)
{
    FunctionNode & function_node = node->as<FunctionNode &>();
    if (function_node.isResolved())
        return;

    const auto & function_name = function_node.getFunctionName();

    /// Resolve function parameters

    resolveExpressionNodeList(function_node.getParametersNode(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

    /// Convert function parameters into constant parameters array

    Array parameters;

    auto & parameters_nodes = function_node.getParameters().getNodes();
    parameters.reserve(parameters_nodes.size());

    for (auto & parameter_node : parameters_nodes)
    {
        auto constant_value = parameter_node->getConstantValueOrNull();

        if (!constant_value)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Parameter for function {} expected to have constant value. Actual {}. In scope {}",
            function_node.getFunctionName(),
            parameter_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());

        parameters.push_back(constant_value->getValue());
    }

    //// If function node is not window function try to lookup function node name as lambda identifier.
    QueryTreeNodePtr lambda_expression_untyped;
    if (!function_node.isWindowFunction())
    {
        auto function_lookup_result = tryResolveIdentifier({Identifier{function_node.getFunctionName()}, IdentifierLookupContext::FUNCTION}, scope);
        lambda_expression_untyped = function_lookup_result.resolved_identifier;
    }

    bool is_special_function_in = false;
    bool is_special_function_dict_get_or_join_get = false;

    if (!lambda_expression_untyped)
    {
        is_special_function_in = isNameOfInFunction(function_name);
        is_special_function_dict_get_or_join_get = functionIsJoinGet(function_name) || functionIsDictGet(function_name);

        /// Handle SELECT count(*) FROM test_table
        if (function_name == "count")
            function_node.getArguments().getNodes().clear();
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
        !function_node.getArguments().getNodes().empty() &&
        function_node.getArguments().getNodes()[0]->getNodeType() == QueryTreeNodeType::IDENTIFIER)
    {
        auto & first_argument = function_node.getArguments().getNodes()[0];
        auto & identifier_node = first_argument->as<IdentifierNode &>();
        IdentifierLookup identifier_lookup{identifier_node.getIdentifier(), IdentifierLookupContext::EXPRESSION};
        auto resolve_result = tryResolveIdentifier(identifier_lookup, scope);

        if (resolve_result.isResolved())
            first_argument = std::move(resolve_result.resolved_identifier);
        else
            first_argument = std::make_shared<ConstantNode>(identifier_node.getIdentifier().getFullName());
    }

    /// Resolve function arguments

    resolveExpressionNodeList(function_node.getArgumentsNode(), scope, true /*allow_lambda_expression*/, is_special_function_in /*allow_table_expression*/);

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
        else if (is_special_function_in && (function_argument->getNodeType() == QueryTreeNodeType::TABLE ||
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

        if (const auto constant_value = function_argument->getConstantValueOrNull())
        {
            argument_column.column = constant_value->getType()->createColumnConst(1, constant_value->getValue());
            argument_column.type = constant_value->getType();
        }
        else
        {
            all_arguments_constants = false;
        }

        argument_types.push_back(argument_column.type);
        argument_columns.emplace_back(std::move(argument_column));
    }

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
            lambda_expression_untyped = tryGetLambdaFromSQLUserDefinedFunction(function_node.getFunctionName());

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
            resolveLambda(lambda_expression_clone, function_arguments, lambda_scope);

            auto & resolved_lambda = lambda_expression_clone->as<LambdaNode &>();
            node = resolved_lambda.getExpression();
            return;
        }

        if (function_name == "untuple")
        {
            /// Special handling of `untuple` function

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

                if (scope.projection_names_calculation_stage && node->hasAlias())
                    scope.node_to_projection_name.emplace(function_query_node, node->getAlias() + '.' + element_name);

                result_list->getNodes().push_back(std::move(function_query_node));
            }

            node = result_list;
            return;
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

            bool force_grouping_standard_compatibility = context->getSettingsRef().force_grouping_standard_compatibility;
            auto grouping_function = std::make_shared<FunctionGrouping>(force_grouping_standard_compatibility);
            auto grouping_function_adaptor = std::make_shared<FunctionToOverloadResolverAdaptor>(std::move(grouping_function));
            function_node.resolveAsFunction(std::move(grouping_function_adaptor), std::make_shared<DataTypeUInt64>());
            return;
        }
    }

    if (function_node.isWindowFunction())
    {
        if (!AggregateFunctionFactory::instance().isAggregateFunctionName(function_name))
           throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
               "Aggregate function with name {} does not exists. In scope {}",
               function_name,
               scope.scope_node->formatASTForErrorMessage());

        AggregateFunctionProperties properties;
        auto aggregate_function = AggregateFunctionFactory::instance().get(function_name, argument_types, parameters, properties);

        function_node.resolveAsWindowFunction(aggregate_function, aggregate_function->getReturnType());
        resolveWindow(function_node.getWindowNode(), scope);
        return;
    }

    FunctionOverloadResolverPtr function = UserDefinedExecutableFunctionFactory::instance().tryGet(function_name, context, parameters);

    if (!function)
        function = FunctionFactory::instance().tryGet(function_name, context);

    if (!function)
    {
        if (!AggregateFunctionFactory::instance().isAggregateFunctionName(function_name))
           throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
               "Function with name {} does not exists. In scope {}",
               function_name,
               scope.scope_node->formatASTForErrorMessage());

        AggregateFunctionProperties properties;
        auto aggregate_function = AggregateFunctionFactory::instance().get(function_name, argument_types, parameters, properties);
        function_node.resolveAsAggregateFunction(aggregate_function, aggregate_function->getReturnType());
        return;
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
            resolveLambda(lambda_to_resolve, lambda_arguments, lambda_scope);

            if (auto * lambda_list_node_result = lambda_to_resolve_typed.getExpression()->as<ListNode>())
            {
                auto & lambda_list_node_result_nodes = lambda_list_node_result->getNodes();
                size_t lambda_list_node_result_nodes_size = lambda_list_node_result->getNodes().size();

                if (lambda_list_node_result_nodes_size != 1)
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                        "Lambda as function argument resolved as list node with size {}. Expected 1. In scope {}",
                        lambda_list_node_result_nodes_size,
                        lambda_to_resolve->formatASTForErrorMessage());

                lambda_to_resolve_typed.getExpression() = lambda_list_node_result_nodes[0];
            }

            argument_types[function_lambda_argument_index] = std::make_shared<DataTypeFunction>(function_data_type_argument_types, lambda_to_resolve->getResultType());
            argument_columns[function_lambda_argument_index].type = argument_types[function_lambda_argument_index];
            function_arguments[function_lambda_argument_index] = std::move(lambda_to_resolve);
        }
    }

    /** Create SET column for special function IN to allow constant folding
      * if left and right arguments are constants.
      *
      * Example: SELECT * FROM test_table LIMIT 1 IN 1;
      */
    if (is_special_function_in &&
        function_arguments.at(0)->hasConstantValue() &&
        function_arguments.at(1)->hasConstantValue())
    {
        const auto & first_argument_constant_value = function_arguments[0]->getConstantValue();
        const auto & second_argument_constant_value = function_arguments[1]->getConstantValue();

        const auto & first_argument_constant_type = first_argument_constant_value.getType();
        const auto & second_argument_constant_literal = second_argument_constant_value.getValue();
        const auto & second_argument_constant_type = second_argument_constant_value.getType();

        auto set = makeSetForConstantValue(first_argument_constant_type, second_argument_constant_literal, second_argument_constant_type, context->getSettingsRef());

        /// Create constant set column for constant folding

        auto column_set = ColumnSet::create(1, std::move(set));
        argument_columns[1].column = ColumnConst::create(std::move(column_set), 1);
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

                function_node.performConstantFolding(std::make_shared<ConstantValue>(std::move(constant_value), result_type));
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
void QueryAnalyzer::resolveExpressionNode(QueryTreeNodePtr & node, IdentifierResolveScope & scope, bool allow_lambda_expression, bool allow_table_expression)
{
    String node_alias = node->getAlias();

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
                node = tryResolveIdentifier({unresolved_identifier, IdentifierLookupContext::TABLE}, scope).resolved_identifier;

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
                    message_clarification = std::string(" or ") + toStringLowercase(IdentifierLookupContext::TABLE);

                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                    "Unknown {}{} identifier {} in scope {}",
                    toStringLowercase(IdentifierLookupContext::EXPRESSION),
                    message_clarification,
                    unresolved_identifier.getFullName(),
                    scope.scope_node->formatASTForErrorMessage());
            }

            break;
        }
        case QueryTreeNodeType::MATCHER:
        {
            node = resolveMatcher(node, scope);
            break;
        }
        case QueryTreeNodeType::TRANSFORMER:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Transformer {} appeared in expression context. In scope {}",
                node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }
        case QueryTreeNodeType::LIST:
        {
            /** Edge case if list expression has alias.
              * Matchers cannot have aliases, but `untuple` function can.
              * Example: SELECT a, untuple(CAST(('hello', 1) AS Tuple(name String, count UInt32))) AS a;
              * During resolveFunction `untuple` function is replaced by list of 2 constants 'hello', 1.
              */
            resolveExpressionNodeList(node, scope, allow_lambda_expression, allow_lambda_expression);
            break;
        }
        case QueryTreeNodeType::CONSTANT:
        {
            /// Already resolved
            break;
        }
        case QueryTreeNodeType::COLUMN:
        {
            auto & column_function_node = node->as<ColumnNode &>();
            if (column_function_node.hasExpression())
                resolveExpressionNode(column_function_node.getExpression(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
            break;
        }
        case QueryTreeNodeType::FUNCTION:
        {
            resolveFunction(node, scope);
            break;
        }
        case QueryTreeNodeType::LAMBDA:
        {
            if (!allow_lambda_expression)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Lambda is not allowed {} in expression. In scope {}",
                    node->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            /// Lambda must be resolved by caller
            break;
        }
        case QueryTreeNodeType::SORT:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Sort {} is not allowed in expression. In scope {}",
                node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }
        case QueryTreeNodeType::INTERPOLATE:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Interpolate column {} is not allowed in expression. In scope {}",
                node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }
        case QueryTreeNodeType::WINDOW:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Window {} is not allowed in expression. In scope {}",
                node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }
        case QueryTreeNodeType::TABLE:
        {
            if (!allow_table_expression)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Table is not allowed {} in expression. In scope {}",
                    node->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());
            break;
        }
        case QueryTreeNodeType::TABLE_FUNCTION:
        {
            if (!allow_table_expression)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Table function is not allowed {} in expression. In scope {}",
                    node->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());
            break;
        }
        case QueryTreeNodeType::QUERY:
        {
            IdentifierResolveScope subquery_scope(node, &scope /*parent_scope*/);
            subquery_scope.subquery_depth = scope.subquery_depth + 1;
            resolveQuery(node, subquery_scope);

            if (!allow_table_expression)
                evaluateScalarSubquery(node, subquery_scope.subquery_depth);

            break;
        }
        case QueryTreeNodeType::UNION:
        {
            IdentifierResolveScope subquery_scope(node, &scope /*parent_scope*/);
            subquery_scope.subquery_depth = scope.subquery_depth + 1;
            resolveUnion(node, subquery_scope);

            if (!allow_table_expression)
                evaluateScalarSubquery(node, subquery_scope.subquery_depth);

            break;
        }
        case QueryTreeNodeType::ARRAY_JOIN:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Array join {} is not allowed in expression. In scope {}",
                node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }
        case QueryTreeNodeType::JOIN:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Join {} is not allowed in expression. In scope {}",
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

    scope.expressions_in_resolve_process_stack.popNode();
    bool expression_was_root = scope.expressions_in_resolve_process_stack.empty();
    if (expression_was_root)
        scope.non_cached_identifier_lookups_during_expression_resolve.clear();
}

/** Resolve expression node list.
  * If expression is CTE subquery node it is skipped.
  * If expression is resolved in list, it is flattened into initial node list.
  *
  * Such examples must work:
  * Example: CREATE TABLE test_table (id UInt64, value UInt64) ENGINE=TinyLog; SELECT plus(*) FROM test_table;
  * Example: SELECT *** FROM system.one;
  */
void QueryAnalyzer::resolveExpressionNodeList(QueryTreeNodePtr & node_list, IdentifierResolveScope & scope, bool allow_lambda_expression, bool allow_table_expression)
{
    auto & node_list_typed = node_list->as<ListNode &>();
    size_t node_list_size = node_list_typed.getNodes().size();

    QueryTreeNodes result_nodes;
    result_nodes.reserve(node_list_size);

    for (auto & node : node_list_typed.getNodes())
    {
        auto node_to_resolve = node;
        resolveExpressionNode(node_to_resolve, scope, allow_lambda_expression, allow_table_expression);

        if (auto * expression_list = node_to_resolve->as<ListNode>())
        {
            for (auto & expression_list_node : expression_list->getNodes())
                result_nodes.push_back(std::move(expression_list_node));
        }
        else
        {
            result_nodes.push_back(std::move(node_to_resolve));
        }
    }

    node_list_typed.getNodes() = std::move(result_nodes);
}

/** Resolve sort columns nodes list.
  */
void QueryAnalyzer::resolveSortColumnsNodeList(QueryTreeNodePtr & sort_node_list, IdentifierResolveScope & scope)
{
    auto & sort_node_list_typed = sort_node_list->as<ListNode &>();

    for (auto & node : sort_node_list_typed.getNodes())
    {
        auto & sort_node = node->as<SortNode &>();
        resolveExpressionNode(sort_node.getExpression(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

        if (sort_node.hasFillFrom())
        {
            resolveExpressionNode(sort_node.getFillFrom(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

            const auto * constant_node = sort_node.getFillFrom()->as<const ConstantNode>();
            if (!constant_node || !isColumnedAsNumber(constant_node->getResultType()))
                throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "WITH FILL FROM expression must be constant with numeric type. Actual {}. In scope {}",
                    sort_node.getFillFrom()->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());
        }
        if (sort_node.hasFillTo())
        {
            resolveExpressionNode(sort_node.getFillTo(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
            const auto * constant_node = sort_node.getFillTo()->as<const ConstantNode>();
            if (!constant_node || !isColumnedAsNumber(constant_node->getResultType()))
                throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "WITH FILL TO expression must be constant with numeric type. Actual {}. In scope {}",
                    sort_node.getFillFrom()->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());
        }
        if (sort_node.hasFillStep())
        {
            resolveExpressionNode(sort_node.getFillStep(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
            const auto * constant_node = sort_node.getFillStep()->as<const ConstantNode>();
            if (!constant_node)
                throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "WITH FILL TO expression must be constant with numeric or interval type. Actual {}. In scope {}",
                    sort_node.getFillStep()->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            bool is_number = isColumnedAsNumber(constant_node->getResultType());
            bool is_interval = WhichDataType(constant_node->getResultType()).isInterval();
            if (!is_number && !is_interval)
                throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "WITH FILL TO expression must be constant with numeric or interval type. Actual {}. In scope {}",
                    sort_node.getFillStep()->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());
        }
    }
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

class SubqueryToProjectionNameMatcher
{
public:
    using Visitor = ConstInDepthQueryTreeVisitor<SubqueryToProjectionNameMatcher, true, true>;

    struct Data
    {
        std::unordered_map<QueryTreeNodePtr, std::string> & node_to_display_name;
        size_t subquery_index = 1;
    };

    static void visit(const QueryTreeNodePtr &, Data &)
    {
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node, Data & data)
    {
        auto child_node_type = child_node->getNodeType();
        if (child_node_type != QueryTreeNodeType::QUERY && child_node_type != QueryTreeNodeType::UNION)
            return true;

        data.node_to_display_name.emplace(child_node, "_subquery_" + std::to_string(data.subquery_index));
        ++data.subquery_index;
        return false;
    }
};

using SubqueryToProjectionNameVisitor = SubqueryToProjectionNameMatcher::Visitor;

String QueryAnalyzer::calculateProjectionNodeDisplayName(QueryTreeNodePtr & node, IdentifierResolveScope & scope)
{
    if (node->hasAlias())
        return node->getAlias();

    auto calculate_inner_expression_display_name = [&](QueryTreeNodePtr & expression_node, IdentifierResolveScope & node_scope)
    {
        auto * expression_function_node = expression_node->as<FunctionNode>();
        if (expression_node->getNodeType() == QueryTreeNodeType::MATCHER)
            expression_node = resolveMatcher(expression_node, node_scope);
        else if (expression_function_node && expression_function_node->getFunctionName() == "untuple")
            resolveFunction(expression_node, node_scope);

        if (expression_node->getNodeType() == QueryTreeNodeType::LIST)
        {
            auto & list_nodes = expression_node->as<ListNode &>().getNodes();
            size_t list_nodes_size = list_nodes.size();

            WriteBufferFromOwnString result_buffer;
            for (size_t i = 0; i < list_nodes_size; ++i)
            {
                auto & list_expression_node = list_nodes[i];
                String list_node_display_name = calculateProjectionNodeDisplayName(list_expression_node, node_scope);
                result_buffer << list_node_display_name;

                if (i + 1 != list_nodes_size)
                    result_buffer << ", ";
            }

            return result_buffer.str();
        }

        return calculateProjectionNodeDisplayName(expression_node, node_scope);
    };

    auto try_to_get_projection_name_from_scope = [&](const QueryTreeNodePtr & expression_node)
    {
        bool check_in_parent_query = scope.scope_node->getNodeType() != QueryTreeNodeType::QUERY;
        IdentifierResolveScope * scope_to_check = &scope;

        while (scope_to_check != nullptr)
        {
            auto projection_name_it = scope_to_check->node_to_projection_name.find(expression_node);
            if (projection_name_it != scope_to_check->node_to_projection_name.end())
                return projection_name_it->second;

            scope_to_check = scope_to_check->parent_scope;
            if (scope_to_check && scope_to_check->scope_node->getNodeType() == QueryTreeNodeType::QUERY)
            {
                if (check_in_parent_query)
                    check_in_parent_query = false;
                else
                    break;
            }
        }

        return String();
    };

    auto projection_name_from_scope = try_to_get_projection_name_from_scope(node);
    if (!projection_name_from_scope.empty())
        return projection_name_from_scope;

    auto node_type = node->getNodeType();
    if (node_type == QueryTreeNodeType::CONSTANT)
    {
        const auto & constant_node = node->as<ConstantNode &>();
        return constant_node.getName();
    }
    else if (node_type == QueryTreeNodeType::COLUMN)
    {
        const auto & column_node = node->as<ColumnNode &>();
        const auto & column_name = column_node.getColumnName();
        return column_name;
    }
    else if (node_type == QueryTreeNodeType::IDENTIFIER)
    {
        const auto & identifier_node = node->as<IdentifierNode &>();
        auto identifier = identifier_node.getIdentifier();
        IdentifierLookup lookup {identifier, IdentifierLookupContext::EXPRESSION};
        auto resolved_identifier_result = tryResolveIdentifier(lookup, scope);

        if (resolved_identifier_result.resolved_identifier && resolved_identifier_result.isResolvedFromJoinTree())
        {
            projection_name_from_scope = try_to_get_projection_name_from_scope(resolved_identifier_result.resolved_identifier);

            if (!projection_name_from_scope.empty())
                return projection_name_from_scope;

            if (auto * column_node = resolved_identifier_result.resolved_identifier->as<ColumnNode>())
                return column_node->getColumnName();
        }

        return identifier_node.getIdentifier().getFullName();
    }
    else if (node_type == QueryTreeNodeType::MATCHER)
    {
        /// Top level matcher
        return {};
    }
    else if (node_type == QueryTreeNodeType::FUNCTION)
    {
        auto & function_node = node->as<FunctionNode &>();

        WriteBufferFromOwnString buffer;
        buffer << function_node.getFunctionName();

        auto & function_parameters_nodes = function_node.getParameters().getNodes();

        if (!function_parameters_nodes.empty())
        {
            buffer << '(';

            size_t function_parameters_nodes_size = function_parameters_nodes.size();
            for (size_t i = 0; i < function_parameters_nodes_size; ++i)
            {
                auto & function_parameter_node = function_parameters_nodes[i];
                String function_parameter_node_display_name = calculate_inner_expression_display_name(function_parameter_node, scope);
                buffer << function_parameter_node_display_name;

                if (i + 1 != function_parameters_nodes_size)
                    buffer << ", ";
            }

            buffer << ')';
        }

        buffer << '(';

        auto & function_arguments_nodes = function_node.getArguments().getNodes();
        size_t function_arguments_nodes_size = function_arguments_nodes.size();
        for (size_t i = 0; i < function_arguments_nodes_size; ++i)
        {
            auto & function_argument_node = function_arguments_nodes[i];
            String function_argument_node_display_name = calculate_inner_expression_display_name(function_argument_node, scope);
            buffer << function_argument_node_display_name;

            if (i + 1 != function_arguments_nodes_size)
                buffer << ", ";
        }

        buffer << ')';

        return buffer.str();
    }
    else if (node_type == QueryTreeNodeType::LAMBDA)
    {
        auto & lambda_node = node->as<LambdaNode &>();
        IdentifierResolveScope lambda_scope(node, &scope /*parent_scope*/);
        lambda_scope.projection_names_calculation_stage = true;
        String lambda_expression_display_name = calculate_inner_expression_display_name(lambda_node.getExpression(), lambda_scope);

        WriteBufferFromOwnString buffer;
        buffer << "lambda(tuple(";

        const auto & lambda_argument_names = lambda_node.getArgumentNames();
        size_t lambda_argument_names_size = lambda_argument_names.size();

        for (size_t i = 0; i < lambda_argument_names_size; ++i)
        {
            const auto & argument_name = lambda_argument_names[i];
            buffer << argument_name;

            if (i + 1 != lambda_argument_names_size)
                buffer << ", ";
        }

        buffer << "), ";
        buffer << lambda_expression_display_name;
        buffer << ')';

        return buffer.str();
    }
    else if (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Subquery name must be already collected");
    }
    else if (node_type == QueryTreeNodeType::TABLE)
    {
        /// Table node can be second argument of IN function
        const auto & table_node = node->as<TableNode &>();
        return table_node.getStorageID().getFullNameNotQuoted();
    }
    else if (node_type == QueryTreeNodeType::TABLE_FUNCTION)
    {
        /// Table function node can be second argument of IN function
        auto & table_function_node = node->as<TableFunctionNode &>();

        WriteBufferFromOwnString buffer;
        buffer << table_function_node.getTableFunctionName();

        buffer << '(';

        auto & table_function_arguments_nodes = table_function_node.getArguments().getNodes();
        size_t table_function_arguments_nodes_size = table_function_arguments_nodes.size();
        for (size_t i = 0; i < table_function_arguments_nodes_size; ++i)
        {
            auto & function_argument_node = table_function_arguments_nodes[i];
            if (auto * identifier_node = function_argument_node->as<IdentifierNode>())
            {
                if (identifier_node->hasAlias())
                    buffer << identifier_node->getAlias();
                else
                    buffer << identifier_node->getIdentifier().getFullName();
            }
            else
            {
                buffer << calculateProjectionNodeDisplayName(function_argument_node, scope);
            }

            if (i + 1 != table_function_arguments_nodes_size)
                buffer << ", ";
        }

        buffer << ')';

        return buffer.str();
    }
    else
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Invalid projection node {} {}", node->getNodeType(), node->dumpTree());
    }
}

NamesAndTypes QueryAnalyzer::resolveProjectionExpressionNodeList(QueryTreeNodePtr & projection_node_list, IdentifierResolveScope & scope)
{
    auto & initial_node_list = projection_node_list->as<ListNode &>();

    scope.projection_names_calculation_stage = true;

    SubqueryToProjectionNameVisitor::Data subquery_to_projection_name_visitor_data {scope.node_to_projection_name};
    SubqueryToProjectionNameVisitor subquery_to_projection_name_visitor(subquery_to_projection_name_visitor_data);
    subquery_to_projection_name_visitor.visit(projection_node_list);

    auto initial_node_list_nodes_copy = initial_node_list.getNodes();
    size_t list_nodes_copy_size = initial_node_list_nodes_copy.size();

    std::vector<std::pair<QueryTreeNodePtr, std::string>> projection_nodes_with_display_name;
    projection_nodes_with_display_name.reserve(list_nodes_copy_size);

    for (size_t i = 0; i < list_nodes_copy_size; ++i)
    {
        auto & node = initial_node_list_nodes_copy[i];

        String display_name = calculateProjectionNodeDisplayName(node, scope);

        auto node_to_resolve = node;
        resolveExpressionNode(node_to_resolve, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

        if (auto * expression_list = node_to_resolve->as<ListNode>())
        {
            for (auto & expression_list_node : expression_list->getNodes())
                projection_nodes_with_display_name.emplace_back(expression_list_node, calculateProjectionNodeDisplayName(expression_list_node, scope));
        }
        else
        {
            projection_nodes_with_display_name.emplace_back(node_to_resolve, std::move(display_name));
        }
    }

    size_t projection_nodes_size = projection_nodes_with_display_name.size();

    auto projection_result_node_list = std::make_shared<ListNode>();
    NamesAndTypes projection_columns;
    projection_columns.reserve(projection_nodes_size);
    projection_result_node_list->getNodes().reserve(projection_nodes_size);

    for (size_t i = 0; i < projection_nodes_size; ++i)
    {
        auto && projection_node =  projection_nodes_with_display_name[i].first;

        if (projection_node->getNodeType() != QueryTreeNodeType::CONSTANT &&
            projection_node->getNodeType() != QueryTreeNodeType::FUNCTION &&
            projection_node->getNodeType() != QueryTreeNodeType::COLUMN &&
            projection_node->getNodeType() != QueryTreeNodeType::QUERY &&
            projection_node->getNodeType() != QueryTreeNodeType::UNION)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Projection node must be constant, function, column, query or union");

        projection_columns.emplace_back(projection_nodes_with_display_name[i].second, projection_node->getResultType());
        projection_result_node_list->getNodes().emplace_back(std::move(projection_node));
    }

    scope.projection_names_calculation_stage = false;
    scope.node_to_projection_name.clear();

    projection_node_list = std::move(projection_result_node_list);
    return projection_columns;
}

/** Initialize query join tree node.
  *
  * 1. Resolve identifiers.
  * 2. Register table, table function, query nodes in scope table expressions in resolve process.
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
                auto table_identifier_lookup = IdentifierLookup{from_table_identifier.getIdentifier(), IdentifierLookupContext::TABLE};

                IdentifierResolveSettings resolve_settings;
                /// In join tree initialization ignore join tree as identifier lookup source
                resolve_settings.allow_to_check_join_tree = false;

                auto table_identifier_resolve_result = tryResolveIdentifier(table_identifier_lookup, scope, resolve_settings);
                auto resolved_identifier = table_identifier_resolve_result.resolved_identifier;

                if (!resolved_identifier)
                    throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                        "Unknown {} identifier {} in scope {}",
                        toStringLowercase(IdentifierLookupContext::TABLE),
                        from_table_identifier.getIdentifier().getFullName(),
                        scope.scope_node->formatASTForErrorMessage());

                resolved_identifier = resolved_identifier->clone();

                auto table_expression_modifiers = from_table_identifier.getTableExpressionModifiers();

                if (auto * resolved_identifier_query_node = resolved_identifier->as<QueryNode>())
                {
                    resolved_identifier_query_node->setIsCTE(false);
                    if (table_expression_modifiers.has_value())
                        resolved_identifier_query_node->setTableExpressionModifiers(*table_expression_modifiers);
                }
                else if (auto * resolved_identifier_union_node = resolved_identifier->as<UnionNode>())
                {
                    resolved_identifier_union_node->setIsCTE(false);
                    if (table_expression_modifiers.has_value())
                        resolved_identifier_union_node->setTableExpressionModifiers(*table_expression_modifiers);
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
                        "Identifier in JOIN TREE {} resolve unexpected table expression. In scope {}",
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

    std::string table_expression_name;
    std::string table_expression_description;

    std::string table_name;
    std::string database_name;

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

        if (table_expression_node->hasAlias())
            table_expression_data.table_expression_name = table_expression_node->getAlias();
    }
    else if (table_function_node)
    {
        table_expression_data.table_expression_description = "table_function";
        if (table_function_node->hasAlias())
            table_expression_data.table_expression_name = table_function_node->getAlias();
    }

    if (table_node || table_function_node)
    {
        const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();

        auto column_names_and_types = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns().withVirtuals());
        const auto & columns_description = storage_snapshot->metadata->getColumns();

        std::vector<std::pair<std::string, ColumnNodePtr>> alias_columns_to_resolve;
        std::unordered_map<std::string, ColumnNodePtr> column_name_to_column_node;
        column_name_to_column_node.reserve(column_names_and_types.size());

        for (const auto & column_name_and_type : column_names_and_types)
        {
            const auto & column_default = columns_description.getDefault(column_name_and_type.name);

            if (column_default && column_default->kind == ColumnDefaultKind::Alias)
            {
                auto column_node = std::make_shared<ColumnNode>(column_name_and_type, buildQueryTree(column_default->expression, context), table_expression_node);
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

            /// Initialize aliases in alias column scope
            QueryExpressionsAliasVisitor::Data data{alias_column_resolve_scope};
            QueryExpressionsAliasVisitor visitor(data);

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

            TableFunctionPtr table_function_ptr = table_function_factory.tryGet(table_function_name, context);
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

            if (context->getSettingsRef().use_structure_from_insertion_table_in_table_functions && table_function_ptr->needStructureHint())
            {
                const auto & insertion_table = context->getInsertionTable();
                if (!insertion_table.empty())
                {
                    const auto & structure_hint
                        = DatabaseCatalog::instance().getTable(insertion_table, context)->getInMemoryMetadataPtr()->columns;
                    table_function_ptr->setStructureHint(structure_hint);
                }
            }

            /// TODO: Special functions that can take query
            /// TODO: Support qualified matchers for table function

            for (auto & argument_node : table_function_node.getArguments().getNodes())
            {
                if (argument_node->getNodeType() == QueryTreeNodeType::MATCHER)
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Matcher as table function argument is not supported {}. In scope {}",
                        join_tree_node->formatASTForErrorMessage(),
                        scope.scope_node->formatASTForErrorMessage());
                }

                auto * function_node = argument_node->as<FunctionNode>();
                if (function_node && table_function_factory.hasNameOrAlias(function_node->getFunctionName()))
                    continue;

                resolveExpressionNode(argument_node, scope, false /*allow_lambda_expression*/, true /*allow_table_expression*/);
            }

            auto table_function_ast = table_function_node.toAST();
            table_function_ptr->parseArguments(table_function_ast, context);

            auto table_function_storage = table_function_ptr->execute(table_function_ast, context, table_function_ptr->getName());
            table_function_node.resolve(std::move(table_function_ptr), std::move(table_function_storage), context);
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

                auto array_join_expression_name = "__array_join_expression_" + std::to_string(array_join_expressions_counter);
                ++array_join_expressions_counter;

                auto array_join_column = std::make_shared<ColumnNode>(NameAndTypePair{array_join_expression_name, result_type}, array_join_expression, join_tree_node);
                array_join_expression = std::move(array_join_column);
                array_join_expression->setAlias(array_join_expression_alias);

                auto it = scope.alias_name_to_expression_node.find(array_join_expression_alias);
                if (it != scope.alias_name_to_expression_node.end())
                    it->second = std::make_shared<ColumnNode>(NameAndTypePair{array_join_expression_name, result_type}, join_tree_node);
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
                for (auto & join_using_node : join_using_list.getNodes())
                {
                    auto * identifier_node = join_using_node->as<IdentifierNode>();
                    if (!identifier_node)
                        continue;

                    const auto & identifier_full_name = identifier_node->getIdentifier().getFullName();

                    IdentifierLookup identifier_lookup {identifier_node->getIdentifier(), IdentifierLookupContext::EXPRESSION};
                    auto result_left_table_expression = tryResolveIdentifierFromJoinTreeNode(identifier_lookup, join_node.getLeftTableExpression(), scope);
                    if (!result_left_table_expression)
                        throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "JOIN {} using identifier {} cannot be resolved from left table expression. In scope {}",
                            join_node.formatASTForErrorMessage(),
                            identifier_full_name,
                            scope.scope_node->formatASTForErrorMessage());

                    auto result_right_table_expression = tryResolveIdentifierFromJoinTreeNode(identifier_lookup, join_node.getRightTableExpression(), scope);
                    if (!result_right_table_expression)
                        throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "JOIN {} using identifier {} cannot be resolved from right table expression. In scope {}",
                            join_node.formatASTForErrorMessage(),
                            identifier_full_name,
                            scope.scope_node->formatASTForErrorMessage());

                    DataTypePtr common_type = tryGetLeastSupertype(DataTypes{result_left_table_expression->getResultType(), result_right_table_expression->getResultType()});

                    if (!common_type)
                        throw Exception(ErrorCodes::NO_COMMON_TYPE,
                            "JOIN {} cannot infer common type in USING for identifier {}. In scope {}",
                            join_node.formatASTForErrorMessage(),
                            identifier_full_name,
                            scope.scope_node->formatASTForErrorMessage());

                    NameAndTypePair join_using_columns_common_name_and_type(identifier_full_name, common_type);
                    ListNodePtr join_using_expression = std::make_shared<ListNode>(QueryTreeNodes{result_left_table_expression, result_right_table_expression});
                    auto join_using_column = std::make_shared<ColumnNode>(join_using_columns_common_name_and_type, std::move(join_using_expression), join_tree_node);

                    join_using_node = std::move(join_using_column);
                }

                for (auto & join_using_node : join_using_list.getNodes())
                {
                    if (!join_using_node->as<ColumnNode>())
                        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                            "JOIN {} USING identifier node must be resolved into column node. Actual {}. In scope {}",
                            join_node.formatASTForErrorMessage(),
                            join_tree_node->formatASTForErrorMessage(),
                            scope.scope_node->formatASTForErrorMessage());
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
    if (join_tree_node_type == QueryTreeNodeType::QUERY ||
        join_tree_node_type == QueryTreeNodeType::UNION ||
        join_tree_node_type == QueryTreeNodeType::TABLE ||
        join_tree_node_type == QueryTreeNodeType::TABLE_FUNCTION)
    {
        validateTableExpressionModifiers(join_tree_node, scope);
        initializeTableExpressionColumns(join_tree_node, scope);
    }

    add_table_expression_alias_into_scope(join_tree_node);
    scope.table_expressions_in_resolve_process.erase(join_tree_node.get());
}

class ValidateGroupByColumnsMatcher
{
public:
    using Visitor = ConstInDepthQueryTreeVisitor<ValidateGroupByColumnsMatcher, true, true>;

    struct Data
    {
        const QueryTreeNodes & group_by_keys_nodes;
    };

    static void visit(const QueryTreeNodePtr & node, Data & data)
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

                for (const auto & group_by_key_node : data.group_by_keys_nodes)
                {
                    if (grouping_function_arguments_node->isEqual(*group_by_key_node))
                    {
                        found_argument_in_group_by_keys = true;
                        break;
                    }
                }

                if (!found_argument_in_group_by_keys)
                    throw Exception(ErrorCodes::NOT_AN_AGGREGATE,
                        "GROUPING function argument {} is not in GROUP BY",
                        grouping_function_arguments_node->formatASTForErrorMessage());
            }

            return;
        }

        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        auto column_node_source = column_node->getColumnSource();
        if (column_node_source->getNodeType() == QueryTreeNodeType::LAMBDA)
            return;

        for (const auto & group_by_key_node : data.group_by_keys_nodes)
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
            "Column {} is not under aggregate function and not in GROUP BY",
            column_name);
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node, Data & data)
    {
        auto * child_function_node = child_node->as<FunctionNode>();
        if (child_function_node)
        {
            if (child_function_node->isAggregateFunction())
                return false;

            for (const auto & group_by_key_node : data.group_by_keys_nodes)
            {
                if (child_node->isEqual(*group_by_key_node))
                    return false;
            }
        }

        return child_node->getNodeType() != QueryTreeNodeType::QUERY || child_node->getNodeType() != QueryTreeNodeType::UNION;
    }
};

using ValidateGroupByColumnsVisitor = ValidateGroupByColumnsMatcher::Visitor;

class ValidateGroupingFunctionNodesMatcher
{
public:
    using Visitor = ConstInDepthQueryTreeVisitor<ValidateGroupingFunctionNodesMatcher, true, false>;

    struct Data
    {
        String assert_no_grouping_function_place_message;
    };

    static void visit(const QueryTreeNodePtr & node, Data & data)
    {
        auto * function_node = node->as<FunctionNode>();
        if (function_node && function_node->getFunctionName() == "grouping")
            throw Exception(ErrorCodes::ILLEGAL_AGGREGATION,
                "GROUPING function {} is found {} in query",
                function_node->formatASTForErrorMessage(),
                data.assert_no_grouping_function_place_message);
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        return child_node->getNodeType() != QueryTreeNodeType::QUERY || child_node->getNodeType() != QueryTreeNodeType::UNION;
    }
};

using ValidateGroupingFunctionNodesVisitor = ValidateGroupingFunctionNodesMatcher::Visitor;

void assertNoGroupingFunction(const QueryTreeNodePtr & node, const String & assert_no_grouping_function_place_message)
{
    ValidateGroupingFunctionNodesVisitor::Data data;
    data.assert_no_grouping_function_place_message = assert_no_grouping_function_place_message;

    ValidateGroupingFunctionNodesVisitor visitor(data);
    visitor.visit(node);
}

/** Resolve query.
  * This function modifies query node during resolve. It is caller responsibility to clone query node before resolve
  * if it is needed for later use.
  *
  * query_node - query_tree_node that must have QueryNode type.
  * scope - query scope. It is caller responsibility to create it.
  *
  * Resolve steps:
  * 1. Initialize query scope with aliases.
  * 2. Register CTE subqueries from WITH section in scope and remove them from WITH section.
  * 3. Resolve FROM section.
  * 4. Resolve projection columns.
  * 5. Resolve expressions in other query parts.
  * 6. Validate nodes with duplicate aliases.
  * 7. Validate aggregates, aggregate functions, GROUPING function, window functions.
  * 8. Remove WITH and WINDOW sections from query.
  * 9. Remove aliases from expression and lambda nodes.
  * 10. Resolve query tree node with projection columns.
  */
void QueryAnalyzer::resolveQuery(const QueryTreeNodePtr & query_node, IdentifierResolveScope & scope)
{
    const auto & settings = context->getSettingsRef();
    if (settings.max_subquery_depth && scope.subquery_depth > settings.max_subquery_depth)
        throw Exception(ErrorCodes::TOO_DEEP_SUBQUERIES,
            "Too deep subqueries. Maximum: {}",
            settings.max_subquery_depth.toString());

    auto & query_node_typed = query_node->as<QueryNode &>();

    /// Initialize aliases in query node scope

    QueryExpressionsAliasVisitor::Data data{scope};
    QueryExpressionsAliasVisitor visitor(data);

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
        if (!subquery_node || !subquery_node->isCTE())
            continue;

        const auto & cte_name = subquery_node->getCTEName();
        auto [_, inserted] = data.scope.cte_name_to_query_node.emplace(cte_name, node);
        if (!inserted)
            throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
                "CTE with name {} already exists. In scope {}",
                cte_name,
                data.scope.scope_node->formatASTForErrorMessage());
    }

    std::erase_if(with_nodes, [](const QueryTreeNodePtr & node)
    {
        auto * subquery_node = node->as<QueryNode>();
        return subquery_node && subquery_node->isCTE();
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
      * IN second join expression ON t1.id = t3.id t1.id must be resolved into join column that wrap test_table.id column.
      */
    scope.use_identifier_lookup_to_result_cache = false;

    if (query_node_typed.getJoinTree())
    {
        TableExpressionsAliasVisitor::Data table_expressions_visitor_data{scope};
        TableExpressionsAliasVisitor table_expressions_visitor(table_expressions_visitor_data);

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

        resolveSortColumnsNodeList(query_node_typed.getOrderByNode(), scope);
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

        node->removeAlias();
    }

    /** Validate aggregates
      *
      * 1. Check that there are no aggregate functions and GROUPING function in WHERE, in PREWHERE, in another aggregate functions.
      * 2. Check that there are no window functions in WHERE, in PREWHERE, in HAVING, in WINDOW, inside another aggregate function,
      * inside window function arguments, inside window function window definition.
      * 3. Check that there are no columns that are not specified in GROUP BY keys.
      * 4. Validate GROUP BY modifiers.
      */
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
                if (grouping_set_key->hasConstantValue())
                    continue;

                group_by_keys_nodes.push_back(grouping_set_key);
            }
        }
        else
        {
            if (node->hasConstantValue())
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
        ValidateGroupByColumnsVisitor::Data validate_group_by_visitor_data {group_by_keys_nodes};
        ValidateGroupByColumnsVisitor validate_group_by_visitor(validate_group_by_visitor_data);

        if (query_node_typed.hasHaving())
            validate_group_by_visitor.visit(query_node_typed.getHaving());

        if (query_node_typed.hasOrderBy())
            validate_group_by_visitor.visit(query_node_typed.getOrderByNode());

        validate_group_by_visitor.visit(query_node_typed.getProjectionNode());
    }

    if (context->getSettingsRef().group_by_use_nulls)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "GROUP BY use nulls is not supported");

    bool is_rollup_or_cube = query_node_typed.isGroupByWithRollup() || query_node_typed.isGroupByWithCube();
    if (!has_aggregation && (query_node_typed.isGroupByWithGroupingSets() || is_rollup_or_cube))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "WITH TOTALS, ROLLUP, CUBE or GROUPING SETS are not supported without aggregation");

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
    QueryAnalyzer analyzer(std::move(context));
    analyzer.resolve(query_tree_node, table_expression);
}

}
