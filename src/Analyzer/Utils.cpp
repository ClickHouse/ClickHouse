#include <Analyzer/Utils.h>

#include <Core/Settings.h>

#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTFunction.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypesBinaryEncoding.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnNullable.h>

#include <Common/FieldVisitorToString.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>

#include <Storages/IStorage.h>

#include <Interpreters/Context.h>

#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>

#include <Analyzer/Resolve/IdentifierResolveScope.h>

#include <ranges>
namespace DB
{
namespace Setting
{
    extern const SettingsBool extremes;
    extern const SettingsUInt64 max_result_bytes;
    extern const SettingsUInt64 max_result_rows;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

bool isNodePartOfTree(const IQueryTreeNode * node, const IQueryTreeNode * root)
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

bool isStorageUsedInTree(const StoragePtr & storage, const IQueryTreeNode * root)
{
    std::vector<const IQueryTreeNode *> nodes_to_process;
    nodes_to_process.push_back(root);

    while (!nodes_to_process.empty())
    {
        const auto * subtree_node = nodes_to_process.back();
        nodes_to_process.pop_back();

        const auto * table_node = subtree_node->as<TableNode>();
        const auto * table_function_node = subtree_node->as<TableFunctionNode>();

        if (table_node || table_function_node)
        {
            const auto & table_storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
            if (table_storage->getStorageID() == storage->getStorageID())
                return true;
        }

        for (const auto & child : subtree_node->getChildren())
        {
            if (child)
                nodes_to_process.push_back(child.get());
        }
    }

    return false;
}

bool isNameOfInFunction(const std::string & function_name)
{
    bool is_special_function_in = function_name == "in" ||
        function_name == "globalIn" ||
        function_name == "notIn" ||
        function_name == "globalNotIn" ||
        function_name == "nullIn" ||
        function_name == "globalNullIn" ||
        function_name == "notNullIn" ||
        function_name == "globalNotNullIn" ||
        function_name == "inIgnoreSet" ||
        function_name == "globalInIgnoreSet" ||
        function_name == "notInIgnoreSet" ||
        function_name == "globalNotInIgnoreSet" ||
        function_name == "nullInIgnoreSet" ||
        function_name == "globalNullInIgnoreSet" ||
        function_name == "notNullInIgnoreSet" ||
        function_name == "globalNotNullInIgnoreSet";

    return is_special_function_in;
}

bool isNameOfLocalInFunction(const std::string & function_name)
{
    bool is_special_function_in = function_name == "in" ||
        function_name == "notIn" ||
        function_name == "nullIn" ||
        function_name == "notNullIn" ||
        function_name == "inIgnoreSet" ||
        function_name == "notInIgnoreSet" ||
        function_name == "nullInIgnoreSet" ||
        function_name == "notNullInIgnoreSet";

    return is_special_function_in;
}

bool isNameOfGlobalInFunction(const std::string & function_name)
{
    bool is_special_function_in = function_name == "globalIn" ||
        function_name == "globalNotIn" ||
        function_name == "globalNullIn" ||
        function_name == "globalNotNullIn" ||
        function_name == "globalInIgnoreSet" ||
        function_name == "globalNotInIgnoreSet" ||
        function_name == "globalNullInIgnoreSet" ||
        function_name == "globalNotNullInIgnoreSet";

    return is_special_function_in;
}

std::string getGlobalInFunctionNameForLocalInFunctionName(const std::string & function_name)
{
    if (function_name == "in")
        return "globalIn";
    if (function_name == "notIn")
        return "globalNotIn";
    if (function_name == "nullIn")
        return "globalNullIn";
    if (function_name == "notNullIn")
        return "globalNotNullIn";
    if (function_name == "inIgnoreSet")
        return "globalInIgnoreSet";
    if (function_name == "notInIgnoreSet")
        return "globalNotInIgnoreSet";
    if (function_name == "nullInIgnoreSet")
        return "globalNullInIgnoreSet";
    if (function_name == "notNullInIgnoreSet")
        return "globalNotNullInIgnoreSet";

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid local IN function name {}", function_name);
}

void makeUniqueColumnNamesInBlock(Block & block)
{
    NameSet block_column_names;
    size_t unique_column_name_counter = 1;

    for (auto & column_with_type : block)
    {
        if (!block_column_names.contains(column_with_type.name))
        {
            block_column_names.insert(column_with_type.name);
            continue;
        }

        column_with_type.name += '_';
        column_with_type.name += std::to_string(unique_column_name_counter);
        ++unique_column_name_counter;
    }
}

bool isExpressionNodeType(QueryTreeNodeType node_type)
{
    return node_type == QueryTreeNodeType::CONSTANT || node_type == QueryTreeNodeType::COLUMN || node_type == QueryTreeNodeType::FUNCTION
        || node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION;
}

bool isFunctionExpressionNodeType(QueryTreeNodeType node_type)
{
    return node_type == QueryTreeNodeType::LAMBDA;
}

bool isSubqueryNodeType(QueryTreeNodeType node_type)
{
    return node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION;
}

bool isTableExpressionNodeType(QueryTreeNodeType node_type)
{
    return node_type == QueryTreeNodeType::TABLE || node_type == QueryTreeNodeType::TABLE_FUNCTION ||
        isSubqueryNodeType(node_type);
}

bool isQueryOrUnionNode(const IQueryTreeNode * node)
{
    auto node_type = node->getNodeType();
    return node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION;
}

bool isQueryOrUnionNode(const QueryTreeNodePtr & node)
{
    return isQueryOrUnionNode(node.get());
}

bool isCorrelatedQueryOrUnionNode(const QueryTreeNodePtr & node)
{
    auto * query_node = node->as<QueryNode>();
    auto * union_node = node->as<UnionNode>();

    return (query_node != nullptr && query_node->isCorrelated()) || (union_node != nullptr && union_node->isCorrelated());
}

bool checkCorrelatedColumn(
    const IdentifierResolveScope * scope_to_check,
    const QueryTreeNodePtr & column
)
{
    const auto * current_scope = scope_to_check;
    chassert(column->getNodeType() == QueryTreeNodeType::COLUMN);
    auto * column_node = column->as<ColumnNode>();
    auto column_source = column_node->getColumnSource();

    /// The case of lambda argument. Example:
    /// arrayMap(X -> X + Y, [0])
    ///
    /// X would have lambda as a source node
    /// Y comes from outer scope and requires ordinary check.
    if (column_source->getNodeType() == QueryTreeNodeType::LAMBDA)
        return false;

    bool is_correlated = false;

    while (scope_to_check != nullptr)
    {
        /// Check if column source is in the FROM section of the current scope (query).
        if (scope_to_check->registered_table_expression_nodes.contains(column_source))
            break;

        /// Previous check wouldn't work in the case of resolution of alias columns.
        /// In that case table expression is not registered yet and table expression data is being computed.
        if (scope_to_check->table_expressions_in_resolve_process.contains(column_source.get()))
            break;

        if (isQueryOrUnionNode(scope_to_check->scope_node))
        {
            is_correlated = true;
            if (auto * query_node = scope_to_check->scope_node->as<QueryNode>())
                query_node->addCorrelatedColumn(column);
            else if (auto * union_node = scope_to_check->scope_node->as<UnionNode>())
                union_node->addCorrelatedColumn(column);
        }
        scope_to_check = scope_to_check->parent_scope;
    }
    if (!scope_to_check)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot find the original scope of the column '{}'. Current scope: {}",
            column_node->getColumnName(),
            current_scope->scope_node->formatASTForErrorMessage());

    return is_correlated;
}

DataTypePtr getExpressionNodeResultTypeOrNull(const QueryTreeNodePtr & query_tree_node)
{
    auto node_type = query_tree_node->getNodeType();

    switch (node_type)
    {
        case QueryTreeNodeType::CONSTANT:
            [[fallthrough]];
        case QueryTreeNodeType::COLUMN:
        {
            return query_tree_node->getResultType();
        }
        case QueryTreeNodeType::FUNCTION:
        {
            auto & function_node = query_tree_node->as<FunctionNode &>();
            if (function_node.isResolved())
                return function_node.getResultType();
            break;
        }
        default:
        {
            break;
        }
    }

    return nullptr;
}

QueryTreeNodePtr buildCastFunction(const QueryTreeNodePtr & expression,
    const DataTypePtr & type,
    const ContextPtr & context,
    bool resolve)
{
    std::string cast_type = type->getName();
    auto cast_type_constant_node = std::make_shared<ConstantNode>(std::move(cast_type), std::make_shared<DataTypeString>());

    std::string cast_function_name = "_CAST";
    auto cast_function_node = std::make_shared<FunctionNode>(cast_function_name);
    cast_function_node->getArguments().getNodes().push_back(expression);
    cast_function_node->getArguments().getNodes().push_back(std::move(cast_type_constant_node));

    if (resolve)
    {
        auto cast_function = FunctionFactory::instance().get(cast_function_name, context);
        cast_function_node->resolveAsFunction(cast_function->build(cast_function_node->getArgumentColumns()));
    }

    return cast_function_node;
}

std::optional<bool> tryExtractConstantFromConditionNode(const QueryTreeNodePtr & condition_node)
{
    const auto * constant_node = condition_node ? condition_node->as<ConstantNode>() : nullptr;
    if (!constant_node)
        return {};

    const auto & value = constant_node->getValue();
    auto constant_type = constant_node->getResultType();
    constant_type = removeNullable(removeLowCardinality(constant_type));

    auto which_constant_type = WhichDataType(constant_type);
    if (!which_constant_type.isUInt8() && !which_constant_type.isNothing())
        return {};

    if (value.isNull())
        return false;

    UInt8 predicate_value = value.safeGet<UInt8>();
    return predicate_value > 0;
}

static ASTPtr convertIntoTableExpressionAST(
    const QueryTreeNodePtr & table_expression_node,
    const ConvertToASTOptions & convert_to_ast_options
)
{
    ASTPtr table_expression_node_ast;
    auto node_type = table_expression_node->getNodeType();

    if (node_type == QueryTreeNodeType::IDENTIFIER)
    {
        const auto & identifier_node = table_expression_node->as<IdentifierNode &>();
        const auto & identifier = identifier_node.getIdentifier();

        if (identifier.getPartsSize() == 1)
            table_expression_node_ast = std::make_shared<ASTTableIdentifier>(identifier[0]);
        else if (identifier.getPartsSize() == 2)
            table_expression_node_ast = std::make_shared<ASTTableIdentifier>(identifier[0], identifier[1]);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Identifier for table expression must contain 1 or 2 parts. Actual '{}'",
                identifier.getFullName());

        table_expression_node_ast->setAlias(identifier_node.getAlias());
    }
    else
    {
        table_expression_node_ast = table_expression_node->toAST(convert_to_ast_options);
    }

    auto result_table_expression = std::make_shared<ASTTableExpression>();
    result_table_expression->children.push_back(table_expression_node_ast);

    std::optional<TableExpressionModifiers> table_expression_modifiers;

    if (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION)
    {
        result_table_expression->subquery = result_table_expression->children.back();
    }
    else if (node_type == QueryTreeNodeType::TABLE || node_type == QueryTreeNodeType::IDENTIFIER)
    {
        if (auto * table_node = table_expression_node->as<TableNode>())
            table_expression_modifiers = table_node->getTableExpressionModifiers();
        else if (auto * identifier_node = table_expression_node->as<IdentifierNode>())
            table_expression_modifiers = identifier_node->getTableExpressionModifiers();

        result_table_expression->database_and_table_name = result_table_expression->children.back();
    }
    else if (node_type == QueryTreeNodeType::TABLE_FUNCTION)
    {
        if (auto * table_function_node = table_expression_node->as<TableFunctionNode>())
            table_expression_modifiers = table_function_node->getTableExpressionModifiers();

        result_table_expression->table_function = result_table_expression->children.back();
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Expected identifier, table, query, union or table function. Actual {}",
            table_expression_node->formatASTForErrorMessage());
    }

    if (table_expression_modifiers)
    {
        result_table_expression->final = table_expression_modifiers->hasFinal();

        const auto & sample_size_ratio = table_expression_modifiers->getSampleSizeRatio();
        if (sample_size_ratio.has_value())
            result_table_expression->sample_size = std::make_shared<ASTSampleRatio>(*sample_size_ratio);

        const auto & sample_offset_ratio = table_expression_modifiers->getSampleOffsetRatio();
        if (sample_offset_ratio.has_value())
            result_table_expression->sample_offset = std::make_shared<ASTSampleRatio>(*sample_offset_ratio);
    }

    return result_table_expression;
}

void addTableExpressionOrJoinIntoTablesInSelectQuery(
    ASTPtr & tables_in_select_query_ast,
    const QueryTreeNodePtr & table_expression,
    const ConvertToASTOptions & convert_to_ast_options
)
{
    auto table_expression_node_type = table_expression->getNodeType();

    switch (table_expression_node_type)
    {
        case QueryTreeNodeType::IDENTIFIER:
            [[fallthrough]];
        case QueryTreeNodeType::TABLE:
            [[fallthrough]];
        case QueryTreeNodeType::QUERY:
            [[fallthrough]];
        case QueryTreeNodeType::UNION:
            [[fallthrough]];
        case QueryTreeNodeType::TABLE_FUNCTION:
        {
            auto table_expression_ast = convertIntoTableExpressionAST(table_expression, convert_to_ast_options);

            auto tables_in_select_query_element_ast = std::make_shared<ASTTablesInSelectQueryElement>();
            tables_in_select_query_element_ast->children.push_back(std::move(table_expression_ast));
            tables_in_select_query_element_ast->table_expression = tables_in_select_query_element_ast->children.back();

            tables_in_select_query_ast->children.push_back(std::move(tables_in_select_query_element_ast));
            break;
        }
        case QueryTreeNodeType::ARRAY_JOIN:
            [[fallthrough]];
        case QueryTreeNodeType::CROSS_JOIN:
            [[fallthrough]];
        case QueryTreeNodeType::JOIN:
        {
            auto table_expression_tables_in_select_query_ast = table_expression->toAST(convert_to_ast_options);
            tables_in_select_query_ast->children.reserve(table_expression_tables_in_select_query_ast->children.size());
            for (auto && table_element_ast : table_expression_tables_in_select_query_ast->children)
                tables_in_select_query_ast->children.push_back(std::move(table_element_ast));
            break;
        }
        default:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Unexpected node type for table expression. "
                            "Expected identifier, table, table function, query, union, join or array join. Actual {}",
                            table_expression->getNodeTypeName());
        }
    }
}

QueryTreeNodes extractAllTableReferences(const QueryTreeNodePtr & tree)
{
    QueryTreeNodes result;

    QueryTreeNodes nodes_to_process;
    nodes_to_process.push_back(tree);

    while (!nodes_to_process.empty())
    {
        auto node_to_process = std::move(nodes_to_process.back());
        nodes_to_process.pop_back();

        auto node_type = node_to_process->getNodeType();

        switch (node_type)
        {
            case QueryTreeNodeType::TABLE:
            {
                result.push_back(std::move(node_to_process));
                break;
            }
            case QueryTreeNodeType::QUERY:
            {
                nodes_to_process.push_back(node_to_process->as<QueryNode>()->getJoinTree());
                break;
            }
            case QueryTreeNodeType::UNION:
            {
                for (const auto & union_node : node_to_process->as<UnionNode>()->getQueries().getNodes())
                    nodes_to_process.push_back(union_node);
                break;
            }
            case QueryTreeNodeType::TABLE_FUNCTION:
            {
                // Arguments of table function can't contain TableNodes.
                break;
            }
            case QueryTreeNodeType::ARRAY_JOIN:
            {
                nodes_to_process.push_back(node_to_process->as<ArrayJoinNode>()->getTableExpression());
                break;
            }
            case QueryTreeNodeType::CROSS_JOIN:
            {
                auto & cross_join_node = node_to_process->as<CrossJoinNode &>();
                for (const auto & expr : cross_join_node.getTableExpressions())
                     nodes_to_process.push_back(expr);
                break;
            }
            case QueryTreeNodeType::JOIN:
            {
                auto & join_node = node_to_process->as<JoinNode &>();
                nodes_to_process.push_back(join_node.getRightTableExpression());
                nodes_to_process.push_back(join_node.getLeftTableExpression());
                break;
            }
            default:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Unexpected node type for table expression. "
                                "Expected table, table function, query, union, join or array join. Actual {}",
                                node_to_process->getNodeTypeName());
            }
        }
    }

    return result;
}

QueryTreeNodes extractTableExpressions(const QueryTreeNodePtr & join_tree_node, bool add_array_join, bool recursive)
{
    QueryTreeNodes result;

    std::deque<QueryTreeNodePtr> nodes_to_process;
    nodes_to_process.push_back(join_tree_node);

    while (!nodes_to_process.empty())
    {
        auto node_to_process = std::move(nodes_to_process.front());
        nodes_to_process.pop_front();

        auto node_type = node_to_process->getNodeType();

        switch (node_type)
        {
            case QueryTreeNodeType::TABLE:
                [[fallthrough]];
            case QueryTreeNodeType::TABLE_FUNCTION:
            {
                result.push_back(std::move(node_to_process));
                break;
            }
            case QueryTreeNodeType::QUERY:
            {
                if (recursive)
                    nodes_to_process.push_back(node_to_process->as<QueryNode>()->getJoinTree());
                result.push_back(std::move(node_to_process));
                break;
            }
            case QueryTreeNodeType::UNION:
            {
                if (recursive)
                {
                    for (const auto & union_node : node_to_process->as<UnionNode>()->getQueries().getNodes())
                        nodes_to_process.push_back(union_node);
                }
                result.push_back(std::move(node_to_process));
                break;
            }
            case QueryTreeNodeType::ARRAY_JOIN:
            {
                auto & array_join_node = node_to_process->as<ArrayJoinNode &>();
                nodes_to_process.push_front(array_join_node.getTableExpression());
                if (add_array_join)
                    result.push_back(std::move(node_to_process));
                break;
            }
            case QueryTreeNodeType::CROSS_JOIN:
            {
                auto & join_node = node_to_process->as<CrossJoinNode &>();
                for (const auto & expr : std::ranges::reverse_view(join_node.getTableExpressions()))
                    nodes_to_process.push_front(expr);
                break;
            }
            case QueryTreeNodeType::JOIN:
            {
                auto & join_node = node_to_process->as<JoinNode &>();
                nodes_to_process.push_front(join_node.getRightTableExpression());
                nodes_to_process.push_front(join_node.getLeftTableExpression());
                break;
            }
            default:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Unexpected node type for table expression. "
                                "Expected table, table function, query, union, join or array join. Actual {}",
                                node_to_process->getNodeTypeName());
            }
        }
    }

    return result;
}

QueryTreeNodePtr extractLeftTableExpression(const QueryTreeNodePtr & join_tree_node)
{
    QueryTreeNodePtr result;

    std::deque<QueryTreeNodePtr> nodes_to_process;
    nodes_to_process.push_back(join_tree_node);

    while (!result)
    {
        auto node_to_process = std::move(nodes_to_process.front());
        nodes_to_process.pop_front();

        auto node_type = node_to_process->getNodeType();

        switch (node_type)
        {
            case QueryTreeNodeType::TABLE:
                [[fallthrough]];
            case QueryTreeNodeType::QUERY:
                [[fallthrough]];
            case QueryTreeNodeType::UNION:
                [[fallthrough]];
            case QueryTreeNodeType::TABLE_FUNCTION:
            {
                result = std::move(node_to_process);
                break;
            }
            case QueryTreeNodeType::ARRAY_JOIN:
            {
                auto & array_join_node = node_to_process->as<ArrayJoinNode &>();
                nodes_to_process.push_front(array_join_node.getTableExpression());
                break;
            }
            case QueryTreeNodeType::CROSS_JOIN:
            {
                auto & cross_join_node = node_to_process->as<CrossJoinNode &>();
                nodes_to_process.push_front(cross_join_node.getTableExpressions().front());
                break;
            }
            case QueryTreeNodeType::JOIN:
            {
                auto & join_node = node_to_process->as<JoinNode &>();
                nodes_to_process.push_front(join_node.getLeftTableExpression());
                break;
            }
            default:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Unexpected node type for table expression. "
                                "Expected table, table function, query, union, join or array join. Actual {}",
                                node_to_process->getNodeTypeName());
            }
        }
    }

    return result;
}

namespace
{

void buildTableExpressionsStackImpl(const QueryTreeNodePtr & join_tree_node, QueryTreeNodes & result)
{
    auto node_type = join_tree_node->getNodeType();

    switch (node_type)
    {
        case QueryTreeNodeType::TABLE:
            [[fallthrough]];
        case QueryTreeNodeType::QUERY:
            [[fallthrough]];
        case QueryTreeNodeType::UNION:
            [[fallthrough]];
        case QueryTreeNodeType::TABLE_FUNCTION:
        {
            result.push_back(join_tree_node);
            break;
        }
        case QueryTreeNodeType::ARRAY_JOIN:
        {
            auto & array_join_node = join_tree_node->as<ArrayJoinNode &>();
            buildTableExpressionsStackImpl(array_join_node.getTableExpression(), result);
            result.push_back(join_tree_node);
            break;
        }
        case QueryTreeNodeType::CROSS_JOIN:
        {
            auto & cross_join_node = join_tree_node->as<CrossJoinNode &>();

            for (const auto & expr : cross_join_node.getTableExpressions())
                buildTableExpressionsStackImpl(expr, result);

            result.push_back(join_tree_node);
            break;
        }
        case QueryTreeNodeType::JOIN:
        {
            auto & join_node = join_tree_node->as<JoinNode &>();
            buildTableExpressionsStackImpl(join_node.getLeftTableExpression(), result);
            buildTableExpressionsStackImpl(join_node.getRightTableExpression(), result);
            result.push_back(join_tree_node);
            break;
        }
        default:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Unexpected node type for table expression. Expected table, table function, query, union, join or array join. Actual {}",
                join_tree_node->getNodeTypeName());
        }
    }
}

}

QueryTreeNodes buildTableExpressionsStack(const QueryTreeNodePtr & join_tree_node)
{
    QueryTreeNodes result;
    buildTableExpressionsStackImpl(join_tree_node, result);

    return result;
}

namespace
{

class CheckFunctionExistsVisitor : public ConstInDepthQueryTreeVisitor<CheckFunctionExistsVisitor>
{
public:
    explicit CheckFunctionExistsVisitor(std::string_view function_name_)
        : function_name(function_name_)
    {}

    void visitImpl(const QueryTreeNodePtr & node)
    {
        if (has_function)
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        has_function = function_node->getFunctionName() == function_name;
    }

    bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node) const
    {
        if (has_function)
            return false;

        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }

    bool hasFunction() const
    {
        return has_function;
    }

private:
    std::string_view function_name;
    bool has_function = false;
};

inline AggregateFunctionPtr resolveAggregateFunction(FunctionNode & function_node, const String & function_name)
{
    Array parameters;
    for (const auto & param : function_node.getParameters())
    {
        auto * constant = param->as<ConstantNode>();
        parameters.push_back(constant->getValue());
    }

    const auto & function_node_argument_nodes = function_node.getArguments().getNodes();

    DataTypes argument_types;
    argument_types.reserve(function_node_argument_nodes.size());

    for (const auto & function_node_argument : function_node_argument_nodes)
        argument_types.emplace_back(function_node_argument->getResultType());

    AggregateFunctionProperties properties;
    auto action = NullsAction::EMPTY;
    return AggregateFunctionFactory::instance().get(function_name, action, argument_types, parameters, properties);
}

}

bool hasFunctionNode(const QueryTreeNodePtr & node, std::string_view function_name)
{
    CheckFunctionExistsVisitor visitor(function_name);
    visitor.visit(node);

    return visitor.hasFunction();
}

namespace
{

class ReplaceColumnsVisitor : public InDepthQueryTreeVisitor<ReplaceColumnsVisitor>
{
public:
    explicit ReplaceColumnsVisitor(const QueryTreeNodePtr & table_expression_node_,
        const std::unordered_map<std::string, QueryTreeNodePtr> & column_name_to_node_)
        : table_expression_node(table_expression_node_)
        , column_name_to_node(column_name_to_node_)
    {}

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        auto column_source = column_node->getColumnSourceOrNull();
        if (column_source != table_expression_node)
            return;

        auto node_it = column_name_to_node.find(column_node->getColumnName());
        if (node_it == column_name_to_node.end())
            return;

        node = node_it->second;
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }

private:
    QueryTreeNodePtr table_expression_node;
    const std::unordered_map<std::string, QueryTreeNodePtr> & column_name_to_node;
};

}

void replaceColumns(QueryTreeNodePtr & node,
    const QueryTreeNodePtr & table_expression_node,
    const std::unordered_map<std::string, QueryTreeNodePtr> & column_name_to_node)
{
    ReplaceColumnsVisitor visitor(table_expression_node, column_name_to_node);
    visitor.visit(node);
}

void rerunFunctionResolve(FunctionNode * function_node, ContextPtr context)
{
    if (!function_node->isResolved())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to rerun resolve of unresolved function '{}'", function_node->getFunctionName());

    const auto & name = function_node->getFunctionName();
    if (function_node->isOrdinaryFunction())
    {
        // Special case, don't need to be resolved. It must be processed by GroupingFunctionsResolvePass.
        if (name == "grouping")
            return;
        auto function = FunctionFactory::instance().get(name, context);
        function_node->resolveAsFunction(function->build(function_node->getArgumentColumns()));
    }
    else if (function_node->isAggregateFunction())
    {
        if (name == "nothing" || name == "nothingUInt64" || name == "nothingNull")
            return;
        function_node->resolveAsAggregateFunction(resolveAggregateFunction(*function_node, function_node->getFunctionName()));
    }
    else if (function_node->isWindowFunction())
    {
        function_node->resolveAsWindowFunction(resolveAggregateFunction(*function_node, function_node->getFunctionName()));
    }
}

namespace
{

class CollectIdentifiersFullNamesVisitor : public ConstInDepthQueryTreeVisitor<CollectIdentifiersFullNamesVisitor>
{
public:
    explicit CollectIdentifiersFullNamesVisitor(NameSet & used_identifiers_)
        : used_identifiers(used_identifiers_) { }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr &) { return true; }

    void visitImpl(const QueryTreeNodePtr & node)
    {
        auto * column_node = node->as<IdentifierNode>();
        if (!column_node)
            return;

        used_identifiers.insert(column_node->getIdentifier().getFullName());
    }

    NameSet & used_identifiers;
};

}

NameSet collectIdentifiersFullNames(const QueryTreeNodePtr & node)
{
    NameSet out;
    CollectIdentifiersFullNamesVisitor visitor(out);
    visitor.visit(node);
    return out;
}

QueryTreeNodePtr createCastFunction(QueryTreeNodePtr node, DataTypePtr result_type, ContextPtr context)
{
    auto enum_literal_node = std::make_shared<ConstantNode>(result_type->getName(), std::make_shared<DataTypeString>());

    auto cast_function = FunctionFactory::instance().get("_CAST", std::move(context));
    QueryTreeNodes arguments{ std::move(node), std::move(enum_literal_node) };

    auto function_node = std::make_shared<FunctionNode>("_CAST");
    function_node->getArguments().getNodes() = std::move(arguments);

    function_node->resolveAsFunction(cast_function->build(function_node->getArgumentColumns()));

    return function_node;
}

void resolveOrdinaryFunctionNodeByName(FunctionNode & function_node, const String & function_name, const ContextPtr & context)
{
    auto function = FunctionFactory::instance().get(function_name, context);
    function_node.resolveAsFunction(function->build(function_node.getArgumentColumns()));
}

void resolveAggregateFunctionNodeByName(FunctionNode & function_node, const String & function_name)
{
    auto aggregate_function = resolveAggregateFunction(function_node, function_name);
    function_node.resolveAsAggregateFunction(std::move(aggregate_function));
}

/** Returns:
  * {_, false} - multiple sources
  * {nullptr, true} - no sources (for constants)
  * {source, true} - single source
  */
std::pair<QueryTreeNodePtr, bool> getExpressionSourceImpl(const QueryTreeNodePtr & node)
{
    if (const auto * column = node->as<ColumnNode>())
    {
        auto source = column->getColumnSourceOrNull();
        if (!source)
            return {nullptr, false};
        return {source, true};
    }

    if (const auto * func = node->as<FunctionNode>())
    {
        QueryTreeNodePtr source = nullptr;
        const auto & args = func->getArguments().getNodes();
        for (const auto & arg : args)
        {
            auto [arg_source, is_ok] = getExpressionSourceImpl(arg);
            if (!is_ok)
                return {nullptr, false};

            if (!source)
                source = arg_source;
            else if (arg_source && !source->isEqual(*arg_source))
                return {nullptr, false};
        }
        return {source, true};

    }

    if (node->as<ConstantNode>())
        return {nullptr, true};

    return {nullptr, false};
}

QueryTreeNodePtr getExpressionSource(const QueryTreeNodePtr & node)
{
    auto [source, is_ok] = getExpressionSourceImpl(node);
    if (!is_ok)
        return nullptr;
    return source;
}

/** There are no limits on the maximum size of the result for the subquery.
  * Since the result of the query is not the result of the entire query.
  */
void updateContextForSubqueryExecution(ContextMutablePtr & mutable_context)
{
    /** The subquery in the IN / JOIN section does not have any restrictions on the maximum size of the result.
      * Because the result of this query is not the result of the entire query.
      * Constraints work instead
      *  max_rows_in_set, max_bytes_in_set, set_overflow_mode,
      *  max_rows_in_join, max_bytes_in_join, join_overflow_mode,
      *  which are checked separately (in the Set, Join objects).
      */
    Settings subquery_settings = mutable_context->getSettingsCopy();
    subquery_settings[Setting::max_result_rows] = 0;
    subquery_settings[Setting::max_result_bytes] = 0;
    /// The calculation of extremes does not make sense and is not necessary (if you do it, then the extremes of the subquery can be taken for whole query).
    subquery_settings[Setting::extremes] = false;
    mutable_context->setSettings(subquery_settings);
}

QueryTreeNodePtr buildQueryToReadColumnsFromTableExpression(const NamesAndTypes & columns,
    const QueryTreeNodePtr & table_expression,
    ContextMutablePtr & context)
{
    auto projection_columns = columns;

    QueryTreeNodes subquery_projection_nodes;
    subquery_projection_nodes.reserve(projection_columns.size());

    for (const auto & column : projection_columns)
        subquery_projection_nodes.push_back(std::make_shared<ColumnNode>(column, table_expression));

    if (subquery_projection_nodes.empty())
    {
        auto constant_data_type = std::make_shared<DataTypeUInt64>();
        subquery_projection_nodes.push_back(std::make_shared<ConstantNode>(1UL, constant_data_type));
        projection_columns.push_back({"1", std::move(constant_data_type)});
    }

    updateContextForSubqueryExecution(context);

    auto query_node = std::make_shared<QueryNode>(std::move(context));

    query_node->getProjection().getNodes() = std::move(subquery_projection_nodes);
    query_node->resolveProjectionColumns(projection_columns);
    query_node->getJoinTree() = table_expression;

    return query_node;
}

QueryTreeNodePtr buildSubqueryToReadColumnsFromTableExpression(const NamesAndTypes & columns,
    const QueryTreeNodePtr & table_expression,
    ContextMutablePtr & context)
{
    auto result = buildQueryToReadColumnsFromTableExpression(columns, table_expression, context);
    result->as<QueryNode &>().setIsSubquery(true);
    return result;
}

QueryTreeNodePtr buildQueryToReadColumnsFromTableExpression(const NamesAndTypes & columns,
    const QueryTreeNodePtr & table_expression,
    const ContextPtr & context)
{
    auto context_copy = Context::createCopy(context);
    return buildQueryToReadColumnsFromTableExpression(columns, table_expression, context_copy);
}

QueryTreeNodePtr buildSubqueryToReadColumnsFromTableExpression(const NamesAndTypes & columns,
    const QueryTreeNodePtr & table_expression,
    const ContextPtr & context)
{
    auto context_copy = Context::createCopy(context);
    return buildSubqueryToReadColumnsFromTableExpression(columns, table_expression, context_copy);
}

QueryTreeNodePtr buildSubqueryToReadColumnsFromTableExpression(const QueryTreeNodePtr & table_node, const ContextPtr & context)
{
    const auto & storage_snapshot = table_node->as<TableNode>()->getStorageSnapshot();
    auto columns_to_select_list = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::Ordinary));
    NamesAndTypes columns_to_select(columns_to_select_list.begin(), columns_to_select_list.end());
    return buildSubqueryToReadColumnsFromTableExpression(columns_to_select, table_node, context);
}

bool hasUnknownColumn(const QueryTreeNodePtr & node, QueryTreeNodePtr table_expression)
{
    QueryTreeNodes stack = { node };
    while (!stack.empty())
    {
        auto current = stack.back();
        stack.pop_back();

        switch (current->getNodeType())
        {
            case QueryTreeNodeType::CONSTANT:
                break;
            case QueryTreeNodeType::COLUMN:
            {
                auto * column_node = current->as<ColumnNode>();
                auto source = column_node->getColumnSourceOrNull();
                if (source && table_expression && !source->isEqual(*table_expression))
                    return true;
                break;
            }
            default:
            {
                for (const auto & child : current->getChildren())
                {
                    if (child)
                        stack.push_back(child);
                }
            }
        }
    }
    return false;
}

void removeExpressionsThatDoNotDependOnTableIdentifiers(
    QueryTreeNodePtr & expression,
    const QueryTreeNodePtr & table_expression,
    const ContextPtr & context)
{
    auto * function = expression->as<FunctionNode>();
    if (!function)
        return;

    if (function->getFunctionName() != "and")
    {
        if (hasUnknownColumn(expression, table_expression))
            expression = nullptr;
        return;
    }

    QueryTreeNodesDeque conjunctions;
    QueryTreeNodesDeque processing{ expression };

    while (!processing.empty())
    {
        auto node = std::move(processing.front());
        processing.pop_front();

        if (auto * function_node = node->as<FunctionNode>())
        {
            if (function_node->getFunctionName() == "and")
                std::ranges::copy(
                    function_node->getArguments(),
                    std::back_inserter(processing)
                );
            else
                conjunctions.push_back(node);
        }
        else
        {
            conjunctions.push_back(node);
        }
    }

    std::swap(processing, conjunctions);

    for (const auto & node : processing)
    {
        if (!hasUnknownColumn(node, table_expression))
            conjunctions.push_back(node);
    }

    if (conjunctions.empty())
    {
        expression = {};
        return;
    }
    if (conjunctions.size() == 1)
    {
        expression = conjunctions[0];
        return;
    }

    function->getArguments().getNodes().clear();
    std::ranges::move(conjunctions, std::back_inserter(function->getArguments().getNodes()));

    const auto function_impl = FunctionFactory::instance().get("and", context);
    function->resolveAsFunction(function_impl->build(function->getArgumentColumns()));
}

namespace
{

Field getFieldFromColumnForASTLiteralImpl(const ColumnPtr & column, size_t row, const DataTypePtr & data_type, bool is_inside_object)
{
    if (isColumnConst(*column))
        return getFieldFromColumnForASTLiteralImpl(assert_cast<const ColumnConst& >(*column).getDataColumnPtr(), 0, data_type, is_inside_object);

    switch (data_type->getTypeId())
    {
        case TypeIndex::Nullable:
        {
            const auto & nullable_data_type = assert_cast<const DataTypeNullable &>(*data_type);
            const auto & nullable_column = assert_cast<const ColumnNullable &>(*column);
            if (nullable_column.isNullAt(row))
                return Null();
            return getFieldFromColumnForASTLiteralImpl(nullable_column.getNestedColumnPtr(), row, nullable_data_type.getNestedType(), is_inside_object);
        }
        case TypeIndex::Date: [[fallthrough]];
        case TypeIndex::Date32: [[fallthrough]];
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::DateTime64:
        {
            WriteBufferFromOwnString buf;
            data_type->getDefaultSerialization()->serializeText(*column, row, buf, {});
            return Field(buf.str());
        }
        case TypeIndex::UInt8:
        {
            auto field = (*column)[row];
            if (isBool(data_type))
                return bool(field.safeGet<bool>());
            return field;
        }

        case TypeIndex::Array:
        {
            const auto & nested_data_type = assert_cast<const DataTypeArray &>(*data_type).getNestedType();
            const auto & array_column = assert_cast<const ColumnArray &>(*column);
            const auto & offsets = array_column.getOffsets();
            const auto & nested_column = array_column.getDataPtr();
            size_t start = offsets[static_cast<ssize_t>(row) - 1];
            size_t end = offsets[row];
            Array array;
            array.reserve(end - start);
            for (size_t i = start; i != end; ++i)
                array.push_back(getFieldFromColumnForASTLiteralImpl(nested_column, i, nested_data_type, is_inside_object));
            return array;
        }
        case TypeIndex::Map:
        {
            /// If we are inside Object, return Map as Object value instead of Array(Tuple).
            if (is_inside_object)
            {
                const auto & map_type = assert_cast<const DataTypeMap &>(*data_type);
                const auto & map_column = assert_cast<const ColumnMap &>(*column);
                const auto & offsets = map_column.getNestedColumn().getOffsets();
                const auto & key_column = map_column.getNestedData().getColumnPtr(0);
                const auto & value_column = map_column.getNestedData().getColumnPtr(1);
                size_t start = offsets[static_cast<ssize_t>(row) - 1];
                size_t end = offsets[row];
                Object object;
                for (size_t i = start; i != end; ++i)
                {
                    auto key_field = convertFieldToString(getFieldFromColumnForASTLiteralImpl(key_column, i, map_type.getKeyType(), is_inside_object));
                    auto value_field = getFieldFromColumnForASTLiteralImpl(value_column, i, map_type.getValueType(), is_inside_object);
                    object[key_field] = value_field;
                }

                return object;
            }

            const auto & nested_type = assert_cast<const DataTypeMap &>(*data_type).getNestedType();
            const auto & nested_column = assert_cast<const ColumnMap &>(*column).getNestedColumnPtr();
            return getFieldFromColumnForASTLiteralImpl(nested_column, row, nested_type, is_inside_object);
        }
        case TypeIndex::Tuple:
        {
            const auto & element_types = assert_cast<const DataTypeTuple &>(*data_type).getElements();
            const auto & element_columns = assert_cast<const ColumnTuple &>(*column).getColumns();
            Tuple tuple;
            tuple.reserve(element_columns.size());
            for (size_t i = 0; i != element_types.size(); ++i)
                tuple.push_back(getFieldFromColumnForASTLiteralImpl(element_columns[i], row, element_types[i], is_inside_object));
            return tuple;
        }
        case TypeIndex::Variant:
        {
            const auto & variant_types = assert_cast<const DataTypeVariant &>(*data_type).getVariants();
            const auto & variant_column = assert_cast<const ColumnVariant &>(*column);
            auto global_discr = variant_column.globalDiscriminatorAt(row);
            if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
                return Null();
            const auto & variant = variant_column.getVariantPtrByGlobalDiscriminator(global_discr);
            size_t variant_offset = variant_column.offsetAt(row);
            return getFieldFromColumnForASTLiteralImpl(variant, variant_offset, variant_types[global_discr], is_inside_object);
        }
        case TypeIndex::Dynamic:
        {
            const auto & dynamic_column = assert_cast<const ColumnDynamic &>(*column);
            const auto & variant_column = dynamic_column.getVariantColumn();
            auto global_discr = variant_column.globalDiscriminatorAt(row);
            if (global_discr != dynamic_column.getSharedVariantDiscriminator())
                return getFieldFromColumnForASTLiteralImpl(dynamic_column.getVariantColumnPtr(), row, dynamic_column.getVariantInfo().variant_type, is_inside_object);

            const auto & shared_variant = dynamic_column.getSharedVariant();
            auto value_data = shared_variant.getDataAt(variant_column.offsetAt(row));
            ReadBufferFromMemory buf(value_data.data, value_data.size);
            auto type = decodeDataType(buf);
            auto tmp_column = type->createColumn();
            tmp_column->reserve(1);
            type->getDefaultSerialization()->deserializeBinary(*tmp_column, buf, {});
            return getFieldFromColumnForASTLiteralImpl(std::move(tmp_column), 0, type, is_inside_object);
        }
        case TypeIndex::Object:
        {
            const auto & object_column = assert_cast<const ColumnObject &>(*column);
            const auto & typed_paths_types = assert_cast<const DataTypeObject &>(*data_type).getTypedPaths();
            Object object;
            for (const auto & [path, path_column] : object_column.getTypedPaths())
                object[path] = getFieldFromColumnForASTLiteralImpl(path_column, row, typed_paths_types.at(path), true);

            for (const auto & [path, path_column] : object_column.getDynamicPaths())
                object[path] = getFieldFromColumnForASTLiteralImpl(path_column, row, std::make_shared<DataTypeDynamic>(), true);

            const auto & shared_data_offsets = object_column.getSharedDataOffsets();
            const auto [shared_paths, shared_values] = object_column.getSharedDataPathsAndValues();
            size_t start = shared_data_offsets[static_cast<ssize_t>(row) - 1];
            size_t end = shared_data_offsets[row];
            auto dynamic_type = std::make_shared<DataTypeDynamic>();
            auto dynamic_serialization = dynamic_type->getDefaultSerialization();
            for (size_t i = start; i != end; ++i)
            {
                String path = shared_paths->getDataAt(i).toString();
                auto value_data = shared_values->getDataAt(i);
                ReadBufferFromMemory buf(value_data.data, value_data.size);
                auto tmp_column = dynamic_type->createColumn();
                tmp_column->reserve(1);
                dynamic_serialization->deserializeBinary(*tmp_column, buf, {});
                object[path] = getFieldFromColumnForASTLiteralImpl(std::move(tmp_column), 0, dynamic_type, true);
            }

            return is_inside_object ? Field(object) : Field(convertObjectToString(object));
        }
        default:
            return (*column)[row];
    }
}

}

Field getFieldFromColumnForASTLiteral(const ColumnPtr & column, size_t row, const DataTypePtr & data_type)
{
    return getFieldFromColumnForASTLiteralImpl(column, row, data_type, false);
}

}
