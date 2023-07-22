#include <Analyzer/Utils.h>

#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTFunction.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>

namespace DB
{

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
    else if (function_name == "notIn")
        return "globalNotIn";
    else if (function_name == "nullIn")
        return "globalNullIn";
    else if (function_name == "notNullIn")
        return "globalNotNullIn";
    else if (function_name == "inIgnoreSet")
        return "globalInIgnoreSet";
    else if (function_name == "notInIgnoreSet")
        return "globalNotInIgnoreSet";
    else if (function_name == "nullInIgnoreSet")
        return "globalNullInIgnoreSet";
    else if (function_name == "notNullInIgnoreSet")
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

QueryTreeNodePtr buildCastFunction(const QueryTreeNodePtr & expression,
    const DataTypePtr & type,
    const ContextPtr & context,
    bool resolve)
{
    std::string cast_type = type->getName();
    auto cast_type_constant_value = std::make_shared<ConstantValue>(std::move(cast_type), std::make_shared<DataTypeString>());
    auto cast_type_constant_node = std::make_shared<ConstantNode>(std::move(cast_type_constant_value));

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
    const auto * constant_node = condition_node->as<ConstantNode>();
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

static ASTPtr convertIntoTableExpressionAST(const QueryTreeNodePtr & table_expression_node)
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
        table_expression_node_ast = table_expression_node->toAST();
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

void addTableExpressionOrJoinIntoTablesInSelectQuery(ASTPtr & tables_in_select_query_ast, const QueryTreeNodePtr & table_expression, const IQueryTreeNode::ConvertToASTOptions & convert_to_ast_options)
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
            auto table_expression_ast = convertIntoTableExpressionAST(table_expression);

            auto tables_in_select_query_element_ast = std::make_shared<ASTTablesInSelectQueryElement>();
            tables_in_select_query_element_ast->children.push_back(std::move(table_expression_ast));
            tables_in_select_query_element_ast->table_expression = tables_in_select_query_element_ast->children.back();

            tables_in_select_query_ast->children.push_back(std::move(tables_in_select_query_element_ast));
            break;
        }
        case QueryTreeNodeType::ARRAY_JOIN:
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

QueryTreeNodes extractTableExpressions(const QueryTreeNodePtr & join_tree_node)
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
            case QueryTreeNodeType::QUERY:
                [[fallthrough]];
            case QueryTreeNodeType::UNION:
                [[fallthrough]];
            case QueryTreeNodeType::TABLE_FUNCTION:
            {
                result.push_back(std::move(node_to_process));
                break;
            }
            case QueryTreeNodeType::ARRAY_JOIN:
            {
                auto & array_join_node = node_to_process->as<ArrayJoinNode &>();
                nodes_to_process.push_front(array_join_node.getTableExpression());
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

}
