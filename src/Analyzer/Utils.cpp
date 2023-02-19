#include <Analyzer/Utils.h>

#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTFunction.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>

#include <Functions/FunctionHelpers.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/IdentifierNode.h>
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

void addTableExpressionOrJoinIntoTablesInSelectQuery(ASTPtr & tables_in_select_query_ast, const QueryTreeNodePtr & table_expression)
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
            auto table_expression_tables_in_select_query_ast = table_expression->toAST();
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

bool nestedIdentifierCanBeResolved(const DataTypePtr & compound_type, IdentifierView nested_identifier)
{
    const IDataType * current_type = compound_type.get();

    for (const auto & identifier_part : nested_identifier)
    {
        while (const DataTypeArray * array = checkAndGetDataType<DataTypeArray>(current_type))
            current_type = array->getNestedType().get();

        const DataTypeTuple * tuple = checkAndGetDataType<DataTypeTuple>(current_type);

        if (!tuple)
            return false;

        auto position = tuple->tryGetPositionByName(identifier_part);
        if (!position)
            return false;

        current_type = tuple->getElements()[*position].get();
    }

    return true;
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
