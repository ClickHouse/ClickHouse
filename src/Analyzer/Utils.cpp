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

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>

#include <Storages/IStorage.h>

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

bool isQueryOrUnionNode(const IQueryTreeNode * node)
{
    auto node_type = node->getNodeType();
    return node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION;
}

bool isQueryOrUnionNode(const QueryTreeNodePtr & node)
{
    return isQueryOrUnionNode(node.get());
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
    auto enum_literal = std::make_shared<ConstantValue>(result_type->getName(), std::make_shared<DataTypeString>());
    auto enum_literal_node = std::make_shared<ConstantNode>(std::move(enum_literal));

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

}
