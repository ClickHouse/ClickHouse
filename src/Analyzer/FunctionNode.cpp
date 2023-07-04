#include <Analyzer/FunctionNode.h>

#include <Common/SipHash.h>
#include <Common/FieldVisitorToString.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeSet.h>

#include <Parsers/ASTFunction.h>

#include <Functions/IFunction.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/Utils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/IdentifierNode.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

FunctionNode::FunctionNode(String function_name_)
    : IQueryTreeNode(children_size)
    , function_name(function_name_)
{
    children[parameters_child_index] = std::make_shared<ListNode>();
    children[arguments_child_index] = std::make_shared<ListNode>();
}

const DataTypes & FunctionNode::getArgumentTypes() const
{
    if (!function)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Function {} is not resolved",
        function_name);
    return function->getArgumentTypes();
}

ColumnsWithTypeAndName FunctionNode::getArgumentColumns() const
{
    const auto & arguments = getArguments().getNodes();
    size_t arguments_size = arguments.size();

    ColumnsWithTypeAndName argument_columns;
    argument_columns.reserve(arguments.size());

    for (size_t i = 0; i < arguments_size; ++i)
    {
        const auto & argument = arguments[i];

        ColumnWithTypeAndName argument_column;

        if (isNameOfInFunction(function_name) && i == 1)
            argument_column.type = std::make_shared<DataTypeSet>();
        else
            argument_column.type = argument->getResultType();

        auto * constant = argument->as<ConstantNode>();
        if (constant && !isNotCreatable(argument_column.type))
            argument_column.column = argument_column.type->createColumnConst(1, constant->getValue());

        argument_columns.push_back(std::move(argument_column));
    }

    return argument_columns;
}

void FunctionNode::resolveAsFunction(FunctionBasePtr function_value)
{
    function_name = function_value->getName();
    function = std::move(function_value);
    kind = FunctionKind::ORDINARY;
}

void FunctionNode::resolveAsAggregateFunction(AggregateFunctionPtr aggregate_function_value)
{
    function_name = aggregate_function_value->getName();
    function = std::move(aggregate_function_value);
    kind = FunctionKind::AGGREGATE;
}

void FunctionNode::resolveAsWindowFunction(AggregateFunctionPtr window_function_value)
{
    if (!hasWindow())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Trying to resolve FunctionNode without window definition as a window function {}", window_function_value->getName());
    resolveAsAggregateFunction(window_function_value);
    kind = FunctionKind::WINDOW;
}

void FunctionNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "FUNCTION id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    buffer << ", function_name: " << function_name;

    std::string function_type = "ordinary";
    if (isAggregateFunction())
        function_type = "aggregate";
    else if (isWindowFunction())
        function_type = "window";

    buffer << ", function_type: " << function_type;

    if (function)
        buffer << ", result_type: " + getResultType()->getName();

    const auto & parameters = getParameters();
    if (!parameters.getNodes().empty())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "PARAMETERS\n";
        parameters.dumpTreeImpl(buffer, format_state, indent + 4);
    }

    const auto & arguments = getArguments();
    if (!arguments.getNodes().empty())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "ARGUMENTS\n";
        arguments.dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasWindow())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "WINDOW\n";
        getWindowNode()->dumpTreeImpl(buffer, format_state, indent + 4);
    }
}

bool FunctionNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const FunctionNode &>(rhs);
    if (function_name != rhs_typed.function_name ||
        isAggregateFunction() != rhs_typed.isAggregateFunction() ||
        isOrdinaryFunction() != rhs_typed.isOrdinaryFunction() ||
        isWindowFunction() != rhs_typed.isWindowFunction())
        return false;

    if (isResolved() != rhs_typed.isResolved())
        return false;
    if (!isResolved())
        return true;

    auto lhs_result_type = getResultType();
    auto rhs_result_type = rhs.getResultType();

    if (lhs_result_type && rhs_result_type && !lhs_result_type->equals(*rhs_result_type))
        return false;
    else if (lhs_result_type && !rhs_result_type)
        return false;
    else if (!lhs_result_type && rhs_result_type)
        return false;

    return true;
}

void FunctionNode::updateTreeHashImpl(HashState & hash_state) const
{
    hash_state.update(function_name.size());
    hash_state.update(function_name);
    hash_state.update(isOrdinaryFunction());
    hash_state.update(isAggregateFunction());
    hash_state.update(isWindowFunction());

    if (!isResolved())
        return;

    if (auto result_type = getResultType())
    {
        auto result_type_name = result_type->getName();
        hash_state.update(result_type_name.size());
        hash_state.update(result_type_name);
    }
}

QueryTreeNodePtr FunctionNode::cloneImpl() const
{
    auto result_function = std::make_shared<FunctionNode>(function_name);

    /** This is valid for clone method to reuse same function pointers
      * because ordinary functions or aggregate functions must be stateless.
      */
    result_function->function = function;
    result_function->kind = kind;
    result_function->wrap_with_nullable = wrap_with_nullable;

    return result_function;
}

ASTPtr FunctionNode::toASTImpl(const ConvertToASTOptions & options) const
{
    auto function_ast = std::make_shared<ASTFunction>();

    function_ast->name = function_name;

    if (isWindowFunction())
    {
        function_ast->is_window_function = true;
        function_ast->kind = ASTFunction::Kind::WINDOW_FUNCTION;
    }

    auto new_options = options;
    /// To avoid surrounding constants with several internal casts.
    if (function_name == "_CAST" && (*getArguments().begin())->getNodeType() == QueryTreeNodeType::CONSTANT)
        new_options.add_cast_for_constants = false;

    const auto & parameters = getParameters();
    if (!parameters.getNodes().empty())
    {
        function_ast->children.push_back(parameters.toAST(new_options));
        function_ast->parameters = function_ast->children.back();
    }

    const auto & arguments = getArguments();
    function_ast->children.push_back(arguments.toAST(new_options));
    function_ast->arguments = function_ast->children.back();

    auto window_node = getWindowNode();
    if (window_node)
    {
        if (auto * identifier_node = window_node->as<IdentifierNode>())
            function_ast->window_name = identifier_node->getIdentifier().getFullName();
        else
            function_ast->window_definition = window_node->toAST(new_options);
    }

    return function_ast;
}

}
