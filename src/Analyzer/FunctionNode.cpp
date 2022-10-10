#include <Analyzer/FunctionNode.h>

#include <Common/SipHash.h>
#include <Common/FieldVisitorToString.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Parsers/ASTFunction.h>

#include <Functions/IFunction.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/IdentifierNode.h>

namespace DB
{

FunctionNode::FunctionNode(String function_name_)
    : IQueryTreeNode(children_size)
    , function_name(function_name_)
{
    children[parameters_child_index] = std::make_shared<ListNode>();
    children[arguments_child_index] = std::make_shared<ListNode>();
}

void FunctionNode::resolveAsFunction(FunctionOverloadResolverPtr function_value, DataTypePtr result_type_value)
{
    aggregate_function = nullptr;
    function = std::move(function_value);
    result_type = std::move(result_type_value);
    function_name = function->getName();
}

void FunctionNode::resolveAsAggregateFunction(AggregateFunctionPtr aggregate_function_value, DataTypePtr result_type_value)
{
    function = nullptr;
    aggregate_function = std::move(aggregate_function_value);
    result_type = std::move(result_type_value);
    function_name = aggregate_function->getName();
}

void FunctionNode::resolveAsWindowFunction(AggregateFunctionPtr window_function_value, DataTypePtr result_type_value)
{
    resolveAsAggregateFunction(window_function_value, result_type_value);
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

    if (result_type)
        buffer << ", result_type: " + result_type->getName();

    if (constant_value)
    {
        buffer << ", constant_value: " << constant_value->getValue().dump();
        buffer << ", constant_value_type: " << constant_value->getType()->getName();
    }

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

String FunctionNode::getName() const
{
    String name = function_name;

    const auto & parameters = getParameters();
    const auto & parameters_nodes = parameters.getNodes();
    if (!parameters_nodes.empty())
    {
        name += '(';
        name += parameters.getName();
        name += ')';
    }

    const auto & arguments = getArguments();
    name += '(';
    name += arguments.getName();
    name += ')';

    return name;
}

bool FunctionNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const FunctionNode &>(rhs);
    if (function_name != rhs_typed.function_name ||
        isAggregateFunction() != rhs_typed.isAggregateFunction() ||
        isOrdinaryFunction() != rhs_typed.isOrdinaryFunction() ||
        isWindowFunction() != rhs_typed.isWindowFunction())
        return false;

    if (result_type && rhs_typed.result_type && !result_type->equals(*rhs_typed.getResultType()))
        return false;
    else if (result_type && !rhs_typed.result_type)
        return false;
    else if (!result_type && rhs_typed.result_type)
        return false;

    if (constant_value && rhs_typed.constant_value && *constant_value != *rhs_typed.constant_value)
        return false;
    else if (constant_value && !rhs_typed.constant_value)
        return false;
    else if (!constant_value && rhs_typed.constant_value)
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

    if (result_type)
    {
        auto result_type_name = result_type->getName();
        hash_state.update(result_type_name.size());
        hash_state.update(result_type_name);
    }

    if (constant_value)
    {
        auto constant_dump = applyVisitor(FieldVisitorToString(), constant_value->getValue());
        hash_state.update(constant_dump.size());
        hash_state.update(constant_dump);

        auto constant_value_type_name = constant_value->getType()->getName();
        hash_state.update(constant_value_type_name.size());
        hash_state.update(constant_value_type_name);
    }
}

QueryTreeNodePtr FunctionNode::cloneImpl() const
{
    auto result_function = std::make_shared<FunctionNode>(function_name);

    /** This is valid for clone method to reuse same function pointers
      * because ordinary functions or aggregate functions must be stateless.
      */
    result_function->function = function;
    result_function->aggregate_function = aggregate_function;
    result_function->result_type = result_type;
    result_function->constant_value = constant_value;

    return result_function;
}

ASTPtr FunctionNode::toASTImpl() const
{
    auto function_ast = std::make_shared<ASTFunction>();

    function_ast->name = function_name;
    function_ast->is_window_function = isWindowFunction();

    auto window_node = getWindowNode();
    if (window_node)
    {
        if (auto * identifier_node = window_node->as<IdentifierNode>())
            function_ast->window_name = identifier_node->getIdentifier().getFullName();
        else
            function_ast->window_definition = window_node->toAST();
    }

    const auto & parameters = getParameters();
    if (!parameters.getNodes().empty())
    {
        function_ast->children.push_back(parameters.toAST());
        function_ast->parameters = function_ast->children.back();
    }

    const auto & arguments = getArguments();
    function_ast->children.push_back(arguments.toAST());
    function_ast->arguments = function_ast->children.back();

    return function_ast;
}

}
