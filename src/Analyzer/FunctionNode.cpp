#include <Analyzer/FunctionNode.h>

#include <Common/FieldVisitorToString.h>
#include <Common/SipHash.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Functions/IFunction.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Parsers/ASTFunction.h>

namespace DB
{

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

void FunctionNode::dumpTree(WriteBuffer & buffer, size_t indent) const
{
    buffer << std::string(indent, ' ') << "FUNCTION ";
    writePointerHex(this, buffer);
    buffer << ' ' << function_name << (result_type ? (" : " + result_type->getName()) : "");

    const auto & parameters = getParameters();
    if (!parameters.getNodes().empty())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "PARAMETERS\n";
        parameters.dumpTree(buffer, indent + 4);
    }

    const auto & arguments = getArguments();
    if (!arguments.getNodes().empty())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "ARGUMENTS\n";
        arguments.dumpTree(buffer, indent + 4);
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
        isNonAggregateFunction() != rhs_typed.isNonAggregateFunction())
        return false;

    if (!result_type && !rhs_typed.result_type)
        return true;
    else if (result_type && !rhs_typed.result_type)
        return false;
    else if (!result_type && rhs_typed.result_type)
        return false;

    return result_type->equals(*rhs_typed.result_type);
}

void FunctionNode::updateTreeHashImpl(HashState & hash_state) const
{
    hash_state.update(function_name.size());
    hash_state.update(function_name);
    hash_state.update(isAggregateFunction());

    if (result_type)
    {
        auto result_type_name = result_type->getName();
        hash_state.update(result_type_name.size());
        hash_state.update(result_type_name);
    }
}

ASTPtr FunctionNode::toASTImpl() const
{
    auto function_ast = std::make_shared<ASTFunction>();

    function_ast->name = function_name;

    const auto & parameters = getParameters();
    if (!parameters.getNodes().empty())
    {
        function_ast->children.push_back(parameters.toAST());
        function_ast->parameters = function_ast->children.back();
    }

    const auto & arguments = getArguments();
    if (!arguments.getNodes().empty())
    {
        function_ast->children.push_back(arguments.toAST());
        function_ast->arguments = function_ast->children.back();
    }

    return function_ast;
}

QueryTreeNodePtr FunctionNode::cloneImpl() const
{
    auto result_function = std::make_shared<FunctionNode>(function_name);
    /// This is valid for clone method function or aggregate function must be stateless
    result_function->function = function;
    result_function->aggregate_function = aggregate_function;
    result_function->result_type = result_type;

    return result_function;
}

}
