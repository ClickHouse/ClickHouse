#include <Analyzer/LambdaNode.h>

#include <Common/SipHash.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

LambdaNode::LambdaNode(Names argument_names_, QueryTreeNodePtr expression_)
    : argument_names(std::move(argument_names_))
{
    children.resize(2);

    auto arguments_list_node = std::make_shared<ListNode>();
    auto & nodes = arguments_list_node->getNodes();

    size_t argument_names_size = argument_names.size();
    nodes.reserve(argument_names_size);

    for (size_t i = 0; i < argument_names_size; ++i)
        nodes.push_back(std::make_shared<IdentifierNode>(Identifier{argument_names[i]}));

    children[arguments_child_index] = std::move(arguments_list_node);
    children[expression_child_index] = std::move(expression_);
}

void LambdaNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "LAMBDA id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    const auto & arguments = getArguments();
    if (!arguments.getNodes().empty())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "ARGUMENTS " << '\n';
        getArguments().dumpTreeImpl(buffer, format_state, indent + 4);
    }

    buffer << '\n' << std::string(indent + 2, ' ') << "EXPRESSION " << '\n';
    getExpression()->dumpTreeImpl(buffer, format_state, indent + 4);
}

String LambdaNode::getName() const
{
    return "lambda(" + children[arguments_child_index]->getName() + ") -> " + children[expression_child_index]->getName();
}

bool LambdaNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const LambdaNode &>(rhs);
    return argument_names == rhs_typed.argument_names;
}

void LambdaNode::updateTreeHashImpl(HashState & state) const
{
    state.update(argument_names.size());
    for (const auto & argument_name : argument_names)
    {
        state.update(argument_name.size());
        state.update(argument_name);
    }
}

ASTPtr LambdaNode::toASTImpl() const
{
    auto lambda_function_arguments_ast = std::make_shared<ASTExpressionList>();

    auto tuple_function = std::make_shared<ASTFunction>();
    tuple_function->name = "tuple";
    tuple_function->children.push_back(children[arguments_child_index]->toAST());
    tuple_function->arguments = tuple_function->children.back();

    lambda_function_arguments_ast->children.push_back(std::move(tuple_function));
    lambda_function_arguments_ast->children.push_back(children[expression_child_index]->toAST());

    auto lambda_function_ast = std::make_shared<ASTFunction>();
    lambda_function_ast->name = "lambda";
    lambda_function_ast->children.push_back(std::move(lambda_function_arguments_ast));
    lambda_function_ast->arguments = lambda_function_ast->children.back();

    return lambda_function_ast;
}

QueryTreeNodePtr LambdaNode::cloneImpl() const
{
    LambdaNodePtr result_lambda(new LambdaNode());

    result_lambda->argument_names = argument_names;

    return result_lambda;
}

}
