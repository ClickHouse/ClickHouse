#include <Analyzer/LambdaNode.h>

#include <Common/assert_cast.h>
#include <Common/SipHash.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

LambdaNode::LambdaNode(Names argument_names_, QueryTreeNodePtr expression_, DataTypePtr result_type_)
    : IQueryTreeNode(children_size)
    , argument_names(std::move(argument_names_))
    , result_type(std::move(result_type_))
{
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

bool LambdaNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    const auto & rhs_typed = assert_cast<const LambdaNode &>(rhs);
    return argument_names == rhs_typed.argument_names;
}

void LambdaNode::updateTreeHashImpl(HashState & state, CompareOptions) const
{
    state.update(argument_names.size());
    for (const auto & argument_name : argument_names)
    {
        state.update(argument_name.size());
        state.update(argument_name);
    }
}

QueryTreeNodePtr LambdaNode::cloneImpl() const
{
    return std::make_shared<LambdaNode>(argument_names, getExpression(), result_type);
}

ASTPtr LambdaNode::toASTImpl(const ConvertToASTOptions & options) const
{
    auto lambda_function_arguments_ast = std::make_shared<ASTExpressionList>();

    auto tuple_function = std::make_shared<ASTFunction>();
    tuple_function->name = "tuple";
    tuple_function->children.push_back(children[arguments_child_index]->toAST(options));
    tuple_function->arguments = tuple_function->children.back();

    lambda_function_arguments_ast->children.push_back(std::move(tuple_function));
    lambda_function_arguments_ast->children.push_back(children[expression_child_index]->toAST(options));

    auto lambda_function_ast = std::make_shared<ASTFunction>();
    lambda_function_ast->name = "lambda";
    lambda_function_ast->children.push_back(std::move(lambda_function_arguments_ast));
    lambda_function_ast->arguments = lambda_function_ast->children.back();

    lambda_function_ast->is_lambda_function = true;

    return lambda_function_ast;
}

}
