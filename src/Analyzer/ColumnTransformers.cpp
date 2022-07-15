#include <Analyzer/ColumnTransformers.h>

#include <Common/SipHash.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsTransformers.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/LambdaNode.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

const char * toString(ColumnTransfomerType type)
{
    switch (type)
    {
        case ColumnTransfomerType::APPLY: return "APPLY";
        case ColumnTransfomerType::EXCEPT: return "EXCEPT";
        case ColumnTransfomerType::REPLACE: return "REPLACE";
    }
}

/// ApplyColumnTransformerNode implementation

const char * toString(ApplyColumnTransformerType type)
{
    switch (type)
    {
        case ApplyColumnTransformerType::LAMBDA: return "LAMBDA";
        case ApplyColumnTransformerType::FUNCTION: return "FUNCTION";
    }
}

ApplyColumnTransformerNode::ApplyColumnTransformerNode(QueryTreeNodePtr expression_node_)
{
    if (expression_node_->getNodeType() == QueryTreeNodeType::LAMBDA)
        apply_transformer_type = ApplyColumnTransformerType::LAMBDA;
    else if (expression_node_->getNodeType() == QueryTreeNodeType::FUNCTION)
        apply_transformer_type = ApplyColumnTransformerType::FUNCTION;
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Apply column transformer expression must be lambda or function. Actual {}",
            expression_node_->getNodeTypeName());

    children.resize(1);
    children[expression_child_index] = std::move(expression_node_);
}

void ApplyColumnTransformerNode::dumpTree(WriteBuffer & buffer, size_t indent) const
{
    buffer << std::string(indent, ' ') << "APPLY COLUMN TRANSFORMER ";
    writePointerHex(this, buffer);
    buffer << ' ' << toString(apply_transformer_type) << '\n';

    buffer << std::string(indent + 2, ' ') << "EXPRESSION" << '\n';

    const auto & expression_node = getExpressionNode();
    expression_node->dumpTree(buffer, indent + 4);
}

void ApplyColumnTransformerNode::updateTreeHashImpl(IQueryTreeNode::HashState & hash_state) const
{
    hash_state.update(static_cast<size_t>(getTransformerType()));
    hash_state.update(static_cast<size_t>(getApplyTransformerType()));

    getExpressionNode()->updateTreeHash(hash_state);
}

ASTPtr ApplyColumnTransformerNode::toASTImpl() const
{
    auto ast_apply_transformer = std::make_shared<ASTColumnsApplyTransformer>();
    const auto & expression_node = getExpressionNode();

    if (apply_transformer_type == ApplyColumnTransformerType::FUNCTION)
    {
        auto & function_expression = expression_node->as<FunctionNode &>();
        ast_apply_transformer->func_name = function_expression.getFunctionName();
        ast_apply_transformer->parameters = function_expression.getParametersNode()->toAST();
    }
    else
    {
        auto & lambda_expression = expression_node->as<LambdaNode &>();
        if (!lambda_expression.getArgumentNames().empty())
            ast_apply_transformer->lambda_arg = lambda_expression.getArgumentNames()[0];
        ast_apply_transformer->lambda = lambda_expression.toAST();
    }

    return ast_apply_transformer;
}

QueryTreeNodePtr ApplyColumnTransformerNode::cloneImpl() const
{
    ApplyColumnTransformerNodePtr result_apply_transformer(new ApplyColumnTransformerNode());
    return result_apply_transformer;
}

/// ExceptColumnTransformerNode implementation

bool ExceptColumnTransformerNode::isColumnMatching(const std::string & column_name) const
{
    if (column_matcher)
        return RE2::PartialMatch(column_name, *column_matcher);

    for (const auto & name : except_column_names)
        if (column_name == name)
            return true;

    return false;
}

const char * toString(ExceptColumnTransformerType type)
{
    switch (type)
    {
        case ExceptColumnTransformerType::REGEXP:
            return "REGEXP";
        case ExceptColumnTransformerType::COLUMN_LIST:
            return "COLUMN_LIST";
    }
}

void ExceptColumnTransformerNode::dumpTree(WriteBuffer & buffer, size_t indent) const
{
    buffer << std::string(indent, ' ') << "EXCEPT COLUMN TRANSFORMER ";
    writePointerHex(this, buffer);
    buffer << ' ' << toString(except_transformer_type) << ' ';

    if (column_matcher)
    {
        buffer << column_matcher->pattern();
        return;
    }

    size_t except_column_names_size = except_column_names.size();
    for (size_t i = 0; i < except_column_names_size; ++i)
    {
        buffer << except_column_names[i];

        if (i + 1 != except_column_names_size)
            buffer << ", ";
    }
}

void ExceptColumnTransformerNode::updateTreeHashImpl(IQueryTreeNode::HashState & hash_state) const
{
    hash_state.update(static_cast<size_t>(getTransformerType()));
    hash_state.update(static_cast<size_t>(getExceptTransformerType()));

    for (const auto & column_name : except_column_names)
    {
        hash_state.update(column_name.size());
        hash_state.update(column_name);
    }

    if (column_matcher)
    {
        const auto & pattern = column_matcher->pattern();
        hash_state.update(pattern.size());
        hash_state.update(pattern);
    }
}

ASTPtr ExceptColumnTransformerNode::toASTImpl() const
{
    auto ast_except_transformer = std::make_shared<ASTColumnsExceptTransformer>();

    if (column_matcher)
    {
        ast_except_transformer->setPattern(column_matcher->pattern());
        return ast_except_transformer;
    }

    ast_except_transformer->children.reserve(except_column_names.size());
    for (const auto & name : except_column_names)
        ast_except_transformer->children.push_back(std::make_shared<ASTIdentifier>(name));

    return ast_except_transformer;
}

QueryTreeNodePtr ExceptColumnTransformerNode::cloneImpl() const
{
    if (except_transformer_type == ExceptColumnTransformerType::REGEXP)
        return std::make_shared<ExceptColumnTransformerNode>(column_matcher);

    return std::make_shared<ExceptColumnTransformerNode>(except_column_names, is_strict);
}

/// ReplaceColumnTransformerNode implementation

ReplaceColumnTransformerNode::ReplaceColumnTransformerNode(const std::vector<Replacement> & replacements_, bool is_strict_)
    : is_strict(is_strict_)
{
    children.resize(1);
    children[replacements_child_index] = std::make_shared<ListNode>();

    auto & replacement_expressions_nodes = getReplacements().getNodes();

    std::unordered_set<std::string> replacement_names_set;

    for (const auto & replacement : replacements_)
    {
        auto [_, inserted] = replacement_names_set.emplace(replacement.column_name);

        if (!inserted)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Expressions in column transformer replace should not contain same replacement {} more than once",
                replacement.column_name);

        replacements_names.push_back(replacement.column_name);
        replacement_expressions_nodes.push_back(replacement.expression_node);
    }
}

QueryTreeNodePtr ReplaceColumnTransformerNode::findReplacementExpression(const std::string & expression_name)
{
    auto it = std::find(replacements_names.begin(), replacements_names.end(), expression_name);
    if (it == replacements_names.end())
        return {};

    size_t replacement_index = it - replacements_names.begin();
    auto & replacement_expressions_nodes = getReplacements().getNodes();
    return replacement_expressions_nodes[replacement_index];
}

void ReplaceColumnTransformerNode::dumpTree(WriteBuffer & buffer, size_t indent) const
{
    buffer << std::string(indent, ' ') << "REPLACE TRANSFORMER ";
    writePointerHex(this, buffer);
    buffer << '\n';

    auto & replacements_nodes = getReplacements().getNodes();
    size_t replacements_size = replacements_nodes.size();
    buffer << std::string(indent + 2, ' ') << "REPLACEMENTS " << replacements_size << '\n';

    for (size_t i = 0; i < replacements_size; ++i)
    {
        const auto & replacement_name = replacements_names[i];
        buffer << std::string(indent + 4, ' ') << "REPLACEMENT NAME " << replacement_name;
        buffer << " EXPRESSION" << '\n';
        const auto & expression_node = replacements_nodes[i];
        expression_node->dumpTree(buffer, indent + 6);

        if (i + 1 != replacements_size)
            buffer << '\n';
    }
}

void ReplaceColumnTransformerNode::updateTreeHashImpl(IQueryTreeNode::HashState & hash_state) const
{
    hash_state.update(static_cast<size_t>(getTransformerType()));

    auto & replacement_expressions_nodes = getReplacements().getNodes();
    size_t replacements_size = replacement_expressions_nodes.size();
    hash_state.update(replacements_size);

    for (size_t i = 0; i < replacements_size; ++i)
    {
        const auto & replacement_name = replacements_names[i];
        hash_state.update(replacement_name.size());
        hash_state.update(replacement_name);
        replacement_expressions_nodes[i]->updateTreeHash(hash_state);
    }
}

ASTPtr ReplaceColumnTransformerNode::toASTImpl() const
{
    auto ast_replace_transformer = std::make_shared<ASTColumnsReplaceTransformer>();

    auto & replacement_expressions_nodes = getReplacements().getNodes();
    size_t replacements_size = replacement_expressions_nodes.size();

    ast_replace_transformer->children.reserve(replacements_size);

    for (size_t i = 0; i < replacements_size; ++i)
    {
        auto replacement_ast = std::make_shared<ASTColumnsReplaceTransformer::Replacement>();
        replacement_ast->name = replacements_names[i];
        replacement_ast->expr = replacement_expressions_nodes[i]->toAST();
        ast_replace_transformer->children.push_back(replacement_ast);
    }

    return ast_replace_transformer;
}

QueryTreeNodePtr ReplaceColumnTransformerNode::cloneImpl() const
{
    ReplaceColumnTransformerNodePtr result_replace_transformers(new ReplaceColumnTransformerNode());

    result_replace_transformers->is_strict = is_strict;
    result_replace_transformers->replacements_names = replacements_names;

    return result_replace_transformers;
}

}
