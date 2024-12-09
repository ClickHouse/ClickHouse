#include <Analyzer/ColumnTransformers.h>

#include <Common/SipHash.h>
#include <Common/re2.h>

#include <IO/WriteBuffer.h>
#include <IO/Operators.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTColumnsTransformers.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/LambdaNode.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// IColumnTransformerNode implementation

const char * toString(ColumnTransfomerType type)
{
    switch (type)
    {
        case ColumnTransfomerType::APPLY: return "APPLY";
        case ColumnTransfomerType::EXCEPT: return "EXCEPT";
        case ColumnTransfomerType::REPLACE: return "REPLACE";
    }
}

IColumnTransformerNode::IColumnTransformerNode(size_t children_size)
    : IQueryTreeNode(children_size)
{}

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
    : IColumnTransformerNode(children_size)
{
    if (expression_node_->getNodeType() == QueryTreeNodeType::LAMBDA)
        apply_transformer_type = ApplyColumnTransformerType::LAMBDA;
    else if (expression_node_->getNodeType() == QueryTreeNodeType::FUNCTION)
        apply_transformer_type = ApplyColumnTransformerType::FUNCTION;
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Apply column transformer expression must be lambda or function. Actual {}",
            expression_node_->getNodeTypeName());

    children[expression_child_index] = std::move(expression_node_);
}

void ApplyColumnTransformerNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "APPLY COLUMN TRANSFORMER id: " << format_state.getNodeId(this);
    buffer << ", apply_transformer_type: " << toString(apply_transformer_type);

    buffer << '\n' << std::string(indent + 2, ' ') << "EXPRESSION" << '\n';

    const auto & expression_node = getExpressionNode();
    expression_node->dumpTreeImpl(buffer, format_state, indent + 4);
}

bool ApplyColumnTransformerNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    const auto & rhs_typed = assert_cast<const ApplyColumnTransformerNode &>(rhs);
    return apply_transformer_type == rhs_typed.apply_transformer_type;
}

void ApplyColumnTransformerNode::updateTreeHashImpl(IQueryTreeNode::HashState & hash_state, CompareOptions) const
{
    hash_state.update(static_cast<size_t>(getTransformerType()));
    hash_state.update(static_cast<size_t>(getApplyTransformerType()));
}

QueryTreeNodePtr ApplyColumnTransformerNode::cloneImpl() const
{
    return std::make_shared<ApplyColumnTransformerNode>(getExpressionNode());
}

ASTPtr ApplyColumnTransformerNode::toASTImpl(const ConvertToASTOptions & options) const
{
    auto ast_apply_transformer = std::make_shared<ASTColumnsApplyTransformer>();
    const auto & expression_node = getExpressionNode();

    if (apply_transformer_type == ApplyColumnTransformerType::FUNCTION)
    {
        auto & function_expression = expression_node->as<FunctionNode &>();
        ast_apply_transformer->func_name = function_expression.getFunctionName();
        ast_apply_transformer->parameters = function_expression.getParametersNode()->toAST(options);
    }
    else
    {
        auto & lambda_expression = expression_node->as<LambdaNode &>();
        if (!lambda_expression.getArgumentNames().empty())
            ast_apply_transformer->lambda_arg = lambda_expression.getArgumentNames()[0];
        ast_apply_transformer->lambda = lambda_expression.toAST(options);
    }

    return ast_apply_transformer;
}

/// ExceptColumnTransformerNode implementation

ExceptColumnTransformerNode::ExceptColumnTransformerNode(Names except_column_names_, bool is_strict_)
    : IColumnTransformerNode(children_size)
    , except_transformer_type(ExceptColumnTransformerType::COLUMN_LIST)
    , except_column_names(std::move(except_column_names_))
    , is_strict(is_strict_)
{
}

ExceptColumnTransformerNode::ExceptColumnTransformerNode(std::shared_ptr<re2::RE2> column_matcher_)
    : IColumnTransformerNode(children_size)
    , except_transformer_type(ExceptColumnTransformerType::REGEXP)
    , column_matcher(std::move(column_matcher_))
{
}

bool ExceptColumnTransformerNode::isColumnMatching(const std::string & column_name) const
{
    if (column_matcher)
        return re2::RE2::PartialMatch(column_name, *column_matcher);

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

void ExceptColumnTransformerNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "EXCEPT COLUMN TRANSFORMER id: " << format_state.getNodeId(this);
    buffer << ", except_transformer_type: " << toString(except_transformer_type);

    if (column_matcher)
    {
        buffer << ", pattern: " << column_matcher->pattern();
        return;
    }

    buffer << ", identifiers: ";

    size_t except_column_names_size = except_column_names.size();
    for (size_t i = 0; i < except_column_names_size; ++i)
    {
        buffer << except_column_names[i];

        if (i + 1 != except_column_names_size)
            buffer << ", ";
    }
}

bool ExceptColumnTransformerNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    const auto & rhs_typed = assert_cast<const ExceptColumnTransformerNode &>(rhs);
    if (except_transformer_type != rhs_typed.except_transformer_type ||
        is_strict != rhs_typed.is_strict ||
        except_column_names != rhs_typed.except_column_names)
        return false;

    const auto & rhs_column_matcher = rhs_typed.column_matcher;

    if (!column_matcher && !rhs_column_matcher)
        return true;
    if (column_matcher && !rhs_column_matcher)
        return false;
    if (!column_matcher && rhs_column_matcher)
        return false;

    return column_matcher->pattern() == rhs_column_matcher->pattern();
}

void ExceptColumnTransformerNode::updateTreeHashImpl(IQueryTreeNode::HashState & hash_state, CompareOptions) const
{
    hash_state.update(static_cast<size_t>(getTransformerType()));
    hash_state.update(static_cast<size_t>(getExceptTransformerType()));

    hash_state.update(except_column_names.size());

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

QueryTreeNodePtr ExceptColumnTransformerNode::cloneImpl() const
{
    if (except_transformer_type == ExceptColumnTransformerType::REGEXP)
        return std::make_shared<ExceptColumnTransformerNode>(column_matcher);

    return std::make_shared<ExceptColumnTransformerNode>(except_column_names, is_strict);
}

ASTPtr ExceptColumnTransformerNode::toASTImpl(const ConvertToASTOptions & /* options */) const
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

/// ReplaceColumnTransformerNode implementation

ReplaceColumnTransformerNode::ReplaceColumnTransformerNode(const std::vector<Replacement> & replacements_, bool is_strict_)
    : IColumnTransformerNode(children_size)
    , is_strict(is_strict_)
{
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

void ReplaceColumnTransformerNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "REPLACE COLUMN TRANSFORMER id: " << format_state.getNodeId(this);

    const auto & replacements_nodes = getReplacements().getNodes();
    size_t replacements_size = replacements_nodes.size();
    buffer << '\n' << std::string(indent + 2, ' ') << "REPLACEMENTS " << replacements_size << '\n';

    for (size_t i = 0; i < replacements_size; ++i)
    {
        const auto & replacement_name = replacements_names[i];
        buffer << std::string(indent + 4, ' ') << "REPLACEMENT NAME " << replacement_name;
        buffer << " EXPRESSION" << '\n';
        const auto & expression_node = replacements_nodes[i];
        expression_node->dumpTreeImpl(buffer, format_state, indent + 6);

        if (i + 1 != replacements_size)
            buffer << '\n';
    }
}

bool ReplaceColumnTransformerNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    const auto & rhs_typed = assert_cast<const ReplaceColumnTransformerNode &>(rhs);
    return is_strict == rhs_typed.is_strict && replacements_names == rhs_typed.replacements_names;
}

void ReplaceColumnTransformerNode::updateTreeHashImpl(IQueryTreeNode::HashState & hash_state, CompareOptions) const
{
    hash_state.update(static_cast<size_t>(getTransformerType()));

    const auto & replacement_expressions_nodes = getReplacements().getNodes();
    size_t replacements_size = replacement_expressions_nodes.size();
    hash_state.update(replacements_size);

    for (size_t i = 0; i < replacements_size; ++i)
    {
        const auto & replacement_name = replacements_names[i];
        hash_state.update(replacement_name.size());
        hash_state.update(replacement_name);
    }
}

QueryTreeNodePtr ReplaceColumnTransformerNode::cloneImpl() const
{
    auto result_replace_transformer = std::make_shared<ReplaceColumnTransformerNode>(std::vector<Replacement>{}, false);

    result_replace_transformer->is_strict = is_strict;
    result_replace_transformer->replacements_names = replacements_names;

    return result_replace_transformer;
}

ASTPtr ReplaceColumnTransformerNode::toASTImpl(const ConvertToASTOptions & options) const
{
    auto ast_replace_transformer = std::make_shared<ASTColumnsReplaceTransformer>();

    const auto & replacement_expressions_nodes = getReplacements().getNodes();
    size_t replacements_size = replacement_expressions_nodes.size();

    ast_replace_transformer->children.reserve(replacements_size);

    for (size_t i = 0; i < replacements_size; ++i)
    {
        auto replacement_ast = std::make_shared<ASTColumnsReplaceTransformer::Replacement>();
        replacement_ast->name = replacements_names[i];
        replacement_ast->children.push_back(replacement_expressions_nodes[i]->toAST(options));
        ast_replace_transformer->children.push_back(std::move(replacement_ast));
    }

    return ast_replace_transformer;
}

}
