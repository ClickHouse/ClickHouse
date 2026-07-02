#include <Analyzer/ColumnTransformers.h>

#include <algorithm>
#include <limits>

#include <Poco/String.h>

#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/re2.h>

#include <IO/WriteBuffer.h>
#include <IO/Operators.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTColumnsTransformers.h>

#include <Analyzer/Identifier.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/LambdaNode.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int AMBIGUOUS_IDENTIFIER;
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
    auto ast_apply_transformer = make_intrusive<ASTColumnsApplyTransformer>();
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

namespace
{

/// Flatten parsed identifier parts into the dot-joined full name.
String joinParts(const std::vector<String> & parts)
{
    String result;
    for (const auto & part : parts)
    {
        if (!result.empty())
            result += '.';
        result += part;
    }
    return result;
}

Names joinAllParts(const std::vector<std::vector<String>> & targets)
{
    Names result;
    result.reserve(targets.size());
    for (const auto & parts : targets)
        result.push_back(joinParts(parts));
    return result;
}

/// Compare two name parts, folding case unless the target part was double-quoted.
bool partsEqualWithQuote(std::string_view target_part, std::string_view column_part, bool target_part_quoted)
{
    if (target_part_quoted)
        return target_part == column_part;
    return Poco::icompare(std::string(target_part), std::string(column_part)) == 0;
}

/// Quote-aware, per-part comparison of a structured transformer target against a column name.
/// The target side uses its PARSED parts (a single part may contain dots, so the flattened name
/// is never re-split). A single-part target compares against the whole column name; a multi-part
/// target splits the column name on '.' (legitimate on the column side, where dots separate
/// nested/subcolumn components).
bool targetMatchesColumnName(
    const std::vector<String> & target_parts,
    const std::vector<bool> & target_parts_double_quoted,
    const std::string & column_name)
{
    auto part_quoted = [&](size_t p) { return p < target_parts_double_quoted.size() && target_parts_double_quoted[p]; };

    if (target_parts.size() == 1)
        return partsEqualWithQuote(target_parts[0], column_name, part_quoted(0));

    Identifier column_identifier(column_name);
    const auto & column_parts = column_identifier.getParts();
    if (target_parts.size() != column_parts.size())
        return false;

    for (size_t p = 0; p < target_parts.size(); ++p)
        if (!partsEqualWithQuote(target_parts[p], column_parts[p], part_quoted(p)))
            return false;
    return true;
}

}

ExceptColumnTransformerNode::ExceptColumnTransformerNode(
    std::vector<std::vector<String>> target_parts_,
    bool is_strict_,
    std::vector<std::vector<bool>> target_parts_double_quoted_)
    : IColumnTransformerNode(children_size)
    , except_transformer_type(ExceptColumnTransformerType::COLUMN_LIST)
    , target_parts(std::move(target_parts_))
    , except_column_names(joinAllParts(target_parts))
    , target_parts_double_quoted(std::move(target_parts_double_quoted_))
    , is_strict(is_strict_)
{
}

ExceptColumnTransformerNode::ExceptColumnTransformerNode(std::shared_ptr<re2::RE2> column_matcher_)
    : IColumnTransformerNode(children_size)
    , except_transformer_type(ExceptColumnTransformerType::REGEXP)
    , column_matcher(std::move(column_matcher_))
{
}

bool ExceptColumnTransformerNode::isColumnMatching(const std::string & column_name, bool standard_mode, std::string * matched_target) const
{
    if (column_matcher)
        return re2::RE2::PartialMatch(column_name, *column_matcher);

    /// First pass: exact match wins regardless of `standard_mode` (mirrors column/alias lookup).
    for (const auto & name : except_column_names)
    {
        if (column_name == name)
        {
            if (matched_target)
                *matched_target = name;
            return true;
        }
    }

    if (!standard_mode)
        return false;

    /// `standard` mode: unquoted target parts fold case-insensitively; double-quoted parts stay
    /// exact. Multiple distinct targets that fold to the same column are ambiguous — mirror the
    /// column/alias/REPLACE rule rather than silently picking the first.
    size_t matched_index = std::numeric_limits<size_t>::max();
    for (size_t i = 0, n = target_parts.size(); i < n; ++i)
    {
        const auto & per_part_quote
            = i < target_parts_double_quoted.size() ? target_parts_double_quoted[i] : std::vector<bool>{};
        if (!targetMatchesColumnName(target_parts[i], per_part_quote, column_name))
            continue;

        if (matched_index != std::numeric_limits<size_t>::max()
            && except_column_names[matched_index] != except_column_names[i])
            throw Exception(ErrorCodes::AMBIGUOUS_IDENTIFIER,
                "EXCEPT column transformer target for column '{}' is ambiguous: matches multiple targets with different cases: '{}' and '{}'",
                column_name, except_column_names[matched_index], except_column_names[i]);
        matched_index = i;
    }
    if (matched_index != std::numeric_limits<size_t>::max())
    {
        if (matched_target)
            *matched_target = except_column_names[matched_index];
        return true;
    }
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
    /// Compare structured parts, not flattened names: single-part `` `a.b` `` and compound `a.b`
    /// flatten identically but match differently.
    if (except_transformer_type != rhs_typed.except_transformer_type ||
        is_strict != rhs_typed.is_strict ||
        target_parts != rhs_typed.target_parts ||
        target_parts_double_quoted != rhs_typed.target_parts_double_quoted)
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

    /// Mix in per-part quote bits only when at least one part was quoted so the hash stays stable
    /// for the common (unquoted) case.
    const bool any_quoted = std::any_of(
        target_parts_double_quoted.begin(),
        target_parts_double_quoted.end(),
        [](const auto & per_part) { return std::any_of(per_part.begin(), per_part.end(), [](bool b) { return b; }); });
    if (any_quoted)
    {
        hash_state.update(target_parts_double_quoted.size());
        for (const auto & per_part : target_parts_double_quoted)
        {
            hash_state.update(per_part.size());
            for (bool b : per_part)
                hash_state.update(static_cast<uint8_t>(b));
        }
    }

    /// Mix in the part structure only when some target is compound, so the common single-part
    /// case keeps the flattened-name hash (compound `a.b` and single-part `` `a.b` `` flatten
    /// identically but must not share a hash).
    const bool any_compound = std::any_of(
        target_parts.begin(), target_parts.end(), [](const auto & parts) { return parts.size() > 1; });
    if (any_compound)
    {
        for (const auto & parts : target_parts)
            hash_state.update(parts.size());
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

    return std::make_shared<ExceptColumnTransformerNode>(target_parts, is_strict, target_parts_double_quoted);
}

ASTPtr ExceptColumnTransformerNode::toASTImpl(const ConvertToASTOptions & /* options */) const
{
    auto ast_except_transformer = make_intrusive<ASTColumnsExceptTransformer>();

    if (column_matcher)
    {
        ast_except_transformer->setPattern(column_matcher->pattern());
        return ast_except_transformer;
    }

    ast_except_transformer->children.reserve(target_parts.size());
    for (size_t i = 0, n = target_parts.size(); i < n; ++i)
    {
        /// Rebuild from the stored parsed parts — never by re-splitting the flattened name, which
        /// would corrupt a single part containing dots (e.g. `` `a.b` `` or a trailing-dot name).
        auto name_parts = target_parts[i];
        auto identifier = make_intrusive<ASTIdentifier>(std::move(name_parts));
        if (i < target_parts_double_quoted.size())
        {
            const auto & per_part = target_parts_double_quoted[i];
            std::vector<IdentifierQuoteStyle> styles;
            styles.reserve(per_part.size());
            for (bool b : per_part)
                styles.push_back(b ? IdentifierQuoteStyle::DoubleQuote : IdentifierQuoteStyle::None);
            identifier->setQuoteStyles(std::move(styles));
        }
        ast_except_transformer->children.push_back(std::move(identifier));
    }

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
        String full_name = joinParts(replacement.parts);
        auto [_, inserted] = replacement_names_set.emplace(full_name);

        if (!inserted)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Expressions in column transformer replace should not contain same replacement {} more than once",
                full_name);

        target_parts.push_back(replacement.parts);
        replacements_names.push_back(std::move(full_name));
        target_parts_double_quoted.push_back(replacement.parts_double_quoted);
        replacement_expressions_nodes.push_back(replacement.expression_node);
    }
}

QueryTreeNodePtr ReplaceColumnTransformerNode::findReplacementExpression(const std::string & expression_name, bool standard_mode, std::string * matched_target)
{
    auto exact_it = std::find(replacements_names.begin(), replacements_names.end(), expression_name);
    if (exact_it != replacements_names.end())
    {
        size_t replacement_index = exact_it - replacements_names.begin();
        if (matched_target)
            *matched_target = replacements_names[replacement_index];
        return getReplacements().getNodes()[replacement_index];
    }

    /// `standard` mode: match per parsed part — unquoted parts fold, double-quoted parts stay
    /// exact. Multiple targets that fold to the same lookup are ambiguous — mirror the
    /// column/alias rule.
    if (standard_mode)
    {
        size_t matched_index = std::numeric_limits<size_t>::max();
        for (size_t i = 0, n = target_parts.size(); i < n; ++i)
        {
            const auto & per_part_quote
                = i < target_parts_double_quoted.size() ? target_parts_double_quoted[i] : std::vector<bool>{};
            if (!targetMatchesColumnName(target_parts[i], per_part_quote, expression_name))
                continue;

            if (matched_index != std::numeric_limits<size_t>::max() && replacements_names[matched_index] != replacements_names[i])
                throw Exception(ErrorCodes::AMBIGUOUS_IDENTIFIER,
                    "REPLACE column transformer target '{}' is ambiguous: matches multiple replacements with different cases: '{}' and '{}'",
                    expression_name, replacements_names[matched_index], replacements_names[i]);
            matched_index = i;
        }
        if (matched_index != std::numeric_limits<size_t>::max())
        {
            if (matched_target)
                *matched_target = replacements_names[matched_index];
            return getReplacements().getNodes()[matched_index];
        }
    }

    return {};
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
    /// Compare structured parts, not flattened names: single-part `` `a.b` `` and compound `a.b`
    /// flatten identically but match differently.
    return is_strict == rhs_typed.is_strict
        && target_parts == rhs_typed.target_parts
        && target_parts_double_quoted == rhs_typed.target_parts_double_quoted;
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

    const bool any_quoted = std::any_of(
        target_parts_double_quoted.begin(),
        target_parts_double_quoted.end(),
        [](const auto & per_part) { return std::any_of(per_part.begin(), per_part.end(), [](bool b) { return b; }); });
    if (any_quoted)
    {
        hash_state.update(target_parts_double_quoted.size());
        for (const auto & per_part : target_parts_double_quoted)
        {
            hash_state.update(per_part.size());
            for (bool b : per_part)
                hash_state.update(static_cast<uint8_t>(b));
        }
    }

    /// Mix in the part structure only when some target is compound, so the common single-part
    /// case keeps the flattened-name hash.
    const bool any_compound = std::any_of(
        target_parts.begin(), target_parts.end(), [](const auto & parts) { return parts.size() > 1; });
    if (any_compound)
    {
        for (const auto & parts : target_parts)
            hash_state.update(parts.size());
    }
}

QueryTreeNodePtr ReplaceColumnTransformerNode::cloneImpl() const
{
    auto result_replace_transformer = std::make_shared<ReplaceColumnTransformerNode>(std::vector<Replacement>{}, false);

    result_replace_transformer->is_strict = is_strict;
    result_replace_transformer->target_parts = target_parts;
    result_replace_transformer->replacements_names = replacements_names;
    result_replace_transformer->target_parts_double_quoted = target_parts_double_quoted;

    return result_replace_transformer;
}

ASTPtr ReplaceColumnTransformerNode::toASTImpl(const ConvertToASTOptions & options) const
{
    auto ast_replace_transformer = make_intrusive<ASTColumnsReplaceTransformer>();

    const auto & replacement_expressions_nodes = getReplacements().getNodes();
    size_t replacements_size = replacement_expressions_nodes.size();

    ast_replace_transformer->children.reserve(replacements_size);

    for (size_t i = 0; i < replacements_size; ++i)
    {
        auto replacement_ast = make_intrusive<ASTColumnsReplaceTransformer::Replacement>();
        replacement_ast->name = replacements_names[i];
        if (i < target_parts_double_quoted.size())
        {
            /// Collapse per-part bits to a single AST-level bool: the AST `Replacement` only carries
            /// one flag because the parser produces single-part targets. If any part is quoted, the
            /// formatter renders the target with double quotes (round-trip survives for the cases
            /// the parser can produce today).
            const auto & per_part = target_parts_double_quoted[i];
            replacement_ast->name_is_double_quoted
                = std::any_of(per_part.begin(), per_part.end(), [](bool b) { return b; });
        }
        replacement_ast->children.push_back(replacement_expressions_nodes[i]->toAST(options));
        ast_replace_transformer->children.push_back(std::move(replacement_ast));
    }

    return ast_replace_transformer;
}

}
