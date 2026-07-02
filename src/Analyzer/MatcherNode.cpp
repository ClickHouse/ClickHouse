#include <Analyzer/MatcherNode.h>

#include <Poco/String.h>

#include <Common/assert_cast.h>
#include <Common/SipHash.h>

#include <IO/WriteBuffer.h>
#include <IO/Operators.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTColumnsTransformers.h>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
}

const char * toString(MatcherNodeType matcher_node_type)
{
    switch (matcher_node_type)
    {
        case MatcherNodeType::ASTERISK:
            return "ASTERISK";
        case MatcherNodeType::COLUMNS_LIST:
            return "COLUMNS_LIST";
        case MatcherNodeType::COLUMNS_REGEXP:
            return "COLUMNS_REGEXP";
    }
}

MatcherNode::MatcherNode(ColumnTransformersNodes column_transformers_)
    : MatcherNode(MatcherNodeType::ASTERISK,
        {} /*qualified_identifier*/,
        {} /*columns_identifiers*/,
        {} /*columns_matcher*/,
        std::move(column_transformers_) /*column_transformers*/)
{
}

MatcherNode::MatcherNode(Identifier qualified_identifier_, ColumnTransformersNodes column_transformers_)
    : MatcherNode(MatcherNodeType::ASTERISK,
        std::move(qualified_identifier_),
        {} /*columns_identifiers*/,
        {} /*columns_matcher*/,
        std::move(column_transformers_))
{
}

MatcherNode::MatcherNode(String pattern_, ColumnTransformersNodes column_transformers_)
    : MatcherNode(MatcherNodeType::COLUMNS_REGEXP,
        {} /*qualified_identifier*/,
        {} /*columns_identifiers*/,
        std::move(pattern_),
        std::move(column_transformers_))
{
}

MatcherNode::MatcherNode(Identifier qualified_identifier_, String pattern_, ColumnTransformersNodes column_transformers_)
    : MatcherNode(MatcherNodeType::COLUMNS_REGEXP,
        std::move(qualified_identifier_),
        {} /*columns_identifiers*/,
        std::move(pattern_),
        std::move(column_transformers_))
{
}

MatcherNode::MatcherNode(Identifiers columns_identifiers_, ColumnTransformersNodes column_transformers_)
    : MatcherNode(MatcherNodeType::COLUMNS_LIST,
        {} /*qualified_identifier*/,
        std::move(columns_identifiers_),
        {} /*columns_matcher*/,
        std::move(column_transformers_))
{
}

MatcherNode::MatcherNode(Identifier qualified_identifier_, Identifiers columns_identifiers_, ColumnTransformersNodes column_transformers_)
    : MatcherNode(MatcherNodeType::COLUMNS_LIST,
        std::move(qualified_identifier_),
        std::move(columns_identifiers_),
        {} /*columns_matcher*/,
        std::move(column_transformers_))
{
}

MatcherNode::MatcherNode(MatcherNodeType matcher_type_,
    Identifier qualified_identifier_,
    Identifiers columns_identifiers_,
    std::optional<String> pattern_,
    ColumnTransformersNodes column_transformers_)
    : IQueryTreeNode(children_size)
    , matcher_type(matcher_type_)
    , qualified_identifier(qualified_identifier_)
    , columns_identifiers(columns_identifiers_)
{
    if (pattern_)
    {
        columns_matcher = std::make_shared<re2::RE2>(*pattern_, re2::RE2::Quiet);
        if (!columns_matcher->ok())
            throw DB::Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                "COLUMNS pattern {} cannot be compiled: {}", *pattern_, columns_matcher->error());
    }

    auto column_transformers_list_node = std::make_shared<ListNode>();

    auto & column_transformers_nodes = column_transformers_list_node->getNodes();
    column_transformers_nodes.reserve(column_transformers_.size());

    for (auto && column_transformer : column_transformers_)
        column_transformers_nodes.emplace_back(std::move(column_transformer));

    children[column_transformers_child_index] = std::move(column_transformers_list_node);

    columns_identifiers_set.reserve(columns_identifiers.size());
    for (auto & column_identifier : columns_identifiers)
        columns_identifiers_set.insert(column_identifier.getFullName());
}

bool MatcherNode::isMatchingColumn(const std::string & column_name, bool standard_mode)
{
    if (matcher_type == MatcherNodeType::ASTERISK)
        return true;

    if (columns_matcher)
        return RE2::PartialMatch(column_name, *columns_matcher);

    if (columns_identifiers_set.contains(column_name))
        return true;

    /// Standard mode: an unquoted COLUMNS argument should match a case-different column name.
    /// Argument quote styles live in `columns_identifiers_quote_styles`; if no part of an
    /// argument was double-quoted, that argument can match case-insensitively.
    if (!standard_mode)
        return false;

    /// Per-part comparison: unquoted parts match case-insensitively, double-quoted parts must match
    /// exactly. So `COLUMNS(data."Name")` matches `Data.Name` (`data` folds; `"Name"` stays exact).
    Identifier column_identifier(column_name);
    for (size_t i = 0; i < columns_identifiers.size(); ++i)
    {
        const auto & matcher_parts = columns_identifiers[i].getParts();
        const auto & column_parts = column_identifier.getParts();
        if (matcher_parts.size() != column_parts.size())
            continue;

        const auto & matcher_quotes = i < columns_identifiers_quote_styles.size()
            ? columns_identifiers_quote_styles[i] : std::vector<IdentifierQuoteStyle>{};

        bool all_parts_match = true;
        for (size_t p = 0; p < matcher_parts.size(); ++p)
        {
            const bool part_quoted = p < matcher_quotes.size() && matcher_quotes[p] == IdentifierQuoteStyle::DoubleQuote;
            const bool match = part_quoted ? matcher_parts[p] == column_parts[p]
                                           : Poco::icompare(matcher_parts[p], column_parts[p]) == 0;
            if (!match)
            {
                all_parts_match = false;
                break;
            }
        }
        if (all_parts_match)
            return true;
    }
    return false;
}

void MatcherNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "MATCHER id: " << format_state.getNodeId(this);

    buffer << ", matcher_type: " << toString(matcher_type);

    if (!qualified_identifier.empty())
        buffer << ", qualified_identifier: " << qualified_identifier.getFullName();

    if (columns_matcher)
    {
        buffer << ", columns_pattern: " << columns_matcher->pattern();
    }
    else if (matcher_type == MatcherNodeType::COLUMNS_LIST)
    {
        buffer << ", " << fmt::format("column_identifiers: {}", fmt::join(columns_identifiers, ", "));
    }

    const auto & column_transformers_list = getColumnTransformers();
    if (!column_transformers_list.getNodes().empty())
    {
        buffer << '\n';
        column_transformers_list.dumpTreeImpl(buffer, format_state, indent + 2);
    }
}

bool MatcherNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    const auto & rhs_typed = assert_cast<const MatcherNode &>(rhs);
    /// Quote styles change resolution rules in `standard` mode (e.g. `COLUMNS("FirstName")` vs
    /// `COLUMNS(FirstName)`, `"T".*` vs `T.*`), so they must participate in node equality.
    if (matcher_type != rhs_typed.matcher_type ||
        qualified_identifier != rhs_typed.qualified_identifier ||
        qualified_identifier_quote_styles != rhs_typed.qualified_identifier_quote_styles ||
        columns_identifiers != rhs_typed.columns_identifiers ||
        columns_identifiers_quote_styles != rhs_typed.columns_identifiers_quote_styles ||
        columns_identifiers_set != rhs_typed.columns_identifiers_set)
        return false;

    const auto & rhs_columns_matcher = rhs_typed.columns_matcher;

    if (!columns_matcher && !rhs_columns_matcher)
        return true;
    if (columns_matcher && !rhs_columns_matcher)
        return false;
    if (!columns_matcher && rhs_columns_matcher)
        return false;

    return columns_matcher->pattern() == rhs_columns_matcher->pattern();
}

void MatcherNode::updateTreeHashImpl(HashState & hash_state, CompareOptions) const
{
    hash_state.update(static_cast<size_t>(matcher_type));

    const auto & qualified_identifier_full_name = qualified_identifier.getFullName();
    hash_state.update(qualified_identifier_full_name.size());
    hash_state.update(qualified_identifier_full_name);

    /// Mix in qualifier quote styles only when at least one part was actually double-quoted.
    /// Backticks are not preserved by the formatter, so including them would diverge the hash
    /// between the original tree and a reparsed copy on the remote side (same shape of bug as
    /// `IdentifierNode::updateTreeHashImpl` — see commit b65a03641a7). Encode only the per-part
    /// double-quote bit; for plain `T.*` / `\`T\`.*` the previous hash is preserved.
    bool any_qualifier_double_quoted = false;
    for (auto style : qualified_identifier_quote_styles)
        if (style == IdentifierQuoteStyle::DoubleQuote)
        {
            any_qualifier_double_quoted = true;
            break;
        }
    if (any_qualifier_double_quoted)
    {
        hash_state.update(qualified_identifier_quote_styles.size());
        for (auto style : qualified_identifier_quote_styles)
            hash_state.update(static_cast<uint8_t>(style == IdentifierQuoteStyle::DoubleQuote));
    }

    for (const auto & identifier : columns_identifiers)
    {
        const auto & identifier_full_name = identifier.getFullName();
        hash_state.update(identifier_full_name.size());
        hash_state.update(identifier_full_name);
    }

    /// Same rule for per-argument quote styles: only DoubleQuote is observable through format.
    bool any_arg_double_quoted = false;
    for (const auto & arg_quotes : columns_identifiers_quote_styles)
    {
        for (auto style : arg_quotes)
            if (style == IdentifierQuoteStyle::DoubleQuote)
            {
                any_arg_double_quoted = true;
                break;
            }
        if (any_arg_double_quoted)
            break;
    }
    if (any_arg_double_quoted)
    {
        hash_state.update(columns_identifiers_quote_styles.size());
        for (const auto & arg_quotes : columns_identifiers_quote_styles)
        {
            hash_state.update(arg_quotes.size());
            for (auto style : arg_quotes)
                hash_state.update(static_cast<uint8_t>(style == IdentifierQuoteStyle::DoubleQuote));
        }
    }

    if (columns_matcher)
    {
        const auto & columns_matcher_pattern = columns_matcher->pattern();
        hash_state.update(columns_matcher_pattern.size());
        hash_state.update(columns_matcher_pattern);
    }
}

QueryTreeNodePtr MatcherNode::cloneImpl() const
{
    MatcherNodePtr matcher_node = std::make_shared<MatcherNode>();

    matcher_node->matcher_type = matcher_type;
    matcher_node->qualified_identifier = qualified_identifier;
    matcher_node->qualified_identifier_quote_styles = qualified_identifier_quote_styles;
    matcher_node->columns_identifiers = columns_identifiers;
    matcher_node->columns_identifiers_quote_styles = columns_identifiers_quote_styles;
    matcher_node->columns_matcher = columns_matcher;
    matcher_node->columns_identifiers_set = columns_identifiers_set;

    return matcher_node;
}

ASTPtr MatcherNode::toASTImpl(const ConvertToASTOptions & options) const
{
    ASTPtr result;
    ASTPtr transformers;

    const auto & column_transformers = getColumnTransformers().getNodes();

    if (!column_transformers.empty())
    {
        transformers = make_intrusive<ASTColumnsTransformerList>();

        for (const auto & column_transformer : column_transformers)
            transformers->children.push_back(column_transformer->toAST(options));
    }

    if (matcher_type == MatcherNodeType::ASTERISK)
    {
        if (qualified_identifier.empty())
        {
            auto asterisk = make_intrusive<ASTAsterisk>();

            if (transformers)
            {
                asterisk->transformers = std::move(transformers);
                asterisk->children.push_back(asterisk->transformers);
            }

            result = asterisk;
        }
        else
        {
            auto qualified_asterisk = make_intrusive<ASTQualifiedAsterisk>();

            auto identifier_parts = qualified_identifier.getParts();
            auto qualifier_identifier = make_intrusive<ASTIdentifier>(std::move(identifier_parts));
            if (!qualified_identifier_quote_styles.empty())
                qualifier_identifier->setQuoteStyles(qualified_identifier_quote_styles);
            qualified_asterisk->qualifier = qualifier_identifier;
            qualified_asterisk->children.push_back(qualified_asterisk->qualifier);

            if (transformers)
            {
                qualified_asterisk->transformers = std::move(transformers);
                qualified_asterisk->children.push_back(qualified_asterisk->transformers);
            }

            result = qualified_asterisk;
        }
    }
    else if (columns_matcher)
    {
        if (qualified_identifier.empty())
        {
            auto regexp_matcher = make_intrusive<ASTColumnsRegexpMatcher>();
            regexp_matcher->setPattern(columns_matcher->pattern());

            if (transformers)
            {
                regexp_matcher->transformers = std::move(transformers);
                regexp_matcher->children.push_back(regexp_matcher->transformers);
            }

            result = regexp_matcher;
        }
        else
        {
            auto regexp_matcher = make_intrusive<ASTQualifiedColumnsRegexpMatcher>();
            regexp_matcher->setPattern(columns_matcher->pattern());

            auto identifier_parts = qualified_identifier.getParts();
            auto qualifier_identifier = make_intrusive<ASTIdentifier>(std::move(identifier_parts));
            if (!qualified_identifier_quote_styles.empty())
                qualifier_identifier->setQuoteStyles(qualified_identifier_quote_styles);
            regexp_matcher->qualifier = qualifier_identifier;
            regexp_matcher->children.push_back(regexp_matcher->qualifier);

            if (transformers)
            {
                regexp_matcher->transformers = std::move(transformers);
                regexp_matcher->children.push_back(regexp_matcher->transformers);
            }

            result = regexp_matcher;
        }
    }
    else
    {
        auto column_list = make_intrusive<ASTExpressionList>();
        column_list->children.reserve(columns_identifiers.size());

        for (size_t i = 0; i < columns_identifiers.size(); ++i)
        {
            auto identifier_parts = columns_identifiers[i].getParts();
            auto column_identifier = make_intrusive<ASTIdentifier>(std::move(identifier_parts));
            if (i < columns_identifiers_quote_styles.size() && !columns_identifiers_quote_styles[i].empty())
                column_identifier->setQuoteStyles(columns_identifiers_quote_styles[i]);
            column_list->children.push_back(std::move(column_identifier));
        }

        if (qualified_identifier.empty())
        {
            auto columns_list_matcher = make_intrusive<ASTColumnsListMatcher>();
            columns_list_matcher->column_list = std::move(column_list);
            columns_list_matcher->children.push_back(columns_list_matcher->column_list);

            if (transformers)
            {
                columns_list_matcher->transformers = std::move(transformers);
                columns_list_matcher->children.push_back(columns_list_matcher->transformers);
            }

            result = columns_list_matcher;
        }
        else
        {
            auto columns_list_matcher = make_intrusive<ASTQualifiedColumnsListMatcher>();

            auto identifier_parts = qualified_identifier.getParts();
            auto qualifier_identifier = make_intrusive<ASTIdentifier>(std::move(identifier_parts));
            if (!qualified_identifier_quote_styles.empty())
                qualifier_identifier->setQuoteStyles(qualified_identifier_quote_styles);
            columns_list_matcher->qualifier = qualifier_identifier;
            columns_list_matcher->column_list = std::move(column_list);
            columns_list_matcher->children.push_back(columns_list_matcher->qualifier);
            columns_list_matcher->children.push_back(columns_list_matcher->column_list);

            if (transformers)
            {
                columns_list_matcher->transformers = std::move(transformers);
                columns_list_matcher->children.push_back(columns_list_matcher->transformers);
            }

            result = columns_list_matcher;
        }
    }

    return result;
}

}
