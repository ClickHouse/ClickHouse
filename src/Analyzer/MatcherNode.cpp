#include <Analyzer/MatcherNode.h>

#include <Common/SipHash.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>

namespace DB
{

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

MatcherNode::MatcherNode(std::shared_ptr<re2::RE2> columns_matcher_, ColumnTransformersNodes column_transformers_)
    : MatcherNode(MatcherNodeType::COLUMNS_REGEXP,
        {} /*qualified_identifier*/,
        {} /*columns_identifiers*/,
        std::move(columns_matcher_),
        std::move(column_transformers_))
{
}

MatcherNode::MatcherNode(Identifier qualified_identifier_, std::shared_ptr<re2::RE2> columns_matcher_, ColumnTransformersNodes column_transformers_)
    : MatcherNode(MatcherNodeType::COLUMNS_REGEXP,
        std::move(qualified_identifier_),
        {} /*columns_identifiers*/,
        std::move(columns_matcher_),
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
    std::shared_ptr<re2::RE2> columns_matcher_,
    ColumnTransformersNodes column_transformers_)
    : matcher_type(matcher_type_)
    , qualified_identifier(qualified_identifier_)
    , columns_identifiers(columns_identifiers_)
    , columns_matcher(columns_matcher_)
{
    auto column_transformers_list_node = std::make_shared<ListNode>();

    auto & column_transformers_nodes = column_transformers_list_node->getNodes();
    column_transformers_nodes.reserve(column_transformers_.size());

    for (auto && column_transformer : column_transformers_)
        column_transformers_nodes.emplace_back(std::move(column_transformer));

    children.resize(1);
    children[column_transformers_child_index] = std::move(column_transformers_list_node);

    columns_identifiers_set.reserve(columns_identifiers.size());

    for (auto & column_identifier : columns_identifiers)
        columns_identifiers_set.insert(column_identifier.getFullName());
}

bool MatcherNode::isMatchingColumn(const std::string & column_name)
{
    if (matcher_type == MatcherNodeType::ASTERISK)
        return true;

    if (columns_matcher)
        return RE2::PartialMatch(column_name, *columns_matcher);

    return columns_identifiers_set.find(column_name) != columns_identifiers_set.end();
}

void MatcherNode::dumpTree(WriteBuffer & buffer, size_t indent) const
{
    buffer << std::string(indent, ' ') << "MATCHER ";
    writePointerHex(this, buffer);

    if (!qualified_identifier.empty())
        buffer << ' ' << qualified_identifier.getFullName();

    buffer << ' ' << toString(matcher_type);

    if (columns_matcher)
    {
        buffer << ' ' << columns_matcher->pattern();
    }
    else if (matcher_type == MatcherNodeType::COLUMNS_LIST)
    {
        buffer << ' ';
        size_t columns_identifiers_size = columns_identifiers.size();
        for (size_t i = 0; i < columns_identifiers_size; ++i)
        {
            buffer << columns_identifiers[i].getFullName();

            if (i + 1 != columns_identifiers_size)
                buffer << ", ";
        }
    }

    const auto & column_transformers_list = getColumnTransformers();
    if (!column_transformers_list.getNodes().empty())
    {
        buffer << '\n';
        column_transformers_list.dumpTree(buffer, indent + 2);
    }
}

String MatcherNode::getName() const
{
    if (matcher_type == MatcherNodeType::ASTERISK)
        return "*";

    WriteBufferFromOwnString buffer;
    buffer << "COLUMNS(";

    if (columns_matcher)
    {
        buffer << ' ' << columns_matcher->pattern();
    }
    else if (matcher_type == MatcherNodeType::COLUMNS_LIST)
    {
        size_t columns_identifiers_size = columns_identifiers.size();
        for (size_t i = 0; i < columns_identifiers_size; ++i)
        {
            buffer << columns_identifiers[i].getFullName();

            if (i + 1 != columns_identifiers_size)
                buffer << ", ";
        }
    }

    buffer << ')';
    return buffer.str();
}

void MatcherNode::updateTreeHashImpl(HashState & hash_state) const
{
    hash_state.update(static_cast<size_t>(matcher_type));

    const auto & qualified_identifier_full_name = qualified_identifier.getFullName();
    hash_state.update(qualified_identifier_full_name.size());
    hash_state.update(qualified_identifier_full_name);

    for (const auto & identifier : columns_identifiers)
    {
        const auto & identifier_full_name = identifier.getFullName();
        hash_state.update(identifier_full_name.size());
        hash_state.update(identifier_full_name.data(), identifier_full_name.size());
    }

    if (columns_matcher)
    {
        const auto & columns_matcher_pattern = columns_matcher->pattern();
        hash_state.update(columns_matcher_pattern.size());
        hash_state.update(columns_matcher_pattern);
    }
}

ASTPtr MatcherNode::toASTImpl() const
{
    ASTPtr result;

    if (matcher_type == MatcherNodeType::ASTERISK)
    {
        /// For COLUMNS qualified identifier is not supported
        if (qualified_identifier.empty())
        {
            result = std::make_shared<ASTAsterisk>();
        }
        else
        {
            auto qualified_asterisk = std::make_shared<ASTQualifiedAsterisk>();
            auto identifier_parts = qualified_identifier.getParts();
            qualified_asterisk->children.push_back(std::make_shared<ASTIdentifier>(std::move(identifier_parts)));

            result = qualified_asterisk;
        }
    }
    else if (columns_matcher)
    {
        auto regexp_matcher = std::make_shared<ASTColumnsRegexpMatcher>();
        regexp_matcher->setPattern(columns_matcher->pattern());
        result = regexp_matcher;
    }
    else
    {
        auto columns_list_matcher = std::make_shared<ASTColumnsListMatcher>();
        columns_list_matcher->children.reserve(columns_identifiers.size());

        for (const auto & identifier : columns_identifiers)
        {
            auto identifier_parts = identifier.getParts();
            columns_list_matcher->children.push_back(std::make_shared<ASTIdentifier>(std::move(identifier_parts)));
        }

        result = columns_list_matcher;
    }

    for (const auto & child : children)
        result->children.push_back(child->toAST());

    return result;
}

QueryTreeNodePtr MatcherNode::cloneImpl() const
{
    MatcherNodePtr matcher_node = std::make_shared<MatcherNode>();

    matcher_node->matcher_type = matcher_type;
    matcher_node->qualified_identifier = qualified_identifier;
    matcher_node->columns_identifiers = columns_identifiers;
    matcher_node->columns_matcher = columns_matcher;
    matcher_node->columns_identifiers_set = columns_identifiers_set;

    return matcher_node;
}

}
