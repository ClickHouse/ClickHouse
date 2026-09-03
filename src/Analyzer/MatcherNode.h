#pragma once

#include <Analyzer/Identifier.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ColumnTransformers.h>
#include <Parsers/ASTAsterisk.h>
#include <Common/re2.h>


namespace DB
{

/** Matcher query tree node.
  * Matcher can be unqualified with identifier and qualified with identifier.
  * It can be asterisk or COLUMNS('regexp') or COLUMNS(column_name_1, ...).
  * In result we have 6 possible options:
  * Unqualified
  * 1. *
  * 2. COLUMNS('regexp')
  * 3. COLUMNS(column_name_1, ...)
  *
  * Qualified:
  * 1. identifier.*
  * 2. identifier.COLUMNS('regexp')
  * 3. identifier.COLUMNS(column_name_1, ...)
  *
  * Matcher must be resolved during query analysis pass.
  *
  * Matchers can be applied to compound expressions.
  * Example: SELECT compound_column AS a, a.* FROM test_table.
  * Example: SELECT compound_column.* FROM test_table.
  *
  * Example: SELECT * FROM test_table;
  * Example: SELECT test_table.* FROM test_table.
  * Example: SELECT a.* FROM test_table AS a.
  *
  * Additionally each matcher can contain transformers, check ColumnTransformers.h.
  * In query tree matchers column transformers are represended as ListNode.
  */
enum class MatcherNodeType : uint8_t
{
    ASTERISK,
    COLUMNS_REGEXP,
    COLUMNS_LIST
};

const char * toString(MatcherNodeType matcher_node_type);

class MatcherNode;
using MatcherNodePtr = std::shared_ptr<MatcherNode>;

class MatcherNode final : public IQueryTreeNode
{
public:
    /// Variant unqualified asterisk
    explicit MatcherNode(ColumnTransformersNodes column_transformers_ = {});

    /// Variant qualified asterisk
    explicit MatcherNode(Identifier qualified_identifier_, ColumnTransformersNodes column_transformers_ = {});

    /// Variant unqualified COLUMNS('regexp')
    explicit MatcherNode(String pattern_, ColumnTransformersNodes column_transformers_ = {});

    /// Variant qualified COLUMNS('regexp')
    explicit MatcherNode(Identifier qualified_identifier_, String pattern_, ColumnTransformersNodes column_transformers_ = {});

    /// Variant unqualified COLUMNS(column_name_1, ...)
    explicit MatcherNode(Identifiers columns_identifiers_, ColumnTransformersNodes column_transformers_ = {});

    /// Variant qualified COLUMNS(column_name_1, ...)
    explicit MatcherNode(Identifier qualified_identifier_, Identifiers columns_identifiers_, ColumnTransformersNodes column_transformers_ = {});

    /// Get matcher type
    MatcherNodeType getMatcherType() const
    {
        return matcher_type;
    }

    /// Returns true if matcher is asterisk matcher, false otherwise
    bool isAsteriskMatcher() const
    {
        return matcher_type == MatcherNodeType::ASTERISK;
    }

    /// Returns true if matcher is qualified, false otherwise
    bool isQualified() const
    {
        return !qualified_identifier.empty();
    }

    /// Returns true if matcher is not qualified, false otherwise
    bool isUnqualified() const
    {
        return qualified_identifier.empty();
    }

    /// Get qualified identifier
    const Identifier & getQualifiedIdentifier() const
    {
        return qualified_identifier;
    }

    /// Get columns matcher. Valid only if this matcher has type COLUMNS_REGEXP.
    const std::shared_ptr<re2::RE2> & getColumnsMatcher() const
    {
        return columns_matcher;
    }

    /// Get columns identifiers. Valid only if this matcher has type COLUMNS_LIST.
    const Identifiers & getColumnsIdentifiers() const
    {
        return columns_identifiers;
    }

    /// Get column transformers
    const ListNode & getColumnTransformers() const
    {
        return children[column_transformers_child_index]->as<const ListNode &>();
    }

    /// Get column transformers
    const QueryTreeNodePtr & getColumnTransformersNode() const
    {
        return children[column_transformers_child_index];
    }

    /// Returns true if matcher match column name, false otherwise
    bool isMatchingColumn(const std::string & column_name);

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::MATCHER;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState & hash_state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    explicit MatcherNode(MatcherNodeType matcher_type_,
        Identifier qualified_identifier_,
        Identifiers columns_identifiers_,
        std::optional<String> pattern_,
        ColumnTransformersNodes column_transformers_);

    MatcherNodeType matcher_type;
    Identifier qualified_identifier;
    Identifiers columns_identifiers;
    std::shared_ptr<re2::RE2> columns_matcher;
    std::unordered_set<std::string> columns_identifiers_set;

    static constexpr size_t column_transformers_child_index = 0;
    static constexpr size_t children_size = column_transformers_child_index + 1;
};

}
