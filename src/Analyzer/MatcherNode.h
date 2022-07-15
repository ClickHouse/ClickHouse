#pragma once

#include <re2/re2.h>

#include <Analyzer/Identifier.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ColumnTransformers.h>
#include <Parsers/ASTAsterisk.h>


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
  * The main difference between matcher and identifier is that matcher cannot have alias.
  * This simplifies analysis for matchers.
  *
  * How to resolve matcher during query analysis pass:
  * 1. If matcher is unqualified, we use tables of current scope and try to resolve matcher from it.
  * 2. If matcher is qualified:
  * First try to resolve identifier part as query expression.
  * Try expressions from aliases, then from tables (can be changed using prefer_column_name_to_alias setting).
  * If identifier is resolved as expression. If expression is compound apply matcher to it, otherwise throw exception.
  * Example: SELECT compound_column AS a, a.* FROM test_table.
  * Example: SELECT compound_column.* FROM test_table.
  *
  * If identifier is not resolved as expression try to resolve it as table.
  * If identifier is resolved as table then apply matcher to it.
  * Example: SELECT test_table.* FROM test_table.
  * Example: SELECT a.* FROM test_table AS a.
  *
  * Additionaly each matcher can contain transformers, check ColumnTransformers.h.
  * In query tree matchers column transformers are represended as ListNode.
  */
enum class MatcherNodeType
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
    explicit MatcherNode(std::shared_ptr<re2::RE2> columns_matcher_, ColumnTransformersNodes column_transformers_ = {});

    /// Variant qualified COLUMNS('regexp')
    explicit MatcherNode(Identifier qualified_identifier_, std::shared_ptr<re2::RE2> columns_matcher_, ColumnTransformersNodes column_transformers_ = {});

    /// Variant unqualified COLUMNS(column_name_1, ...)
    explicit MatcherNode(Identifiers columns_identifiers_, ColumnTransformersNodes column_transformers_ = {});

    /// Variant qualified COLUMNS(column_name_1, ...)
    explicit MatcherNode(Identifier qualified_identifier_, Identifiers columns_identifiers_, ColumnTransformersNodes column_transformers_ = {});

    /// Get matcher type
    MatcherNodeType getMatcherType() const
    {
        return matcher_type;
    }

    /// Is this matcher represented by asterisk
    bool isAsteriskMatcher() const
    {
        return matcher_type == MatcherNodeType::ASTERISK;
    }

    /// Is this matcher represented by COLUMNS
    bool isColumnsMatcher() const
    {
        return matcher_type == MatcherNodeType::COLUMNS_REGEXP || matcher_type == MatcherNodeType::COLUMNS_LIST;
    }

    /// Returns true if matcher qualified with identifier, false otherwise
    bool isQualified() const
    {
        return !qualified_identifier.empty();
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

    /// Get columns matcher. Valid only if this matcher has type COLUMNS_LIST.
    const Identifiers & getColumnsIdentifiers() const
    {
        return columns_identifiers;
    }

    /** Get column transformers
      * Client can expect that node in this list is subclass of IColumnTransformerNode.
      */
    const ListNode & getColumnTransformers() const
    {
        return children[column_transformers_child_index]->as<const ListNode &>();
    }

    const QueryTreeNodePtr & getColumnTransformersNode() const
    {
        return children[column_transformers_child_index];
    }

    /// Returns true if matcher match column name, false otherwise
    bool isMatchingColumn(const std::string & column_name);

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::ASTERISK;
    }

    void dumpTree(WriteBuffer & buffer, size_t indent) const override;

    String getName() const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs) const override;

    void updateTreeHashImpl(HashState & hash_state) const override;

    ASTPtr toASTImpl() const override;

    QueryTreeNodePtr cloneImpl() const override;

private:
    explicit MatcherNode(MatcherNodeType matcher_type_,
        Identifier qualified_identifier_,
        Identifiers columns_identifiers_,
        std::shared_ptr<re2::RE2> columns_matcher_,
        ColumnTransformersNodes column_transformers_);

    MatcherNodeType matcher_type;
    Identifier qualified_identifier;
    Identifiers columns_identifiers;
    std::shared_ptr<re2::RE2> columns_matcher;
    std::unordered_set<std::string> columns_identifiers_set;

    static constexpr size_t column_transformers_child_index = 0;
};

}
