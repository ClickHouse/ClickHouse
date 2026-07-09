#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>
#include <Core/Names.h>

namespace re2
{
    class RE2;
}

namespace DB
{

/** Transformers are query tree nodes that handle additional logic that you can apply after MatcherQueryTreeNode is resolved.
  * Check MatcherQueryTreeNode.h before reading this documentation.
  *
  * They main purpose is to apply some logic for expressions after matcher is resolved.
  * There are 3 types of transformers:
  *
  * 1. APPLY transformer:
  * APPLY transformer transform matched expression using lambda or function into another expression.
  * It has 2 syntax variants:
  *     1. lambda variant: SELECT matcher APPLY (x -> expr(x)).
  *     2. function variant: SELECT matcher APPLY function_name(optional_parameters).
  *
  * 2. EXCEPT transformer:
  * EXCEPT transformer discard some columns.
  * It has 2 syntax variants:
  *     1. regexp variant: SELECT matcher EXCEPT ('regexp').
  *     2. column names list variant: SELECT matcher EXCEPT (column_name_1, ...).
  *
  * 3. REPLACE transformer:
  * REPLACE transformer applies similar transformation as APPLY transformer, but only for expressions
  * that match replacement expression name.
  *
  * Example:
  * CREATE TABLE test_table (id UInt64) ENGINE=TinyLog;
  * SELECT * REPLACE (id + 1 AS id) FROM test_table.
  * This query is transformed into SELECT id + 1 FROM test_table.
  * It is important that AS id is not alias, it is replacement name. id + 1 is replacement expression.
  *
  * REPLACE transformer cannot contain multiple replacements with same name.
  *
  * REPLACE transformer expression does not necessary include replacement column name.
  * Example:
  * SELECT * REPLACE (1 AS id) FROM test_table.
  *
  * REPLACE transformer expression does not throw exception if there are no columns to apply replacement.
  * Example:
  * SELECT * REPLACE (1 AS unknown_column) FROM test_table;
  *
  * REPLACE transform can contain multiple replacements.
  * Example:
  * SELECT * REPLACE (1 AS id, 2 AS value).
  *
  * Matchers can be combined together and chained.
  * Example:
  * SELECT * EXCEPT (id) APPLY (x -> toString(x)) APPLY (x -> length(x)) FROM test_table.
  */

/// Column transformer type
enum class ColumnTransfomerType : uint8_t
{
    APPLY,
    EXCEPT,
    REPLACE
};

/// Get column transformer type name
const char * toString(ColumnTransfomerType type);

class IColumnTransformerNode;
using ColumnTransformerNodePtr = std::shared_ptr<IColumnTransformerNode>;
using ColumnTransformersNodes = std::vector<ColumnTransformerNodePtr>;

/// IColumnTransformer base interface.
class IColumnTransformerNode : public IQueryTreeNode
{
public:
    /// Get transformer type
    virtual ColumnTransfomerType getTransformerType() const = 0;

    /// Get transformer type name
    const char * getTransformerTypeName() const
    {
        return toString(getTransformerType());
    }

    QueryTreeNodeType getNodeType() const final
    {
        return QueryTreeNodeType::TRANSFORMER;
    }

protected:
    /// Construct column transformer node and resize children to children size
    explicit IColumnTransformerNode(size_t children_size);
};

enum class ApplyColumnTransformerType : uint8_t
{
    LAMBDA,
    FUNCTION
};

/// Get apply column transformer type name
const char * toString(ApplyColumnTransformerType type);


/// Apply column transformer
class ApplyColumnTransformerNode final : public IColumnTransformerNode
{
public:
    /** Initialize apply column transformer with expression node.
      * Expression node must be lambda or function otherwise exception is thrown.
      */
    explicit ApplyColumnTransformerNode(QueryTreeNodePtr expression_node_);

    /// Get apply transformer type
    ApplyColumnTransformerType getApplyTransformerType() const
    {
        return apply_transformer_type;
    }

    /// Get apply transformer expression node
    const QueryTreeNodePtr & getExpressionNode() const
    {
        return children[expression_child_index];
    }

    ColumnTransfomerType getTransformerType() const override
    {
        return ColumnTransfomerType::APPLY;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(IQueryTreeNode::HashState & hash_state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    ApplyColumnTransformerType apply_transformer_type = ApplyColumnTransformerType::LAMBDA;

    static constexpr size_t expression_child_index = 0;
    static constexpr size_t children_size = expression_child_index + 1;
};

/// Except column transformer type
enum class ExceptColumnTransformerType : uint8_t
{
    REGEXP,
    COLUMN_LIST,
};

const char * toString(ExceptColumnTransformerType type);


/** Except column transformer.
  * Strict EXCEPT column transformer must use all column names during matched nodes transformation.
  *
  * Example:
  * CREATE TABLE test_table (id UInt64, value String) ENGINE=TinyLog;
  * SELECT * EXCEPT STRICT (id, value1) FROM test_table;
  * Such query will throw exception because column with name `value1` was not matched by strict EXCEPT transformer.
  *
  * Strict is valid only for EXCEPT COLUMN_LIST transformer.
  */
class ExceptColumnTransformerNode final : public IColumnTransformerNode
{
public:
    /// Initialize except column transformer with structured target identifiers: parsed parts per
    /// target (a single part may contain dots) plus a parallel per-part double-quote flag.
    explicit ExceptColumnTransformerNode(
        std::vector<std::vector<String>> target_parts_,
        bool is_strict_,
        std::vector<std::vector<bool>> target_parts_double_quoted_ = {});

    /// Initialize except column transformer with regexp column matcher
    explicit ExceptColumnTransformerNode(std::shared_ptr<re2::RE2> column_matcher_);

    /// Get except transformer type
    ExceptColumnTransformerType getExceptTransformerType() const
    {
        return except_transformer_type;
    }

    /** Returns true if except column transformer is strict, false otherwise.
      * Valid only for EXCEPT COLUMN_LIST transformer.
      */
    bool isStrict() const
    {
        return is_strict;
    }

    /// Returns true if except transformer matches column name. `standard_mode` folds targets that
    /// were not double-quoted; `matched_target` receives the target as written, for STRICT bookkeeping.
    bool isColumnMatching(const std::string & column_name, bool standard_mode = false, std::string * matched_target = nullptr) const;

    /** Get except column names.
      * Valid only for column list except transformer.
      */
    const Names & getExceptColumnNames() const
    {
        return except_column_names;
    }

    ColumnTransfomerType getTransformerType() const override
    {
        return ColumnTransfomerType::EXCEPT;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(IQueryTreeNode::HashState & hash_state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    ExceptColumnTransformerType except_transformer_type;
    /// Structured target identifiers, one inner vector of parsed parts per target. A single part
    /// may contain dots (`` EXCEPT (`a.b`) `` is one part "a.b"), so flattened names are never re-split.
    std::vector<std::vector<String>> target_parts;
    /// Flattened full names derived from `target_parts` (parts joined with '.'). Kept for the
    /// public `getExceptColumnNames` API, exact-match passes, and error messages.
    Names except_column_names;
    /// Parallel to `target_parts`; element `j` is true when part `j` was written `"Name"`. Quoted
    /// parts stay exact in `standard` mode while unquoted parts fold. Empty without quote tracking.
    std::vector<std::vector<bool>> target_parts_double_quoted;
    std::shared_ptr<re2::RE2> column_matcher;
    bool is_strict = false;

    static constexpr size_t children_size = 0;
};


/** Replace column transformer.
  * Strict replace column transformer must use all replacements during matched nodes transformation.
  *
  * Example:
  * CREATE TABLE test_table (id UInt64, value String) ENGINE=TinyLog;
  * SELECT * REPLACE STRICT (1 AS id, 2 AS value_1) FROM test_table;
  * Such query will throw exception because column with name `value1` was not matched by strict REPLACE transformer.
  */
class ReplaceColumnTransformerNode final : public IColumnTransformerNode
{
public:
    /// Structured target identifier and replace expression. `parts` holds the parsed identifier
    /// parts (today the parser produces one part, which may contain dots); `parts_double_quoted` is parallel.
    struct Replacement
    {
        std::vector<String> parts;
        QueryTreeNodePtr expression_node;
        std::vector<bool> parts_double_quoted;
    };

    /// Initialize replace column transformer with replacements
    explicit ReplaceColumnTransformerNode(const std::vector<Replacement> & replacements_, bool is_strict);

    ColumnTransfomerType getTransformerType() const override
    {
        return ColumnTransfomerType::REPLACE;
    }

    /// Get replacements
    const ListNode & getReplacements() const
    {
        return children[replacements_child_index]->as<ListNode &>();
    }

    /// Get replacements node
    const QueryTreeNodePtr & getReplacementsNode() const
    {
        return children[replacements_child_index];
    }

    /// Get replacements names
    const Names & getReplacementsNames() const
    {
        return replacements_names;
    }

    /// Returns true if replace column transformer is strict, false otherwise
    bool isStrict() const
    {
        return is_strict;
    }

    /** Returns replacement expression if replacement is registered for expression name, null otherwise.
      * Returned replacement expression must be cloned by caller. `standard_mode` folds targets that
      * were not double-quoted; `matched_target` receives the target as written, for STRICT bookkeeping.
      */
    QueryTreeNodePtr findReplacementExpression(const std::string & expression_name, bool standard_mode = false, std::string * matched_target = nullptr);

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(IQueryTreeNode::HashState & hash_state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    ListNode & getReplacements()
    {
        return children[replacements_child_index]->as<ListNode &>();
    }

    /// Structured target identifiers (parsed parts, never re-split from the flattened name; a
    /// single part may contain dots).
    std::vector<std::vector<String>> target_parts;
    /// Flattened full names derived from `target_parts`. Kept for the `getReplacementsNames` API,
    /// exact-match passes, and error messages.
    Names replacements_names;
    /// Parallel to `target_parts`; element `j` is true when part `j` was double-quoted. Quoted
    /// parts stay exact in `standard` mode while unquoted parts fold. Empty without quote tracking.
    std::vector<std::vector<bool>> target_parts_double_quoted;
    bool is_strict = false;

    static constexpr size_t replacements_child_index = 0;
    static constexpr size_t children_size = replacements_child_index + 1;
};

}
