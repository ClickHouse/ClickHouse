#pragma once

#include <memory>
#include <vector>
#include <deque>

#include <base/types.h>
#include <Parsers/IAST_fwd.h>
#include <Common/Exception.h>
#include <Common/TypePromotion.h>

#include <city.h>

class SipHash;

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
}

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

class WriteBuffer;

/// Query tree node type
enum class QueryTreeNodeType : uint8_t
{
    IDENTIFIER,
    MATCHER,
    TRANSFORMER,
    LIST,
    CONSTANT,
    FUNCTION,
    COLUMN,
    LAMBDA,
    SORT,
    INTERPOLATE,
    WINDOW,
    TABLE,
    TABLE_FUNCTION,
    QUERY,
    ARRAY_JOIN,
    CROSS_JOIN,
    JOIN,
    UNION,
};

/// Convert query tree node type to string
const char * toString(QueryTreeNodeType type);

/** Query tree is a semantic representation of query.
  * Query tree node represent node in query tree.
  * IQueryTreeNode is base class for all query tree nodes.
  *
  * Important property of query tree is that a column node keeps a weak pointer to its column
  * source together with the column source id. Column source can be table, lambda, subquery, and
  * preserving of such information can significantly simplify query planning.
  *
  * Another important property of query tree it must be convertible to AST without losing information.
  */
class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;
using QueryTreeNodes = std::vector<QueryTreeNodePtr>;
using QueryTreeNodesDeque = std::deque<QueryTreeNodePtr>;
using QueryTreeNodeWeakPtr = std::weak_ptr<IQueryTreeNode>;

class IColumnSourceNode;

/** Id of a column source instance (a node derived from IColumnSourceNode), unique within the process.
  * Value 0 is reserved for "no source". Ids are not deterministic between runs and must never appear
  * in user-visible output such as EXPLAIN QUERY TREE.
  */
using ColumnSourceId = UInt64;
constexpr ColumnSourceId INVALID_COLUMN_SOURCE_ID = 0;

struct ConvertToASTOptions
{
    /// Add _CAST if constant literal type is different from column type
    bool add_cast_for_constants = true;

    /// Identifiers are fully qualified (`database.table.column`), otherwise names are just column names (`column`)
    bool fully_qualified_identifiers = true;

    /// Identifiers are qualified but database name is not added (`table.column`) if set to false.
    bool qualify_indentifiers_with_database = true;

    /// Set CTE name in ASTSubquery field.
    bool set_subquery_cte_name = true;
};

class IQueryTreeNode : public TypePromotion<IQueryTreeNode>
{
public:
    virtual ~IQueryTreeNode() = default;

    /// Get query tree node type
    virtual QueryTreeNodeType getNodeType() const = 0;

    /// Get query tree node type name
    const char * getNodeTypeName() const
    {
        return toString(getNodeType());
    }

    /// Returns true if this node can be a column source, i.e. derives from IColumnSourceNode
    bool isColumnSource() const
    {
        return is_column_source;
    }

    /** Get result type of query tree node that can be used as part of expression.
      * If node does not support this method exception is thrown.
      * TODO: Maybe this can be a part of ExpressionQueryTreeNode.
      */
    virtual DataTypePtr getResultType() const
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Method getResultType is not supported for {} query tree node", getNodeTypeName());
    }

    virtual void convertToNullable()
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Method convertToNullable is not supported for {} query tree node", getNodeTypeName());
    }

    struct CompareOptions
    {
        bool compare_aliases = true;
        /// Do not compare the cte name or check the is_cte flag for the query node.
        /// Calculate a hash as if is_cte is false and cte_name is empty.
        bool ignore_cte = false;
    };

    /** Is tree equal to other tree with node root.
      *
      * Column references are compared by their column source ids, i.e. two columns are equal only
      * if they reference the same column source instance. Because of that the result is meaningful
      * only for trees from the same id space: the same query tree, or its clones (a clone of a
      * column keeps referencing the original source unless the source was cloned with it). For
      * trees built independently use isEqualGlobal.
      *
      * With default compare options aliases of query tree nodes are compared.
      * Original ASTs of query tree nodes are not compared.
      */
    bool isEqualLocal(const IQueryTreeNode & rhs, CompareOptions compare_options = { .compare_aliases = true, .ignore_cte = false }) const;

    /** Is tree equal to other tree with node root, with equality canonical across query trees.
      *
      * Unlike isEqualLocal, column references are compared by the content of their column sources,
      * with repeated references handled through a correspondence map, so structurally equal trees
      * built independently compare equal. Traverses into column sources, so it is more expensive
      * than isEqualLocal.
      */
    bool isEqualGlobal(const IQueryTreeNode & rhs, CompareOptions compare_options = { .compare_aliases = true, .ignore_cte = false }) const;

    using Hash = CityHash_v1_0_2::uint128;
    using HashState = SipHash;

    /** Get tree hash identifying current tree, consistent with isEqualLocal.
      *
      * Column references are hashed by their column source ids, so the hash is meaningful only
      * within one id space (one query tree and its clones) and must never be used as an identifier
      * that outlives the query tree or crosses servers — use getTreeHashGlobal for that.
      *
      * Alias of query tree node is part of query tree hash.
      * Original AST is not part of query tree hash.
      *
      * The result is not cached: every call traverses the whole subtree, so the cost is
      * proportional to the subtree size.
      */
    Hash getTreeHashLocal(CompareOptions compare_options = { .compare_aliases = true, .ignore_cte = false }) const;

    /** Get tree hash that is canonical across query trees and across processes, consistent with
      * isEqualGlobal.
      *
      * The result does not depend on the identity of column source instances: column sources
      * referenced from the tree are hashed by their content, with repeated visits replaced by
      * visitation-order identifiers, so structurally equal trees built independently hash equal.
      * Use it for identifiers that must be stable across trees, queries or servers, e.g. prepared
      * set keys and distributed set names shared between the initiator and shards, or hash-join
      * cache keys shared between queries.
      *
      * Hashing a `ColumnNode` hashes its column source content (e.g. a `TableNode` or `QueryNode`
      * with all its columns). Because of that, computing the hash per node — e.g. looking up each
      * projection node in a container keyed by this hash — can make query analysis quadratic in
      * the number of columns for queries over wide tables. When such a container is usually empty,
      * skip the lookup explicitly for the empty case (see `QueryAnalyzer::resolveExpressionNode`
      * and `PlannerActionsVisitorImpl::visitColumn`).
      */
    Hash getTreeHashGlobal(CompareOptions compare_options = { .compare_aliases = true, .ignore_cte = false }) const;

    /** Get a deep copy of the query tree.
      * Column source ids are preserved: cloned column sources keep the id of the original, so the
      * clone is the same logical source and compares equal to the original (see IColumnSourceNode).
      */
    QueryTreeNodePtr clone() const;

    /** Get a deep copy of the query tree, assigning a fresh id to every cloned column source.
      * Use it where the clone is a genuinely separate source instance that may coexist with the
      * original in the same query tree, e.g. a CTE expanded at several references.
      */
    QueryTreeNodePtr cloneWithFreshColumnSourceIds() const;

    /** Get a deep copy of the query tree.
      * If node to clone is key in replacement map, then instead of clone it
      * use value node from replacement map.
      */
    using ReplacementMap = std::unordered_map<const IQueryTreeNode *, QueryTreeNodePtr>;
    QueryTreeNodePtr cloneAndReplace(const ReplacementMap & replacement_map) const;

    /** Get a deep copy of the query tree.
      * If node to clone is node to replace, then instead of clone it use replacement node.
      */
    QueryTreeNodePtr cloneAndReplace(const QueryTreeNodePtr & node_to_replace, QueryTreeNodePtr replacement_node) const;

    /// Returns true if node has alias, false otherwise
    bool hasAlias() const
    {
        return !alias.empty();
    }

    /// Get node alias
    const String & getAlias() const
    {
        return alias;
    }

    const String & getOriginalAlias() const
    {
        return original_alias.empty() ? alias : original_alias;
    }

    /// Set node alias
    void setAlias(String alias_value)
    {
        if (original_alias.empty())
            original_alias = std::move(alias);

        alias = std::move(alias_value);
    }

    /// Remove node alias
    void removeAlias()
    {
        alias = {};
    }

    /// Returns true if the expression was parenthesized in the original query
    bool isParenthesized() const
    {
        return parenthesized;
    }

    /// Set parenthesized flag
    void setParenthesized(bool value)
    {
        parenthesized = value;
    }

    /// Returns true if query tree node has original AST, false otherwise
    bool hasOriginalAST() const
    {
        return original_ast != nullptr;
    }

    /// Get query tree node original AST
    const ASTPtr & getOriginalAST() const
    {
        return original_ast;
    }

    /** Set query tree node original AST.
      * This AST will not be modified later.
      */
    void setOriginalAST(ASTPtr original_ast_value)
    {
        original_ast = std::move(original_ast_value);
    }

    /** If query tree has original AST format it for error message.
      * Otherwise exception is thrown.
      */
    String formatOriginalASTForErrorMessage() const;

    /// Convert query tree to AST
    ASTPtr toAST(const ConvertToASTOptions & options = {}) const;

    /// Convert query tree to AST and then format it for error message.
    String formatConvertedASTForErrorMessage() const;

    /** Format AST for error message.
      * If original AST exists use `formatOriginalASTForErrorMessage`.
      * Otherwise use `formatConvertedASTForErrorMessage`.
      */
    String formatASTForErrorMessage() const
    {
        if (original_ast)
            return formatOriginalASTForErrorMessage();

        return formatConvertedASTForErrorMessage();
    }

    /// Dump query tree to string
    String dumpTree() const;

    /// Dump query tree to buffer
    void dumpTree(WriteBuffer & buffer) const;

    class FormatState
    {
    public:
        size_t getNodeId(const IQueryTreeNode * node);

    private:
        std::unordered_map<const IQueryTreeNode *, size_t> node_to_id;
    };

    /** Dump query tree to buffer starting with indent.
      *
      * Node must also dump its children.
      */
    virtual void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const = 0;

    /// Get query tree node children
    QueryTreeNodes & getChildren()
    {
        return children;
    }

    /// Get query tree node children
    const QueryTreeNodes & getChildren() const
    {
        return children;
    }

protected:
    /// Construct query tree node and resize children to children size
    explicit IQueryTreeNode(size_t children_size);

    /** Subclass must compare its internal state with rhs node internal state and do not compare children or referenced column
      * sources.
      */
    virtual bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions compare_options) const = 0;

    /** Subclass must update tree hash with its internal state and do not update tree hash for children or referenced column
      * sources.
      */
    virtual void updateTreeHashImpl(HashState & hash_state, CompareOptions compare_options) const = 0;

    /** Subclass must clone its internal state and do not clone children or referenced column
      * sources.
      */
    virtual QueryTreeNodePtr cloneImpl() const = 0;

    /// Subclass must convert its internal state and its children to AST
    virtual ASTPtr toASTImpl(const ConvertToASTOptions & options) const = 0;

    QueryTreeNodes children;

private:
    friend class IColumnSourceNode;

    static bool areTreesEqual(
        const IQueryTreeNode & lhs, const IQueryTreeNode & rhs, CompareOptions compare_options, bool compare_column_sources_by_content);

    static Hash computeTreeHash(const IQueryTreeNode & root, CompareOptions compare_options, bool hash_column_sources_by_content);

    QueryTreeNodePtr cloneAndReplaceImpl(const ReplacementMap & replacement_map, bool fresh_column_source_ids) const;

    String alias;
    /// An alias from query. Alias can be replaced by query passes,
    /// but we need to keep the original one to support additional_table_filters.
    String original_alias;
    ASTPtr original_ast;
    /// If the expression has extra parentheses around it in the original query
    bool parenthesized = false;
    /// Set by IColumnSourceNode constructor
    bool is_column_source = false;
};

}
