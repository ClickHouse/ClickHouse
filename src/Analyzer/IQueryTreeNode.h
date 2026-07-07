#pragma once

#include <memory>
#include <string>
#include <vector>
#include <deque>

#include <Common/TypePromotion.h>

#include <city.h>

class SipHash;

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
}

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

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
  * Important property of query tree is that each query tree node can contain weak pointers to other
  * query tree nodes. Keeping weak pointer to other query tree nodes can be useful for example for column
  * to keep weak pointer to column source, column source can be table, lambda, subquery and preserving of
  * such information can significantly simplify query planning.
  *
  * Another important property of query tree it must be convertible to AST without losing information.
  */
class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;
using QueryTreeNodes = std::vector<QueryTreeNodePtr>;
using QueryTreeNodesDeque = std::deque<QueryTreeNodePtr>;
using QueryTreeNodeWeakPtr = std::weak_ptr<IQueryTreeNode>;
using QueryTreeWeakNodes = std::vector<QueryTreeNodeWeakPtr>;

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
        bool compare_types = true;
        /// Do not compare the cte name or check the is_cte flag for the query node.
        /// Calculate a hash as if is_cte is false and cte_name is empty.
        bool ignore_cte = false;
    };

    /** Is tree equal to other tree with node root.
      *
      * With default compare options aliases of query tree nodes are compared during isEqual call.
      * Original ASTs of query tree nodes are not compared during isEqual call.
      */
    bool isEqual(const IQueryTreeNode & rhs, CompareOptions compare_options = { .compare_aliases = true, .compare_types = true, .ignore_cte = false }) const;

    using Hash = CityHash_v1_0_2::uint128;
    using HashState = SipHash;

    /** Get tree hash identifying current tree
      *
      * Alias of query tree node is part of query tree hash.
      * Original AST is not part of query tree hash.
      */
    Hash getTreeHash(CompareOptions compare_options = { .compare_aliases = true, .compare_types = true, .ignore_cte = false }) const;

    /// Get a deep copy of the query tree
    QueryTreeNodePtr clone() const;

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
    /** Construct query tree node.
      * Resize children to children size.
      * Resize weak pointers to weak pointers size.
      */
    explicit IQueryTreeNode(size_t children_size, size_t weak_pointers_size);

    /// Construct query tree node and resize children to children size
    explicit IQueryTreeNode(size_t children_size);

    /** Subclass must compare its internal state with rhs node internal state and do not compare children or weak pointers to other
      * query tree nodes.
      */
    virtual bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions compare_options) const = 0;

    /** Subclass must update tree hash with its internal state and do not update tree hash for children or weak pointers to other
      * query tree nodes.
      */
    virtual void updateTreeHashImpl(HashState & hash_state, CompareOptions compare_options) const = 0;

    /** Subclass must clone its internal state and do not clone children or weak pointers to other
      * query tree nodes.
      */
    virtual QueryTreeNodePtr cloneImpl() const = 0;

    /// Subclass must convert its internal state and its children to AST
    virtual ASTPtr toASTImpl(const ConvertToASTOptions & options) const = 0;

    QueryTreeNodes children;
    QueryTreeWeakNodes weak_pointers;

private:
    String alias;
    /// An alias from query. Alias can be replaced by query passes,
    /// but we need to keep the original one to support additional_table_filters.
    String original_alias;
    ASTPtr original_ast;
};

}
