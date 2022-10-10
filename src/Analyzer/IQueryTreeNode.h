#pragma once

#include <memory>
#include <string>
#include <vector>

#include <Common/TypePromotion.h>

#include <DataTypes/IDataType.h>

#include <Parsers/IAST_fwd.h>

#include <Analyzer/Identifier.h>
#include <Analyzer/ConstantValue.h>

class SipHash;

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
}

class WriteBuffer;

/// Query tree node type
enum class QueryTreeNodeType
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
    JOIN,
    UNION
};

/// Convert query tree node type to string
const char * toString(QueryTreeNodeType type);

/** Query tree is semantical representation of query.
  * Query tree node represent node in query tree.
  * Query tree node is base class for all query tree nodes.
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
using QueryTreeNodeWeakPtr = std::weak_ptr<IQueryTreeNode>;
using QueryTreeWeakNodes = std::vector<QueryTreeNodeWeakPtr>;

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

    /** Get name of query tree node that can be used as part of expression.
      * TODO: Projection name, expression name must be refactored in better interface.
      */
    virtual String getName() const
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Method getName is not supported for {} query node", getNodeTypeName());
    }

    /** Get result type of query tree node that can be used as part of expression.
      * If node does not support this method exception is thrown.
      * TODO: Maybe this can be a part of ExpressionQueryTreeNode.
      */
    virtual DataTypePtr getResultType() const
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Method getResultType is not supported for {} query node", getNodeTypeName());
    }

    /// Returns true if node has constant value
    bool hasConstantValue() const
    {
        return getConstantValueOrNull() != nullptr;
    }

    /** Returns constant value with type if node has constant value, and can be replaced with it.
      * Examples: scalar subquery, function with constant arguments.
      */
    virtual const ConstantValue & getConstantValue() const
    {
        auto constant_value = getConstantValueOrNull();
        if (!constant_value)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Node does not have constant value");

        return *constant_value;
    }

    /// Returns constant value with type if node has constant value or null otherwise
    virtual ConstantValuePtr getConstantValueOrNull() const
    {
        return {};
    }

    /** Is tree equal to other tree with node root.
      *
      * Aliases of query tree nodes are compared during isEqual call.
      * Original ASTs of query tree nodes are not compared during isEqual call.
      */
    bool isEqual(const IQueryTreeNode & rhs) const;

    using Hash = std::pair<UInt64, UInt64>;
    using HashState = SipHash;

    /** Get tree hash identifying current tree
      *
      * Alias of query tree node is part of query tree hash.
      * Original AST is not part of query tree hash.
      */
    Hash getTreeHash() const;

    /// Get a deep copy of the query tree
    QueryTreeNodePtr clone() const;

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

    /// Set node alias
    void setAlias(String alias_value)
    {
        alias = std::move(alias_value);
    }

    /// Remove node alias
    void removeAlias()
    {
        alias = {};
    }

    /// Check if query tree node has original AST
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
      * Otherwise throw an exception.
      */
    String formatOriginalASTForErrorMessage() const;

    /// Convert query tree to AST
    ASTPtr toAST() const;

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
    virtual bool isEqualImpl(const IQueryTreeNode & rhs) const = 0;

    /** Subclass must update tree hash with its internal state and do not update tree hash for children or weak pointers to other
      * query tree nodes.
      */
    virtual void updateTreeHashImpl(HashState & hash_state) const = 0;

    /** Subclass must clone its internal state and do not clone children or weak pointers to other
      * query tree nodes.
      */
    virtual QueryTreeNodePtr cloneImpl() const = 0;

    /// Subclass must convert its internal state and its children to AST
    virtual ASTPtr toASTImpl() const = 0;

    QueryTreeNodes children;
    QueryTreeWeakNodes weak_pointers;

private:
    String alias;
    ASTPtr original_ast;
};

}
