#pragma once

#include <memory>
#include <string>
#include <vector>

#include <Common/TypePromotion.h>

#include <DataTypes/IDataType.h>

#include <Parsers/IAST_fwd.h>

#include <Analyzer/Identifier.h>

class SipHash;

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

class WriteBuffer;

/// Query tree node type
enum class QueryTreeNodeType
{
    IDENTIFIER,
    ASTERISK,
    TRANSFORMER,
    LIST,
    CONSTANT,
    FUNCTION,
    COLUMN,
    LAMBDA,
    TABLE,
    QUERY,
};

/// Convert query tree node type to string
const char * toString(QueryTreeNodeType type);

/** Query tree node represent node in query tree.
  * This is base class for all query tree nodes.
  */
class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;
using QueryTreeNodeWeakPtr = std::weak_ptr<IQueryTreeNode>;
using QueryTreeNodes = std::vector<QueryTreeNodePtr>;

class IQueryTreeNode : public TypePromotion<IQueryTreeNode>
{
public:
    virtual ~IQueryTreeNode() = default;

    /// Get query tree node type
    virtual QueryTreeNodeType getNodeType() const = 0;

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
      * If node does not support this method exception is throwed.
      * TODO: Maybe this can be a part of ExpressionQueryTreeNode.
      */
    virtual DataTypePtr getResultType() const
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Method getResultType is not supported for {} query node", getNodeTypeName());
    }

    /// Dump query tree to string
    String dumpTree() const;

    /// Dump query tree to buffer
    void dumpTree(WriteBuffer & buffer) const;

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

    /// Update tree hash
    void updateTreeHash(HashState & state) const;

    /// Get a deep copy of the query tree
    QueryTreeNodePtr clone() const;

    /// Check if node has alias
    bool hasAlias() const
    {
        return !alias.empty();
    }

    /// Get node alias value if specified
    const String & getAlias() const
    {
        return alias;
    }

    /// Set node alias value
    void setAlias(String alias_value)
    {
        alias = std::move(alias_value);
    }

    /// Remove node alias value
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

    /** Subclass must compare its internal state with rhs node and do not compare its children with rhs node children.
      * Caller must compare node and rhs node children.
      *
      * This method is not protected because if subclass node has weak pointers to other query tree nodes it must use it
      * as part of its isEqualImpl method.
      */
    virtual bool isEqualImpl(const IQueryTreeNode & rhs) const = 0;

    /** Subclass must update tree hash with its internal state and do not update tree hash for children.
      * Caller must update tree hash for node children.
      *
      * This method is not protected because if subclass node has weak pointers to other query tree nodes it must use it
      * as part of its updateTreeHashImpl method.
      */
    virtual void updateTreeHashImpl(HashState & hash_state) const = 0;

protected:

    /** Subclass node must convert itself to AST.
      * Subclass must convert children to AST.
      */
    virtual ASTPtr toASTImpl() const = 0;

    /** Subclass must clone only it internal state.
      * Subclass children will be cloned separately by caller.
      */
    virtual QueryTreeNodePtr cloneImpl() const = 0;

    /** If node has weak pointers to other tree nodes during clone they will point to other tree nodes.
      * Keeping weak pointer to other tree nodes can be useful for example for column to keep weak pointer to column source.
      * Source can be table, lambda, subquery and such information is necessary to preserve.
      *
      * Example:
      * SELECT id FROM table;
      * id during query analysis will be resolved as ColumnNode and source will be TableNode.
      * During clone we must update id ColumnNode source pointer.
      *
      * Subclass must save old pointer and place of pointer update into pointers_to_update.
      * This method will be called on query tree node after clone.
      *
      * Root of clone process will update pointers as necessary.
      */
    using QueryTreePointerToUpdate = std::pair<const IQueryTreeNode *, QueryTreeNodeWeakPtr *>;
    using QueryTreePointersToUpdate = std::vector<QueryTreePointerToUpdate>;

    virtual void getPointersToUpdateAfterClone(QueryTreePointersToUpdate & pointers_to_update)
    {
        (void)(pointers_to_update);
    }

    QueryTreeNodes children;

private:
    String alias;
    ASTPtr original_ast;
};

}
