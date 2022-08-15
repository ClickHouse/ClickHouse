#include <Analyzer/IQueryTreeNode.h>

#include <unordered_map>

#include <Common/SipHash.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTWithAlias.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

const char * toString(QueryTreeNodeType type)
{
    switch (type)
    {
        case QueryTreeNodeType::IDENTIFIER: return "IDENTIFIER";
        case QueryTreeNodeType::MATCHER: return "MATCHER";
        case QueryTreeNodeType::TRANSFORMER: return "TRANSFORMER";
        case QueryTreeNodeType::LIST: return "LIST";
        case QueryTreeNodeType::CONSTANT: return "CONSTANT";
        case QueryTreeNodeType::FUNCTION: return "FUNCTION";
        case QueryTreeNodeType::COLUMN: return "COLUMN";
        case QueryTreeNodeType::LAMBDA: return "LAMBDA";
        case QueryTreeNodeType::TABLE: return "TABLE";
        case QueryTreeNodeType::TABLE_FUNCTION: return "TABLE_FUNCTION";
        case QueryTreeNodeType::QUERY: return "QUERY";
        case QueryTreeNodeType::ARRAY_JOIN: return "ARRAY_JOIN";
        case QueryTreeNodeType::JOIN: return "JOIN";
    }
}

String IQueryTreeNode::dumpTree() const
{
    WriteBufferFromOwnString buff;
    dumpTree(buff);
    return buff.str();
}

size_t IQueryTreeNode::FormatState::getNodeId(const IQueryTreeNode * node)
{
    auto [it, _] = node_to_id.emplace(node, node_to_id.size());
    return it->second;
}

void IQueryTreeNode::dumpTree(WriteBuffer & buffer) const
{
    FormatState state;
    dumpTreeImpl(buffer, state, 0);
}

bool IQueryTreeNode::isEqual(const IQueryTreeNode & rhs) const
{
    if (getNodeType() != rhs.getNodeType())
        return false;

    if (alias != rhs.alias)
        return false;

    if (!isEqualImpl(rhs))
        return false;

    size_t children_size = children.size();
    if (children_size != rhs.children.size())
        return false;

    for (size_t i = 0; i < children_size; ++i)
    {
        const auto & child = children[i];
        const auto & rhs_child = rhs.children[i];

        if (!child && !rhs_child)
            continue;
        else if (child && !rhs_child)
            return false;
        else if (!child && rhs_child)
            return false;

        if (!child->isEqual(*rhs.children[i]))
            return false;
    }

    return true;
}

IQueryTreeNode::Hash IQueryTreeNode::getTreeHash() const
{
    HashState hash_state;
    updateTreeHash(hash_state);

    Hash result;
    hash_state.get128(result);

    return result;
}

void IQueryTreeNode::updateTreeHash(HashState & state) const
{
    state.update(static_cast<size_t>(getNodeType()));
    if (!alias.empty())
    {
        state.update(alias.size());
        state.update(alias);
    }

    updateTreeHashImpl(state);

    state.update(children.size());

    for (const auto & child : children)
    {
        if (!child)
            continue;

        child->updateTreeHash(state);
    }
}

QueryTreeNodePtr IQueryTreeNode::clone() const
{
    /** Main motivation for this method is to allow nodes in query tree have weak pointers to other nodes.
      * Main use cases is for column node to have weak pointer to its source.
      * Source can be lambda, table, subquery and such information is useful for later analysis stages.
      *
      * Algorithm
      * For each node we clone state and also create mapping old_pointer to new pointer.
      * For each cloned node we also update node_pointer_to_update_node_pointers array.
      *
      * After that we can update pointer in node_pointer_to_update_node_pointers using old_pointer to new pointer mapping.
      */
    std::unordered_map<const IQueryTreeNode *, QueryTreeNodePtr> old_pointer_to_new_pointer;
    QueryTreePointersToUpdate pointers_to_update_after_clone;

    QueryTreeNodePtr result_cloned_node_place;

    std::deque<std::pair<const IQueryTreeNode *, QueryTreeNodePtr *>> nodes_to_clone;
    nodes_to_clone.emplace_back(this, &result_cloned_node_place);

    while (!nodes_to_clone.empty())
    {
        const auto [node_to_clone, cloned_node_place] = nodes_to_clone.front();
        nodes_to_clone.pop_front();

        auto node_clone = node_to_clone->cloneImpl();
        *cloned_node_place = node_clone;

        node_clone->setAlias(node_to_clone->alias);
        node_clone->setOriginalAST(node_to_clone->original_ast);
        node_clone->children = node_to_clone->children;

        node_clone->getPointersToUpdateAfterClone(pointers_to_update_after_clone);
        old_pointer_to_new_pointer.emplace(node_to_clone, node_clone);

        for (auto & child : node_clone->children)
        {
            if (!child)
                continue;

            nodes_to_clone.emplace_back(child.get(), &child);
        }
    }

    for (auto & [old_pointer, new_pointer] : pointers_to_update_after_clone)
    {
        auto it = old_pointer_to_new_pointer.find(old_pointer);

        /** If node had weak pointer to some other node and this node is not valid in cloned subtree part do not clone it.
              * It will continue to point to previous location and it is expected.
              *
              * For example: SELECT id as a, a FROM test_table.
              * id is resolved as column and test_table is source.
              * a is resolved as id and after resolve must be cloned.
              * weak pointer to source from a will point to old id location.
              */
        if (it == old_pointer_to_new_pointer.end())
            continue;

        *new_pointer = it->second;
    }

    return result_cloned_node_place;
}

ASTPtr IQueryTreeNode::toAST() const
{
    auto converted_node = toASTImpl();

    if (auto * ast_with_alias = typeid_cast<ASTWithAlias *>(converted_node.get()))
        converted_node->setAlias(alias);

    return converted_node;
}

String IQueryTreeNode::formatOriginalASTForErrorMessage() const
{
    if (!original_ast)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Original AST was not set");

    return original_ast->formatForErrorMessage();
}

String IQueryTreeNode::formatConvertedASTForErrorMessage() const
{
    return toAST()->formatForErrorMessage();
}

}
