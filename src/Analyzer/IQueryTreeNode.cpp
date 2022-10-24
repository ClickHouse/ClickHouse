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
        case QueryTreeNodeType::SORT: return "SORT";
        case QueryTreeNodeType::INTERPOLATE: return "INTERPOLATE";
        case QueryTreeNodeType::WINDOW: return "WINDOW";
        case QueryTreeNodeType::TABLE: return "TABLE";
        case QueryTreeNodeType::TABLE_FUNCTION: return "TABLE_FUNCTION";
        case QueryTreeNodeType::QUERY: return "QUERY";
        case QueryTreeNodeType::ARRAY_JOIN: return "ARRAY_JOIN";
        case QueryTreeNodeType::JOIN: return "JOIN";
        case QueryTreeNodeType::UNION: return "UNION";
    }
}

IQueryTreeNode::IQueryTreeNode(size_t children_size, size_t weak_pointers_size)
{
    children.resize(children_size);
    weak_pointers.resize(weak_pointers_size);
}

IQueryTreeNode::IQueryTreeNode(size_t children_size)
{
    children.resize(children_size);
}

namespace
{

using NodePair = std::pair<const IQueryTreeNode *, const IQueryTreeNode *>;

struct NodePairHash
{
    size_t operator()(const NodePair & node_pair) const
    {
        auto hash = std::hash<const IQueryTreeNode *>();

        size_t result = 0;
        boost::hash_combine(result, hash(node_pair.first));
        boost::hash_combine(result, hash(node_pair.second));

        return result;
    }
};

}

bool IQueryTreeNode::isEqual(const IQueryTreeNode & rhs) const
{
    std::vector<NodePair> nodes_to_process;
    std::unordered_set<NodePair, NodePairHash> equals_pairs;

    nodes_to_process.emplace_back(this, &rhs);

    while (!nodes_to_process.empty())
    {
        auto nodes_to_compare = nodes_to_process.back();
        nodes_to_process.pop_back();

        const auto * lhs_node_to_compare = nodes_to_compare.first;
        const auto * rhs_node_to_compare = nodes_to_compare.second;

        if (equals_pairs.contains(std::make_pair(lhs_node_to_compare, rhs_node_to_compare)))
            continue;

        assert(lhs_node_to_compare);
        assert(rhs_node_to_compare);

        if (lhs_node_to_compare->getNodeType() != rhs_node_to_compare->getNodeType() ||
            lhs_node_to_compare->alias != rhs_node_to_compare->alias ||
            !lhs_node_to_compare->isEqualImpl(*rhs_node_to_compare))
        {
            return false;
        }

        const auto & lhs_children = lhs_node_to_compare->children;
        const auto & rhs_children = rhs_node_to_compare->children;

        size_t lhs_children_size = lhs_children.size();
        if (lhs_children_size != rhs_children.size())
            return false;

        for (size_t i = 0; i < lhs_children_size; ++i)
        {
            const auto & lhs_child = lhs_children[i];
            const auto & rhs_child = rhs_children[i];

            if (!lhs_child && !rhs_child)
                continue;
            else if (lhs_child && !rhs_child)
                return false;
            else if (!lhs_child && rhs_child)
                return false;

            nodes_to_process.emplace_back(lhs_child.get(), rhs_child.get());
        }

        const auto & lhs_weak_pointers = lhs_node_to_compare->weak_pointers;
        const auto & rhs_weak_pointers = rhs_node_to_compare->weak_pointers;

        size_t lhs_weak_pointers_size = lhs_weak_pointers.size();

        if (lhs_weak_pointers_size != rhs_weak_pointers.size())
            return false;

        for (size_t i = 0; i < lhs_weak_pointers_size; ++i)
        {
            auto lhs_strong_pointer = lhs_weak_pointers[i].lock();
            auto rhs_strong_pointer = rhs_weak_pointers[i].lock();

            if (!lhs_strong_pointer && !rhs_strong_pointer)
                continue;
            else if (lhs_strong_pointer && !rhs_strong_pointer)
                return false;
            else if (!lhs_strong_pointer && rhs_strong_pointer)
                return false;

            nodes_to_process.emplace_back(lhs_strong_pointer.get(), rhs_strong_pointer.get());
        }

        equals_pairs.emplace(lhs_node_to_compare, rhs_node_to_compare);
    }

    return true;
}

IQueryTreeNode::Hash IQueryTreeNode::getTreeHash() const
{
    HashState hash_state;

    std::unordered_map<const IQueryTreeNode *, size_t> node_to_identifier;

    std::vector<const IQueryTreeNode *> nodes_to_process;
    nodes_to_process.push_back(this);

    while (!nodes_to_process.empty())
    {
        const auto * node_to_process = nodes_to_process.back();
        nodes_to_process.pop_back();

        auto node_identifier_it = node_to_identifier.find(node_to_process);
        if (node_identifier_it != node_to_identifier.end())
        {
            hash_state.update(node_identifier_it->second);
            continue;
        }

        node_to_identifier.emplace(node_to_process, node_to_identifier.size());

        hash_state.update(static_cast<size_t>(node_to_process->getNodeType()));
        if (!node_to_process->alias.empty())
        {
            hash_state.update(node_to_process->alias.size());
            hash_state.update(node_to_process->alias);
        }

        node_to_process->updateTreeHashImpl(hash_state);

        hash_state.update(node_to_process->children.size());

        for (const auto & node_to_process_child : node_to_process->children)
        {
            if (!node_to_process_child)
                continue;

            nodes_to_process.push_back(node_to_process_child.get());
        }

        hash_state.update(node_to_process->weak_pointers.size());

        for (const auto & weak_pointer : node_to_process->weak_pointers)
        {
            auto strong_pointer = weak_pointer.lock();
            if (!strong_pointer)
                continue;

            nodes_to_process.push_back(strong_pointer.get());
        }
    }

    Hash result;
    hash_state.get128(result);

    return result;
}

QueryTreeNodePtr IQueryTreeNode::clone() const
{
    /** Clone tree with this node as root.
      *
      * Algorithm
      * For each node we clone state and also create mapping old pointer to new pointer.
      * For each cloned node we update weak pointers array.
      *
      * After that we can update pointer in weak pointers array using old pointer to new pointer mapping.
      */
    std::unordered_map<const IQueryTreeNode *, QueryTreeNodePtr> old_pointer_to_new_pointer;
    std::vector<QueryTreeNodeWeakPtr *> weak_pointers_to_update_after_clone;

    QueryTreeNodePtr result_cloned_node_place;

    std::vector<std::pair<const IQueryTreeNode *, QueryTreeNodePtr *>> nodes_to_clone;
    nodes_to_clone.emplace_back(this, &result_cloned_node_place);

    while (!nodes_to_clone.empty())
    {
        const auto [node_to_clone, place_for_cloned_node] = nodes_to_clone.back();
        nodes_to_clone.pop_back();

        auto node_clone = node_to_clone->cloneImpl();
        *place_for_cloned_node = node_clone;

        node_clone->setAlias(node_to_clone->alias);
        node_clone->setOriginalAST(node_to_clone->original_ast);
        node_clone->children = node_to_clone->children;
        node_clone->weak_pointers = node_to_clone->weak_pointers;

        old_pointer_to_new_pointer.emplace(node_to_clone, node_clone);

        for (auto & child : node_clone->children)
        {
            if (!child)
                continue;

            nodes_to_clone.emplace_back(child.get(), &child);
        }

        for (auto & weak_pointer : node_clone->weak_pointers)
        {
            weak_pointers_to_update_after_clone.push_back(&weak_pointer);
        }
    }

    /** Update weak pointers to new pointers if they were changed during clone.
      * To do this we check old pointer to new pointer map, if weak pointer
      * strong pointer exists as old pointer in map, reinitialize weak pointer with new pointer.
      */
    for (auto & weak_pointer_ptr : weak_pointers_to_update_after_clone)
    {
        assert(weak_pointer_ptr);
        auto strong_pointer = weak_pointer_ptr->lock();
        auto it = old_pointer_to_new_pointer.find(strong_pointer.get());

        /** If node had weak pointer to some other node and this node is not part of cloned subtree do not update weak pointer.
          * It will continue to point to previous location and it is expected.
          *
          * Example: SELECT id FROM test_table;
          * During analysis `id` is resolved as column node and `test_table` is column source.
          * If we clone `id` column, result column node weak source pointer will point to the same `test_table` column source.
          */
        if (it == old_pointer_to_new_pointer.end())
            continue;

        *weak_pointer_ptr = it->second;
    }

    return result_cloned_node_place;
}

ASTPtr IQueryTreeNode::toAST() const
{
    auto converted_node = toASTImpl();

    if (auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(converted_node.get()))
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

String IQueryTreeNode::dumpTree() const
{
    WriteBufferFromOwnString buffer;
    dumpTree(buffer);

    return buffer.str();
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

}
