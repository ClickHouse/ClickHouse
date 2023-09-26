#include <Analyzer/IQueryTreeNode.h>

#include <unordered_map>

#include <Common/SipHash.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTWithAlias.h>

#include <boost/functional/hash.hpp>

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

bool IQueryTreeNode::isEqual(const IQueryTreeNode & rhs, CompareOptions compare_options) const
{
    if (this == &rhs)
        return true;

    std::vector<NodePair> nodes_to_process;
    std::unordered_set<NodePair, NodePairHash> equals_pairs;

    nodes_to_process.emplace_back(this, &rhs);

    while (!nodes_to_process.empty())
    {
        auto nodes_to_compare = nodes_to_process.back();
        nodes_to_process.pop_back();

        const auto * lhs_node_to_compare = nodes_to_compare.first;
        const auto * rhs_node_to_compare = nodes_to_compare.second;

        assert(lhs_node_to_compare);
        assert(rhs_node_to_compare);

        if (equals_pairs.contains(std::make_pair(lhs_node_to_compare, rhs_node_to_compare)))
            continue;

        if (lhs_node_to_compare == rhs_node_to_compare)
        {
            equals_pairs.emplace(lhs_node_to_compare, rhs_node_to_compare);
            continue;
        }

        if (lhs_node_to_compare->getNodeType() != rhs_node_to_compare->getNodeType() ||
            !lhs_node_to_compare->isEqualImpl(*rhs_node_to_compare))
            return false;

        if (compare_options.compare_aliases && lhs_node_to_compare->alias != rhs_node_to_compare->alias)
            return false;

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
    /** Compute tree hash with this node as root.
      *
      * Some nodes can contain weak pointers to other nodes. Such weak nodes are not necessary
      * part of tree that we try to hash, but we need to update hash state with their content.
      *
      * Algorithm
      * For each node in tree we update hash state with their content.
      * For weak nodes there is special handling. If we visit weak node first time we update hash state with weak node content and register
      * identifier for this node, for subsequent visits of this weak node we hash weak node identifier instead of content.
      */
    HashState hash_state;

    std::unordered_map<const IQueryTreeNode *, size_t> weak_node_to_identifier;

    std::vector<std::pair<const IQueryTreeNode *, bool>> nodes_to_process;
    nodes_to_process.emplace_back(this, false);

    while (!nodes_to_process.empty())
    {
        const auto [node_to_process, is_weak_node] = nodes_to_process.back();
        nodes_to_process.pop_back();

        if (is_weak_node)
        {
            auto node_identifier_it = weak_node_to_identifier.find(node_to_process);
            if (node_identifier_it != weak_node_to_identifier.end())
            {
                hash_state.update(node_identifier_it->second);
                continue;
            }

            weak_node_to_identifier.emplace(node_to_process, weak_node_to_identifier.size());
        }

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

            nodes_to_process.emplace_back(node_to_process_child.get(), false);
        }

        hash_state.update(node_to_process->weak_pointers.size());

        for (const auto & weak_pointer : node_to_process->weak_pointers)
        {
            auto strong_pointer = weak_pointer.lock();
            if (!strong_pointer)
                continue;

            nodes_to_process.emplace_back(strong_pointer.get(), true);
        }
    }

    return getSipHash128AsPair(hash_state);
}

QueryTreeNodePtr IQueryTreeNode::clone() const
{
    return cloneAndReplace({});
}

QueryTreeNodePtr IQueryTreeNode::cloneAndReplace(const ReplacementMap & replacement_map) const
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

        auto already_cloned_node_it = old_pointer_to_new_pointer.find(node_to_clone);
        if (already_cloned_node_it != old_pointer_to_new_pointer.end())
        {
            *place_for_cloned_node = already_cloned_node_it->second;
            continue;
        }

        auto it = replacement_map.find(node_to_clone);
        auto node_clone = it != replacement_map.end() ? it->second : node_to_clone->cloneImpl();
        *place_for_cloned_node = node_clone;

        old_pointer_to_new_pointer.emplace(node_to_clone, node_clone);

        if (it != replacement_map.end())
            continue;

        node_clone->setAlias(node_to_clone->alias);
        node_clone->children = node_to_clone->children;
        node_clone->weak_pointers = node_to_clone->weak_pointers;

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

QueryTreeNodePtr IQueryTreeNode::cloneAndReplace(const QueryTreeNodePtr & node_to_replace, QueryTreeNodePtr replacement_node) const
{
    ReplacementMap replacement_map;
    replacement_map.emplace(node_to_replace.get(), std::move(replacement_node));

    return cloneAndReplace(replacement_map);
}

ASTPtr IQueryTreeNode::toAST(const ConvertToASTOptions & options) const
{
    auto converted_node = toASTImpl(options);

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
