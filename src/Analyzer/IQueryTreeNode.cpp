#include <Analyzer/ColumnNode.h>
#include <Analyzer/IColumnSourceNode.h>
#include <Analyzer/IQueryTreeNode.h>

#include <unordered_map>

#include <Common/SipHash.h>
#include <Common/assert_cast.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTWithAlias.h>
#include <Parsers/IAST.h>

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
        case QueryTreeNodeType::CROSS_JOIN: return "CROSS_JOIN";
        case QueryTreeNodeType::JOIN: return "JOIN";
        case QueryTreeNodeType::UNION: return "UNION";
    }
}

IQueryTreeNode::IQueryTreeNode(size_t children_size)
{
    children.resize(children_size);
}

namespace
{

using NodePair = std::pair<const IQueryTreeNode *, const IQueryTreeNode *>;

/** Collect all nodes reachable from root through children only (not through column source
  * references). A column source reachable this way is "internal" to the subtree: it lives inside
  * the compared/hashed expression (a lambda, an interpolate, or a subquery used as an expression).
  *
  * Internal sources are compared by content even in local mode, because two independently built
  * copies of the same expression contain distinct but structurally equal instances of them
  * (alpha-equivalence) — e.g. a GROUP BY key written as a full expression and the equal projection
  * expression each resolve their own lambda / subquery. Sources that are NOT reachable through
  * children are external references, typically the table expressions of the enclosing query's join
  * tree; those keep id comparison so that the two sides of a self join stay distinguished.
  */
void collectNodesReachableViaChildren(const IQueryTreeNode * root, std::unordered_set<const IQueryTreeNode *> & result)
{
    std::vector<const IQueryTreeNode *> nodes_to_process;
    nodes_to_process.push_back(root);

    while (!nodes_to_process.empty())
    {
        const auto * node = nodes_to_process.back();
        nodes_to_process.pop_back();

        if (!result.insert(node).second)
            continue;

        for (const auto & child : node->getChildren())
        {
            if (child)
                nodes_to_process.push_back(child.get());
        }
    }
}

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

/** Compare two trees.
  *
  * If compare_column_sources_by_content is false, column references are compared by their column
  * source ids. Otherwise column sources are compared by content via a bijective correspondence
  * (lhs_source_to_rhs plus the set of already-taken rhs sources): a source is compared once on first
  * sight and every later reference must map to the same counterpart, mirroring the per-source
  * visitation identifiers used by computeTreeHash so that equal trees always hash equal.
  */
bool IQueryTreeNode::areTreesEqual(
    const IQueryTreeNode & lhs,
    const IQueryTreeNode & rhs,
    IQueryTreeNode::CompareOptions compare_options,
    bool compare_column_sources_by_content)
{
    if (&lhs == &rhs)
        return true;

    /// In local mode, a column source that is internal to the compared subtree (reachable through
    /// children) is compared by content; an external source (e.g. the enclosing query's join tree)
    /// is compared by id. In global mode all sources are compared by content, so the sets are not
    /// needed.
    std::unordered_set<const IQueryTreeNode *> lhs_internal_sources;
    std::unordered_set<const IQueryTreeNode *> rhs_internal_sources;
    if (!compare_column_sources_by_content)
    {
        collectNodesReachableViaChildren(&lhs, lhs_internal_sources);
        collectNodesReachableViaChildren(&rhs, rhs_internal_sources);
    }

    std::vector<NodePair> nodes_to_process;
    std::unordered_set<NodePair, NodePairHash> equals_pairs;

    /// Bijective correspondence between content-compared column sources, matching the per-source
    /// visitation identifiers used by computeTreeHash. Without it a repeated source on one side
    /// could compare equal to several distinct sources on the other side (e.g.
    /// `List(col(s), col(s))` vs `List(col(s1), col(s2))`), which would disagree with the hash and
    /// break hash-keyed containers. The lhs side maps to its rhs counterpart (to verify a repeated
    /// lhs source keeps mapping to the same rhs source); the rhs side only needs to know which rhs
    /// sources are already taken, so a set is enough.
    std::unordered_map<const IQueryTreeNode *, const IQueryTreeNode *> lhs_source_to_rhs;
    std::unordered_set<const IQueryTreeNode *> rhs_corresponded_sources;

    nodes_to_process.emplace_back(&lhs, &rhs);

    while (!nodes_to_process.empty())
    {
        auto nodes_to_compare = nodes_to_process.back();
        nodes_to_process.pop_back();

        const auto * lhs_node_to_compare = nodes_to_compare.first;
        const auto * rhs_node_to_compare = nodes_to_compare.second;

        chassert(lhs_node_to_compare);
        chassert(rhs_node_to_compare);

        if (equals_pairs.contains(std::make_pair(lhs_node_to_compare, rhs_node_to_compare)))
            continue;

        if (lhs_node_to_compare == rhs_node_to_compare)
        {
            equals_pairs.emplace(lhs_node_to_compare, rhs_node_to_compare);
            continue;
        }

        if (lhs_node_to_compare->getNodeType() != rhs_node_to_compare->getNodeType() ||
            !lhs_node_to_compare->isEqualImpl(*rhs_node_to_compare, compare_options))
            return false;

        if (compare_options.compare_aliases && lhs_node_to_compare->getAlias() != rhs_node_to_compare->getAlias())
            return false;

        if (lhs_node_to_compare->getNodeType() == QueryTreeNodeType::COLUMN)
        {
            const auto & lhs_column_node = assert_cast<const ColumnNode &>(*lhs_node_to_compare);
            const auto & rhs_column_node = assert_cast<const ColumnNode &>(*rhs_node_to_compare);

            auto lhs_source = lhs_column_node.getColumnSourceOrNull();
            auto rhs_source = rhs_column_node.getColumnSourceOrNull();

            /// Sources internal to the compared subtree are compared by content (alpha-equivalence
            /// of independently built copies); external sources keep id comparison (self-join sides
            /// stay distinguished). In global mode everything is compared by content.
            bool lhs_internal = lhs_source && lhs_internal_sources.contains(lhs_source.get());
            bool rhs_internal = rhs_source && rhs_internal_sources.contains(rhs_source.get());

            if (lhs_internal != rhs_internal)
                return false;

            if (compare_column_sources_by_content || lhs_internal)
            {
                if (static_cast<bool>(lhs_source) != static_cast<bool>(rhs_source))
                    return false;

                if (lhs_source)
                {
                    /// Enforce a bijection: a given lhs source must always correspond to the same
                    /// rhs source and vice versa. Compare the source content only on first sight.
                    auto [lhs_it, lhs_inserted] = lhs_source_to_rhs.try_emplace(lhs_source.get(), rhs_source.get());
                    if (!lhs_inserted)
                    {
                        if (lhs_it->second != rhs_source.get())
                            return false;
                    }
                    else
                    {
                        /// lhs source is seen for the first time; its rhs counterpart must be free.
                        if (!rhs_corresponded_sources.insert(rhs_source.get()).second)
                            return false;
                        nodes_to_process.emplace_back(lhs_source.get(), rhs_source.get());
                    }
                }
            }
            else
            {
                if (lhs_column_node.getColumnSourceId() != rhs_column_node.getColumnSourceId())
                    return false;
            }
        }

        const auto & lhs_children = lhs_node_to_compare->getChildren();
        const auto & rhs_children = rhs_node_to_compare->getChildren();

        size_t lhs_children_size = lhs_children.size();
        if (lhs_children_size != rhs_children.size())
            return false;

        for (size_t i = 0; i < lhs_children_size; ++i)
        {
            const auto & lhs_child = lhs_children[i];
            const auto & rhs_child = rhs_children[i];

            if (!lhs_child && !rhs_child)
                continue;
            if (lhs_child && !rhs_child)
                return false;
            if (!lhs_child && rhs_child)
                return false;

            nodes_to_process.emplace_back(lhs_child.get(), rhs_child.get());
        }

        equals_pairs.emplace(lhs_node_to_compare, rhs_node_to_compare);
    }

    return true;
}

/** Compute tree hash with the given node as root.
  *
  * If hash_column_sources_by_content is false, column references are hashed by their column source
  * ids. Otherwise column sources are hashed by content: when a column source is visited the first
  * time, the hash state is updated with its content and an identifier is registered for it; for
  * subsequent visits the identifier is hashed instead of the content. This makes the result
  * canonical across query trees.
  */
IQueryTreeNode::Hash IQueryTreeNode::computeTreeHash(
    const IQueryTreeNode & root,
    IQueryTreeNode::CompareOptions compare_options,
    bool hash_column_sources_by_content)
{
    IQueryTreeNode::HashState hash_state;

    std::unordered_map<const IQueryTreeNode *, size_t> column_source_to_identifier;

    /// In local mode, a column source internal to the hashed subtree (reachable through children) is
    /// hashed by content, an external source by id. In global mode all sources are hashed by content.
    std::unordered_set<const IQueryTreeNode *> internal_sources;
    if (!hash_column_sources_by_content)
        collectNodesReachableViaChildren(&root, internal_sources);

    std::vector<std::pair<const IQueryTreeNode *, bool>> nodes_to_process;
    nodes_to_process.emplace_back(&root, false);

    while (!nodes_to_process.empty())
    {
        const auto [node_to_process, is_column_source_node] = nodes_to_process.back();
        nodes_to_process.pop_back();

        if (is_column_source_node)
        {
            auto node_identifier_it = column_source_to_identifier.find(node_to_process);
            if (node_identifier_it != column_source_to_identifier.end())
            {
                hash_state.update(node_identifier_it->second);
                continue;
            }

            column_source_to_identifier.emplace(node_to_process, column_source_to_identifier.size());
        }

        hash_state.update(static_cast<size_t>(node_to_process->getNodeType()));
        const auto & alias = node_to_process->getAlias();
        if (compare_options.compare_aliases && !alias.empty())
        {
            hash_state.update(alias.size());
            hash_state.update(alias);
        }

        node_to_process->updateTreeHashImpl(hash_state, compare_options);

        if (node_to_process->getNodeType() == QueryTreeNodeType::COLUMN)
        {
            const auto & column_node = assert_cast<const ColumnNode &>(*node_to_process);
            auto column_source = column_node.getColumnSourceOrNull();

            /// Mirror areTreesEqual: sources internal to the hashed subtree are hashed by content,
            /// external sources by id; in global mode everything is hashed by content.
            bool internal = column_source && internal_sources.contains(column_source.get());

            if (hash_column_sources_by_content || internal)
            {
                hash_state.update(column_source != nullptr);

                if (column_source)
                    nodes_to_process.emplace_back(column_source.get(), true);
            }
            else
            {
                hash_state.update(column_node.getColumnSourceId());
            }
        }

        hash_state.update(node_to_process->getChildren().size());

        for (const auto & node_to_process_child : node_to_process->getChildren())
        {
            if (!node_to_process_child)
                continue;

            nodes_to_process.emplace_back(node_to_process_child.get(), false);
        }
    }

    return getSipHash128AsPair(hash_state);
}

bool IQueryTreeNode::isEqualLocal(const IQueryTreeNode & rhs, CompareOptions compare_options) const
{
    return areTreesEqual(*this, rhs, compare_options, false /*compare_column_sources_by_content*/);
}

bool IQueryTreeNode::isEqualGlobal(const IQueryTreeNode & rhs, CompareOptions compare_options) const
{
    return areTreesEqual(*this, rhs, compare_options, true /*compare_column_sources_by_content*/);
}

IQueryTreeNode::Hash IQueryTreeNode::getTreeHashLocal(CompareOptions compare_options) const
{
    return computeTreeHash(*this, compare_options, false /*hash_column_sources_by_content*/);
}

IQueryTreeNode::Hash IQueryTreeNode::getTreeHashGlobal(CompareOptions compare_options) const
{
    return computeTreeHash(*this, compare_options, true /*hash_column_sources_by_content*/);
}

QueryTreeNodePtr IQueryTreeNode::clone() const
{
    return cloneAndReplaceImpl({}, false /*fresh_column_source_ids*/);
}

QueryTreeNodePtr IQueryTreeNode::cloneWithFreshColumnSourceIds() const
{
    return cloneAndReplaceImpl({}, true /*fresh_column_source_ids*/);
}

QueryTreeNodePtr IQueryTreeNode::cloneAndReplace(const ReplacementMap & replacement_map) const
{
    return cloneAndReplaceImpl(replacement_map, false /*fresh_column_source_ids*/);
}

QueryTreeNodePtr IQueryTreeNode::cloneAndReplaceImpl(const ReplacementMap & replacement_map, bool fresh_column_source_ids) const
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
    std::vector<ColumnNode *> cloned_column_nodes;

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

        node_clone->original_ast = node_to_clone->original_ast;
        node_clone->setAlias(node_to_clone->alias);
        node_clone->parenthesized = node_to_clone->parenthesized;
        node_clone->children = node_to_clone->children;

        /** cloneImpl mints a fresh column source id at construction. By default the clone is the
          * same logical source, so preserve the original id; only when fresh ids were requested do
          * we keep the freshly minted one.
          */
        if (!fresh_column_source_ids && node_clone->isColumnSource())
            static_cast<IColumnSourceNode &>(*node_clone).column_source_id
                = static_cast<const IColumnSourceNode &>(*node_to_clone).column_source_id;

        for (auto & child : node_clone->children)
        {
            if (!child)
                continue;

            nodes_to_clone.emplace_back(child.get(), &child);
        }

        if (auto * column_node_clone = typeid_cast<ColumnNode *>(node_clone.get()))
            cloned_column_nodes.push_back(column_node_clone);
    }

    /** Ensure all replacement_map entries are in old_pointer_to_new_pointer.
      * When a node is replaced, its children are not traversed and thus not added
      * to old_pointer_to_new_pointer. If those children are also in the replacement_map
      * (e.g., inner column sources of an ARRAY_JOIN being replaced), their entries
      * must be available for weak pointer updates below.
      */
    for (const auto & [old_ptr, new_ptr] : replacement_map)
        old_pointer_to_new_pointer.emplace(old_ptr, new_ptr);

    /** Re-point cloned column nodes to the clones of their column sources.
      * If a column source is not part of the cloned subtree or the replacement map, the column
      * keeps referencing the previous source and it is expected.
      *
      * Example: SELECT id FROM test_table;
      * During analysis `id` is resolved as column node and `test_table` is column source.
      * If we clone `id` column, result column node weak source pointer will point to the same `test_table` column source.
      */
    for (auto * column_node : cloned_column_nodes)
        column_node->remapColumnSourceAfterClone(old_pointer_to_new_pointer);

    result_cloned_node_place->original_ast = original_ast;

    return result_cloned_node_place;
}

QueryTreeNodePtr IQueryTreeNode::cloneAndReplace(const QueryTreeNodePtr & node_to_replace, ColumnSourceNodePtr replacement_node) const
{
    ReplacementMap replacement_map;
    replacement_map.emplace(node_to_replace.get(), std::move(replacement_node));

    return cloneAndReplace(replacement_map);
}

ASTPtr IQueryTreeNode::toAST(const ConvertToASTOptions & options) const
{
    auto converted_node = toASTImpl(options);

    if (auto * /*ast_with_alias*/ _ = dynamic_cast<ASTWithAlias *>(converted_node.get()))
        converted_node->setAlias(alias);

    converted_node->setParenthesized(parenthesized);

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
