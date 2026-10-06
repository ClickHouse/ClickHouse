#include <Analyzer/inlineMaterializedCTEIfNeeded.h>

#include <unordered_map>
#include <unordered_set>

#include <Analyzer/TableNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/traverseQueryTree.h>
#include <Analyzer/Utils.h>

#include <Common/SipHash.h>
#include <Interpreters/Context.h>
#include <Interpreters/MaterializedCTE.h>

namespace DB
{

namespace
{

using ReusedMaterializedCTEs = std::unordered_set<MaterializedCTEPtr>;

struct MergeKey
{
    std::string cte_name;
    IQueryTreeNode::Hash subquery_hash;

    bool operator==(const MergeKey &) const = default;
};

struct MergeKeyHash
{
    size_t operator()(const MergeKey & key) const
    {
        SipHash hash;
        hash.update(key.cte_name);
        hash.update(key.subquery_hash.low64);
        hash.update(key.subquery_hash.high64);
        return hash.get64();
    }
};

using MaterializedCTEUseCount = std::unordered_map<MaterializedCTEPtr, size_t>;

/// A `MATERIALIZED` CTE referenced from multiple `UNION` branches is duplicated: `ApplyWithGlobalVisitor`
/// clones `WITH` definitions into each branch's AST, so the analyzer creates one `TableNode` +
/// one `MaterializedCTE` (fresh random `temporary_table_name`) per branch. This pass merges such
/// duplicates back into a single canonical `MaterializedCTE` (keyed by CTE name + subquery hash),
/// so that the second stage below can correctly detect reuse and materialize the CTE only once.
///
/// Single post-order pass over the whole tree. Returns the per-`MaterializedCTEPtr` use-count map;
/// an empty map means no materialized CTEs are present (driver fast-exits).
MaterializedCTEUseCount mergeDuplicateMaterializedCTEs(const QueryTreeNodePtr & node, const ContextPtr & context)
{
    /// Quirk: `UnionNode::updateTreeHashImpl`/`isEqualImpl` ignore `CompareOptions` entirely and
    /// always hash/compare `is_cte`, `cte_name`, `is_materialized`; `QueryNode::updateTreeHashImpl`
    /// hashes `is_materialized` unconditionally too (see `src/Analyzer/UnionNode.cpp`). Harmless
    /// here: the hash and `isEqual` below stay consistent with each other, and merge candidates are
    /// clones of the same `WITH` definition, so they already share those flags.
    constexpr auto compare_options = IQueryTreeNode::CompareOptions{.compare_aliases = true, .ignore_cte = true};

    std::unordered_map<MergeKey, std::vector<TableNode *>, MergeKeyHash> canonical_ctes;  /// vector defends hash collisions
    MaterializedCTEUseCount use_count;

    traverseQueryTree(node, Everything{}, NoOp{},
    /// Act on LEAVE: nested materialized CTE TableNodes inside the subquery are already
    /// merged, so temporary table names inside the subquery are canonical and its hash
    /// is stable across UNION-branch clones.
    [&](const QueryTreeNodePtr & current_node)
    {
        auto * table_node = current_node->as<TableNode>();
        if (!table_node || !table_node->getMaterializedCTE())
            return;

        /// Non-definition-bearing occurrences (no subquery child) merely inherited their
        /// `MaterializedCTE` from an already-materialized `StorageMemory` looked up by ordinary
        /// table resolution (see `TableNode`'s `extractCTE`) -- e.g. a nested re-resolve of a
        /// shard-local plan for a `Distributed` table, where the CTE's temporary table name was
        /// already substituted in place of the original CTE reference. Such occurrences are not
        /// CTE reference sites: they must not count towards reuse detection or be considered as
        /// merge candidates/canonicals, or an already-registered external table would be
        /// re-registered and throw `TABLE_ALREADY_EXISTS`.
        if (!table_node->isMaterializedCTE())
            return;

        const auto & subquery = table_node->getMaterializedCTESubquery();
        MergeKey key{table_node->getMaterializedCTE()->cte_name, subquery->getTreeHash(compare_options)};

        auto & candidates = canonical_ctes[key];
        TableNode * canonical = nullptr;
        for (auto * candidate : candidates)
        {
            /// A candidate qualifies only if the subqueries are structurally equal AND the
            /// canonical's storage schema matches this node's projection. A schema mismatch
            /// (should be impossible past the isEqual gate) must not throw here - the node
            /// simply stays a separate materialization.
            if (subquery->isEqual(*candidate->getMaterializedCTESubquery(), compare_options)
                && verifyMaterializedCTESubqueryMatchesStorage(
                    subquery,
                    candidate->getMaterializedCTE()->storage,
                    context,
                    candidate->getMaterializedCTE()->cte_name,
                    node /*scope_node, used only in the throwing path*/,
                    false /*throw_on_mismatch*/))
            {
                canonical = candidate;
                break;
            }
        }

        if (!canonical)
            candidates.push_back(table_node);                      /// first encountered = canonical (deterministic)
        else if (canonical->getMaterializedCTE() != table_node->getMaterializedCTE())
            table_node->adoptMaterializedCTE(canonical->getMaterializedCTE(), context);
        /// same pointer already (in-branch repeat) -> merge is a no-op

        /// Count AFTER adoption so in-branch shared-pointer repeats and cross-branch
        /// merged repeats both accumulate on the canonical pointer.
        ++use_count[table_node->getMaterializedCTE()];
    });

    return use_count;
}

class InlineMaterializedCTEsVisitor : public InDepthQueryTreeVisitorWithContext<InlineMaterializedCTEsVisitor>
{
    using Base = InDepthQueryTreeVisitorWithContext<InlineMaterializedCTEsVisitor>;
public:

    explicit InlineMaterializedCTEsVisitor(const ReusedMaterializedCTEs & reused_materialized_cte_, ContextPtr context_)
        : Base(std::move(context_))
        , reused_materialized_cte(reused_materialized_cte_)
    {
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (auto * table_node = node->as<TableNode>())
        {
            if (table_node->isMaterializedCTE())
            {
                const auto & materialized_cte = table_node->getMaterializedCTE();
                if (!reused_materialized_cte.contains(materialized_cte))
                {
                    /// If the materialized CTE is not reused, we can inline it and remove the temporary table from the context.
                    /// If table is not removed from Context, it would cause an exception in distributed queries,
                    /// because the temporary table would already exist in the Context
                    /// and there would be an attempt to read it to send the temporary table to remote servers.
                    getContext()->getQueryContext()->removeExternalTable(table_node->getTemporaryTableName());
                    replacement_map.emplace(table_node, table_node->getMaterializedCTESubquery());
                }
            }
        }
    }

    const IQueryTreeNode::ReplacementMap & getReplacementMap() const
    {
        return replacement_map;
    }

private:
    const ReusedMaterializedCTEs & reused_materialized_cte;
    IQueryTreeNode::ReplacementMap replacement_map;
};

}

void inlineMaterializedCTEIfNeeded(QueryTreeNodePtr & node, ContextPtr context)
{
    auto use_count = mergeDuplicateMaterializedCTEs(node, context);

    /// Fast exit: no materialized CTEs in the query -> nothing to register or inline,
    /// skip the second traversal and cloneAndReplace entirely.
    if (use_count.empty())
        return;

    ReusedMaterializedCTEs reused_materialized_cte;
    for (const auto & [materialized_cte, count] : use_count)
        if (count >= 2)
            reused_materialized_cte.insert(materialized_cte);

    if (context->hasQueryContext())
    {
        /// Register reused Materialized CTEs as external tables.
        /// Skip CTEs whose holder was already extracted: `inlineViewSubqueryIfNeeded` runs a
        /// nested QueryAnalyzer::resolve on the view subtree, which registers the view's
        /// reused CTEs before the outer resolve reaches this point again.
        auto query_context = context->getQueryContext();
        for (const auto & materialized_cte : reused_materialized_cte)
            if (materialized_cte->table_holder.has_value())
                query_context->addExternalTable(materialized_cte->temporary_table_name, materialized_cte->extractTableHolder());
    }
    else
    {
        reused_materialized_cte.clear();
    }

    InlineMaterializedCTEsVisitor visitor(reused_materialized_cte, std::move(context));
    visitor.visit(node);

    const auto & replacement_map = visitor.getReplacementMap();
    if (!replacement_map.empty())
        node = node->cloneAndReplace(replacement_map);
}

}
