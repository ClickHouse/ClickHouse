#include <Analyzer/inlineMaterializedCTEIfNeeded.h>

#include <unordered_map>
#include <unordered_set>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
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
        {
            /// An automatically materialized CTE that cannot be materialized at one reference is inlined at all of them.
            if (table_node->getMaterializedCTE()->must_inline)
                canonical->getMaterializedCTE()->must_inline = true;
            table_node->adoptMaterializedCTE(canonical->getMaterializedCTE(), context);
        }
        /// same pointer already (in-branch repeat) -> merge is a no-op

        /// Count AFTER adoption so in-branch shared-pointer repeats and cross-branch
        /// merged repeats both accumulate on the canonical pointer.
        ++use_count[table_node->getMaterializedCTE()];
    });

    return use_count;
}

/// Do not descend into nested queries.
struct ExceptSubqueries
{
    bool operator()(const QueryTreeNodePtr &, const QueryTreeNodePtr & child) const
    {
        auto child_type = child->getNodeType();
        return child_type != QueryTreeNodeType::QUERY && child_type != QueryTreeNodeType::UNION;
    }
};

NamesAndTypes getSubqueryProjectionColumns(const QueryTreeNodePtr & subquery)
{
    if (const auto * query_node = subquery->as<QueryNode>())
        return query_node->getProjectionColumns();
    return subquery->as<UnionNode &>().computeProjectionColumns();
}

/// Whether a filter by the projection column `column_index` of the resolved subquery of a CTE can be pushed down
/// below its aggregation or `DISTINCT`: the column is a key of `GROUP BY` or a column of `SELECT DISTINCT`,
/// and nothing between the aggregation and the result of the subquery depends on the other rows.
bool canPushFilterIntoSubquery(const QueryTreeNodePtr & subquery, size_t column_index)
{
    if (const auto * union_node = subquery->as<UnionNode>())
    {
        for (const auto & query : union_node->getQueries().getNodes())
            if (canPushFilterIntoSubquery(query, column_index))
                return true;
        return false;
    }

    const auto * query_node = subquery->as<QueryNode>();
    if (!query_node)
        return false;

    if (query_node->hasLimit() || query_node->hasOffset() || query_node->hasLimitBy()
        || query_node->hasLimitAfter() || query_node->hasLimitUntil()
        || query_node->isGroupByWithRollup() || query_node->isGroupByWithCube() || query_node->isGroupByWithGroupingSets()
        || query_node->hasWindow())
        return false;

    const auto & projection_nodes = query_node->getProjection().getNodes();
    if (column_index >= projection_nodes.size())
        return false;

    bool has_aggregate_function = false;
    bool has_window_function = false;
    auto find_functions = [&](const QueryTreeNodePtr & node)
    {
        if (const auto * function_node = node->as<FunctionNode>())
        {
            has_aggregate_function |= function_node->isAggregateFunction();
            has_window_function |= function_node->isWindowFunction();
        }
    };

    traverseQueryTree(query_node->getProjectionNode(), ExceptSubqueries{}, find_functions);
    if (query_node->hasHaving())
        traverseQueryTree(query_node->getHaving(), ExceptSubqueries{}, find_functions);

    if (has_window_function)
        return false;

    if (query_node->hasOrderBy())
    {
        for (const auto & sort_node : query_node->getOrderBy().getNodes())
            if (sort_node->as<SortNode &>().withFill())
                return false;
    }

    if (query_node->hasGroupBy())
    {
        for (const auto & key : query_node->getGroupBy().getNodes())
            if (key->isEqual(*projection_nodes[column_index], {.compare_aliases = false}))
                return true;
        return false;
    }

    return query_node->isDistinct() && !has_aggregate_function;
}

/// An automatically materialized CTE (see the setting `enable_automatic_cte_materialization`) must not make the query
/// do more work than inlining it at each reference. It is inlined at all references when:
/// - a reference is filtered by a column that is a key of the aggregation of the CTE: the filter is pushed down
///   into an inlined subquery, below the aggregation, and can use the indexes of the tables, while a materialized CTE
///   is calculated for all keys;
/// - a column of the CTE is not used by any reference: it is not calculated in an inlined subquery,
///   while a materialized CTE calculates all of its columns.
void inlineUnsuitableAutomaticMaterializedCTEs(const QueryTreeNodePtr & node, const MaterializedCTEUseCount & use_count)
{
    auto is_candidate = [](const MaterializedCTEPtr & materialized_cte)
    {
        return materialized_cte->is_automatic && !materialized_cte->must_inline && materialized_cte->isStorageInitialized();
    };

    if (std::ranges::none_of(use_count, [&](const auto & entry) { return is_candidate(entry.first); }))
        return;

    /// The reference to an automatically materialized CTE that is the source of the column, if any.
    auto get_referenced_cte = [](const ColumnNode & column_node) -> const TableNode *
    {
        auto column_source = column_node.getColumnSourceOrNull();
        const auto * table_node = column_source ? column_source->as<TableNode>() : nullptr;
        if (table_node && table_node->isMaterializedCTE() && table_node->getMaterializedCTE()->is_automatic)
            return table_node;
        return nullptr;
    };

    auto check_filter = [&](const QueryTreeNodePtr & filter)
    {
        traverseQueryTree(filter, ExceptSubqueries{}, [&](const QueryTreeNodePtr & filter_node)
        {
            const auto * column_node = filter_node->as<ColumnNode>();
            const auto * table_node = column_node ? get_referenced_cte(*column_node) : nullptr;
            if (!table_node || table_node->getMaterializedCTE()->must_inline)
                return;

            const auto & subquery = table_node->getMaterializedCTESubquery();
            const auto projection_columns = getSubqueryProjectionColumns(subquery);

            for (size_t column_index = 0; column_index < projection_columns.size(); ++column_index)
            {
                if (projection_columns[column_index].name == column_node->getColumnName())
                {
                    if (canPushFilterIntoSubquery(subquery, column_index))
                        table_node->getMaterializedCTE()->must_inline = true;
                    break;
                }
            }
        });
    };

    std::unordered_map<const MaterializedCTE *, std::unordered_set<String>> used_columns;
    std::unordered_map<const MaterializedCTE *, size_t> columns_count;

    traverseQueryTree(node, Everything{}, [&](const QueryTreeNodePtr & current_node)
    {
        if (const auto * column_node = current_node->as<ColumnNode>())
        {
            if (const auto * table_node = get_referenced_cte(*column_node))
                used_columns[table_node->getMaterializedCTE().get()].insert(column_node->getColumnName());
        }
        else if (const auto * table_node = current_node->as<TableNode>())
        {
            if (table_node->isMaterializedCTE() && table_node->getMaterializedCTE()->is_automatic)
                columns_count[table_node->getMaterializedCTE().get()] = getSubqueryProjectionColumns(table_node->getMaterializedCTESubquery()).size();
        }
        else if (const auto * query_node = current_node->as<QueryNode>())
        {
            if (query_node->hasPrewhere())
                check_filter(query_node->getPrewhere());
            if (query_node->hasWhere())
                check_filter(query_node->getWhere());
        }
    });

    for (const auto & [materialized_cte, _] : use_count)
    {
        if (!is_candidate(materialized_cte))
            continue;

        if (used_columns[materialized_cte.get()].size() < columns_count[materialized_cte.get()])
            materialized_cte->must_inline = true;
    }
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

    /// Post-order: the references nested in the subquery of a reference are visited before it,
    /// so they can be replaced in that subquery (`cloneAndReplace` does not apply the map within a replacement).
    void leaveImpl(QueryTreeNodePtr & node)
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
                    /// E.g. `WITH a AS (...), b AS (SELECT ... FROM a) SELECT ... FROM b`: `a` is referenced only once,
                    /// from the subquery of `b`, so it must be inlined into the subquery that replaces `b`.
                    auto subquery = table_node->getMaterializedCTESubquery();
                    if (!replacement_map.empty())
                        subquery = subquery->cloneAndReplace(replacement_map);
                    replacement_map.emplace(table_node, static_pointer_cast<ITableExpressionNode>(subquery));
                }
            }
        }
    }

    [[nodiscard]] const IQueryTreeNode::ReplacementMap & getReplacementMap() const
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

    inlineUnsuitableAutomaticMaterializedCTEs(node, use_count);

    /// A CTE with a single static reference site is inlined, unless that site is inside a recursive member
    /// of a recursive CTE: `RecursiveCTESource` re-executes the recursive members once per recursion step,
    /// so the snapshot must be kept to be evaluated once (see `QueryAnalyzer::resolveUnion`).
    /// An automatically materialized CTE that turned out to be unsuitable for materialization is inlined at every reference.
    ReusedMaterializedCTEs reused_materialized_cte;
    for (const auto & [materialized_cte, count] : use_count)
        if ((count >= 2 || materialized_cte->is_referenced_from_recursive_cte_member) && !materialized_cte->must_inline)
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
