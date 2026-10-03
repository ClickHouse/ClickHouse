#include <Analyzer/Passes/RemoveRedundantSemiJoinPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Core/Settings.h>
#include <Functions/IFunction.h>
#include <Interpreters/StorageID.h>

#include <set>
#include <tuple>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_remove_redundant_semi_join;
}

namespace
{

bool isDeterministicTree(const QueryTreeNodePtr & node)
{
    if (!node)
        return true;

    if (const auto * func = node->as<FunctionNode>())
    {
        if (!func->isOrdinaryFunction())
            return false;

        const auto & function_base = func->getFunction();
        if (!function_base || !function_base->isDeterministicInScopeOfQuery())
            return false;
    }

    for (const auto & child : node->getChildren())
    {
        if (!isDeterministicTree(child))
            return false;
    }

    return true;
}

/// Right-side `WHERE` filter as the flat list of its top-level conjuncts. `isSubsetOf` is
/// intentionally coarse — purely structural per-conjunct equality. Future refinement could
/// reason about value domains, e.g. `col >= 18 AND col < 30` ⊆ `col >= 12 AND col < 30`.
struct ExtractedFilter
{
    /// Top-level conjuncts of the `WHERE` clause; empty means "no filter" (matches everything).
    QueryTreeNodes conjuncts;

    bool isSubsetOf(const ExtractedFilter & other) const
    {
        if (other.conjuncts.size() > conjuncts.size())
            return false;

        IQueryTreeNode::CompareOptions opts;
        opts.compare_aliases = false;
        for (const auto & required : other.conjuncts)
        {
            bool found = false;
            for (const auto & mine : conjuncts)
            {
                if (mine->isEqual(*required, opts))
                {
                    found = true;
                    break;
                }
            }
            if (!found)
                return false;
        }
        return true;
    }
};

ExtractedFilter extractFilter(const QueryTreeNodePtr & predicate)
{
    ExtractedFilter result;
    if (!predicate)
        return result;

    if (const auto * func = predicate->as<FunctionNode>(); func && func->getFunctionName() == "and")
        result.conjuncts = func->getArguments().getNodes();
    else
        result.conjuncts.push_back(predicate);

    return result;
}

/// Facts extracted from the right side of a SEMI/ANTI JOIN when the shape is simple enough
/// to participate in redundancy elimination.
struct RightSideInfo
{
    /// The underlying `TableNode`. Non-null.
    TableNodePtr table_expr;
    /// The `WHERE` predicate to analyze, or null if there is none.
    QueryTreeNodePtr where_filter;
};

/// Analyze the right side of a SEMI/ANTI JOIN. Returns `std::nullopt` when the shape is not
/// a direct table nor a plain `SELECT ... FROM table [WHERE ...]` subquery.
///
/// TODO: the subquery whitelist can be relaxed in the future to support more operators (e.g.
/// `GROUP BY`).
std::optional<RightSideInfo> analyzeRightSide(const QueryTreeNodePtr & right_side)
{
    RightSideInfo info;

    if (right_side->as<TableNode>())
    {
        info.table_expr = std::static_pointer_cast<TableNode>(right_side);
        info.where_filter = nullptr;
    }
    else
    {
        const auto * query = right_side->as<QueryNode>();
        if (!query)
            return std::nullopt;

        if (query->isCorrelated()
            || query->hasWith()
            || query->hasPrewhere()
            || query->hasGroupBy()
            || query->hasHaving()
            || query->hasWindow()
            || query->hasQualify()
            || query->hasOrderBy()
            || query->isDistinct()
            || query->hasLimitBy()
            || query->hasLimitByLimit()
            || query->hasLimitByOffset()
            || query->hasLimit()
            || query->hasOffset())
            return std::nullopt;

        const auto & join_tree = query->getJoinTree();
        if (!join_tree->as<TableNode>())
            return std::nullopt;

        info.table_expr = std::static_pointer_cast<TableNode>(join_tree);
        info.where_filter = query->getWhere();
    }

    if (info.table_expr->hasTableExpressionModifiers())
        return std::nullopt;

    return info;
}

/// Resolve a right-side column reference to its underlying physical column name on
/// `base_table_expr`, transparently following a subquery projection if any. Returns
/// `std::nullopt` if it cannot be reduced to a single base-table column (e.g. computed in a
/// projection like `SELECT id + 1 AS uid FROM users`). The physical name is the only safe
/// grouping identity — display names from `ColumnNode::getColumnName` may be subquery aliases.
std::optional<String> resolveRightSideColumn(
    const ColumnNode * col,
    const QueryTreeNodePtr & right_side,
    const QueryTreeNodePtr & base_table_expr)
{
    auto source = col->getColumnSourceOrNull();
    if (!source)
        return std::nullopt;

    /// Column references the base table directly: either the right side is a bare table, or the
    /// column was resolved past a trivial subquery wrapper.
    if (source.get() == base_table_expr.get())
        return col->getColumnName();

    /// Column references the subquery wrapper. Look it up in the projection and follow it to the
    /// underlying physical column on the base table.
    if (source.get() == right_side.get())
    {
        const auto * query_node = right_side->as<QueryNode>();
        if (!query_node)
            return std::nullopt;

        const auto & projection_columns = query_node->getProjectionColumns();
        const auto & projection_nodes = query_node->getProjection().getNodes();

        chassert(projection_columns.size() == projection_nodes.size());

        for (size_t i = 0; i < projection_columns.size(); ++i)
        {
            if (projection_columns[i].name != col->getColumnName())
                continue;

            const auto * proj_col = projection_nodes[i]->as<ColumnNode>();
            if (!proj_col)
                return std::nullopt;

            auto proj_source = proj_col->getColumnSourceOrNull();
            if (!proj_source || proj_source.get() != base_table_expr.get())
                return std::nullopt;

            return proj_col->getColumnName();
        }
    }

    return std::nullopt;
}

/// One `equals` conjunct of an ON clause as `(left expression, right base-table column)`.
/// Right side is canonicalised to a physical column name (subquery aliases looked through);
/// left side stays as AST and is compared structurally via `IQueryTreeNode::isEqual`.
struct JoinKeyPair
{
    QueryTreeNodePtr left_expr;
    String right_phys_col;
};

/// Extract join keys from a join's ON clause, sorted by right-side physical column so two
/// permutations of the same equi-join (e.g. `a=u.id AND b=u.x` vs `b=u.x AND a=u.id`) produce
/// the same vector. Returns `std::nullopt` if any conjunct is not a simple equi-join whose
/// right side resolves to a single base-table column.
std::optional<std::vector<JoinKeyPair>> extractJoinKeys(
    const JoinNode * join_node, const QueryTreeNodePtr & base_table_expr)
{
    if (!join_node->hasJoinExpression())
        return std::nullopt;

    const auto & expr = join_node->getJoinExpression();
    const auto & right_side = join_node->getRightTableExpression();

    std::vector<JoinKeyPair> pairs;
    bool all_resolved = true;

    auto extract_from_equals = [&](const QueryTreeNodePtr & node)
    {
        const auto * func = node->as<FunctionNode>();
        if (!func || func->getFunctionName() != "equals")
        {
            all_resolved = false;
            return;
        }

        const auto & args = func->getArguments().getNodes();
        if (args.size() != 2)
        {
            all_resolved = false;
            return;
        }

        /// User can write `a.x = u.uid` or `u.uid = a.x`; whichever side resolves to a base-table
        /// column is the right key, the other side becomes the left expression.
        const auto * right_col = args[1]->as<ColumnNode>();
        if (right_col)
        {
            if (auto resolved = resolveRightSideColumn(right_col, right_side, base_table_expr))
            {
                pairs.push_back({.left_expr = args[0], .right_phys_col = std::move(*resolved)});
                return;
            }
        }

        const auto * left_col = args[0]->as<ColumnNode>();
        if (left_col)
        {
            if (auto resolved = resolveRightSideColumn(left_col, right_side, base_table_expr))
            {
                pairs.push_back({.left_expr = args[1], .right_phys_col = std::move(*resolved)});
                return;
            }
        }

        all_resolved = false;
    };

    const auto * func = expr->as<FunctionNode>();
    if (func && func->getFunctionName() == "and")
    {
        for (const auto & arg : func->getArguments().getNodes())
            extract_from_equals(arg);
    }
    else
    {
        extract_from_equals(expr);
    }

    if (!all_resolved)
        return std::nullopt;

    std::sort(pairs.begin(), pairs.end(),
        [](const JoinKeyPair & a, const JoinKeyPair & b) { return a.right_phys_col < b.right_phys_col; });
    return pairs;
}

struct SemiJoinCandidate
{
    /// The SEMI/ANTI join node itself; rewritten/removed by `removeJoinFromTree`.
    JoinNode * join_node;
    StorageID base_table = StorageID::createEmpty();
    /// Right-side join-key columns (physical names, sorted); part of the `GroupKey` fingerprint.
    Names right_phys_cols;
    /// Left-side join-key expressions, aligned 1:1 with `right_phys_cols`; checked by `sameLeftKeys`.
    QueryTreeNodes left_key_exprs;
    ExtractedFilter filter;
    bool is_anti;
    /// True if any column from this candidate's right side is referenced elsewhere in the
    /// query; if so the candidate must not be removed (its right-side columns are still live).
    bool right_columns_used;
    /// Number of `RIGHT`/`FULL` outer joins between this candidate and the join-tree root.
    /// Compared between two redundant candidates to detect whether a NULL-padding
    /// `RIGHT`/`FULL` join sits between them, in which case removing one of them is unsafe.
    size_t outer_join_barriers = 0;
};

/// True iff `target_join`'s right-side columns are referenced outside of the join itself,
/// in which case the join cannot be removed. For example:
///   SELECT u.email                         -- u.email used in projection
///   FROM orders o
///   SEMI JOIN users u ON o.uid = u.id
///   SEMI JOIN users u2 ON o.uid = u2.id    -- redundant w.r.t. the first SEMI, but the
///                                          -- first one is kept because `u.email` is live.
bool checkRightColumnsUsed(const QueryNode & parent_query, const JoinNode * target_join)
{
    auto right_table = target_join->getRightTableExpression();
    const auto * right_query = right_table->as<QueryNode>();

    std::function<bool(const IQueryTreeNode *)> has_right_col = [&](const IQueryTreeNode * node) -> bool
    {
        if (!node)
            return false;

        if (node == target_join)
            return false;

        if (const auto * col = node->as<ColumnNode>())
        {
            auto source = col->getColumnSourceOrNull();
            if (source)
            {
                if (source.get() == right_table.get())
                    return true;
                /// Columns may be bound to the subquery's inner table rather than the wrapper.
                if (right_query && source.get() == right_query->getJoinTree().get())
                    return true;
            }
        }

        for (const auto & child : node->getChildren())
        {
            if (has_right_col(child.get()))
                return true;
        }
        return false;
    };

    for (const auto & child : parent_query.getChildren())
    {
        if (has_right_col(child.get()))
            return true;
    }
    return false;
}

/// Walk down the (left-deep) join tree and, for each `LEFT SEMI` / `LEFT ANTI` join whose
/// right side has a recognisable shape (analyzable by `analyzeRightSide` and `extractJoinKeys`),
/// produce a `SemiJoinCandidate` carrying the data needed by the pairing loop:
/// base storage, sorted right-side key columns, paired left-side expressions, the right-side
/// `WHERE` conjuncts, the SEMI vs ANTI flag, whether the right-side columns are still used
/// elsewhere, and the count of `RIGHT`/`FULL` barriers above this join (`outer_join_barriers`).
/// Joins that don't fit the recognisable shape are silently skipped — they simply won't
/// participate in redundancy elimination.
void collectCandidates(
    const QueryNode & parent_query,
    const QueryTreeNodePtr & join_tree,
    std::vector<SemiJoinCandidate> & candidates,
    size_t barriers = 0)
{
    auto * join_node = join_tree->as<JoinNode>();
    if (!join_node)
        return;

    auto kind = join_node->getKind();
    /// `RIGHT`/`FULL` joins null-pad the left side, which breaks SEMI/ANTI subsumption — bump
    /// the barrier count so candidates below this node are tagged accordingly.
    bool is_barrier = (kind == JoinKind::Right || kind == JoinKind::Full);
    size_t barriers_below = barriers + (is_barrier ? 1 : 0);

    collectCandidates(parent_query, join_node->getLeftTableExpression(), candidates, barriers_below);

    auto strictness = join_node->getStrictness();

    bool is_semi = (kind == JoinKind::Left && strictness == JoinStrictness::Semi);
    bool is_anti = (kind == JoinKind::Left && strictness == JoinStrictness::Anti);

    if (!is_semi && !is_anti)
        return;

    auto right = join_node->getRightTableExpression();
    auto info = analyzeRightSide(right);
    if (!info)
        return;

    auto filter = extractFilter(info->where_filter);
    auto pairs = extractJoinKeys(join_node, info->table_expr);

    if (!pairs || pairs->empty())
        return;

    for (const auto & conj : filter.conjuncts)
        if (!isDeterministicTree(conj))
            return;
    for (const auto & p : *pairs)
        if (!isDeterministicTree(p.left_expr))
            return;

    Names right_phys_cols;
    QueryTreeNodes left_key_exprs;
    right_phys_cols.reserve(pairs->size());
    left_key_exprs.reserve(pairs->size());
    for (auto & p : *pairs)
    {
        right_phys_cols.push_back(std::move(p.right_phys_col));
        left_key_exprs.push_back(std::move(p.left_expr));
    }

    bool right_used = checkRightColumnsUsed(parent_query, join_node);

    candidates.push_back(SemiJoinCandidate{
        .join_node = join_node,
        .base_table = info->table_expr->getStorageID(),
        .right_phys_cols = std::move(right_phys_cols),
        .left_key_exprs = std::move(left_key_exprs),
        .filter = std::move(filter),
        .is_anti = is_anti,
        .right_columns_used = right_used,
        .outer_join_barriers = barriers,
    });
}

struct GroupKey
{
    StorageID base_table = StorageID::createEmpty();
    Names right_phys_cols;
    bool is_anti = false;

    bool operator<(const GroupKey & other) const
    {
        return std::tie(base_table.uuid, base_table.database_name, base_table.table_name,
                        right_phys_cols, is_anti)
             < std::tie(other.base_table.uuid, other.base_table.database_name, other.base_table.table_name,
                        other.right_phys_cols, other.is_anti);
    }
};

/// Two candidates in the same group can have the same right-side physical columns yet different
/// left-side expressions (e.g. `o.x = u.uid` vs `o.y = u.uid`). They must agree pairwise on
/// the left side or one cannot subsume the other.
bool sameLeftKeys(const SemiJoinCandidate & a, const SemiJoinCandidate & b)
{
    if (a.left_key_exprs.size() != b.left_key_exprs.size())
        return false;
    for (size_t i = 0; i < a.left_key_exprs.size(); ++i)
    {
        if (!a.left_key_exprs[i]->isEqual(*b.left_key_exprs[i]))
            return false;
    }
    return true;
}

/// Replace a JoinNode in the join tree with its left child, effectively removing the join.
void removeJoinFromTree(QueryTreeNodePtr & join_tree, JoinNode * to_remove)
{
    if (join_tree.get() == to_remove)
    {
        join_tree = to_remove->getLeftTableExpression();
        return;
    }

    auto * parent_join = join_tree->as<JoinNode>();
    if (!parent_join)
        return;

    if (parent_join->getLeftTableExpression().get() == to_remove)
    {
        parent_join->getLeftTableExpression() = to_remove->getLeftTableExpression();
        return;
    }

    if (parent_join->getRightTableExpression().get() == to_remove)
    {
        parent_join->getRightTableExpression() = to_remove->getLeftTableExpression();
        return;
    }

    removeJoinFromTree(parent_join->getLeftTableExpression(), to_remove);
    removeJoinFromTree(parent_join->getRightTableExpression(), to_remove);
}

class RemoveRedundantSemiJoinVisitor : public InDepthQueryTreeVisitorWithContext<RemoveRedundantSemiJoinVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RemoveRedundantSemiJoinVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        auto & join_tree = query_node->getJoinTree();
        if (!join_tree || join_tree->getNodeType() != QueryTreeNodeType::JOIN)
            return;

        std::vector<SemiJoinCandidate> candidates;
        collectCandidates(*query_node, join_tree, candidates);

        if (candidates.size() < 2)
            return;

        /// Bucket by (base_table, right_phys_cols, is_anti) so the pairwise check below only
        /// runs on candidates that could plausibly subsume each other. For example, given
        ///   ... SEMI JOIN users   ON o.uid = u.id
        ///   ... SEMI JOIN orders  ON o.oid = o2.id     -- different base_table, skip
        ///   ... SEMI JOIN users   ON o.uid = u.email   -- different right column, skip
        ///   ... ANTI JOIN users   ON o.uid = u.id      -- different is_anti, skip
        /// only candidates landing in the same bucket are worth comparing further.
        std::map<GroupKey, std::vector<size_t>> groups;
        for (size_t i = 0; i < candidates.size(); ++i)
        {
            GroupKey key{candidates[i].base_table, candidates[i].right_phys_cols, candidates[i].is_anti};
            groups[key].push_back(i);
        }

        std::set<size_t> to_remove;

        for (auto & [key, indices] : groups)
        {
            if (indices.size() < 2)
                continue;

            /// Pairwise subsumption check within each group.
            for (size_t i = 0; i < indices.size(); ++i)
            {
                if (to_remove.contains(indices[i]))
                    continue;

                for (size_t j = i + 1; j < indices.size(); ++j)
                {
                    if (to_remove.contains(indices[j]))
                        continue;

                    auto & a = candidates[indices[i]];
                    auto & b = candidates[indices[j]];

                    /// The bucket key only matches the right side; the left side must agree too.
                    /// Otherwise e.g.
                    ///   ... SEMI JOIN users ON o.x = u.uid     -- filters by o.x
                    ///   ... SEMI JOIN users ON o.y = u.uid     -- filters by o.y
                    /// share a bucket but apply independent filters; dropping either changes the
                    /// result.
                    if (!sameLeftKeys(a, b))
                        continue;

                    /// Compare the right-side `WHERE` filters: one candidate's data must be a
                    /// subset of the other's for a redundancy to exist. For example, given
                    ///   ... SEMI JOIN (SELECT * FROM users WHERE active)              -- A
                    ///   ... SEMI JOIN (SELECT * FROM users WHERE active AND vip)      -- B
                    /// every row passing B's filter also passes A's, so B's dataset ⊆ A's →
                    /// `b_sub_a` is true. If neither direction holds (e.g. `WHERE active` vs
                    /// `WHERE vip`), the candidates filter disjoint rows and neither is redundant.
                    bool a_sub_b = a.filter.isSubsetOf(b.filter);
                    bool b_sub_a = b.filter.isSubsetOf(a.filter);

                    if (!a_sub_b && !b_sub_a)
                        continue;

                    /// Decide which side to remove. `a_sub_b` means a's right-side dataset ⊆
                    /// b's, i.e. a has the stricter filter; SEMI and ANTI then disagree on which
                    /// side is redundant. With
                    ///   A: ... JOIN users WHERE active AND vip   -- stricter (smaller right set)
                    ///   B: ... JOIN users WHERE active           -- looser (larger right set)
                    /// SEMI keeps left rows that match → A is more restrictive, drop the looser B.
                    /// ANTI keeps left rows that DON'T match → A excludes fewer rows, drop A.
                    ///
                    /// `outer_join_barriers` counts `RIGHT`/`FULL` joins between the candidate and
                    /// the join-tree root. Crossing a barrier null-pads the left side; SEMI drops
                    /// those NULL rows and ANTI keeps them, so removing across a barrier can leak
                    /// or drop rows. Allowed directions:
                    ///   SEMI: removed candidate's barriers >= kept candidate's
                    ///   ANTI: removed candidate's barriers <= kept candidate's
                    /// Equal barriers (no NULL-padding between them) → either direction is fine.
                    ///
                    /// `right_columns_used` is checked on the candidate being REMOVED only — if
                    /// downstream code still references its right-side columns, dropping the join
                    /// would lose them.
                    if (key.is_anti)
                    {
                        if (a_sub_b && !a.right_columns_used
                            && a.outer_join_barriers <= b.outer_join_barriers)
                            to_remove.insert(indices[i]);  /// remove A
                        else if (b_sub_a && !b.right_columns_used
                                 && b.outer_join_barriers <= a.outer_join_barriers)
                            to_remove.insert(indices[j]);  /// remove B
                    }
                    else
                    {
                        if (a_sub_b && !b.right_columns_used
                            && b.outer_join_barriers >= a.outer_join_barriers)
                            to_remove.insert(indices[j]);  /// remove B
                        else if (b_sub_a && !a.right_columns_used
                                 && a.outer_join_barriers >= b.outer_join_barriers)
                            to_remove.insert(indices[i]);  /// remove A
                    }
                }
            }
        }

        for (size_t idx : to_remove)
            removeJoinFromTree(join_tree, candidates[idx].join_node);
    }
};

}

void RemoveRedundantSemiJoinPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    if (!context->getSettingsRef()[Setting::optimize_remove_redundant_semi_join])
        return;

    RemoveRedundantSemiJoinVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
