#include <Processors/QueryPlan/Optimizations/eelConflictDetector.h>

#include <algorithm>
#include <bit>
#include <numeric>

namespace DB
{

namespace
{

/// Internal EEL operator categories, matching the reference's Ajoin / Alojoin / Alsjoin /
/// Alajoin / Afojoin. ClickHouse encodes semi/anti as a JoinStrictness on top of a JoinKind,
/// so we fold (kind, strictness) into one category here. "Right" variants are the mirror image
/// of the "Left" ones (left/right subtrees swapped), since the reference only defines Left forms.
enum class Category
{
    Join,       /// inner / cross / comma
    LeftOuter,  /// Alojoin
    RightOuter, /// mirror of Alojoin
    FullOuter,  /// Afojoin
    LeftSemi,   /// Alsjoin
    RightSemi,  /// mirror of Alsjoin
    LeftAnti,   /// Alajoin
    RightAnti,  /// mirror of Alajoin
};

Category classify(JoinKind kind, JoinStrictness strictness)
{
    if (strictness == JoinStrictness::Semi)
        return isRight(kind) ? Category::RightSemi : Category::LeftSemi;
    if (strictness == JoinStrictness::Anti)
        return isRight(kind) ? Category::RightAnti : Category::LeftAnti;

    switch (kind)
    {
        case JoinKind::Left:  return Category::LeftOuter;
        case JoinKind::Right: return Category::RightOuter;
        case JoinKind::Full:  return Category::FullOuter;
        default:              return Category::Join; /// Inner / Cross / Comma
    }
}

/// OR together the per-relation sets (`outer`/`anti`) of every relation present in `mask`.
/// Mask equivalent of the reference's `foreach(T, S) result += (*T).outer()`.
UInt32 unionOver(const std::vector<UInt32> & per_relation, UInt32 mask)
{
    UInt32 result = 0;
    while (mask)
    {
        result |= per_relation[std::countr_zero(mask)];
        mask &= mask - 1; /// clear lowest set bit
    }
    return result;
}

}

std::vector<EelOperator>
computeEelOperators(const std::vector<EelJoinOpMask> & ops, size_t num_relations, LoggerPtr log)
{
    /// Per base-relation `outer` and `anti` sets from the EEL paper, indexed by relation id.
    /// Both start as the singleton {relation} (calcEEL's leaf case) and only grow as operators
    /// are processed bottom-up.
    std::vector<UInt32> outer_set(num_relations, 0);
    std::vector<UInt32> anti_set(num_relations, 0);
    for (size_t i = 0; i < num_relations; ++i)
    {
        outer_set[i] = static_cast<UInt32>(1) << i;
        anti_set[i] = static_cast<UInt32>(1) << i;
    }

    /// Process operators bottom-up (post-order). In a join tree a child operator's combined
    /// relation set is a strict subset of its parent's, so ascending popcount(left|right) is a
    /// valid post-order (ties are between disjoint/incomparable subtrees, where order is
    /// irrelevant). This avoids reconstructing the tree pointer structure.
    std::vector<size_t> order(ops.size());
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b)
    {
        return std::popcount(ops[a].left | ops[a].right) < std::popcount(ops[b].left | ops[b].right);
    });

    std::vector<EelOperator> result;
    result.reserve(ops.size());

    for (size_t idx : order)
    {
        const auto & op = ops[idx];
        const UInt32 left = op.left;
        const UInt32 right = op.right;
        const UInt32 combined = left | right;
        const UInt32 nel = op.nel;
        const Category category = classify(op.kind, op.strictness);

        /// "good" (preserving) and "bad" (null-producing / removed) relation sets per operator,
        /// matching calcEEL's good()/bad().
        UInt32 good = 0;
        UInt32 bad = 0;
        switch (category)
        {
            case Category::Join:
            case Category::LeftSemi:
            case Category::RightSemi:
                good = combined;
                bad = 0;
                break;
            case Category::LeftOuter:
            case Category::LeftAnti:
                good = left;
                bad = right;
                break;
            case Category::RightOuter:
            case Category::RightAnti:
                good = right;
                bad = left;
                break;
            case Category::FullOuter:
                good = 0;
                bad = combined;
                break;
        }

        /// Part A: this operator's EEL, starting from its NEL (calcEEL Part A).
        UInt32 eel = nel;
        if (category == Category::LeftOuter || category == Category::RightOuter)
        {
            /// Outer join: widen by the `outer` sets of the null-producing relations the
            /// predicate references (fix_eel = false: aPredBad = NEL & bad).
            eel |= unionOver(outer_set, nel & bad);
        }
        else if (category == Category::LeftAnti || category == Category::RightAnti)
        {
            /// Antijoin: widen by the `anti` sets of the preserving relations the predicate
            /// references, and add the null-producing side (calcEEL's Alajoin: eel += V; eel += bad).
            eel |= unionOver(anti_set, nel & good);
            eel |= bad;
        }
        /// Join / semijoin / full outer join do not widen the EEL in Part A.

        /// Part B: propagate `outer`/`anti` sets upward for ancestor operators (calcEEL Part B).
        if (category == Category::Join || category == Category::LeftAnti || category == Category::RightAnti)
        {
            /// Join and antijoin: all relations linked by this predicate come to share one
            /// `outer` set (reference: Ajoin and Alajoin).
            const UInt32 w = unionOver(outer_set, nel);
            for (UInt32 m = w; m; m &= m - 1)
                outer_set[std::countr_zero(m)] = w;
        }
        else if (category == Category::LeftOuter || category == Category::RightOuter)
        {
            /// Outer join: the `anti` sets of the null-producing side accumulate the `anti` sets
            /// of the preserving relations the predicate references.
            const UInt32 v = unionOver(anti_set, nel & good);
            for (UInt32 m = bad; m; m &= m - 1)
                anti_set[std::countr_zero(m)] |= v;
        }
        /// Semijoin / full outer join: no propagation (calcEEL's Alsjoin / Afojoin cases).

        /// Emit the operator descriptor. The split-TES is the EEL restricted to each input side;
        /// DPsub's einbaubar check tests `tes_left ⊆ S1 && tes_right ⊆ S2`.
        EelOperator desc;
        desc.relations = combined;
        desc.tes_left = eel & left;
        desc.tes_right = eel & right;
        desc.nel = nel;
        desc.kind = op.kind;
        desc.strictness = op.strictness;
        desc.freely_reorderable = (category == Category::Join);
        result.push_back(desc);

        LOG_TEST(log, "EEL op: relations {} tesL {} tesR {} kind {} strictness {} free {}",
                 combined, desc.tes_left, desc.tes_right,
                 static_cast<int>(op.kind), static_cast<int>(op.strictness), desc.freely_reorderable);
    }

    /// Second pass: mark semi/anti operators whose subtree contains no other non-freely-reorderable
    /// operator. Such a semi/anti join is a pure filter sitting above only inner joins, so it may be
    /// pushed down through them (CD-B l-asscom). If any outer/full/nested semi-anti operator lives
    /// in its subtree, pushdown is not generally sound and we keep the strict `within` rule.
    for (auto & desc : result)
    {
        if (desc.strictness != JoinStrictness::Semi && desc.strictness != JoinStrictness::Anti)
            continue;
        bool has_non_reorderable_descendant = false;
        for (const auto & other : result)
        {
            if (&other == &desc || other.freely_reorderable)
                continue;
            /// `other` is strictly inside `desc`'s subtree: its relations are a proper subset.
            const bool proper_subset = (other.relations & ~desc.relations) == 0 && other.relations != desc.relations;
            if (proper_subset)
            {
                has_non_reorderable_descendant = true;
                break;
            }
        }
        desc.pushdown_safe = !has_non_reorderable_descendant;
    }

    return result;
}

}
