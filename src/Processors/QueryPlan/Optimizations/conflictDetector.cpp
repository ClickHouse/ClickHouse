#include <Processors/QueryPlan/Optimizations/conflictDetector.h>

namespace DB
{

namespace
{

enum class Category : size_t
{
    Join = 0,  /// inner / cross / comma
    LeftOuter, /// left/right outer join
    FullOuter, /// full outer join
    Semi,      /// left/right semi join
    Anti,      /// left/right anti join
    Count
};

constexpr size_t NUM_CATEGORIES = static_cast<size_t>(Category::Count);
constexpr size_t idx(Category c) { return static_cast<size_t>(c); }

/// One operator in left-canonical form: `left` is the preserved side for outer/semi/anti.
struct NormOp
{
    Category category = Category::Join;
    UInt32 left = 0;
    UInt32 right = 0;
    /// Relations the ON predicate rejects nulls on (swap-invariant: it is a set of relation ids,
    /// unaffected by the left/right normalisation). Used to resolve the footnoted matrix entries.
    UInt32 nr_rels = 0;
    JoinKind kind = JoinKind::Inner;
    JoinStrictness strictness = JoinStrictness::All;
};

NormOp normalize(const ConflictOpMask & op)
{
    const UInt32 nr = op.nr_rels;
    if (op.strictness == JoinStrictness::Semi)
    {
        if (isRight(op.kind))
            return {Category::Semi, op.right, op.left, nr, JoinKind::Left, JoinStrictness::Semi};
        return {Category::Semi, op.left, op.right, nr, JoinKind::Left, JoinStrictness::Semi};
    }
    if (op.strictness == JoinStrictness::Anti)
    {
        if (isRight(op.kind))
            return {Category::Anti, op.right, op.left, nr, JoinKind::Left, JoinStrictness::Anti};
        return {Category::Anti, op.left, op.right, nr, JoinKind::Left, JoinStrictness::Anti};
    }
    switch (op.kind)
    {
        case JoinKind::Left:
            return {Category::LeftOuter, op.left, op.right, nr, JoinKind::Left, JoinStrictness::All};
        case JoinKind::Right:
            return {Category::LeftOuter, op.right, op.left, nr, JoinKind::Left, JoinStrictness::All};
        case JoinKind::Full:
            return {Category::FullOuter, op.left, op.right, nr, JoinKind::Full, JoinStrictness::All};
        default: /// Inner / Cross / Comma
            return {Category::Join, op.left, op.right, nr, op.kind, JoinStrictness::All};
    }
}

/// The property matrices of the paper (Tables 1-3), transcribed directly. Rows and columns are
/// indexed by `Category` in the order {Join, LeftOuter, FullOuter, Semi, Anti}.
using PropRow = UInt8[NUM_CATEGORIES];

/// Table 1: comm(op). Commutative operators (inner/cross and full outer).
constexpr UInt8 COMM[NUM_CATEGORIES] =
{
    /* Join */ 1, /* LeftOuter */ 0, /* FullOuter */ 1, /* Semi */ 0, /* Anti */ 0,
};

/// Table 2: assoc(op_a, op_b). Not symmetric -- op_a selects the row, op_b the column.
constexpr PropRow ASSOC[NUM_CATEGORIES] =
{
    /*                  Join LeftOuter FullOuter Semi Anti */
    /* Join      */ {    1,      1,        0,      1,   1 },
    /* LeftOuter */ {    0,      0,        0,      0,   0 },
    /* FullOuter */ {    0,      0,        0,      0,   0 },
    /* Semi      */ {    0,      0,        0,      0,   0 },
    /* Anti      */ {    0,      0,        0,      0,   0 },
};

/// Table 3, left component: l-asscom(op_a, op_b). Symmetric.
constexpr PropRow L_ASSCOM[NUM_CATEGORIES] =
{
    /*                  Join LeftOuter FullOuter Semi Anti */
    /* Join      */ {    1,      1,        0,      1,   1 },
    /* LeftOuter */ {    1,      1,        0,      1,   1 },
    /* FullOuter */ {    0,      0,        0,      0,   0 },
    /* Semi      */ {    1,      1,        0,      1,   1 },
    /* Anti      */ {    1,      1,        0,      1,   1 },
};

/// Table 3, right component: r-asscom(op_a, op_b). Symmetric.
constexpr PropRow R_ASSCOM[NUM_CATEGORIES] =
{
    /*                  Join LeftOuter FullOuter Semi Anti */
    /* Join      */ {    1,      0,        0,      0,   0 },
    /* LeftOuter */ {    0,      0,        0,      0,   0 },
    /* FullOuter */ {    0,      0,        0,      0,   0 },
    /* Semi      */ {    0,      0,        0,      0,   0 },
    /* Anti      */ {    0,      0,        0,      0,   0 },
};

bool comm(Category a) { return COMM[idx(a)] != 0; }
bool assoc(Category a, Category b) { return ASSOC[idx(a)][idx(b)] != 0; }
bool lAsscom(Category a, Category b) { return L_ASSCOM[idx(a)][idx(b)] != 0; }
bool rAsscom(Category a, Category b) { return R_ASSCOM[idx(a)][idx(b)] != 0; }

bool isFreelyReorderable(Category c)
{
    return comm(c) && assoc(c, c) && lAsscom(c, c) && rAsscom(c, c);
}

/// Aware variants of assoc / l-asscom / r-asscom. When the base matrix entry is a
/// conflict, the null-rejection-dependent (footnoted) entries of Tables 2-3 may still upgrade it to
/// "holds". Each footnote asks whether one or both operators' predicates reject nulls on a specific
/// operand of the transformation (`nr(op) & operand != 0`). The caller passes the exact operand
/// mask because it depends on the nesting geometry (which operand is shared), which differs between
/// the left-subtree and right-subtree traversals -- see the call sites for the mapping.

/// assoc(row, col) with `shared` = A(E2), the operand shared by the two operators in Eqv. 1.
/// Footnoted cells (Table 2): (LeftOuter|FullOuter, LeftOuter) needs col's predicate null-rejecting
/// on `shared`. (FullOuter, FullOuter) needs both operators' predicates null-rejecting on `shared`.
bool assocHolds(const NormOp & row, const NormOp & col, UInt32 shared)
{
    if (assoc(row.category, col.category))
        return true;
    const bool col_nr = (col.nr_rels & shared) != 0;
    const bool row_nr = (row.nr_rels & shared) != 0;
    if ((row.category == Category::LeftOuter || row.category == Category::FullOuter) && col.category == Category::LeftOuter)
        return col_nr;
    if (row.category == Category::FullOuter && col.category == Category::FullOuter)
        return row_nr && col_nr;
    return false;
}

/// l-asscom(row, col) for Eqv. 2, where `e1` is the shared operand (left of `row`) and `e3` is the
/// operand `col` brings in. Footnoted cells (Table 3, left component): (LeftOuter, FullOuter) needs
/// row's predicate null-rejecting on `e1`; (FullOuter, LeftOuter) needs col's predicate on `e3`;
/// (FullOuter, FullOuter) needs both predicates on `e1`.
bool lAsscomHolds(const NormOp & row, const NormOp & col, UInt32 e1, UInt32 e3)
{
    if (lAsscom(row.category, col.category))
        return true;
    if (row.category == Category::LeftOuter && col.category == Category::FullOuter)
        return (row.nr_rels & e1) != 0;
    if (row.category == Category::FullOuter && col.category == Category::LeftOuter)
        return (col.nr_rels & e3) != 0;
    if (row.category == Category::FullOuter && col.category == Category::FullOuter)
        return (row.nr_rels & e1) != 0 && (col.nr_rels & e1) != 0;
    return false;
}

/// r-asscom(row, col) for Eqv. 3, where `e3` is the shared operand (right of `col`). The only
/// footnoted cell (Table 3, right component) is (FullOuter, FullOuter): both predicates must reject
/// nulls on `e3`.
bool rAsscomHolds(const NormOp & row, const NormOp & col, UInt32 e3)
{
    if (rAsscom(row.category, col.category))
        return true;
    if (row.category == Category::FullOuter && col.category == Category::FullOuter)
        return (row.nr_rels & e3) != 0 && (col.nr_rels & e3) != 0;
    return false;
}

}

std::vector<ConflictOperator>
computeConflictOperators(const std::vector<ConflictOpMask> & ops, ConflictDetector detector, LoggerPtr log)
{
    const size_t n = ops.size();
    const bool cdc = detector == ConflictDetector::CDC;

    std::vector<NormOp> norm;
    norm.reserve(n);
    for (const auto & op : ops)
        norm.push_back(normalize(op));

    std::vector<ConflictOperator> result;
    result.reserve(n);

    /// For each operator b we walk every operator a in b's subtrees and consult the reorderability
    /// matrices. CD-A and CD-C differ only in how a detected conflict is recorded:
    ///   - CD-A (Section 5.2): widen b's TES by the offending relations (an unconditioned
    ///     containment) -- coarse but simple.
    ///   - CD-C (Section 5.4): keep b's required set at SES and instead emit a conflict rule
    ///     T1 -> T2 (a conditioned containment), narrowed to the predicate-referenced tables. This
    ///     keeps valid reorderings CD-A's widening would forbid, making CD-C complete.
    /// Both read only static subtree relation sets and the property matrices, so operators may be
    /// processed in any order (no bottom-up traversal is needed).
    for (size_t j = 0; j < n; ++j)
    {
        const NormOp & b = norm[j];
        const UInt32 b_left = b.left;
        const UInt32 b_right = b.right;
        const UInt32 b_rel = b_left | b_right;
        const UInt32 nel = ops[j].nel;

        /// CalcSES for a non-degenerate predicate with no dependent operators: the ON-clause
        /// relations, restricted to this operator's own relations. CD-C keeps `required` == SES.
        const UInt32 ses = nel & b_rel;
        UInt32 tes = ses;                  /// CD-A widens this
        std::vector<ConflictRule> rules;   /// CD-C fills this

        for (size_t i = 0; i < n; ++i)
        {
            if (i == j)
                continue;
            const NormOp & a = norm[i];
            const UInt32 a_rel = a.left | a.right;
            const UInt32 fta = ops[i].nel;  /// FT(a): tables referenced by a's predicate

            /// a is in STO(left(b)) iff all of a's relations lie in b's left subtree, and in
            /// STO(right(b)) iff they lie in b's right subtree. In a tree the two subtrees are
            /// disjoint, so at most one holds; ancestors and disjoint operators satisfy neither.
            if ((a_rel & ~b_left) == 0)
            {
                /// Left-nesting (Eqv. 1/2 with a below-left of b). For assoc the shared operand is
                /// A(E2) = right(a); for l-asscom the shared operand is e1 = left(a) and the operand
                /// b brings in is e3 = right(b).
                if (!assocHolds(a, b, a.right))
                {
                    if (cdc) rules.push_back({a.right, (a.left & fta) ? (a.left & fta) : a.left});
                    else     tes |= a.left;
                }
                if (!lAsscomHolds(a, b, a.left, b.right))
                {
                    if (cdc) rules.push_back({a.left, (a.right & fta) ? (a.right & fta) : a.right});
                    else     tes |= a.right;
                }
            }
            else if ((a_rel & ~b_right) == 0)
            {
                /// Right-nesting (Eqv. 1/3 with a below-right of b). For assoc(b, a) the shared
                /// operand is A(E2) = left(a); for r-asscom(b, a) the shared operand is e3 = right(a).
                if (!assocHolds(b, a, a.left))
                {
                    if (cdc) rules.push_back({a.left, (a.right & fta) ? (a.right & fta) : a.right});
                    else     tes |= a.right;
                }
                if (!rAsscomHolds(b, a, a.right))
                {
                    if (cdc) rules.push_back({a.right, (a.left & fta) ? (a.left & fta) : a.left});
                    else     tes |= a.left;
                }
            }
        }

        /// The required set is the split of TES (CD-A) or SES (CD-C) over the operator's
        /// (left-canonical) input sides, which is what the validity test compares against S1 and S2.
        const UInt32 required = cdc ? ses : tes;
        ConflictOperator desc;
        desc.relations = b_rel;
        desc.left_relations = b_left;
        desc.required_left = required & b_left;
        desc.required_right = required & b_right;
        desc.nel = nel;
        desc.kind = b.kind;
        desc.strictness = b.strictness;
        desc.freely_reorderable = isFreelyReorderable(b.category);
        desc.rules = std::move(rules);

        LOG_TEST(log, "{} op: relations {} nel {} nrRels {} reqL {} reqR {} rules {} kind {} strictness {} free {}",
                 cdc ? "CD-C" : "CD-A", b_rel, nel, b.nr_rels, desc.required_left, desc.required_right,
                 desc.rules.size(), static_cast<int>(desc.kind), static_cast<int>(desc.strictness),
                 desc.freely_reorderable);

        result.push_back(std::move(desc));
    }

    return result;
}

}
