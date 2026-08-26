#include <Processors/QueryPlan/Optimizations/cdaConflictDetector.h>

namespace DB
{

namespace
{

/// Operator types after normalisation to the paper's "left" canonical form. ClickHouse encodes
/// semi/anti as a JoinStrictness on top of a JoinKind, and it also has RIGHT variants that the
/// paper only defines as left forms; `normalize` folds all of that into these five categories by
/// swapping the children of every RIGHT variant (a valid equivalence: A OP_right B == B OP_left A).
///
/// The enumerators double as row/column indices into the property matrices below, so their order
/// and contiguity matter: adding a new operator type means adding an entry here (before `Count`)
/// and a matching row/column to each matrix -- no code logic changes. This is the extensibility the
/// paper's table-driven approach is designed for (SIGMOD'13, Section 5.1).
enum class Category : size_t
{
    Join = 0,  /// inner / cross / comma  (paper: join, cross product)
    LeftOuter, /// left/right outer join  (paper: left outerjoin)
    FullOuter, /// full outer join        (paper: full outerjoin)
    Semi,      /// left/right semi join   (paper: left semijoin)
    Anti,      /// left/right anti join   (paper: left antijoin)
    Count,
};

constexpr size_t NUM_CATEGORIES = static_cast<size_t>(Category::Count);
constexpr size_t idx(Category c) { return static_cast<size_t>(c); }

/// One operator in left-canonical form: `left` is the preserved side for outer/semi/anti.
struct NormOp
{
    Category category = Category::Join;
    UInt32 left = 0;
    UInt32 right = 0;
    JoinKind kind = JoinKind::Inner;
    JoinStrictness strictness = JoinStrictness::All;
};

NormOp normalize(const CdaJoinOpMask & op)
{
    if (op.strictness == JoinStrictness::Semi)
    {
        if (isRight(op.kind))
            return {Category::Semi, op.right, op.left, JoinKind::Left, JoinStrictness::Semi};
        return {Category::Semi, op.left, op.right, JoinKind::Left, JoinStrictness::Semi};
    }
    if (op.strictness == JoinStrictness::Anti)
    {
        if (isRight(op.kind))
            return {Category::Anti, op.right, op.left, JoinKind::Left, JoinStrictness::Anti};
        return {Category::Anti, op.left, op.right, JoinKind::Left, JoinStrictness::Anti};
    }
    switch (op.kind)
    {
        case JoinKind::Left:
            return {Category::LeftOuter, op.left, op.right, JoinKind::Left, JoinStrictness::All};
        case JoinKind::Right:
            return {Category::LeftOuter, op.right, op.left, JoinKind::Left, JoinStrictness::All};
        case JoinKind::Full:
            return {Category::FullOuter, op.left, op.right, JoinKind::Full, JoinStrictness::All};
        default: /// Inner / Cross / Comma
            return {Category::Join, op.left, op.right, op.kind, JoinStrictness::All};
    }
}

/// The property matrices of the paper (Tables 1-3), transcribed directly. Rows and columns are
/// indexed by `Category` in the order {Join, LeftOuter, FullOuter, Semi, Anti}. `1` == the paper's
/// `+`, `0` == `-`. `comm` is indexed by a single category; `assoc`/`lAsscom`/`rAsscom` are indexed
/// [row = op_a][col = op_b].
///
/// Several paper entries hold only when the relevant predicate rejects nulls (the footnoted ones).
/// Since we do not analyse predicates yet, every such entry is transcribed as `0` (a conflict).
/// That is safe: it only ever *widens* a TES, so CD-A stays correct -- just less complete for those
/// outer/outer and full-outer reorderings. When predicate null-rejection is added, those specific
/// cells become the place to flip to `1`.
/// `1` == the paper's `+`, `0` == `-`. Stored as UInt8 (not bool) so the compact table transcription
/// reads like the paper; the accessors below expose it as bool.
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

/// An operator imposes no reordering constraint of its own -- it may be combined in either
/// orientation and needs no TES gate beyond connectivity -- exactly when it commutes and associates
/// with itself in both nesting directions. For our operator set this selects the inner joins, but
/// deriving it from the matrices keeps it correct if new self-reorderable operators are added.
bool isFreelyReorderable(Category c)
{
    return comm(c) && assoc(c, c) && lAsscom(c, c) && rAsscom(c, c);
}

}

std::vector<CdaOperator>
computeCdaOperators(const std::vector<CdaJoinOpMask> & ops, LoggerPtr log)
{
    const size_t n = ops.size();

    std::vector<NormOp> norm;
    norm.reserve(n);
    for (const auto & op : ops)
        norm.push_back(normalize(op));

    std::vector<CdaOperator> result;
    result.reserve(n);

    /// CD-A (SIGMOD'13, Section 5.2). For each operator b, TES(b) starts from its SES and is
    /// widened by every operator a in b's subtrees with which b does not associate. TES(b) reads
    /// only static subtree relation sets and the property matrices -- not other operators' TES --
    /// so operators may be processed in any order (no bottom-up traversal is needed).
    for (size_t j = 0; j < n; ++j)
    {
        const NormOp & b = norm[j];
        const UInt32 b_left = b.left;
        const UInt32 b_right = b.right;
        const UInt32 b_rel = b_left | b_right;
        const UInt32 nel = ops[j].nel;

        /// CalcSES for a non-degenerate predicate with no dependent operators: the ON-clause
        /// relations, restricted to this operator's own relations.
        UInt32 tes = nel & b_rel;

        for (size_t i = 0; i < n; ++i)
        {
            if (i == j)
                continue;
            const NormOp & a = norm[i];
            const UInt32 a_rel = a.left | a.right;

            /// a is in STO(left(b)) iff all of a's relations lie in b's left subtree, and in
            /// STO(right(b)) iff they lie in b's right subtree. In a tree the two subtrees are
            /// disjoint, so at most one holds; ancestors and disjoint operators satisfy neither.
            if ((a_rel & ~b_left) == 0)
            {
                if (!assoc(a.category, b.category))
                    tes |= a.left;
                if (!lAsscom(a.category, b.category))
                    tes |= a.right;
            }
            else if ((a_rel & ~b_right) == 0)
            {
                if (!assoc(b.category, a.category))
                    tes |= a.right;
                if (!rAsscom(b.category, a.category))
                    tes |= a.left;
            }
        }

        /// L-TES / R-TES: the split of TES over the operator's (left-canonical) input sides, which
        /// is exactly what `applicable_A` compares against S1 and S2.
        CdaOperator desc;
        desc.relations = b_rel;
        desc.tes_left = tes & b_left;
        desc.tes_right = tes & b_right;
        desc.nel = nel;
        desc.kind = b.kind;
        desc.strictness = b.strictness;
        desc.freely_reorderable = isFreelyReorderable(b.category);
        result.push_back(desc);

        LOG_TEST(log, "CD-A op: relations {} tesL {} tesR {} kind {} strictness {} free {}",
                 b_rel, desc.tes_left, desc.tes_right,
                 static_cast<int>(desc.kind), static_cast<int>(desc.strictness), desc.freely_reorderable);
    }

    return result;
}

}
