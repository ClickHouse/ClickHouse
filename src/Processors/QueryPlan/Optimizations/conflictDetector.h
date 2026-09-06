#pragma once

#include <vector>
#include <Core/Joins.h>
#include <Common/logger_useful.h>
#include <base/types.h>

namespace DB
{

/** Table-driven conflict detectors for join reordering, from
  * Moerkotte et al. "On the Correct and Complete Enumeration of the Core Search Space"
  * (SIGMOD 2013)
  *
  * Two detectors share this module (select with `ConflictDetector`):
  *   - CD-A: *correct* but not *complete*. For each operator it computes one Total Eligibility Set
  *     (TES), widened from the operator's SES by every descendant it does not associate with. Its
  *     validity test is the unconditioned containment `L-TES ⊆ S1 ∧ R-TES ⊆ S2`.
  *   - CD-C: *correct and complete* -- it generates exactly the core search space. Its TES stays at
  *     SES; conflicts are recorded as *conflict rules* `T1 → T2` (a conditioned containment: "if any
  *     table of T1 is present, all of T2 must be too"). CD-C keeps valid reorderings that CD-A's
  *     coarse TES widening throws away.
  *
  * Both are exposed through one descriptor (`ConflictOperator`) and one validity test:
  *     required_left ⊆ S1 ∧ required_right ⊆ S2   (or the mirrored orientation)
  *   AND every conflict rule obeyed.
  * For CD-A, `required_*` is the split TES and `rules` is empty, so the test reduces to CD-A's plain
  * TES containment. For CD-C, `required_*` is the split SES and `rules` carries the conflict rules.
  * This is why non-commutative outer and semi/anti joins can be reordered correctly: their required
  * relations are pinned, and orientation is decided per operator rather than assumed symmetric.
  *
  * The properties are looked up in four static matrices encoding `comm`, `assoc`, `l-asscom`, and
  * `r-asscom`. The null-rejection-dependent entries are resolved from each operator's `nr_rels`.
  */
enum class ConflictDetector : UInt8
{
    CDA, /// correct, incomplete
    CDC, /// correct and complete
};

struct ConflictOpMask
{
    UInt32 left = 0;
    UInt32 right = 0;
    UInt32 nel = 0;
    /// Relations on whose attributes this operator's ON predicate rejects nulls (Definition 1 of
    /// the paper): the predicate is false/unknown whenever all of that relation's columns are null.
    /// Always a subset of `nel`. Resolves the null-rejection-dependent matrix entries.
    /// An equi-join predicate rejects nulls on both of its sides.
    UInt32 nr_rels = 0;
    JoinKind kind = JoinKind::Inner;
    JoinStrictness strictness = JoinStrictness::All;
};

/// A CD-B/CD-C conflict rule: if any table of `t1` is in the joined set, all of `t2` must be too.
struct ConflictRule
{
    UInt32 t1 = 0;
    UInt32 t2 = 0;
};

/// Per-operator descriptor consumed by DPsub's validity check (`isValidJoinOrderMaskConflict`).
struct ConflictOperator
{
    UInt32 relations = 0;      /// T(left) | T(right): every relation under this operator, T: the set of relations in a subtree
    UInt32 left_relations = 0; /// T(left): the (left-canonical) preserved-side subtree. Used to orient a degenerate
                               /// (predicate-less) operator, whose empty required_* sets cannot decide the side.
    UInt32 required_left = 0;  /// relations required on the operator's left input  (TES ∩ T(left) for CD-A, SES ∩ T(left) for CD-C)
    UInt32 required_right = 0; /// relations required on the operator's right input
    UInt32 nel = 0;            /// ON-clause relations, used to locate the operator at a split boundary
    JoinKind kind = JoinKind::Inner;
    JoinStrictness strictness = JoinStrictness::All;
    /// True for plain inner/cross/comma joins (comm + assoc among themselves): they impose no join
    /// kind. False for outer/semi/anti/full joins, which pin orientation and fix the kind.
    bool freely_reorderable = true;
    /// CD-C conflict rules attached to this operator (always empty for CD-A).
    std::vector<ConflictRule> rules;
};

/// Compute the per-operator conflict descriptors from the operators of the original join tree,
/// using the requested detector (CD-A or CD-C).
std::vector<ConflictOperator>
computeConflictOperators(const std::vector<ConflictOpMask> & ops, ConflictDetector detector, LoggerPtr log);

}
