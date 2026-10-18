#pragma once

#include <vector>
#include <Core/Joins.h>
#include <Common/logger_useful.h>
#include <base/types.h>

namespace DB
{

/** Table-driven conflict detectors that decide which join reorderings preserve results when the
  * plan mixes inner joins with outer and semi/anti joins (not all reorderings are valid then).
  *
  * Two detectors share this module (select with `ConflictDetector`):
  *   - CD-A: correct but incomplete. Each operator gets one required set, widened to forbid every
  *     potentially-invalid reordering -- simple, but it also rejects some valid ones.
  *   - CD-C: correct and complete. The required set stays minimal, and conflicts are recorded as
  *     rules `T1 -> T2` ("if any table of T1 is joined, all of T2 must be too"), keeping the valid
  *     reorderings CD-A's coarse widening discards.
  *
  * Both use one descriptor (`ConflictOperator`) and one validity test:
  *     required_left subseteq S1  AND  required_right subseteq S2   (or the mirrored orientation),
  *   AND every conflict rule obeyed. CD-A has empty rule sets, so its test reduces to the containment.
  * Pinning the required relations per side (instead of assuming symmetry) is what lets
  * non-commutative outer and semi/anti joins reorder correctly. Reorderability comes from four static
  * matrices (comm, assoc, l-asscom, r-asscom); their null-rejection-dependent entries use `nr_rels`.
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
    /// Relations on whose attributes this operator's ON predicate rejects nulls: it is false or
    /// unknown whenever all of that relation's columns are null. A subset of `nel` (an equi-join
    /// predicate rejects nulls on both sides). Resolves the null-rejection-dependent matrix entries.
    UInt32 nr_rels = 0;
    JoinKind kind = JoinKind::Inner;
    JoinStrictness strictness = JoinStrictness::All;
};

/// A conflict rule: if any table of `t1` is in the joined set, all of `t2` must be too.
struct ConflictRule
{
    UInt32 t1 = 0;
    UInt32 t2 = 0;
};

/// Per-operator descriptor consumed by DPsub's validity check (`isValidJoinOrderMaskConflict`).
struct ConflictOperator
{
    UInt32 relations = 0;      /// every relation under this operator (left subtree | right subtree)
    UInt32 left_relations = 0; /// the (left-canonical) preserved-side subtree; used to orient a
                               /// degenerate operator whose empty required_* sets cannot pick a side
    UInt32 required_left = 0;  /// relations that must be present on the operator's left input
    UInt32 required_right = 0; /// relations that must be present on the operator's right input
    UInt32 nel = 0;            /// ON-clause relations, used to locate the operator at a split boundary
    JoinKind kind = JoinKind::Inner;
    JoinStrictness strictness = JoinStrictness::All;
    /// True when the ON predicate references relations on at most one input side (a one-sided
    /// predicate, or none at all for a cross product). Then the required sets cannot orient the
    /// operator, so the validity test checks each input subtree lands on its own side instead.
    bool degenerate = false;
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
