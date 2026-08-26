#pragma once

#include <vector>
#include <Core/Joins.h>
#include <Common/logger_useful.h>
#include <base/types.h>

namespace DB
{

/** CD-A conflict detector for join reordering, from
  * Moerkotte, Fender, Eich, "On the Correct and Complete Enumeration of the Core Search Space"
  * (SIGMOD 2013), Section 5.2.
  *
  * CD-A is a table-driven conflict detector. It is *correct* (never admits an invalid reordering)
  * but not *complete* (it may forbid some valid reorderings; CD-B and CD-C in the same paper
  * recover those). For each binary operator of the original join tree it computes a single Total
  * Eligibility Set (TES): the set of relations that must be present before the operator may be
  * applied. TES starts from the operator's SES (the relations its ON clause references) and is
  * widened, in one bottom-up pass, by every descendant operator with which this operator does not
  * commute/associate. The properties are looked up in four static matrices encoding `comm`,
  * `assoc`, `l-asscom`, and `r-asscom` for the operator types (Tables 1-3 of the paper).
  *
  * `computeCdaOperators` returns one descriptor per operator with the split TES:
  * `tes_left = TES \intersect T(left)`, `tes_right = TES \intersect T(right)`. DPsub's per-operator
  * validity check (`applicable_A`) then admits joining a left set `S1` and a right set `S2` with
  * this operator iff `tes_left \subseteq S1 AND tes_right \subseteq S2` (or the mirrored orientation
  * for a commutative operator, or when the required sides fit the other way -- see
  * `isValidJoinOrderMaskCDA`). This is what lets non-commutative semi/anti and outer joins be
  * reordered correctly: their required relations are pinned by the TES, and orientation is decided
  * per operator rather than assumed symmetric.
  *
  * Null rejection: several table entries (the footnoted ones in the paper) only hold when the
  * relevant predicate rejects nulls. This detector does not yet analyse predicates, so it treats
  * every footnoted entry as a conflict. That is safe (it only widens the TES, never narrows it),
  * at the cost of missing the outer/outer and outer/full reorderings those entries would allow.
  *
  * DPsub packs relation sets into native integer masks, so this detector works on native masks
  * throughout to match the DPsub hot path.
  */
struct CdaJoinOpMask
{
    UInt32 left = 0;
    UInt32 right = 0;
    UInt32 nel = 0;
    JoinKind kind = JoinKind::Inner;
    JoinStrictness strictness = JoinStrictness::All;
};

/// Per-operator CD-A descriptor consumed by DPsub's `applicable_A` validity check.
struct CdaOperator
{
    UInt32 relations = 0; /// T(left) | T(right): every relation under this operator
    UInt32 tes_left = 0;  /// TES \intersect T(left): relations required on the operator's left input
    UInt32 tes_right = 0; /// TES \intersect T(right): relations required on the operator's right input
    UInt32 nel = 0;       /// ON-clause relations, used to locate the operator at a split boundary
    JoinKind kind = JoinKind::Inner;
    JoinStrictness strictness = JoinStrictness::All;
    /// True for plain inner/cross/comma joins (comm + assoc among themselves): freely reorderable,
    /// so the applicable check imposes no constraint on them. False for outer/semi/anti/full joins,
    /// which pin orientation and required relations via TES.
    bool freely_reorderable = true;
};

/// Compute the per-operator CD-A descriptors from the operators of the original join tree.
std::vector<CdaOperator>
computeCdaOperators(const std::vector<CdaJoinOpMask> & ops, LoggerPtr log);

}
