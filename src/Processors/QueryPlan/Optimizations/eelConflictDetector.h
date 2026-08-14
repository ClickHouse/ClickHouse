#pragma once

#include <utility>
#include <vector>
#include <Core/Joins.h>
#include <Common/logger_useful.h>
#include <base/types.h>

namespace DB
{

/** EEL (Extended Eligibility List) conflict detector for join reordering, from
  * Rao, Lindsay, Lohman, Pirahesh, Simmen, "Using EELs, a Practical Approach to Outerjoin
  * and Antijoin Reordering" (ICDE 2001). Same detector as the reference `main_gen` DPsub
  * research harness (`ConflictDetectors/node_eel.*`).
  *
  * For each binary operator of the original join tree it computes the operator's EEL: the set
  * of relations that must be present before the operator may be applied. The EEL starts from the
  * operator's NEL (the relations its ON clause references) and is widened by conflict rules that
  * propagate per-relation `outer`/`anti` sets bottom-up, capturing reordering conflicts among
  * nested outer/anti joins that a naive ON-clause set misses.
  *
  * `computeEelOperators` returns one descriptor per operator with the split "TES" (Total
  * Eligibility Set): `tes_left = EEL ∩ tablesLeft`, `tes_right = EEL ∩ tablesRight`. DPsub's
  * per-operator validity check (`einbaubar`) then admits joining a left set `S1` and right set
  * `S2` with this operator iff `tes_left ⊆ S1 ∧ tes_right ⊆ S2` (or the mirrored orientation for
  * commutative operators). This is what lets non-commutative semi/anti joins be reordered
  * correctly: their required preserved-side relations are pinned by the TES, and orientation is
  * decided per operator rather than assumed symmetric.
  *
  * DPsub packs relation sets into native integer masks (< 32 relations), so this detector works
  * on `UInt32` masks throughout to match the DPsub hot path.
  */

/// Native-mask view of a captured join operator (mirrors `EelJoinOp`, relation sets packed into
/// `UInt32`). `left`/`right` are the two input subtrees' relations, `nel` the ON-clause relations.
struct EelJoinOpMask
{
    UInt32 left = 0;
    UInt32 right = 0;
    UInt32 nel = 0;
    JoinKind kind = JoinKind::Inner;
    JoinStrictness strictness = JoinStrictness::All;
};

/// Per-operator EEL descriptor consumed by DPsub's `einbaubar` validity check.
struct EelOperator
{
    UInt32 relations = 0; /// tablesLeft | tablesRight: every relation under this operator
    UInt32 tes_left = 0;  /// EEL ∩ tablesLeft: relations required on the operator's left input
    UInt32 tes_right = 0; /// EEL ∩ tablesRight: relations required on the operator's right input
    UInt32 nel = 0;       /// ON-clause relations (kept for diagnostics)
    JoinKind kind = JoinKind::Inner;
    JoinStrictness strictness = JoinStrictness::All;
    /// True for plain inner/cross/comma joins (All): freely reorderable, no orientation or
    /// straddle constraint. False for outer/semi/anti/full joins, which pin orientation via TES.
    bool freely_reorderable = true;
};

/// Compute the per-operator EEL descriptors. `num_relations` bounds relation ids (< 32).
std::vector<EelOperator>
computeEelOperators(const std::vector<EelJoinOpMask> & ops, size_t num_relations, LoggerPtr log);

}
