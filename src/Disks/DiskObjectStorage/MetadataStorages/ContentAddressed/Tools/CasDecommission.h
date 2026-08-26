#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace DB::Cas
{

/// Counts the work performed by `decommissionPoolMember` for one pool member. The namespace counters
/// describe metadata and ref-log transitions; the object counters describe physical objects deleted by
/// the manifest, staging, and mountpoint drains. Blob bytes are intentionally not reported: removing
/// ref edges makes them eligible for ordinary GC, but this operation does not synchronously reclaim
/// shared content.
///
/// A decommission is resumable. A previous run may already have moved namespaces to `Removing`, and a
/// warning means that the corresponding drain was not confirmed. In either case the report lets the
/// caller distinguish work done by this invocation from work observed from an earlier invocation.
struct DecommissionReport
{
    String srid;                                  /// The decommissioned member's `server_root_id`.
    uint64_t namespaces_removed = 0;              /// Namespaces erased by this invocation.
    uint64_t namespaces_already_removed = 0;      /// Namespaces already `Removing` on entry.
    uint64_t committed_refs_removed = 0;          /// Committed ref records removed by namespace drops.
    uint64_t precommits_removed = 0;              /// Precommit records removed by namespace drops.
    uint64_t edge_deltas_emitted = 0;             /// The sum of `committed_refs_removed` and `precommits_removed`.
    uint64_t manifest_debris_removed = 0;         /// Eligible manifest objects deleted from old build prefixes.
    uint64_t staging_objects_removed = 0;         /// Objects deleted from the member's staging prefix.
    uint64_t mountpoint_objects_removed = 0;      /// Objects deleted from the member's roots/mountpoint prefix.
    bool slot_removed = false;                    /// Whether mount and epoch were deleted and the owner was tombstoned.
    std::vector<String> warnings;                 /// Drain or slot-retirement failures; a non-empty list keeps the slot.
};

/// Erases all content owned by a permanently dead pool member. The operation first claims the member's
/// slot as an administrative writer; a live lease is refused, and the claim fences the dead member from
/// writing while cleanup runs. It then drops each table namespace through `Pool::dropNamespace`, drains
/// eligible manifest debris, staging objects, and mountpoint objects, and retires the slot only after all
/// drains are confirmed. Namespace drops are idempotent: a rerun resumes any missing terminal append
/// and leaves exact catalog-row deletion to GC. The member slot remains while any catalog entry still
/// belongs to the victim.
///
/// This is a writer operation, not GC: it emits the normal ref-edge deltas and does not invent ref
/// transitions. Per-object drain failures are recorded in `DecommissionReport::warnings` and leave the
/// terminated slot as a resume anchor; other failures, including refusal to claim the member, propagate
/// as exceptions. When set, `sink` receives `MemberDecommission` audit events for the run's begin,
/// per-namespace, and end milestones.
DecommissionReport decommissionPoolMember(BackendPtr backend, PoolConfig config,
                                          const String & victim_srid, const CasEventSink & sink = {},
                                          const std::function<void()> & request_gc_round = {});

}
