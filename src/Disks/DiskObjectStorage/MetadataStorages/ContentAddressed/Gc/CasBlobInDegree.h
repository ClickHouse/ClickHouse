#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRecordStreamFormat.h>
#include <base/types.h>
#include <base/extended_types.h>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace DB::Cas
{

/// In-memory description of a blob incarnation condemned by the in-degree merge. The exact token and
/// size are captured from the blob HEAD so the GC caller can issue an exact-token deletion later;
/// `condemn_round` controls round-paced graduation. Entries are decoded from `RunMarker::Condemned` rows and are
/// returned through `RetiredMergeResult`; the type itself has no serialized representation.
struct RetiredEntry
{
    ObjectKind kind = ObjectKind::Blob;
    BlobRef ref{};
    PersistedEtag token;   /// the exact incarnation GC observed; the delete re-heads and compares it
    uint64_t size = 0;
    uint64_t condemn_round = 0;   /// the GC round that condemned this incarnation (round-paced
                                  /// graduation: an entry graduates only once condemn_round < the
                                  /// current round). Consulted by GC only; the writer never reads it.
    bool delete_pending = false;  /// Two-phase graduation: floor-passed and
                                  /// published for deletion; the NEXT pass executes the exact-token
                                  /// delete (pre-CAS, safe at any leader staleness) and drops the entry.
                                  /// Terminal: a pending entry is never un-pended (writers keep seeing
                                  /// it condemned and recreate).
    bool marker_confirmed = false;   /// Durable `Condemned` meta CONFIRMED for this entry — the
                                     /// graduation gate (triage 2026-07-17 §3.4): the per-hash condemn
                                     /// marker is the writer's adopt gate, so graduation to
                                     /// delete_pending requires confirmed durable evidence; an
                                     /// unconfirmed entry is CARRIED, never fail-open deleted. Set at
                                     /// graduation (delete_pending rows always carry it).
};

/// The fold's HEAD hooks (`head_blob`, `peek_head`): the caller issues the request on its own admitted
/// operation and reports what it saw, or nothing when the object is absent.
using BlobHeadFn = std::function<std::optional<Meta>(const BlobRef &)>;

/// Backend-independent codec for source-edge keys. A key is `algo` (u8), the digest at that algorithm's
/// native width, and `source_id` (16 bytes, big-endian). The packed byte order is exactly
/// `(BlobRef, source_id)` order, which lets the fold merge compare keys directly. The leading algorithm
/// byte makes the digest width self-describing; supported algorithms may therefore be mixed in one run.
class SourceEdgeKeyCodec
{
public:
    SourceEdgeKeyCodec() = delete;

    /// key = algo(u8) ++ digest[blobHashLenFor(algo)] ++ source_id(16 BE); 33 or 49 bytes.
    static String key(const BlobRef & ref, const UInt128 & source_id);
    /// Parse a key. Throws `NOT_IMPLEMENTED` on an unknown algo byte, `CORRUPTED_DATA` on a wrong
    /// total length for a known algo. Zero-tails the digest (beyond the algo's own width).
    static void parse(std::string_view key, BlobRef & ref, UInt128 & source_id);
};

/// Deterministic 16-byte id of a source edge (ManifestId, path). Distinctness only — not reconstructable.
UInt128 sourceEdgeId(const ManifestId & id, const String & path);

/// The zero source_id is the reserved sentinel key (used internally for the zero-marker row) —
/// producers of real source edges must fail closed on a hash collision with it.
void assertValidSourceEdgeId(const UInt128 & source_id);

/// Serialized payload of a condemned source-edge sentinel. The payload retains the full incarnation,
/// dialect included, because deletion must remain exact-token guarded. Its fixed prefix is
/// `[0x02][flags][token_type][round BE64][size BE64][token_len BE16]`, followed by token bytes;
/// `token_type` is the dialect byte of `CasWireVocab`'s vocabulary.
/// `flags` bit 0 is `delete_pending`, bit 1 is `marker_confirmed`.
struct CondemnedRow
{
    bool delete_pending = false;
    PersistedEtag token;   // the incarnation the exact-token delete must re-observe
    uint64_t size = 0;
    uint64_t condemn_round = 0;
    bool marker_confirmed = false;   // durable Condemned meta confirmed (graduation gate)
    /// Spelled out rather than defaulted because `PersistedEtag` carries no equality of its
    /// own. A field added above belongs here too.
    bool operator==(const CondemnedRow & o) const
    {
        return delete_pending == o.delete_pending
            && token.dialect == o.token.dialect && token.value == o.token.value
            && size == o.size && condemn_round == o.condemn_round
            && marker_confirmed == o.marker_confirmed;
    }
};

/// Encode a condemned-row payload. Throws `CORRUPTED_DATA` if the token cannot fit in its u16 length
/// or the dialect is not one this vocabulary knows.
String encodeCondemnedRow(const CondemnedRow & row);

/// Decode and validate a condemned-row payload. Unknown flags, dialect bytes, or inconsistent lengths
/// throw `CORRUPTED_DATA`.
CondemnedRow decodeCondemnedRow(std::string_view payload);

/// Bridges the backend-free `Formats/CasRecordStreamFormat` NDJSON reader to the
/// `(key, payload)` BYTE interface the fold / `zeroInDegree` / `previewDeletes` / `fsck` consumers use:
/// `next` reconstructs the packed `SourceEdgeKeyCodec` key and the original payload bytes (a single
/// marker byte for an edge / zero row, or the `encodeCondemnedRow` blob for a condemned row) from the
/// decoded NDJSON record. So the codec stays backend-free while the consumers keep their exact parse /
/// compare logic. The whole-object chained CityHash128 is accumulated as the run streams; `verifyAgainst`
/// checks it against the fold seal's `RunRef.checksum` AFTER the run is fully drained and BEFORE the
/// caller acts on it (a deletion decision).
class SourceEdgeRunView
{
public:
    /// false once the run's `{"n"}` trailer is consumed (the trailer count is verified there). `key` is
    /// the reconstructed `SourceEdgeKeyCodec::key(ref, source_id)`; `payload` is the original marker byte
    /// or `encodeCondemnedRow` bytes.
    bool next(String & key, String & payload);
    /// Verify the accumulated whole-file checksum against the seal's `RunRef.checksum`; CORRUPTED_DATA on
    /// mismatch. Call after draining the run and before acting on its records.
    void verifyAgainst(const UInt128 & expected);
    /// The accumulated whole-file checksum after draining the run — non-throwing, for a read-only auditor
    /// (fsck) that catalogues a mismatch and continues instead of failing closed. Call after draining.
    UInt128 accumulatedChecksum();

private:
    friend SourceEdgeRunView openSourceEdgeRun(std::string_view bytes);
    friend SourceEdgeRunView openSourceEdgeRun(CasOperation & op, const String & key);
    /// Keep the underlying stream alive for the reader, which borrows it rather than owning it.
    explicit SourceEdgeRunView(std::unique_ptr<ReadBuffer> stream_);

    std::unique_ptr<ReadBuffer> stream;             /// owns the backend stream / memory buffer the reader borrows
    std::unique_ptr<SourceEdgeRunReader> reader;    /// over *stream (non-movable => held by pointer, destroyed before stream)
};

/// Open a typed source-edge run. The NDJSON header must identify a `cas_run` of kind `source_edge`;
/// otherwise opening fails closed. The memory overload borrows caller-owned bytes. The backend overload
/// streams the write-once object: the reader itself holds one record-sized buffer, but the open stream
/// underneath it buffers on its own terms, unbounded and unmeasured here.
SourceEdgeRunView openSourceEdgeRun(std::string_view bytes);
SourceEdgeRunView openSourceEdgeRun(CasOperation & op, const String & key);

/// Store a deterministic write-once artifact (same inputs => byte-identical bytes): the blob in-degree
/// runs and fold seals. `create`; a `Conflict` means the write was refused, and only what its resolve
/// read OBSERVED decides what that means: byte-equal bytes are our own deterministic replay (adopt,
/// no-op), divergent bytes or a key that reads as absent are impossible under correct operation and
/// fail closed with `CORRUPTED_DATA` rather than let a divergent artifact disagree with the adopted
/// snapshot, and an unobserved key proves nothing and is reported as the ordinary refusal it is --
/// a corruption verdict there would be a deterministic local failure no caller retries. It is
/// NOT for observation-bearing artifacts (outcome logs) — those carry HEAD-observed
/// incarnations that two observers may legitimately differ on and keep first-durable-write-wins semantics.
void putDeterministicArtifact(CasOperation & op, const String & key, const String & bytes);

/// One source-edge update before merging: the edge `(ref, source_id)`, and whether it is an activation
/// (+edge) or a removal (−edge). Idempotent under re-fold at the merge (set membership, not a counter).
struct BlobDelta
{
    BlobRef ref{};
    UInt128 source_id{};   /// `sourceEdgeId(ManifestId, path)` — an edge identity, not a content hash
    bool remove = false;
    /// Round-local index of the ref transaction that emitted this delta, into `Cas::TxnApplyLedger`.
    /// NEVER persisted and never part of any comparator: it exists only so the reducer can prove it
    /// consumed at least one delta from every transaction the round declared covered (probe B2). It
    /// lands in the struct's existing tail padding — see `kBlobDeltaSize` below.
    ///
    /// DECLARED LAST, and that is load-bearing: several tests brace-initialise a `BlobDelta`
    /// POSITIONALLY as `{ref, source_id, remove}`. Inserting a field ahead of `remove` silently rebinds
    /// that third initialiser to the ordinal and turns every removal delta into an activation — a
    /// compiling, type-correct, wrong-answer change. Any future field goes after this one, or every
    /// aggregate initialiser gets converted to designated form first.
    uint32_t txn_ordinal = 0;
};

/// The fold's per-edge row runs over potentially millions of records per round, so its size is a
/// deliberate property, not an accident. Pinned here (rather than left to a perf run to discover)
/// because probe B2's `txn_ordinal` was added into the existing tail padding: the row did NOT grow.
/// A future field that breaks this assertion is a hot-path change and must be argued as one.
static constexpr size_t kBlobDeltaSize = 64;
static_assert(sizeof(BlobDelta) == kBlobDeltaSize,
              "BlobDelta is the fold's hot per-edge row; growing it is a hot-path change");

/// One orphan-manifest source edge to retire without pretending it came from a ref transaction.
/// The reducer applies this as an idempotent exact-key removal, but it deliberately consumes no B2
/// transaction ordinal and an already-absent edge is not an unmatched-remove correctness signal.
struct BlobSourceRetirement
{
    BlobRef ref{};
    UInt128 source_id{};
};

/// A blob whose active source-edge set became empty this generation — a retire candidate.
struct BlobCandidate
{
    BlobRef ref{};
};

/// Merge the prior generation's blob source-edge run for `shard` with `scattered` deltas, producing the
/// new generation's write-once run under blobTargetRunKey(new_generation, attempt, shard, 0). Streaming:
/// prior run + scattered deltas are sorted by (blob_hash, source_id) and merged via two-cursor scan; the
/// source-edge set is idempotent under re-fold (identical (blob_hash, source_id) pairs deduplicate). A blob
/// whose active edge set transitions to exactly empty this generation is written as an explicit zero-marker
/// row so `zeroInDegree` can stream it; prior-generation zero markers are dropped. Appends the produced
/// run's `RunRef` (key + footer checksum + `shard` + `new_generation`) to `out_runs` for the fold seal.
///
/// `prior_runs` are the parent generation's run segments for this shard, resolved by the caller from the
/// parent fold seal's `blob_target_runs` (filtered to `shard`). An empty vector is the fresh-pool / empty
/// baseline. A run sealed for one generation may physically live under an older generation's key, so the
/// seal's exact reference is authoritative and key construction is not used. `new_generation`, `attempt`,
/// and `shard` name only the output run's key namespace.
/// The fresh entry that re-condemns the current
/// incarnation, paired with the STALE entry's it superseded. Kept as its own struct (rather than a
/// field bolted onto `RetiredEntry`) so the common merge element stays slim — only replaced entries
/// carry the extra superseded incarnation.
struct ReplacedEntry
{
    RetiredEntry fresh;   /// the freshly condemned CURRENT incarnation (also pushed into still_retired byte-identically)
    PersistedEtag old_token;   /// the superseded (stale) entry's — what republication replaced
};

/// One example of an unmatched-remove delta, kept for the caller's single once-per-round WARNING
/// (see `RetiredMergeResult::unmatched_removes` below) — naming the blob and source id is enough to
/// start an investigation without logging every occurrence from the hot inner loop.
struct UnmatchedRemoveExample
{
    BlobRef ref{};
    UInt128 source_id{};
};

/// Outcome of the retired merge: the same
/// streaming pass that folds edges settles every prior retired entry and detects new candidates.
struct RetiredMergeResult
{
    std::vector<RetiredEntry> still_retired;   /// carried + newly-condemned + newly-PENDING entries (the next list)
    std::vector<RetiredEntry> graduated;       /// newly floor-passed this pass — published pending, deleted NEXT pass
    std::vector<RetiredEntry> spared;          /// in-degree recovered — entry dropped
    std::vector<RetiredEntry> redelete;        /// pending in the PRIOR list — execute the exact-incarnation delete pre-CAS, drop
    std::vector<ReplacedEntry> replaced;  /// re-condemned CURRENT tokens that superseded a stale entry after republication; caller emits blob_retire_replaced

    /// Count of `remove == true` deltas that matched no presence for their `(BlobRef, source_id)` key —
    /// neither the prior run nor an earlier delta in the same `scattered` batch had activated it. The
    /// in-degree model is a SET, so an unmatched remove is a per-key no-op BY DESIGN (never a false
    /// deletion), but a persistent nonzero rate means removal deltas are reaching the reducer without
    /// their matching activation, which is a correctness signal worth paging on. See
    /// `ProfileEvents::CASGCUnmatchedRemoveDeltas`.
    uint64_t unmatched_removes = 0;
    /// The first unmatched remove observed this merge, for the caller's WARNING (one example is enough
    /// to start an investigation; logging every occurrence would flood a hot per-edge inner loop).
    std::optional<UnmatchedRemoveExample> unmatched_remove_example;
};

/// Cumulative per-round cap over EVERY destructive-or-observability-write work family a round touches —
/// blob graduation and redelete (this file), the orphan-manifest planner's namespace fan-out and recovery
/// walk (`CasOrphanManifestSweep.cpp`), ref-object/generation-prefix cleanup, the post-CAS hand-off
/// reclaim, the post-CAS manifest-body cleanup, and the `GcOutcomes` audit log (`CasGc.cpp`). One instance
/// is owned by `Gc::runRegularRound` per round and passed by reference/pointer into each family's call
/// in turn, so a cap is cumulative across the WHOLE round, not reset per shard or per namespace. `0` in
/// any bound is unbounded — the same opt-out convention every other CAS round budget uses, so a
/// default-constructed instance reproduces pre-budget behavior everywhere. Exhausting a bound never drops
/// WORK: each destructive family's caller carries the excess in its own already-durable pipeline (still
/// `delete_pending`/condemned rows, a retained sweep candidate, a ref object left for next round) and
/// retries next round with a fresh budget, or -- where nothing durable is left to carry (manifest cleanup,
/// the outcome log) -- exhaustion drops only the redundant record of a decision the round already made
/// safely elsewhere (see each field's own comment). Reusable as-is by a future bounded-parallel-walk: give
/// each worker an atomic increment (or a partitioned share of a bound) instead of inventing a new
/// accounting shape.
struct GcRoundWorkBudget
{
    uint64_t max_graduations = 0;
    uint64_t max_redeletes = 0;
    uint64_t graduations_used = 0;
    uint64_t redeletes_used = 0;

    bool graduationAvailable() const { return max_graduations == 0 || graduations_used < max_graduations; }
    bool redeleteAvailable() const { return max_redeletes == 0 || redeletes_used < max_redeletes; }

    /// Orphan-manifest planner (`planManifestCursorPage` / `activeManifestKeys`): how many DISTINCT
    /// namespaces the sweep may build a fresh protection view for in total this round (cumulative
    /// across pages, not per page), and how many ref-log GET/decode
    /// operations the committed-tail recovery walk may spend in total across every namespace this
    /// round. Both caps exist because building a namespace's protection view (a catalog-authoritative
    /// table recovery plus a committed-tail walk) is the expensive, potentially-unbounded step the
    /// nomination/list budgets never covered.
    uint64_t max_sweep_namespaces = 0;
    uint64_t max_sweep_recovery_ops = 0;
    uint64_t sweep_namespaces_used = 0;
    uint64_t sweep_recovery_ops_used = 0;

    bool sweepNamespaceAvailable() const { return max_sweep_namespaces == 0 || sweep_namespaces_used < max_sweep_namespaces; }
    bool sweepRecoveryOpAvailable() const { return max_sweep_recovery_ops == 0 || sweep_recovery_ops_used < max_sweep_recovery_ops; }

    /// Cleanup families (`Gc::cleanupRefObjects`, `deletePrefixWholesale`'s caller in
    /// `Gc::pruneSupersededGenerations`). `deletePrefixWholesale` already takes a `bounded_remaining`
    /// count; `prefixWholesaleRemaining` turns the shared round budget into that same count (its ONE
    /// unbounded sentinel, `0 == max_prefix_wholesale_objects`, maps to the function's own "no cap"
    /// value, `UINT64_MAX`) so every caller can pass "the round's remainder" instead of `UINT64_MAX`
    /// outright.
    uint64_t max_ref_cleanup_objects = 0;
    uint64_t ref_cleanup_objects_used = 0;
    uint64_t max_prefix_wholesale_objects = 0;
    uint64_t prefix_wholesale_objects_used = 0;

    bool refCleanupAvailable() const { return max_ref_cleanup_objects == 0 || ref_cleanup_objects_used < max_ref_cleanup_objects; }
    uint64_t prefixWholesaleRemaining() const
    {
        if (max_prefix_wholesale_objects == 0)
            return std::numeric_limits<uint64_t>::max();
        return prefix_wholesale_objects_used < max_prefix_wholesale_objects
            ? max_prefix_wholesale_objects - prefix_wholesale_objects_used : 0;
    }

    /// The post-CAS hand-off reclaim (`Gc::runRegularRound`) draws from its OWN reserve, never from
    /// `max_prefix_wholesale_objects` above. The prune is safe to under-serve in any one round -- its
    /// cursor never regresses, so a partially-drained generation is simply finished next round -- but the
    /// hand-off is a ONE-SHOT event with no reclaimer behind it besides `fsck`: a generation it cannot
    /// fully reclaim this round is never revisited (the parent-seal difference that triggers it does not
    /// recur once the ref has moved). Sharing one pool would let a prune-heavy round starve the hand-off
    /// to zero every time; a separate reserve makes that impossible regardless of how much the prune
    /// consumes.
    uint64_t max_handoff_prefix_wholesale_objects = 0;
    uint64_t handoff_prefix_wholesale_objects_used = 0;

    uint64_t handoffPrefixWholesaleRemaining() const
    {
        if (max_handoff_prefix_wholesale_objects == 0)
            return std::numeric_limits<uint64_t>::max();
        return handoff_prefix_wholesale_objects_used < max_handoff_prefix_wholesale_objects
            ? max_handoff_prefix_wholesale_objects - handoff_prefix_wholesale_objects_used : 0;
    }

    /// `GcOutcomes` per-shard body (`Gc::runRegularRound`'s `redelete`/`spared` loops): a pure
    /// observability record of a settlement decision that has ALREADY happened -- the merge unconditionally
    /// decides `spared` for any entry whose in-degree recovered (INV_NO_LOSS: a fresh dedup-adopt must
    /// never be treated as still condemned, past any budget), so this cap governs only whether the decision
    /// gets an audit-log row, never the decision itself. Nothing is retained or retried on exhaustion --
    /// there is nothing left to retry, the entry already left the retired pipeline correctly.
    uint64_t max_outcome_entries = 0;
    uint64_t outcome_entries_used = 0;

    bool outcomeEntryAvailable() const { return max_outcome_entries == 0 || outcome_entries_used < max_outcome_entries; }
};

/// Merge the prior generation's source-edge run with new deltas. The prior run's `RunMarker::Condemned` rows RIDE
/// the source-edge run itself at the zero-sentinel key (`source_id = 0`), so there is no separate
/// `prior_retired` cursor — the prior run IS the retired input. `PriorEdgeCursor` decodes each sentinel
/// `RunMarker::Condemned` row and hands it to the per-blob close-out (in ascending hash order, exactly the order
/// the old sorted vector had). Settlement rules, in order, per condemned row for blob `h` with post-merge
/// in-degree `d`:
///   delete_pending (prior pass)                -> redelete if d = 0 (the caller executes the exact-token
///                                                 delete pre-CAS and the entry drops); d > 0 for a pending
///                                                 entry is structurally impossible but reachable under
///                                                 races — spared + a loud log, never a
///                                                 fail-closed abort;
///   d > 0                                       -> spared (recovery wins even past the floor);
///   d = 0 and condemn_round < current_round     -> graduated: REPUBLISHED as delete_pending (two-phase
///                                                 graduation) — deleted the NEXT pass. GATED on a
///                                                 confirmed durable condemn marker (see
///                                                 `confirm_condemned_marker` below): an unconfirmed
///                                                 entry is carried unchanged instead;
///   d = 0 otherwise                             -> still_retired, carried byte-unchanged.
/// A carried `RunMarker::Condemned` row is SETTLEMENT-ONLY: it never sets the blob's `cur_touched` bit, so a
/// generation that only carries the row emits no zero-marker and pays no `peek_head` HEAD. The surviving
/// `still_retired` entries are re-emitted as `RunMarker::Condemned` sentinel rows into the OUTPUT run (one sentinel
/// per blob, emitted before the blob's edges since the sentinel key sorts first), so the next generation
/// reads them back — `still_retired` mirrors exactly those rows, in the same order.
/// When the pass is clamped on any shard, landed-before-cut events may remain unfolded behind the clamp,
/// so graduating or executing pending
/// deletes over this pass's in-degrees can delete a blob whose +1 is pending behind the clamp (the
/// model's SabotageSkipChangedShard counterexample, realized). With `suppress_destructive` the merge
/// neither graduates nor redeletes: pending entries carry UNCHANGED (still delete_pending) and
/// floor-passed entries stay condemned-only. Condemnation and sparing remain (both non-destructive).
/// A blob that transitions to zero THIS pass with no prior entry is condemned: `head_blob` captures
/// the exact incarnation token/size (absent object or empty head_blob -> nothing to delete, skipped)
/// and the entry is minted at `condemn_round`. Entries for blobs the merged stream never visits
/// (no edges, no deltas) settle at in-degree 0 by definition. The retired cursor never changes the
/// snapshot run bytes. Defaults preserve the empty-retired behavior
/// current_round 0 => nothing graduates, no head_blob => nothing condemned).
///
/// `peek_head` is a side-effect-free HEAD used only by the republication-supersede
/// branch (a `prior_retired` entry whose blob re-touched this pass at net in-degree 0, current token
/// differs from the stale entry's token). `head_blob` is the FRESH-CONDEMN observation hook — it emits
/// the `IndegZero`/`GcRetireObserve`/`BlobRetire` trail and increments `CASGCRetiredCondemned`, which is
/// wrong for a supersede (the supersede's own event is `blob_retire_replaced`, emitted once by the
/// caller from `RetiredMergeResult::replaced`). Calling `head_blob` from the supersede branch used to
/// double-emit `blob_retire` alongside `blob_retire_replaced` and double-count the condemned counter;
/// `peek_head` is a plain HEAD with no events and no counters. No supersede detection happens if
/// `peek_head` is unset (default `{}`), independent of whether `head_blob` is set.
///
/// `confirm_condemned_marker` is the GRADUATION GATE (triage 2026-07-17 §3.4): graduation to
/// `delete_pending` is the one edge that authorizes an irreversible delete, and the per-hash condemn
/// marker (`writeCondemnedMeta`) is the writer's adopt gate — an entry whose marker write was silently
/// swallowed can be same-token adopted by a writer invisible to this fold's cut, so it must NOT
/// graduate. The callback returns whether durable `Condemned` evidence is confirmed for the entry's
/// exact (hash, token); on false the entry is carried unchanged (fail-safe delay — the caller is
/// expected to retry the marker so a later pass can confirm). An entry whose `marker_confirmed` bit is
/// already set skips the callback. Unset (default `{}`) means UNGATED — the pre-gate merge semantics,
/// for merge-mechanics unit tests only; the real GC round always passes the gate.
/// The merge comparator is exactly `(ref.algo, ref.digest, source_id)` (that is, `BlobRef::operator<`
/// followed by `source_id`), which is also the raw key order produced by `SourceEdgeKeyCodec`.
void foldDeltasIntoGeneration(CasOperation & op, const Layout & layout,
                              const std::vector<RunRef> & prior_runs,
                              uint64_t new_generation, uint64_t attempt,
                              uint64_t shard,
                              std::vector<BlobDelta> scattered, std::vector<RunRef> & out_runs,
                              uint64_t current_round = 0, uint64_t condemn_round = 0,
                              const BlobHeadFn & head_blob = {},
                              const BlobHeadFn & peek_head = {},
                              const std::function<bool(const RetiredEntry &)> & confirm_condemned_marker = {},
                              RetiredMergeResult * out_retired = nullptr,
                              bool suppress_destructive = false,
                              /// PROBE B2 (see `Cas::TxnApplyLedger`): when set, one byte is stored per
                              /// CONSUMED delta at `(*out_applied_by_txn_ordinal)[d.txn_ordinal]`. A raw
                              /// vector rather than a callback: this runs once per delta over a stream
                              /// that can reach millions of rows, and a `std::function` call there is
                              /// not free. Never read by the merge; write-only. The caller must size it
                              /// to cover every `txn_ordinal` present in `scattered`.
                              std::vector<uint8_t> * out_applied_by_txn_ordinal = nullptr,
                              std::vector<BlobSourceRetirement> source_retirements = {},
                              /// Shared by reference across every shard's call within one round;
                              /// `nullptr` (default) is unbounded, matching every existing caller.
                              GcRoundWorkBudget * work_budget = nullptr);

/// Stream the sealed in-degree runs named by `runs` (the current seal's `blob_target_runs` filtered to one
/// shard) and return every blob written at in-degree 0 (the candidates that transitioned to zero). An
/// empty `runs` is an empty baseline. Each `RunRef` supplies the exact object key, so resolution never
/// reconstructs a key from generation metadata.
std::vector<BlobCandidate> zeroInDegree(CasOperation & op, const std::vector<RunRef> & runs);

}
