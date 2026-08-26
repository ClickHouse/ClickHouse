#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <base/types.h>
#include <base/extended_types.h>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace DB::Cas
{

class Layout;

/// A reference to one write-once run object and its whole-object checksum. A retry can compare the
/// checksum with the bytes already sealed before it adopts or consumes the run.
///
/// `shard` and `generation` are required on `blob_target_runs`. An idle shard can carry a parent's run
/// into a newer seal, even though the object remains under the older generation's key namespace. The
/// explicit fields therefore preserve both the shard association and the physical generation without
/// making consumers parse a storage key. Run references for objects that are always local to the current
/// generation may leave these fields at their defaults because their consumers resolve them by key.
struct RunRef
{
    String key;
    UInt128 checksum{};
    uint64_t shard = 0;        /// gc-shard this run belongs to (REQUIRED for blob_target_runs)
    uint64_t generation = 0;   /// generation whose key namespace physically holds the object (for retention)
    bool operator==(const RunRef &) const = default;
};

/// Why one namespace is held below its ref-log frontier. A BOUNDED enum: every value is a shape the
/// fold can name exactly, so an operator reading a seal learns what stopped the namespace without
/// correlating logs. Persisted as a word, so an unknown word is `CORRUPTED_DATA` rather than a silently
/// reinterpreted integer.
///
/// THESE ARE WIRE VALUES, AND THEY ARE APPEND-ONLY. A durable seal written by one build is read by
/// another, so a renumbered value or a reused word makes an older seal describe a hold that is not the
/// one it recorded — and a hold's whole job is to say truthfully what stopped a namespace and where.
/// Add new reasons at the end; never renumber, never repurpose a retired word.
enum class HoldReason : uint8_t
{
    GapBelowWitness = 1,        /// 404 at the expected id with a durable witness above it, same epoch
    UnconsumedSealCrossing = 2, /// a later epoch is reachable but this epoch's closing seal was never consumed
    WitnessDisappeared = 3,     /// an above-cursor record stopped answering GETs — corruption, never clearance
    BodyUndecodable = 4,        /// the ref-log body at the position is present but cannot be decoded/extracted
    ManifestBodyMissing = 5,    /// a part-manifest body the position's edges name is absent (the fold barrier)
    CheckpointUndecodable = 6,  /// the namespace's `_ckpt` is present but its body cannot be decoded
};

/// The wire word one `HoldReason` is persisted as. Exported because the reason is operator-facing well
/// beyond the codec — the sweep names it when the §6 deletion premise retains a manifest under a held
/// namespace — and a second rendering of these words elsewhere would be a second place for them to drift.
std::string_view holdReasonToWord(HoldReason r);

/// The durable hold on one namespace. It rides `RefCoverage` across rounds and across `REBUILD`, and
/// clears ONLY by folding through `offending_position` and adopting the result in `gc/state` — never by
/// observing another absent, because an absent is exactly the observation a lying store produces.
struct RefHold
{
    HoldReason reason = HoldReason::GapBelowWitness;

    /// The exact position the fold must resolve before this namespace may advance. A carried hold makes
    /// the next round read this key even when the round's hint omits the namespace entirely.
    ///
    /// A CANONICAL id: both components are nonzero, and both codecs enforce it. `{0, 0}` is not a
    /// degenerate hold, it is a hold that ERASES ITSELF — every position compares at or above it, so the
    /// first fold that reaches any record clears the hold as resolved, and the namespace advances with
    /// nothing recording that it ever stopped. It is also unnameable: `renderRefTxnId` refuses a zero
    /// component, so the sweep that retains a manifest "because the namespace is held at <position>"
    /// cannot even state its reason. The decoder rejects it as `CORRUPTED_DATA` and the encoder as
    /// `LOGICAL_ERROR`.
    RefTxnId offending_position{};

    /// How many rounds have retried `offending_position` without resolving it. Purely observational:
    /// it is what distinguishes a transient barrier (a writer still uploading a manifest body) from a
    /// namespace that has been stuck for hours.
    uint32_t retry_count = 0;

    /// The first round that retries `offending_position`. The fold retries every round today (a hold
    /// costs one exact `GET`, and a transient barrier must clear the moment its body lands), so this is
    /// always `current_round + 1`; it exists so a future backoff policy needs no format change.
    uint64_t next_retry_round = 0;

    bool operator==(const RefHold &) const = default;
};

/// Records what the current round did for one life-keyed `CasFoldSeal::ref_lives` row.
/// `classification` is a persisted byte:
/// 0 means absent, 1 means unchanged, 2 means all records through the observed cursor were folded, and 4
/// means folding was clamped below the ref-log cursor. A clamped entry must be read again in the next
/// round, because an unfolded event may become foldable by then.
///
/// THE SET {0, 1, 2, 4} IS CLOSED, and both codecs enforce it (decode `CORRUPTED_DATA`, encode
/// `LOGICAL_ERROR`). Every consumer branches on exact values — the sweep's §6 deletion premise refuses a
/// row by testing `== 4` and `== 0` — so an unrecognized byte is not a variant to tolerate: it passes
/// every refusal stated in terms of the set and reaches the delete. The decoder also validates BEFORE
/// narrowing to the byte, because a wide integer on the wire (258, say) truncates into the set and would
/// otherwise claim a coverage the fold never proved.
struct RefCoverage
{
    uint8_t classification = 0;

    /// The greatest `RefTxnId` whose owner changes have contributed their manifest-edge deltas. There is
    /// one ref-log stream per namespace life, so this cursor is stored in that life-keyed row.
    /// `{0, 0}` means that no transaction has been folded yet. A clamp leaves the cursor below the
    /// offending transaction so the complete transaction is retried rather than partially applied.
    RefTxnId last_folded_ref_id{};

    /// STRICT GRAMMAR: present if and only if `classification == 4`. Both directions enforce it — the
    /// encoder refuses to write a classification-4 row without a hold (a clamp whose reason was lost is
    /// indistinguishable from a clean cursor once it is durable) and refuses to write a hold on any
    /// other classification (`LOGICAL_ERROR`); the decoder rejects both shapes as `CORRUPTED_DATA`. The
    /// pairing lives in the type, not only in the codec, so no producer can construct the forbidden
    /// combination by forgetting a field.
    std::optional<RefHold> hold = std::nullopt;

    bool operator==(const RefCoverage &) const = default;
};

/// Positive evidence that the terminal `remove_namespace` transaction for one life was folded into
/// the adopted seal. The owning opaque life id is the `CasFoldSeal::ref_lives` map key; the evidence
/// therefore carries no duplicate namespace or incarnation and has no pending/completed state.
struct RefCleanupEvidence
{
    RefTxnId remove_txn_id{};

    bool operator==(const RefCleanupEvidence &) const = default;
};

/// The complete durable fold state for one cataloged ref life. Coverage and optional terminal
/// evidence live in the same row so neither can be admitted by an independent producer.
struct RefLifeFoldState
{
    RefCoverage coverage;
    std::optional<RefCleanupEvidence> cleanup_evidence = std::nullopt;

    bool operator==(const RefLifeFoldState &) const = default;
};

/// Per-shard summary of condemned rows carried in the sealed source-edge run. It lets graduation and
/// pure reference-carry decisions inspect the seal without reading a run. Every newly written seal has
/// an entry for every shard in `0..gc_shards-1`: a folding shard computes its entry from its remaining
/// condemned rows, while a pure-carry shard copies the parent's entry. Missing entries are invalid and
/// must not be interpreted as zero.
struct CondemnedSummary
{
    uint64_t condemned_total = 0;   /// count of `kCondemned` rows in this shard's sealed run
    uint64_t pending_total = 0;     /// how many of those are `delete_pending` (a graduation is due)
    uint64_t oldest_nonpending_condemn_round = UINT64_MAX;   /// min condemn_round over non-pending; UINT64_MAX = none
    bool operator==(const CondemnedSummary &) const = default;
};

/// The write-once fold seal for one GC generation at
/// `<prefix>/gc/gen/<generation>/attempt/<attempt>/fold_seal`. It is the generation's durable coverage
/// record: it stores cursors and run references, not one record per edge, manifest, or candidate. A retry
/// and the next round use the adopted seal to determine what was folded and which parent runs can be
/// carried forward. Its run references and ref-life rows are also the durable inputs to retention.
/// Manifest cleanup is intentionally not represented here: those cleanups execute
/// inline from the in-memory cleanup map, and no durable cleanup-run reader exists.
struct CasFoldSeal
{
    uint64_t generation = 0;
    uint64_t parent_generation = 0;
    /// Exactly one row per `Live` or `Removing` catalog life admitted by `buildRefWalkPlan`.
    std::map<UInt128, RefLifeFoldState> ref_lives;
    std::vector<RunRef> blob_target_runs;           /// the blob in-degree run segments this gen sealed
    std::map<uint64_t, CondemnedSummary> condemned_summary;   /// gc-shard -> summary; TOTAL over gc_shards
    bool operator==(const CasFoldSeal &) const = default;
};

/// The two byte caps a fold seal is measured against, read from the format registry so the writer's
/// gate and its boundary tests can never drift from the reader's limits.
struct FoldSealCaps
{
    uint64_t line_cap;     /// longest decodable record, excluding the '\n' terminator
    uint64_t object_cap;   /// largest whole seal object
};
FoldSealCaps foldSealCaps();

/// PRE-PUT GATE. A seal larger than the object cap is writable but not readable — nothing enforces the
/// cap on the fold-seal read path, so an oversized PUT would leave the pool with a durable seal that no
/// later round can decode, which is unrecoverable. `encodeFoldSeal` therefore refuses BEFORE returning
/// any bytes, which is before either PUT site can issue its write. `LIMIT_EXCEEDED`, not
/// `CORRUPTED_DATA`: the bytes are well formed, the round is over budget, and the round fails closed and
/// retries. Equality fits; one byte more does not.
void checkFoldSealObjectBytes(uint64_t encoded_bytes);

/// Encodes a fold seal as a strict, raw text control object. The header and meta lines are followed by
/// tagged records in the fixed `rfl`/`btr`/`cnd` order and a record-count trailer. Map iteration and
/// run references are sorted so retries produce byte-identical output for write-once adoption.
///
/// Enforces the whole coverage grammar — the closed classification set, the classification-4 hold
/// pairing, and the hold's canonical offending position — and BOTH byte caps: every emitted line against
/// `line_cap` — header, meta, records and trailer alike, with no exception — and the whole object
/// against `object_cap`. Both PUT sites go through this function, so the gate cannot be bypassed by
/// adding a third one.
///
/// A grammar violation here is `LOGICAL_ERROR`, not `CORRUPTED_DATA`: these bytes do not come from a
/// store, they come from THIS process, so the seal it is about to make durable is a bug in our own
/// writer — the same code `encodeGcState` raises for its own impossible input. The caps stay
/// `LIMIT_EXCEEDED` (a well-formed seal, over budget). Nothing is returned on any refusal, so no PUT
/// site can issue the write.
String encodeFoldSeal(const CasFoldSeal & seal);

/// Decodes and validates a fold seal, rejecting unknown fields, malformed records, trailing bytes, a
/// second record for a key already read, and a trailer count that differs from the records read.
/// Invalid persisted data raises `CORRUPTED_DATA` — including every shape the encoder refuses, since
/// these bytes may have been written by anything at all.
CasFoldSeal decodeFoldSeal(std::string_view data, std::optional<uint64_t> expected_generation = std::nullopt);

/// Decodes a seal at an adoption boundary and validates the shard-indexed structure against the
/// pool-authoritative nonzero `gc_shards`: canonical blob-target keys, at most one run per shard,
/// and an exactly total, semantically consistent condemned summary.
CasFoldSeal decodeFoldSeal(
    std::string_view data, const Layout & layout, uint64_t gc_shards,
    std::optional<uint64_t> expected_generation = std::nullopt);

/// Applies the same shard-indexed structural checks to an in-memory producer immediately before its
/// seal PUT. Invalid producer state is a `LOGICAL_ERROR`; no bytes are written.
void validateFoldSealForWrite(const CasFoldSeal & seal, const Layout & layout, uint64_t gc_shards);

}
