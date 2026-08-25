#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <atomic>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <vector>

namespace DB { class WriteBuffer; }

namespace DB::Cas
{

/// Re-readable source for one content-addressed blob upload.
///
/// `open` returns a FRESH reader over exactly `size` logical bytes and may be called more than once: an
/// upload can race with another writer or with GC, in which case the transaction retries from the writer's
/// own source and each attempt must read from the beginning. `server_side_copy_from` is set only for an S3
/// staging object; the ordinary create then promotes it by a server-side copy instead of streaming through
/// ClickHouse, and `open` reads that same staging object when a resurrection has to re-upload it
/// (`Backend::resurrect` streams from a `ReadBuffer`, it does not copy). The staging object must remain
/// available through a condemned-object resurrection.
struct BlobSource
{
    uint64_t size = 0;
    std::function<std::unique_ptr<ReadBuffer>()> open;   /// yields exactly `size` bytes, from the start
    /// When set, the blob's bytes already live in an S3 staging object with this key, and `putBlob` promotes it by a
    /// WRITE-ONCE conditional SERVER-SIDE COPY (`Backend::promoteStaged`) instead of streaming
    /// `open` — and resurrects a condemned incarnation by an unconditional re-upload streamed from
    /// the SAME staging object via `open` (`Backend::resurrect`), never a read of the condemned blob
    /// (revival must always be a fresh write from the source). Unset (the default, `StagingBackend::Local`) ⇒ the local
    /// streaming path is byte-for-byte unchanged and `open` is the source.
    std::optional<String> server_side_copy_from;
    /// Build a re-readable source backed by an owned string; intended for small payloads and tests.
    static BlobSource fromString(String bytes);
};

/// putBlob's return value: the `BlobRef` it was addressed by (the write mint's algo + digest pair)
/// plus the admitted logical size.
struct PutBlobResult
{
    BlobRef ref;
    uint64_t size = 0;
};

/// One blob dependency this build contributes — EXACTLY the record `putBlob` folds into `deps`.
/// A token identifies an incarnation uploaded by this transaction and must be retained through
/// promotion; a tokenless entry relies on the durable source-manifest edge instead. `adopted`
/// distinguishes trusted committed-source evidence (`adoptEvidence`) from a pending upload that has
/// not yet been completed. CAS-owned public value type so a transaction-detached upload can RETURN
/// its complete dep effect instead of folding it as a side effect (spec §1: "no branch may leave its
/// dep effect behind as a side effect").
struct BlobDepRecord
{
    ObjectKind kind = ObjectKind::Blob;
    std::optional<Token> token;                       /// nullopt = live-source evidence
    uint64_t size = 0;
    bool adopted = false;                             /// true only for `adoptEvidence`

    bool operator==(const BlobDepRecord &) const = default;
};

/// Which branch of the upload primitive admitted a blob. Carried out of `uploadBlobDetached` so the
/// merge/fan-out layers (and tests) can assert the branch taken without inferring it from the dep.
enum class BlobUploadOutcome
{
    DeduplicationCacheHit,      /// dedup cache said present; HEAD-first confirmed a live incarnation; adopted
    HeadHit,            /// size-triggered HEAD-first found a present live incarnation; adopted
    HeadMissAdopted,    /// the write-once create 412'd on a live incarnation (or a racing writer's); adopted
    FreshUpload,        /// the write-once conditional create streamed a fresh local body
    StagingPromoted,    /// the write-once conditional server-side copy promoted an S3 staging object
    ResurrectedLocal,   /// a condemned incarnation displaced by a fresh local `putOverwrite`
    ResurrectedS3,      /// a condemned incarnation displaced by a fresh server-side copy from staging
};

/// Public, CAS-owned input to `uploadBlobDetached`; the transaction's private dep representation is not
/// exposed. `source` mirrors `putBlob`'s re-readable `BlobSource` (local streaming / S3 staging copy).
/// `declared_size` is the value the fan-out layer groups and conflict-checks on; it mirrors
/// `source.size`, which stays the authority for the per-attempt streaming byte check.
struct BlobUploadRequest
{
    BlobRef ref;
    BlobSource source;
    uint64_t declared_size = 0;
};

/// Complete result of one detached upload: the addressed ref, the COMPLETE dep effect the upload
/// contributes (no side channel), and the branch outcome.
struct BlobUploadResult
{
    BlobRef ref;
    BlobDepRecord dep;
    BlobUploadOutcome outcome = BlobUploadOutcome::FreshUpload;
};

/// Hash `payload` with `algo` using the same convention as the streaming blob writer and return the complete
/// `BlobRef` identity. The algorithm travels with the digest; callers must not reconstruct a blob identity from
/// a bare digest or from an independently supplied digest width.
BlobRef poolContentHash(BlobHashAlgo algo, std::string_view payload);

/// Coordinates one part write from manifest staging through blob admission and ref publication. The transaction
/// owns the in-memory dependency set and the identities of manifests staged by this build; `Pool` owns the
/// durable object store and ref-log operations. A transaction is normally used by one writer thread, while
/// `cancelForNamespaceRemoval` may set its cancellation flag from the namespace-removal thread.
///
/// The durable write order is `stageManifest` → `precommitAdd` → `putBlob` → `promote`. The precommit edge
/// must be durable before any existing blob incarnation is adopted, because the edge is what protects that
/// incarnation from GC while this build is in flight. `promote` then moves the same manifest owner binding
/// atomically from precommit to committed. A failed or abandoned transaction never resumes after a process
/// restart; its precommit is removed by the live owner or a fenced successor, and GC reclaims the resulting
/// debris only after the corresponding ref-log decrements are durable.
class PartWriteTxn
{
public:
    /// Start a build and emit its durable in-flight-build attribution. The identity arguments identify the
    /// build for ownership and GC fencing; `info_` is retained as immutable build context.
    PartWriteTxn(PoolPtr store_, UInt128 build_id_,
          uint64_t build_seq_, uint64_t epoch_, PartWriteInfo info_);

    /// Retire this build's sequence so the pool's active-build watermark can advance. This is idempotent when
    /// `promote` or `abandon` already retired the sequence, and also covers destruction during unwinding.
    ~PartWriteTxn();

    /// Every upload attempt mints a fresh random `incarnation_tag`.
    /// New content: streaming PUT If-None-Match:*; on PreconditionFailed ⇒ the cold-reuse rule
    /// (observe current token; condemned ⇒ uploadFromSource — re-upload from the writer's source
    /// bytes; else adopt — free).
    /// Ordering: `putBlob` is always called after `precommitAdd` (the wiring order is
    /// `stageManifest` → `precommitAdd` → `putBlob` → `promote`). Its
    /// ADOPT paths observe an existing incarnation, so they are safe only under this build's durable
    /// precommit closure — enforced by a fail-closed throw (LOGICAL_ERROR, not a `chassert`, which is
    /// compiled out in release) in observeAndAdmit. A FRESH upload before precommit is legal
    /// (newborn-debris watermark), but production never does it.
    PutBlobResult putBlob(const BlobRef & ref, BlobSource source);

    /// Transaction-DETACHED upload primitive (spec §1). Runs the SAME durable, ordering-sensitive pool
    /// effects `putBlob` runs — the HEAD-first dedup gate, the write-once conditional create, condemned
    /// resurrection (INV-1: never GET a condemned object), the freshness-meta `Clean` transition,
    /// dedup-cache reads/inserts, event emission, ProfileEvents — but folds NOTHING into `build`
    /// (`deps`), returning the complete dep effect + branch outcome as a value instead. It is therefore
    /// safe to run off the owning writer thread while `PartWriteTxn` stays single-writer for `build`.
    /// `putBlob` = this primitive + a single-result `deps` fold on the calling thread.
    BlobUploadResult uploadBlobDetached(const BlobUploadRequest & req) const;

    /// Applies a fan-out's `uploadBlobDetached` results into `deps` on the CALLING thread, after the
    /// fan-out's join -- so this is an owning-writer-thread API exactly like `putBlob`, and MUST NOT be
    /// called from a pool task. Merge failure must not leave a partially merged build (spec §1): every
    /// result is prevalidated FIRST -- completeness (a result's dep must carry a token; every branch of
    /// `uploadBlobDetached` sets one, so a tokenless result is a caller bug, not a valid dep-only-evidence
    /// state, which is folded through `adoptEvidence` instead) and duplicate-grouping consistency
    /// (two results for the same `BlobRef` must carry an identical dep record; a conflict -- most
    /// commonly a conflicting size -- means the fan-out's one-task-per-unique-ref invariant was
    /// violated) -- BEFORE any result is applied. Application then runs against a COPY of `deps` (a
    /// "build"), so a mid-application exception (including one raised by `setMergeHookForTest`'s hook, or
    /// a `bad_alloc` from map-node allocation) never touches the live `deps`; the copy is committed by a
    /// single no-throw `swap` only after every result has applied. The build is therefore either fully
    /// merged or byte-for-byte untouched -- never partially merged.
    void mergeBlobUploadResults(std::span<const BlobUploadResult> results);

    /// Test-only fault-injection seam (inert in production): invoked after each result has applied to
    /// the in-progress merge copy, with the count of results applied so far (1-based). A hook that
    /// throws (e.g. to model a `bad_alloc` mid-merge) aborts `mergeBlobUploadResults` before its final
    /// swap, so the live `deps` stays untouched -- the seam exists to prove that all-or-nothing property
    /// under injected failure at every application point, not just the first or the last.
    void setMergeHookForTest(std::function<void(size_t applied_so_far)> hook) { merge_hook_for_test = std::move(hook); }

    /// Test-only DEEP snapshot of this build's recorded deps, keyed by `BlobRef`. A plain copy of the
    /// private `deps` map -- lets a test assert the whole build is byte-for-byte untouched after a
    /// rejected or aborted merge, rather than probing one ref at a time via `depIsTokened`.
    std::map<BlobRef, BlobDepRecord> depsSnapshotForTest() const { return deps; }

    /// Return whether this build holds a TOKENED Blob dep for `ref` (`putBlob` ⇒
    /// tokened) versus a tokenless evidence dep (`adoptEvidence` ⇒ tokenless)? False also when this
    /// build has no dep for the ref at all.
    bool depIsTokened(const BlobRef & ref) const;

    /// Record a TOKENLESS evidence blob dep directly from a `ManifestEntry` — no HEAD or backend
    /// call. Lets staging adopt sites record the dep by hash without asserting presence before
    /// precommit; the promote gate observes/resurrects it post-precommit. Inline entries record nothing.
    void adoptEvidence(const ManifestEntry & entry);

    /// Record a TOKENLESS pending blob dep by ref (without a HEAD) for a blob whose bytes are staged locally and
    /// will be putBlob'd post-precommit. putBlob later overwrites it with the tokened dep on upload.
    void recordPendingBlobDep(const BlobRef & ref, uint64_t size);

    /// Mint a root-local part `ManifestId`, write its body under
    /// `cas/manifests/<ns>/<writer_epoch>/<build_sequence>/000001.zst` via the pool's shared request
    /// controller. It uses budgeted attempts with resolve-before-reissue and performs no preliminary HEAD,
    /// because `manifest_ordinal` is monotone within this build. It enforces manifest-size caps before the body
    /// write returns and therefore before any owner transition is published. The body is not retained after a
    /// successful write; on retry the caller re-stages from source. Every call uses a fresh manifest ordinal.
    /// The id is recorded for best-effort `abandon` cleanup.
    ManifestId stageManifest(std::vector<ManifestEntry> entries);

    /// Add this transaction's precommit owner intent; there is no `_precommits` namespace. One
    /// `appendRefOps` call appending an OwnerTransition `RefOp` (new_binding = {Precommit,
    /// final_ref_name, id.ref}) to final_ref_name's ref-log entry, so the later promote is an atomic
    /// owner move over that same entry. Needs NO body-exists HEAD as a safety authority: GC and
    /// promotion handle a missing precommit manifest body by failing closed (a missing-body precommit
    /// is a non-activating, non-promotable intent).
    ///
    /// A3 mint-tightening: `id` must either be one THIS transaction minted via `stageManifest`, or
    /// already be `final_ref_name`'s current committed manifest (the idempotent re-drive -- see the
    /// closure in the .cpp). Any other id names a manifest this transaction never staged and does not
    /// currently own -- most commonly one a DIFFERENT, since-abandoned/dropped transaction once staged
    /// -- and granting it fresh ownership here would let a later exact-`ManifestRef` equality check
    /// (the relink confirm) compare true against a token whose blobs may already be reclaimed (an ABA).
    /// Rejected with `LOGICAL_ERROR`: this is a programming-invariant violation, never an operational one.
    void precommitAdd(const RootNamespace & target_ns, const String & final_ref_name, const ManifestId & id);

    /// Atomically promote the precommit to the committed ref with one `appendRefOps` call on the target ref's
    /// ref-log entry.
    ///  1. tokened leaves are already protected by the durable precommit edge, so no writer-side retired-view
    ///     refresh is needed;
    ///  2. stream-read the precommit manifest body; validate RefMatchesBody / ManifestNamespaceMatches;
    ///  3. the NON-tokened blob leaves (tokened leaves are edge-protected, not re-checked): a committed-source
    ///     adoptEvidence leaf is TRUSTED via the durable manifest edge — NO per-file HEAD/loadMeta probe (§4
    ///     manifest-trust: the live source pins the blob, in-degree >= 1); a genuinely
    ///     absent adopted blob is an invariant violation caught by fsck, not here;
    ///  4. a body-absent precommit or a lost owner-liveness ⇒ ABORTED; a non-tokened, non-adopted leaf (no
    ///     tokened dep and no committed-source adopt — a staging bug) ⇒ LOGICAL_ERROR (fail closed);
    ///  5. atomically replace precommit(build_id) owner with committed(final_ref_name) owner by appending
    ///     ONE pure-move `RefOp` (old_binding={Precommit,final_ref_name,T}, new_binding={Committed,final_ref_name,T},
    ///     same manifest_ref T) and setting refs[final_ref_name];
    ///  6. promotion never emits blob deltas. A missing-body precommit
    ///     is non-activating and was rejected at step 4 (the writer re-stages with a fresh ManifestId).
    ///
    /// PROCESS-RESTART INVARIANT: a `PartWriteTxn` is a plain in-memory C++ object owned by the wiring's
    /// `ContentAddressedTransaction` — it is NEVER persisted and NEVER resumed across a process
    /// restart. There is no "replay a precommit" code path anywhere in the core: `promote` is called
    /// synchronously, in-process, strictly AFTER every referenced blob's `putBlob` (which for S3
    /// staging drives `promoteStaged`'s conditional copy) has already returned successfully. If the
    /// process exits between `precommitAdd` and `promote` (e.g. between staging a blob and its
    /// server-side-copy promote completing), the `PartWriteTxn` object is simply lost with it: nothing ever
    /// "wakes up" that precommit and finishes promoting it. The precommit's owner binding is left as a
    /// dead intent in the ref log and is REMOVED (never promoted) by an exact precommit-removal ref-log
    /// transaction -- the current writer's own `PartWriteTxn::abandon` if it is still mounted, otherwise a
    /// fenced successor's stale-precommit sweep. `GC` folds the resulting
    /// `-1` manifest edge but never detects or removes a dead precommit itself. So the
    /// hazard — "promote a precommit whose copy did not complete" — has no code
    /// path to occur through: promotion is not a recoverable/resumable operation, only a synchronous
    /// one that either completes within the writing process or never happens at all.
    /// `allow_repoint` opts into an intended
    /// repoint of a committed ref that already names a DIFFERENT manifest -- a standalone write/remove
    /// on an already-committed part (the committed-publish machinery's missing piece; the promote guard's
    /// own error text already named it: "use republishRef for an intended repoint"). Default `false`
    /// preserves the existing unique-ref guard byte-for-byte: a committed ref naming a different manifest
    /// still throws ABORTED. With `true`, the guard is skipped and the old committed binding is retired
    /// in the SAME ref-log record as the ordinary precommit->committed promotion, plus a
    /// `CasEventType::RefRepoint` audit event -- every effective repoint is loud by construction.
    ///
    /// Returns `created`: whether `final_ref_name` had NO committed row before this call. Derived
    /// INSIDE the `appendRefOps` builder (the same in-closure-output pattern the builder already uses
    /// for `repoint_old`) as `!state.getCommitted().contains(final_ref_name)`, evaluated once at the
    /// top of the closure -- so it is correct on every path the closure can take: a first-time bind
    /// (true), a repoint of an existing binding (false), and the idempotent re-promote no-op (false,
    /// since a committed row for this exact manifest already existed). `build_ops` runs at most once,
    /// on the flush leader, so this single evaluation is authoritative.
    bool promote(const RootNamespace & target_ns, const String & final_ref_name, UInt128 build_id, const ManifestId & id, bool allow_repoint = false);

    /// Retire the build sequence so the GC watermark floor can advance; staged manifest debris is best-effort
    /// cleaned, with the orphan sweep as the durable backstop.
    void abandon();

    /// Called by `Pool::dropNamespace` for every in-flight build once its namespace-removal transaction is
    /// durable. If
    /// this build's owning namespace equals `removed_ns`, mark it cancelled so every further operation
    /// fails closed at `requireAlive` (ABORTED); a build in any other namespace is left untouched.
    /// Cross-thread safe: reads only the immutable `info` (the owning namespace) and stores ONE atomic --
    /// it touches no other member, so the build's own thread may keep running concurrently. Staged debris
    /// is cleaned best-effort when the build's own thread later runs `abandon` (or via the GC backstop).
    void cancelForNamespaceRemoval(const RootNamespace & removed_ns);

    UInt128 buildId() const { return build_id; }
    /// The strictly increasing per-process sequence used by the active-build watermark.
    uint64_t buildSeq() const { return build_seq; }

    /// Lifecycle of THIS build's create-precommit `owner_transition`.
    ///
    /// It is a state and not a bool because `appendRefOps` has three outcomes and only two of them are
    /// knowledge: an `Unresolved` `PUT` MAY HAVE LANDED (`CasRefLedger.cpp`, the `Unresolved` arm), so a
    /// `precommitAdd` that threw can still have made this build's manifest a LIVE precommit owner. The
    /// intent is therefore recorded BEFORE the ambiguous append -- the same "preconstruct before the
    /// PUT" discipline the ref lane's wedge uses -- and settled either way when it returns.
    ///
    ///   `NotAttempted` -- `precommitAdd` never reached its append; nothing can be owned, and this
    ///                     build's staged manifest bodies are ordinary writer debris.
    ///   `Uncertain`    -- the append was ATTEMPTED and its outcome is unknown. The build OWES the
    ///                     precommit removal exactly as if it were durable, and its manifest body is
    ///                     NOT writer-deletable: it may be a live precommit input whose deletion would
    ///                     strand GC's fold barrier.
    ///   `Durable`      -- the append returned, so the precommit binding is this build's.
    ///   `Settled`      -- the owed terminal operation (`promote` or `abandon`) has discharged the duty.
    ///                     The body stays non-deletable: after a promote it belongs to a committed ref,
    ///                     and after an abandon it is GC's to reclaim after the sealed decrement.
    enum class PrecommitState : uint8_t { NotAttempted, Uncertain, Durable, Settled };
    PrecommitState precommitState() const { return precommit_state; }

    /// Lifecycle of THIS build's promote (precommit -> committed) append, recorded for the same reason
    /// and in the same way as `precommitState`: a promote whose append threw may still have COMMITTED
    /// the ref. The distinction is load-bearing one layer up -- the interserver relink maps a promote
    /// that definitely did not commit to "fetch the bytes from the same source instead", and doing that
    /// after a commit that actually landed would publish the same part twice.
    ///
    ///   `NotAttempted` -- promote failed (or was never called) strictly BEFORE its append: a rejected
    ///                     validation, an absent body, a lost owner liveness. Proof of the negative.
    ///   `Uncertain`    -- the append was attempted and its outcome is unknown. Conservative: the ref
    ///                     lane collapses `DefiniteFailure` and a pre-attempt refusal into the same
    ///                     retry-later class as a genuinely ambiguous `PUT`, so both are reported here
    ///                     as uncertain. That costs a retry, never correctness.
    ///   `Durable`      -- the append returned; the ref is committed.
    enum class CommitState : uint8_t { NotAttempted, Uncertain, Durable };
    CommitState commitState() const { return commit_state; }

private:
    /// Keyed on the full `BlobRef` pair (algorithm + digest), because a bare digest is not a blob identity.
    /// This remains an ordered `std::map` (not `unordered_map`): `BlobRef` already provides `operator<=>`, so
    /// no hasher is needed here. `BlobRefHash` is for unordered dedup-cache/set consumers elsewhere. The
    /// dependencies are blob-only, so `ObjectKind` is not part of the key.
    using DepKey = BlobRef;

    /// Apply the cold-reuse rule: HEAD the key; absent ⇒ FILE_DOESNT_EXIST;
    /// condemned-at-current-token ⇒ throw ABORTED (caller must re-upload from its own source bytes);
    /// else RETURN the adopt dep record (current token, admitted logical size). Build-neutral: it folds
    /// nothing into `deps` (its callers compose the returned record into a `BlobUploadResult`).
    BlobDepRecord observeAndAdmit(ObjectKind kind, const BlobRef & ref, const String & key) const;
    /// Overload for callers that already hold a fresh, present HeadResult for `key` (the putBlob
    /// HEAD-before-PUT path), avoiding a redundant second HEAD. `hr.exists` MUST be true.
    BlobDepRecord observeAndAdmit(ObjectKind kind, const BlobRef & ref, const String & key, const HeadResult & hr) const;
    /// INV-1 (revival-from-source): revive a condemned or absent object by re-uploading from the writer's
    /// OWN re-readable source without reading the dying object (no backend().get). On a Native backend the
    /// source is STREAMED into the put sink (header + `source.open`); the emulated backend materializes
    /// one body at a time inside `Backend::resurrect`;
    /// `source.open` may be re-invoked on each conditional-write attempt (it re-reads the staged
    /// temp file / re-emits the captured String), so it is taken by const ref and not consumed. Build-neutral:
    /// RETURNS the complete `BlobUploadResult` (dep + branch outcome); it folds nothing into `deps`.
    BlobUploadResult uploadFromSource(ObjectKind kind, const BlobRef & ref, const String & key, const BlobSource & source) const;

    /// The build's owning root namespace, derived from PartWriteInfo::intended_ref ("ns/ref" — the ref is the
    /// last `/`-segment; the namespace is everything before it). Sets a manifest body's root_namespace_id.
    RootNamespace manifestNamespace() const;

    /// Reject operations after `abandon`, namespace cancellation, or writer-epoch fencing. These checks happen
    /// before backend work so a stale transaction cannot publish a new owner or stage more debris.
    void requireAlive() const;
    /// Best-effort exact-token delete of THIS build's staged `_manifests` debris; the precommit body (if
    /// any) is SKIPPED -- left for GC's delete-after-sealed-decrements. Never throws (the namespace-scoped orphan
    /// sweep is the durable backstop). Shared by the normal and the namespace-removal-cancelled `abandon`
    /// paths; only ever called on the build's OWN thread.
    void cleanupStagedManifestDebrisBestEffort();

    /// A leaf is trusted at promote iff this build holds a TOKENLESS dep recorded by
    /// `adoptEvidence` (adopted=true) — a committed-source evidence adopt. The live source pins the blob
    /// (in-degree >= 1, not condemnable) and this build's precommit edge is durable, so the durable manifest
    /// edge is the liveness evidence: no HEAD, no loadMeta, no copy-forward. A tokened dep (edge-protected,
    /// handled by `depIsTokened`), a tokenless PENDING-upload dep (adopted=false), or NO dep at all (a
    /// staging bug) is NOT trusted — it fails closed. The single gate for the promote non-tokened leaf.
    bool isTrustedAdopt(const BlobRef & ref) const;

    PoolPtr store;
    UInt128 build_id{};
    uint64_t build_seq{};                                 /// per-process monotone sequence
    uint64_t epoch{};                                     /// owning Pool's process_epoch
    uint32_t next_manifest_ordinal = 1;                   /// per-build monotone manifest ordinal
    PartWriteInfo info;
    bool alive = true;
    /// Set by `cancelForNamespaceRemoval` from `Pool::dropNamespace`'s thread
    /// once this build's owning namespace is durably removed. Atomic because it is WRITTEN cross-thread
    /// and READ by `requireAlive` on the build's own thread. Once cancelled, every further op fails closed.
    std::atomic<bool> cancelled{false};
    PrecommitState precommit_state = PrecommitState::NotAttempted;
    CommitState commit_state = CommitState::NotAttempted;

    /// The precommit's target, recorded by `precommitAdd` BEFORE its append and never cleared
    /// afterwards. Two consumers with different lifetimes read them: `abandon` needs them while the
    /// duty is owed (`Uncertain`/`Durable`), and `cleanupStagedManifestDebrisBestEffort` needs them for
    /// as long as this object lives, because a body that was ever the target of a precommit attempt
    /// must never be writer-deleted -- which is why the terminal operations move `precommit_state` to
    /// `Settled` rather than resetting these back to their unset values.
    RootNamespace precommit_target_ns;
    String precommit_final_ref;
    ManifestRef precommit_manifest;

    std::vector<ManifestId> staged_manifests;             /// for best-effort abandon cleanup

    /// Every `ManifestId` this transaction has minted via `stageManifest`, checked by `precommitAdd`:
    /// an unowned id may enter ownership only from the transaction that freshly staged it. Kept separate from
    /// `staged_manifests` above -- that vector's role (best-effort abandon cleanup) is unrelated and
    /// could change independently; this set exists purely as the ABA barrier's identity check, so it
    /// stays correct even if the cleanup vector's contents or lifetime ever change.
    std::set<ManifestId> staged_manifest_ids;

    std::map<DepKey, BlobDepRecord> deps;                 /// dependencies recorded by this build (blobs only)

    /// Backing state for `setMergeHookForTest`; empty (no-op) in production.
    std::function<void(size_t)> merge_hook_for_test;
};

}
