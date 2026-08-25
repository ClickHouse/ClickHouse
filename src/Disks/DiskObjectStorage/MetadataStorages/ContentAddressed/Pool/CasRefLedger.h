#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestControl.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <string_view>
#include <vector>

namespace DB::Cas
{

/// Controls whether `resolveRef` emits its `RefResolve` audit event. `Emit` (the default) preserves
/// today's behavior for every existing caller (`listRefs`, `dropRef`, GC, and ordinary reads).
/// `Deferred` is for a caller that itself decides, after inspecting the resolve outcome, whether the
/// access as a whole did real resolve work worth auditing — see `CachedPartFolderAccess::getView`,
/// which re-emits the identical event on every path except a warm view-cache hit that served without
/// re-validating anything.
enum class ResolveAudit : uint8_t { Emit, Deferred };

/// The complete state of one table's append lane. It is guarded by `RefTableRuntime::state_mutex`;
/// there is no independent apply marker or durable-id floor whose combinations form a second,
/// implicit state machine.
///
/// `Ready` is the only state that admits a new append or certifies a cached row. `Writing` owns the
/// exact attempt before its first possible send. `Wedged` owns that same attempt after an ambiguous
/// result. `NeedsRecovery` means a transaction is known durable but cannot be installed in this cache;
/// it is a hard write and certification fence until replay completes. `Closed` records a successor's
/// epoch seal, and `Faulted` records foreign or internally inconsistent durable state.
enum class RefLaneState : uint8_t
{
    Ready,
    Writing,
    Wedged,
    NeedsRecovery,
    Closed,
    Faulted,
};

/// The answer of the relink confirm's gate 1 (`CasRefLedger::confirmExactRef`).
///
/// `Yes` is the only answer that AUTHORIZES anything, so it is the only one that must be earned: it is
/// returned exclusively when every rule of the lane snapshot holds. `Unknown` is the catch-all for
/// every ambiguity, and it is the answer this primitive is biased towards: a cold, evicted, recovering,
/// busy or non-`Ready` table answers `Unknown` rather than doing any work to find out.
///
/// `No` means "this runtime's committed row for that ref is not the manifest you asked about" -- and
/// nothing more. It is NOT a proof of the negative about the durable table, because the mount fence is
/// evaluated LAST (rule 6, deliberately): a mount that has already lost its fence, and whose view may
/// therefore be behind another writer's repoint, still answers `No` rather than `Unknown`. That is
/// sound only because `No` and `Unknown` are the SAME outcome for the caller -- both are
/// `SourceProofFailed` (spec §failure-taxonomy) -- so nothing is authorized by either. Do not build a
/// consumer that treats `No` as knowledge; only `Yes` is gated on the fence.
enum class ConfirmAnswer : uint8_t { Yes, No, Unknown };

/// Coordinates the writer-side ref-log and ref-table protocol for all namespaces in one mounted pool.
/// It owns the recovered whole-table cache, the flat-combining append lane and its unresolved-`PUT`
/// wedge, snapshot publication, stale-precommit cleanup, cache-budget eviction, and remount/shutdown
/// draining. `ref_queue_mutex` protects cache membership and queue leadership; each table's
/// `state_mutex` protects its decoded state and per-table lifecycle. Network I/O is deliberately performed
/// without holding `state_mutex`, so readers and other maintenance operations are not blocked by retries.
///
/// The ledger receives storage, configuration, event delivery, and retry-budget dependencies directly.
/// Mount state remains owned by `Pool` and is exposed through callbacks: the live writer epoch, append
/// fence, clocks, mutation gate, unclean-boundary observation, anomaly reaction, owner lifetime pin, and
/// cancellation of in-flight builds. The detached snapshot publisher uses the lifetime pin; no
/// `Pool &` back-reference is retained. `Pool` forwards its existing public operations to this component.
class CasRefLedger
{
public:
    CasRefLedger(
        BackendPtr backend_ptr,
        const Layout & layout_,
        RefLedgerConfig config_,
        const CasEventSink & event_sink_,
        CasRequestBudget cas_request_budget_,
        /// This mount's own `server_root_id` (spec §3's `CreatorFence`): the ledger mints its OWN
        /// creator fence out of this plus the live writer epoch and admission fence generation when it
        /// resolves a namespace's catalog life (`resolveNamespaceLife`) -- never injected as a callback,
        /// unlike the mount-state functions below, because it is a fixed identity for this ledger's
        /// whole lifetime (mirrors `CasMountRuntime`'s own by-value `server_root_id`).
        String server_root_id_,
        /// Monotonic mount clock used by the retry controller; it may be empty when the controller's
        /// default clock is appropriate.
        std::function<uint64_t()> controller_boot_ms_fn,
        /// Callbacks into mount and watermark state owned by `Pool`, bound for this ledger's lifetime:
        std::function<uint64_t()> live_epoch_fn_,
        std::function<bool()> fence_ok_fn_,
        /// The two fence-GENERATION primitives (`CasMountRuntime::fenceGeneration`/`checkFenceOrThrow`),
        /// injected exactly as `CasPlainObjects` takes them. `fence_ok_fn` above answers "may this mount
        /// write AT ALL, right now"; these two answer the different question an append lane must ask
        /// across an I/O window: "is this still the SAME mount incarnation that admitted the transaction
        /// I am about to act on?" A wedge captures the generation at admission and presents it back on
        /// every later retry and before every install, so a result that returns after a fence loss or a
        /// re-arm is inert for the superseded runtime instead of installing a stale view (spec §3,
        /// "the mount-fence generation is captured at admission and required on every slot-occupy and
        /// install").
        std::function<uint64_t()> fence_generation_fn_,
        std::function<void(uint64_t)> check_fence_or_throw_,
        std::function<uint64_t()> boot_ms_now_fn_,
        std::function<bool()> may_mutate_,
        std::function<void(const String &, const String &, const std::optional<String> &)> on_impossible_interference_,
        std::function<std::shared_ptr<void>()> pin_owner_,
        std::function<void(const RootNamespace &)> cancel_inflight_builds_);

    /// Recovers `ns` on first access and resolves `ref_name` from the authoritative cached table.
    /// The optional staleness argument remains for API compatibility; this mounted writer has no
    /// alternate shard cache, so the recovered table is always the view used for the result.
    /// `audit` defaults to `Emit` so every existing caller keeps emitting `RefResolve` unchanged;
    /// `Deferred` suppresses the emit for a caller that re-emits it conditionally itself.
    std::optional<Resolved> resolveRef(const RootNamespace & ns, const String & ref_name, bool allow_stale = false,
                                       ResolveAudit audit = ResolveAudit::Emit);

    /// Recovers `ns` on first access and returns every committed ref in canonical name order. Read-side
    /// maintenance may schedule snapshot publication and stale-precommit cleanup, but those actions do
    /// not change the returned committed view or make a read fail when maintenance has an uncertain `PUT`.
    std::map<String, Resolved> listRefs(const RootNamespace & ns);

    /// Recovers `ns` on first access and reports whether any committed ref name starts with `prefix`,
    /// without materializing the full ref map `listRefs` returns. An empty `prefix` means "any ref at
    /// all" and short-circuits on the first entry, so this is O(1) for that (dominant, emptiness-probe)
    /// case; a non-empty prefix still stays a no-allocation scan.
    bool hasAnyRefWithPrefix(const RootNamespace & ns, std::string_view prefix);

    /// Gate 1 of the relink confirm (spec §confirm-primitive): does `ref_name` in `ns` still name
    /// EXACTLY `manifest_ref` in this writer's committed view, read under a lane snapshot that cannot
    /// observe a stale cache?
    ///
    /// Performs ZERO object-store I/O: a cold, evicted or recovering table answers `Unknown` rather
    /// than recovering from storage, and no runtime is created as a side effect of asking. That is a
    /// contract, not an optimization -- the confirm is a read-only interserver query that a remote
    /// receiver drives, so it must never be able to make this writer do work.
    ///
    /// The rules are evaluated as one snapshot spanning both lane mutexes, in this order: table warm
    /// and resident; lane state `Ready`; exact committed-row equality; mount fence live last. Every
    /// ambiguity answers `Unknown` -- see `ConfirmAnswer`, and the .cpp for why the order and the
    /// two-mutex hold are what make a `Yes` a linearization point rather than a guess.
    ConfirmAnswer confirmExactRef(const RootNamespace & ns, const String & ref_name,
                                  const ManifestRef & manifest_ref) const;

    /// Appends the transaction that removes one ref and waits for its durable result. A failed append
    /// propagates its exception and does not apply the removal to the in-memory table.
    void dropRef(const RootNamespace & ns, const String & ref_name);

    /// Builds and appends a published_at_ms update for one ref. The mutator is invoked while
    /// constructing the transaction, and its changes become visible only after the append is durable.
    void updateRefPublishedAt(const RootNamespace & ns, const String & ref_name,
                          std::function<void(RefPublishedAtUpdate &)> mutator);

    /// Durably removes the complete namespace, including its current ref/precommit state, then performs
    /// the associated cancellation work. The catalog transition to `Removing` happens first; a failed
    /// terminal append after that leaves the namespace `Removing` (not `Live`) and propagates.
    DropNamespaceStats dropNamespace(const RootNamespace & ns);

    /// Decommission-only exact-life form. Pins recovery to the immutable catalog cut selected by the
    /// admin command, so a same-name replacement can never redirect destructive work.
    DropNamespaceStats dropNamespace(const NamespaceLifeId & life);

    /// The catalog life every one of this namespace's objects -- ref-layer AND namespace-file -- is keyed
    /// under, resolved ONCE per table-open and read from the cache afterwards. This is the WRITE-side
    /// resolution, and the ONLY one that CREATES: recovery's step 0 (`resolveNamespaceLife`) mints a
    /// life when the catalog names none, so the first namespace file a table ever writes births the
    /// namespace exactly as its first ref op would. A read or a removal must not use this -- see the
    /// sibling below for why that is a correctness matter and not a preference.
    NamespaceLifeId namespaceLife(const RootNamespace & ns);

    /// The life a READER (or a REMOVER) of this namespace's files must use, or `nullopt` when there are
    /// no readable files at all -- which is the same answer for a namespace that never existed, one
    /// still being created, and one whose catalog row is `Removing` or absent.
    ///
    /// IT NEVER CREATES A NAMESPACE, and that is the property the callers depend on rather than a
    /// side-effect of how it happens to be written: for an uncataloged namespace it answers from a
    /// catalog-only lookup and returns without recovering, so an `existsFile` or an
    /// `unlinkFile(..., if_exists = true)` against a never-opened table cannot admit an entry into the
    /// single pool-wide catalog object. See the implementation for how the guarantee survives a
    /// concurrent removal.
    ///
    /// ONE call, not a predicate plus a resolution, and that is deliberate: readability and the life are
    /// answered from the SAME `state_mutex` hold over the SAME recovered runtime, so a reader can never
    /// pair "readable" from one observation with a life from another. Returning `optional` rather than a
    /// life plus a bool also makes the unreadable case unusable by construction -- a caller that forgets
    /// to check gets no life to read with, instead of a plausible-looking one that names the wrong
    /// prefix. Absence is the fail-closed direction: only a KNOWN-readable namespace surfaces files, and
    /// every failure mode of the underlying reads throws rather than degrading to `nullopt`, so "no life"
    /// is only ever reached for a namespace whose absence is durable knowledge.
    std::optional<NamespaceLifeId> namespaceFilesLifeIfReadable(const RootNamespace & ns);

    /// Table-root cleanup-completeness probe: whether this logical namespace still has foreground
    /// removal work outstanding, or has never proven that none remains. `true` for `Creating`, every
    /// `Live` row (including zero refs and zero namespace files), and `Removing` before its terminal
    /// `remove_namespace` transaction is durable; `false` for no catalog row at all, or for `Removing`
    /// whose terminal is durably proven (a non-`Live` `RefLifecycle` WITH a `remove_txn_id` -- the same
    /// distinction `dropNamespaceImpl` makes before returning early, because a files-only life that
    /// never emitted a ref transaction can otherwise look `Removed` without ever having been removed).
    ///
    /// NEVER creates or mutates a catalog entry -- a probe is the wrong event to birth a namespace on --
    /// and NEVER answers `false` for an unreadable or ambiguous observation, or when THIS namespace's
    /// row changes under the probe: those cases throw (or answer present) instead, because the caller
    /// (`existsDirectory`) uses `false` as permission to physically remove a directory tree. Unrelated
    /// catalog churn between the probe's reads is permitted -- it says nothing about this row. A resident runtime already proven `Live` is trusted as an O(1) fast
    /// path so an ordinary warm table does not pay a `ref_catalog` fetch per probe; every other shape
    /// re-reads the exact catalog row.
    bool namespaceStillLogicallyPresent(const RootNamespace & ns);

    /// Called after a complete catalog observation proves an exact resident life absent or replaced.
    /// It does not
    /// destroy the runtime: existing callers that already captured the old physical life may finish
    /// with their stale-or-not-found contract. The name slot is detached by exact pointer identity; a
    /// later name-based touch may publish a distinct successor runtime.
    void invalidateRemovedCatalogLife(const NamespaceLifeId & life);

    /// Reconciles resident removal-closed runtimes against one complete catalog observation. An exact
    /// life absent from or replaced in the cut is invalidated and exactly detached; a matching current
    /// life stays closed. No runtime is reset or rebound.
    void reconcileCatalogCut(const CasRefCatalog::Snapshot & catalog_cut);

    /// Queues a mutation for flat-combining with compatible callers. `build_ops` runs at most once in
    /// the flush leader and must return operations without writing storage itself. The leader validates
    /// the complete batch, writes one ref-log object behind the append fence, and applies the batch to the
    /// cache only after a durable result; an unresolved conditional `PUT` wedges the table and blocks
    /// later appends until the same object is resolved or definitely rejected.
    RefTxnId appendRefOps(const RootNamespace & ns, MutationScope scope,
                         std::function<std::vector<RefOp>(const RefTableState &)> build_ops,
                         RootMutationOrigin origin, RootMutationKind kind,
                         bool skip_stale_precommit_sweep = false);

    /// Attempts one snapshot publication from a copy of the live state. The copy is made under
    /// `state_mutex`, the conditional `PUT` is performed without that mutex, and counters are adopted
    /// only when this attempt successfully publishes the captured snapshot.
    bool tryPublishSnapshotAndAdvanceCheckpointOnce(const RootNamespace & ns);

    /// Counts tables with an unresolved append `PUT`; the walk takes each table lock briefly and never
    /// waits for the network operation that caused a wedge.
    size_t wedgedRefLaneCount();

    /// Marks cached runtimes obsolete before a self-remount reopens the append fence. Leaders holding an
    /// orphaned runtime therefore fail closed instead of mutating state from the previous epoch.
    void quiesceRefTablesForRemount();

    /// The self-remount's CANCEL-OR-JOIN barrier over in-flight recoveries (spec §3: "self-remount
    /// cancels or waits out recovery before rearming"). Requests cancellation on every cached table,
    /// then WAITS until no recovery attempt is in flight anywhere, then clears the request. Returns only
    /// once that is true, so the caller may re-arm the mount fence knowing no recovery straddles it.
    ///
    /// Why this exists on top of the install recheck: the recheck protects the INSTALL, the barrier
    /// protects the WINDOW. A recovery admitted under the outgoing incarnation would otherwise keep
    /// issuing writes (its seal CAS-walk WRITES) across the whole re-arm, and each one would have to be
    /// caught individually at its own site; here it is stopped once, at the boundary, before the
    /// incarnation changes underneath it.
    ///
    /// The narrow window it does NOT close -- a recovery that starts after this returns and before the
    /// fence is re-armed -- is closed by the other two members of the same guard: the re-arm bumps the
    /// generation, so that recovery's `_ckpt` CAS and its install both refuse; and
    /// `quiesceRefTablesForRemount` (which the caller runs next, BEFORE the re-arm) publishes
    /// `superseded_by_remount`, which the walk polls at every I/O boundary.
    void cancelRecoveriesAndAwaitQuiescence();

    /// Closes admission, snapshots the current table set, and waits up to `wait_budget_ms` for queued
    /// mutations and leaders to finish. The check and enqueue paths share `ref_queue_mutex`, so no new
    /// mutation can appear after shutdown has taken its snapshot.
    bool drainRefLanesForShutdown(uint64_t wait_budget_ms);

    /// Performs a staged conditional create through the ledger's retry controller and append-fence
    /// predicate. Callers do not access either dependency directly, so every attempt observes the same
    /// mount admission rule.
    CasWriteOutcome stagingPutIfAbsent(std::string_view key, std::string_view bytes, Token * out_token);

    /// Same retry/fence policy as `stagingPutIfAbsent`, for a MUTABLE If-Match overwrite whose bytes
    /// are deterministic (safe for GET-based resolution).
    CasOverwriteResult stagingConditionalOverwrite(std::string_view key, std::string_view bytes, const Token & expected);

    /// Same retry/fence policy as `stagingPutIfAbsent`, for a MUTABLE marker where an existing
    /// DIFFERENT value at the key is a normal Conflict outcome, not corruption (see
    /// `CasRequestController::putIfAbsentControlledMutable`).
    CasOverwriteResult stagingPutIfAbsentMutable(std::string_view key, std::string_view bytes);

    /// Hooks required by `EventEmitter`: events are delivered to the injected sink when one is present.
    bool hasEventSink() const noexcept { return static_cast<bool>(event_sink); }
    void emitEvent(CasEvent && e) const { if (event_sink) event_sink(std::move(e)); }

    /// Replaces the retry controller's delay seam for deterministic tests; production callers leave it
    /// untouched.
    void setCasRetrySleepForTest(std::function<void(uint64_t)> sleep_fn);

    /// Test-only observability and fault-injection seams for recovery, wedges, cleanup, and publication.
    /// The counters expose recovery and publication progress; wedge methods create and inspect the
    /// unresolved-`PUT` state; cleanup methods expose sweep eligibility; publication methods expose
    /// settling, snapshot identity, and tail accounting. Every observer below is resident-only: it
    /// performs no catalog/backend I/O and never materializes or recovers a runtime.
    /// Returns the number of exact-read recovery restarts recorded for `ns`.
    uint64_t refRecoveryRestartsForTest(const RootNamespace & ns);
    /// Reports whether `ns` currently has an unresolved append `PUT`.
    bool refLaneWedgedForTest(const RootNamespace & ns);
    /// Returns the object key retained for the unresolved append of `ns`.
    String wedgedKeyForTest(const RootNamespace & ns);
    /// Returns the fence generation retained with the unresolved append of `ns` (0 when not wedged).
    uint64_t wedgedAdmittedGenerationForTest(const RootNamespace & ns);
    /// Installs a synthetic unresolved append for `ns` so callers can exercise resolution and blocking.
    /// `admitted_generation` defaults to the CURRENT fence generation, which is what a real wedge born
    /// now would carry; pass an explicit value to model a wedge admitted under an older incarnation.
    void forceWedgeForTest(const RootNamespace & ns, uint64_t writer_epoch, uint64_t ref_sequence,
                           const String & key, const String & bytes,
                           std::optional<uint64_t> admitted_generation = std::nullopt);
    /// Returns the seal that closed `ns`'s previous writer epoch -- the `prev_epoch_seal` its next
    /// sequence-1 append will carry (`nullopt` at genesis). See `RefTableRuntime::last_epoch_seal`.
    std::optional<RefTxnId> lastEpochSealForTest(const RootNamespace & ns);
    /// Installs `seal` as `ns`'s last epoch seal, standing in for the recovery CAS-walk that produces it
    /// in production (Task 6). Lets a writer-side test drive the ordinary post-transition append without
    /// a whole recovery.
    void setLastEpochSealForTest(const RootNamespace & ns, const std::optional<RefTxnId> & seal);
    /// Returns the append lane state of `ns` without forcing recovery.
    RefLaneState laneStateForTest(const RootNamespace & ns);
    /// Reports whether recovery or a prior incomplete sweep requires stale-precommit cleanup.
    bool needsStalePrecommitSweepForTest(const RootNamespace & ns);
    /// Waits until all background snapshot publications for `ns` have completed.
    void waitForSnapshotPublishSettleForTest(const RootNamespace & ns);
    /// Returns the number of background snapshot publications currently in flight for `ns`.
    int pendingSnapshotPublishesForTest(const RootNamespace & ns);
    /// Returns the newest snapshot id adopted by the cached runtime, if any.
    std::optional<RefTxnId> newestPublishedSnapshotIdForTest(const RootNamespace & ns);
    /// Whether `ns` currently has a RECOVERED cached runtime -- WITHOUT forcing a recovery. That is the
    /// whole point: the fail-closed tests assert that a refused recovery
    /// installed nothing, and a seam that recovered on demand would answer its own question.
    bool refTableRecoveredForTest(const RootNamespace & ns);
    /// Whether the self-remount barrier has PUBLISHED its cancellation request for `ns` (also without
    /// forcing a recovery). The barrier-blocks test needs this as a handshake: it must not release the
    /// parked recovery until the request is actually visible to it, or the recovery races past a flag
    /// that was set a moment too late and the test observes a completion instead of a cancellation.
    bool refRecoveryCancelRequestedForTest(const RootNamespace & ns);
    /// Returns the number of applied transactions newer than the adopted snapshot.
    size_t tailSinceSnapshotCountForTest(const RootNamespace & ns);
    /// Returns the number of committed entries in the mutable overlay, when the COW representation has one.
    size_t committedOverlayEntriesForTest(const RootNamespace & ns);
    /// Returns this table's LIVE precommit view: the exact `{ref_name, manifest}` owner bindings that
    /// `precommitAdd` creates and that `promote` (move to committed) or `abandon` (exact precommit
    /// removal) take away again. A leaked binding here is the same-epoch precommit leak the stale sweep
    /// -- prior-epoch-scoped -- can never reclaim, so it is what an abandon-path test must assert on.
    std::set<std::pair<String, ManifestRef>> livePrecommitsForTest(const RootNamespace & ns);
    /// Installs the test hook invoked immediately before the leader carves a compatible batch.
    void setRefPreCarveHookForTest(std::function<void()> hook) { ref_pre_carve_hook_for_test = std::move(hook); }

    /// Test-only fault seam for the two-phase carve/validation protocol (same `*ForTest` pattern as
    /// `setRefPreCarveHookForTest`). `flushRefBatch` fires the hook at each named point of the carve's
    /// plan/publish phases and of the per-item validation loop, so a test can inject `std::bad_alloc`
    /// and assert the append queue and the batch-validation `working` state stay intact. `PlanSeenRefs`,
    /// `PlanBatchGrow` and `PlanReserveOwned` fire in the non-mutating PLAN phase (nothing has been
    /// popped yet); `PublishPop` fires at the start of the no-throw PUBLISH phase; `ValidateFinalOps`
    /// fires once per admitted item, at the last throwing point before that item's effects are published
    /// into `working`/`final_ops`. `ChunkReseed` fires once at each chunk boundary of a chunked flush --
    /// immediately AFTER the just-full chunk committed durably (its survivors already completed) and
    /// BEFORE `working`/the trial-id high-water mark are reseeded from the now-live state; it is the
    /// injection point for the tenure-exception-containment contract (a throw here, or from the reseed
    /// itself, must leave the committed chunk's callers with their success). `PostDurableInstall` fires
    /// inside `commitRefChunk` after that chunk's `PUT` returned `Committed` and BEFORE the prepared
    /// candidate is installed into the live state -- the seam a test uses to prove that the region
    /// between "durable" and "recorded" can no longer strand a transaction (a throw injected there is
    /// the only way left to simulate the OLD post-durable apply failure, since the install itself is now
    /// allocation-free and cannot throw). `PostInstallPreAck` fires after the candidate swap and overlay
    /// materialization, outside the allocation-denied scope and state lock, but before any waiter is
    /// marked done or notified. It is the deterministic acknowledgment-order seam. Null in production.
    enum class CarvePhaseForTest
    {
        PlanSeenRefs,
        PlanBatchGrow,
        PlanReserveOwned,
        PublishPop,
        ValidateFinalOps,
        ChunkReseed,
        PostDurableInstall,
        PostInstallPreAck,
    };
    void setCarveHookForTest(std::function<void(CarvePhaseForTest)> hook) { carve_hook_for_test = std::move(hook); }

    /// Installs the negative control for the post-durable install region (see
    /// `install_region_probe_for_test`).
    void setInstallRegionProbeForTest(std::function<void()> probe) { install_region_probe_for_test = std::move(probe); }

    /// Installs the pre-tenure fault seam (see `ref_pre_tenure_hook_for_test`).
    void setRefPreTenureHookForTest(std::function<void()> hook) { ref_pre_tenure_hook_for_test = std::move(hook); }

    /// Pauses an ordinary append after it captured an exact runtime but before recovery or enqueue.
    void setAppendAfterRuntimeCaptureHookForTest(std::function<void()> hook)
    {
        append_after_runtime_capture_hook_for_test = std::move(hook);
    }

    /// Pauses `resolveRef` after it captured/recovered an exact runtime but before the result state lock.
    void setReadBeforeStateLockHookForTest(std::function<void()> hook)
    {
        read_before_state_lock_hook_for_test = std::move(hook);
    }

    /// Pauses a cold read after its catalog `GET` returned but before the observed life can be
    /// published into the local name slot.
    void setReadableCatalogAfterObservationHookForTest(std::function<void()> hook)
    {
        readable_catalog_after_observation_hook_for_test = std::move(hook);
    }

    /// Pauses `namespaceStillLogicallyPresent`'s cold path after its FIRST catalog `GET` but before any
    /// decision is made from it (including the "no row" revalidation's own second read).
    void setNamespacePresenceProbeAfterFirstReadHookForTest(std::function<void()> hook)
    {
        namespace_presence_probe_after_first_read_hook_for_test = std::move(hook);
    }

    /// Pauses `namespaceStillLogicallyPresent`'s `Removing` branch after it has proven the observed
    /// incarnation's terminal (the exact-life recovery/lock section is done) but before its
    /// post-terminal catalog revalidation read.
    void setNamespacePresenceProbeAfterTerminalProvenHookForTest(std::function<void()> hook)
    {
        namespace_presence_probe_after_terminal_proven_hook_for_test = std::move(hook);
    }

    /// Pauses a wedge retry after it captured the exact predecessor attempt but before the request
    /// controller is allowed to send a retry.
    void setWedgeBeforeSlotOccupyHookForTest(std::function<void()> hook)
    {
        wedge_before_slot_occupy_hook_for_test = std::move(hook);
    }

    /// Counts exact recovery-result publications.
    uint64_t recoveryInstallCountForTest() const
    {
        return recovery_install_count_for_test.load(std::memory_order_relaxed);
    }

    /// Pauses a direct snapshot publisher after it captured the runtime state but before its first
    /// durable effect. Used only to exercise predecessor deletion/rebirth races deterministically.
    void setSnapshotAfterCaptureHookForTest(std::function<void()> hook)
    {
        snapshot_after_capture_hook_for_test = std::move(hook);
    }

    /// Pauses a snapshot publisher after its body PUT and at the admission check immediately before
    /// each `_ckpt` CAS attempt. This is intentionally inside the retrying checkpoint primitive.
    void setSnapshotBeforeCkptCasHookForTest(std::function<void()> hook)
    {
        snapshot_before_ckpt_cas_hook_for_test = std::move(hook);
    }

    /// Returns the number of queued mutations for `ns` under the queue mutex.
    size_t refQueuePendingForTest(const RootNamespace & ns)
    {
        std::lock_guard<std::mutex> g(ref_queue_mutex);
        const auto it = ref_name_slots.find(ns.string());
        return it == ref_name_slots.end() ? 0 : it->second.current->pending.size();
    }

    /// Reports whether `ns` currently has an active append-lane leader (the baton). Under the queue mutex.
    bool refLeaderActiveForTest(const RootNamespace & ns)
    {
        std::lock_guard<std::mutex> g(ref_queue_mutex);
        const auto it = ref_name_slots.find(ns.string());
        return it != ref_name_slots.end() && it->second.current->leader_active;
    }

    /// Returns the number of callers currently waiting for `ns` recovery under its state mutex.
    uint64_t refRecoveryWaitersForTest(const RootNamespace & ns)
    {
        const auto rt = lookupRefTableRuntime(ns);
        if (!rt)
            return 0;
        std::lock_guard<std::mutex> g(rt->state_mutex);
        return rt->recovery_waiters_for_test;
    }

    /// Returns the number of namespace runtimes currently retained in the cache.
    size_t refTablesCachedCountForTest()
    {
        std::lock_guard<std::mutex> g(ref_queue_mutex);
        return std::count_if(ref_name_slots.begin(), ref_name_slots.end(), [](const auto & entry)
        {
            return static_cast<bool>(entry.second.current);
        });
    }
    /// Reports whether `ns` has a cached runtime whose recovery completed.
    bool refTableCachedForTest(const RootNamespace & ns)
    {
        const auto rt = lookupRefTableRuntime(ns);
        if (!rt)
            return false;
        std::lock_guard<std::mutex> g(rt->state_mutex);
        return rt->recovered;
    }
    /// Stable identity of the cached runtime object, or zero when no slot exists. This distinguishes
    /// explicit life invalidation from an eviction/remount that would make a rebirth test pass by
    /// constructing a different cache object.
    uint64_t refTableRuntimeIdentityForTest(const RootNamespace & ns)
    {
        std::lock_guard<std::mutex> g(ref_queue_mutex);
        const auto it = ref_name_slots.find(ns.string());
        return it == ref_name_slots.end() || !it->second.current ? 0 : it->second.current->runtime_id;
    }
    uint64_t refTableRuntimeAdmittedFenceGenerationForTest(const RootNamespace & ns)
    {
        std::lock_guard<std::mutex> g(ref_queue_mutex);
        const auto it = ref_name_slots.find(ns.string());
        return it == ref_name_slots.end() || !it->second.current ? 0 : it->second.current->admitted_fence_generation;
    }
    /// The physical life currently pinned in the cached runtime, without resolving or recovering it.
    std::optional<NamespaceLifeId> refTableLifeForTest(const RootNamespace & ns)
    {
        std::shared_ptr<RefTableRuntime> rt;
        {
            std::lock_guard<std::mutex> g(ref_queue_mutex);
            const auto it = ref_name_slots.find(ns.string());
            if (it == ref_name_slots.end())
                return std::nullopt;
            rt = it->second.current;
            if (!rt)
                return std::nullopt;
        }
        return rt->life;
    }
    /// Recovery-publication inventory accessors: the seeded per-table admission budgets, the recovered
    /// base snapshot's encoded body size and the tail-since-snapshot byte sum. Together with
    /// `newestPublishedSnapshotIdForTest`,
    /// `tailSinceSnapshotCountForTest`, `needsStalePrecommitSweepForTest` and the resolved state, they
    /// let a test assert EVERY `RecoveryResult` field the install seeds.
    uint64_t refSnapshotBudgetForTest(const RootNamespace & ns)
    {
        const auto rt = lookupRefTableRuntime(ns);
        if (!rt)
            return 0;
        std::lock_guard<std::mutex> g(rt->state_mutex);
        return rt->snapshot_budget;
    }
    uint64_t refRemovalBudgetForTest(const RootNamespace & ns)
    {
        const auto rt = lookupRefTableRuntime(ns);
        if (!rt)
            return 0;
        std::lock_guard<std::mutex> g(rt->state_mutex);
        return rt->removal_budget;
    }
    uint64_t refBaseSnapshotBytesForTest(const RootNamespace & ns)
    {
        const auto rt = lookupRefTableRuntime(ns);
        if (!rt)
            return 0;
        return rt->base_snapshot_bytes.load(std::memory_order_relaxed);
    }
    uint64_t refTailBytesSinceSnapshotForTest(const RootNamespace & ns)
    {
        const auto rt = lookupRefTableRuntime(ns);
        if (!rt)
            return 0;
        return rt->tail_bytes_since_snapshot.load(std::memory_order_relaxed);
    }
    /// Describes the one conditional `PUT` whose outcome is still uncertain for a table. It is owned by
    /// `Writing` or `Wedged` and by no other lane state; it stays installed until the object is confirmed
    /// durable and applied to the cache, or definitely rejected.
    ///
    /// The four fields together are the attempt's IDENTITY. THREE of them are compared before any later
    /// result is acted on -- `txn_id`, `bytes` and `admitted_fence_generation` (`resolveWedgeOnce`'s
    /// post-I/O recheck, which calls them "all three components of the identity"). The key is not
    /// compared because it adds nothing: `Layout::refLogKey` is a function of
    /// `(NamespaceLifeId, RefTxnId)`, and the namespace life is fixed for the runtime that holds the
    /// attempt, so within one runtime an equal `txn_id` already implies an equal key. (Should a runtime
    /// ever span two incarnations of one namespace -- it does not today -- that implication is what
    /// would need re-checking, not the comparison list.)
    ///
    /// Comparing the id alone -- or the admission generation alone -- is the aliasing bug the phase-0
    /// model found: two attempts of the same table can carry the same id under the same generation and
    /// describe DIFFERENT bytes, and installing one attempt's candidate because the other's key resolved
    /// is precisely the acked-then-lost class this every-attempt rule exists to close.
    ///
    /// Public because it is a member of `PreparedRefChunk`, which `prepareRefChunk` returns.
    struct RefAppendAttempt
    {
        RefTxnId txn_id;
        String key;
        String bytes;
        /// `CasMountRuntime::fenceGeneration()` as read at this transaction's ADMISSION -- the same
        /// critical section that snapshotted the state and derived the id, i.e. one atomic reading of
        /// "what this attempt was allowed to do". Every later `slotOccupy` retry is gated on THIS value
        /// (never the current one), and every install is preceded by presenting it back through
        /// `checkFenceOrThrow`: a retry admitted under a dead incarnation must send nothing, and a
        /// result that returns after a fence bump/re-arm must install nothing.
        uint64_t admitted_fence_generation = 0;
    };

    /// Everything `commitRefChunk` DECIDES before this chunk can have any durable effect, as one value.
    struct PreparedRefChunk
    {
        RefTableState candidate;         /// the snapshot with this chunk applied -- deliberately NOT materialized
        RefTxnId candidate_base_id;      /// greatest-applied of the state prepared FROM
        RefLogTxn chunk_txn;             /// includes INV-2's chain link
        RefAppendAttempt prepared_attempt;  /// COMPLETE: txn id, canonical key, sealed bytes, admitted generation
        /// Set iff this chunk births the namespace. The VALUE only: `commitRefChunk` still publishes it,
        /// because for a birth chunk that publish IS the first durable effect and preparation is by
        /// definition everything strictly before it.
        std::optional<RefCkpt> birth_contribution;
        /// Published after the log object commits and before this candidate may be installed or
        /// acknowledged. Every transaction advances `committed_through`; an epoch-seal transaction
        /// contributes the same id as `last_epoch_seal` in this one atomic checkpoint merge.
        RefCkpt commit_contribution;
    };

    /// The pure half of `commitRefChunk`: derive the candidate state, the transaction, the canonical key
    /// and sealed bytes, the complete attempt, a namespace birth's pre-log `_ckpt` contribution, and
    /// the post-log committed-frontier contribution -- all decided before anything can be durable.
    ///
    /// `static` on purpose, and it is load-bearing rather than stylistic: with no `this` no MEMBER backend
    /// is reachable, and `static` is what removes the injected one -- so "backend-free" is
    /// CHECKABLE instead of promised. That is what lets the protocol arithmetic be swept exhaustively
    /// with no store at all: `gtest_cas_ref_chunk_preparation.cpp` names no backend and constructs none,
    /// and no future edit inside this function can quietly reach for one and still compile there.
    ///
    /// `state` is CONSUMED: the caller snapshots `rt->state` under `state_mutex` and hands the snapshot
    /// over, so the candidate IS that snapshot rather than a second copy of it (the pre-extraction code
    /// made exactly one copy, under the lock, and this keeps it at one). The snapshot deliberately shares
    /// the live state's COW bases and is NOT materialized here -- folding a base-sharing state would
    /// rebuild the whole base, O(table) per chunk, which the install path exists to avoid.
    ///
    /// `id`, `chain_link` and `admitted_generation` are INPUTS, not derived here, because each traces back
    /// to the SAME critical section that snapshots the state (INV-1): `id` and `admitted_generation` are
    /// read inside that hold, and `chain_link` is derived just outside it from the `last_epoch_seal` read
    /// inside it. Deriving the id from a different instant than the state it is applied to would be
    /// deriving it from a different stream. The critical section therefore stays in `commitRefChunk`, and
    /// this is a pure function of its arguments -- what that one atomic reading saw, plus `layout`,
    /// `life` and `ops`.
    ///
    /// Every throw out of here is an ordinary PRE-durability rejection: no object exists, the cache is
    /// untouched, and the id is simply never used (the next attempt re-derives it from the same unchanged
    /// state). The reachable ones are a rejected apply and a failed seal; an allocation failure anywhere
    /// inside is the same class, and so is `checkNamespace`'s `BAD_ARGUMENTS` on the key-building path
    /// (unreachable for a mounted table, listed so the enumeration does not read closed). See the FAULT
    /// CLASS note on the definition for what changed about WHO catches them.
    ///
    /// `life` (Stage B, Task 4-C) is `RefTableRuntime::life` -- the caller's already-resolved catalog
    /// life, read under the SAME critical section as `id`/`admitted_generation` (a caller with no
    /// resolved life yet has no business preparing a chunk at all: `ensureRefTableRecovered` resolves
    /// it before this table is exposed as recovered).
    static PreparedRefChunk prepareRefChunk(const Layout & layout, const NamespaceLifeId & life,
                                            RefTableState state, const RefTxnId & id,
                                            const std::optional<RefTxnId> & chain_link,
                                            std::span<const RefOp> ops, uint64_t admitted_generation);

private:
    /// Injected storage and mount environment. The member order is part of construction/destruction
    /// behavior because the callbacks and references are used by the runtime owned below.
    Backend & backend;
    const Layout & layout;
    RefLedgerConfig config;
    const CasEventSink & event_sink;
    CasRequestBudget cas_request_budget;
    /// This mount's `server_root_id`; see the constructor parameter's doc for why it is a plain
    /// member rather than an injected callback.
    String server_root_id;
    std::function<uint64_t()> live_epoch_fn;
    std::function<bool()> fence_ok_fn;
    std::function<uint64_t()> fence_generation_fn;
    std::function<void(uint64_t)> check_fence_or_throw;
    std::function<uint64_t()> boot_ms_now_fn;
    std::function<bool()> may_mutate;
    std::function<void(const String &, const String &, const std::optional<String> &)> on_impossible_interference;
    std::function<std::shared_ptr<void>()> pin_owner;
    std::function<void(const RootNamespace &)> cancel_inflight_builds;

    /// Backoff sleep used by `ensureRefTableRecovered`'s transient-retry loop. Default is an
    /// interruptible slice-sleep (bails early if `fence_ok_fn` drops, e.g. on shutdown/lease loss);
    /// `setCasRetrySleepForTest` overrides it (a unit test injects a clock-advancing no-op).
    std::function<void(uint64_t)> recovery_retry_sleep_fn;

    /// One queued append caller. `build_ops` is invoked at most once by the flush leader and returns the
    /// caller's operations rather than mutating storage directly. Completion fields are synchronized by
    /// `ref_queue_mutex`.
    struct RefMutationItem
    {
        MutationScope scope;
        std::function<std::vector<RefOp>(const RefTableState &)> build_ops;
        RootMutationOrigin origin = RootMutationOrigin::Writer;
        RootMutationKind kind = RootMutationKind::Publish;
        /// Capability carried only by `dropNamespaceImpl` after it closed the exact runtime's positive
        /// lane. `RootMutationKind::DropNamespace` is descriptive metadata, not removal authority: the
        /// public generic append surface accepts that enum and must not thereby gain terminal rights.
        bool terminal_removal_authorized = false;
        bool done = false;                       /// guarded by ref_queue_mutex
        std::exception_ptr error;                /// guarded by ref_queue_mutex
        RefTxnId committed_id{};                  /// written by the leader before done = true
    };

    /// One coherent decoded `RefTableState` and append runtime for a namespace. It is recovered lazily
    /// and evicted only as a whole. `state_mutex` is separate from
    /// `ref_queue_mutex` (which only ever guards `pending`/`leader_active`) so a reader (resolveRef/
    /// listRefs) can observe `state` without contending with the flush leader's network round trip --
    /// the leader only holds `state_mutex` for the brief copy-out-before-validate and the
    /// apply-after-commit steps, never for the `putIfAbsentControlled` call itself.
    struct RefTableRuntime
    {
        /// An allocator can reuse an evicted predecessor's address for its successor. This monotone id
        /// is therefore the only diagnostic identity used to test exact detach/rebirth.
        const uint64_t runtime_id;

        /// Exact catalog identity accepted before publication. It is part of the runtime key and can
        /// never be rebound; a same-name successor is a different runtime object.
        const NamespaceLifeId life;

        RefTableRuntime(uint64_t runtime_id_, NamespaceLifeId life_, uint64_t admitted_fence_generation_)
            : runtime_id(runtime_id_)
            , life(std::move(life_))
            , admitted_fence_generation(admitted_fence_generation_)
        {
        }

        /// A later re-arm cannot retarget this runtime; it creates a distinct object instead.
        const uint64_t admitted_fence_generation;

        std::mutex state_mutex;
        bool recovered = false;
        /// Gates the recovery-seal I/O that runs outside `state_mutex`. A second caller waits on
        /// `recovery_cv` and rechecks `recovered` after the first caller finishes; otherwise it could
        /// perform a competing LIST/replay/seal and misclassify the losing conditional `PUT` as failure.
        /// Both this flag and the condition variable are guarded by `state_mutex`.
        bool recovery_in_progress = false;
        std::condition_variable recovery_cv;
        /// The self-remount cancellation request (spec §3: "self-remount cancels or waits out recovery
        /// before rearming"). Set by `cancelRecoveriesAndAwaitQuiescence` from the remount thread and
        /// polled by the recovery walk at EVERY I/O boundary; a recovery that observes it abandons its
        /// attempt having written nothing and installed nothing.
        ///
        /// ATOMIC, not `state_mutex`-guarded like its two neighbours, and that is the point: the
        /// canceller must be able to publish the request WITHOUT queueing behind the very recovery it is
        /// trying to stop. The condition variable is still the acknowledgment channel -- the canceller
        /// waits for `recovery_in_progress` to fall under the mutex -- so the request is lock-free and
        /// only the JOIN takes the lock.
        std::atomic<bool> recovery_cancel_requested{false};
        /// Test-only count of callers currently waiting for recovery; guarded by `state_mutex` so tests
        /// can observe that a concurrent caller reached the wait without depending on scheduling.
        uint64_t recovery_waiters_for_test = 0;
        RefTableState state;
        /// Exact attempt owned by `Writing` or `Wedged`, and retained by `NeedsRecovery` while an
        /// otherwise-admitted writer recovery must still adjudicate the precise durable successor it
        /// expected. It is cleared only when recovery installs its result or the attempt reaches a
        /// conclusive terminal outcome; losing these bytes would turn a foreign replacement into an
        /// indistinguishable ordinary recovery transaction.
        std::optional<RefAppendAttempt> append_attempt;
        RefLaneState lane_state = RefLaneState::Ready;
        /// The `EpochSeal` transaction that closed this namespace's PREVIOUS writer epoch -- exactly the
        /// `prev_epoch_seal` that this table's next sequence-1 transaction must carry (INV-2's grammar:
        /// required on sequence 1 of every epoch above the namespace's genesis, forbidden everywhere
        /// else). Guarded by `state_mutex`, and read in the SAME hold that derives the id, so the field
        /// and the sequence number it qualifies are one reading.
        ///
        /// `nullopt` means GENESIS, and it means it exactly: the namespace's recovered state contains no
        /// seal and its `greatest_applied.writer_epoch` is its own `life_epoch`, so its first
        /// transaction opens the stream rather than continuing one across a transition. A namespace born
        /// at global epoch 5 therefore appends `{5, 1}` with NO `prev_epoch_seal`; its first transition
        /// (5 -> 6) seals `{5, T+1}`, and the `{6, 1}` that follows carries that seal.
        ///
        /// THREE producers set it, and only one of them is the one `commitRefChunk` normally reads from.
        /// Recovery's CAS-walk installs the last seal of the chain it walked (Task 6): that is the
        /// production path, because a real epoch change arrives with a self-remount, which DISCARDS every
        /// cached runtime (`quiesceRefTablesForRemount`) and hands the fresh one its chain link through
        /// recovery. The other two are the conclusive-rejection arms -- `resolveWedgeOnce`'s and
        /// `commitRefChunk`'s -- which record a seal this runtime observed with its own eyes at a key it
        /// owns. Within THIS runtime that record is mostly introspection (the seal it saw closes the
        /// epoch it is still living in, so the stamp guard correctly suppresses it); it is written anyway
        /// because it is durable evidence nothing else holds, and because it becomes the right answer the
        /// moment the live epoch advances past the seal's. Nothing else writes it: a seal is durable
        /// evidence, never a local guess.
        std::optional<RefTxnId> last_epoch_seal;
        uint64_t recovery_restarts = 0;           /// diagnostic: exact-GET restarts forced by a vanished object
        /// Per-table admission budgets: raw configured limits minus the table's `4 + ns.size()` wire
        /// overhead and the fixed safety margin, computed once at recovery.
        uint64_t snapshot_budget = 0;
        uint64_t removal_budget = 0;

        /// Number and encoded-byte sum of applied transactions strictly newer than `newest_snapshot_id`.
        /// The live `state` is the next snapshot candidate, so no separate tail replay is retained.
        /// These counters are atomic because the cache-budget pass reads them while holding
        /// `ref_queue_mutex`, whereas append and publication paths hold `state_mutex`; relaxed ordering
        /// is sufficient because the counters do not publish any other state.
        std::atomic<uint64_t> tail_count_since_snapshot{0};
        std::atomic<uint64_t> tail_bytes_since_snapshot{0};
        std::optional<RefTxnId> newest_snapshot_id;
        /// Whole-table cache-weight bookkeeping for `enforceRefTableCacheBudget`.
        /// `base_snapshot_bytes` is the encoded body size of the snapshot
        /// at `newest_snapshot_id` (0 for a never-published table), captured for free from the
        /// recovered/published snapshot body -- refreshed only when that snapshot changes (recovery +
        /// each publish), never per mutation. The estimated resident weight is
        /// `base_snapshot_bytes + tail_bytes_since_snapshot`. `base_snapshot_bytes` is ATOMIC (relaxed)
        /// for the same cross-lock `total`-loop read as `tail_bytes_since_snapshot` above. `last_touch_tick`
        /// is the monotonic access stamp (`Pool::ref_table_access_tick`) used to evict least-recently-
        /// touched tables first; it is read only in the `use_count()==1`-gated candidate loop (no
        /// concurrent writer there), so it stays a plain `uint64_t`.
        std::atomic<uint64_t> base_snapshot_bytes{0};
        uint64_t last_touch_tick = 0;
        /// Set true by recovery; cleared when a sweep attempt is
        /// dispatched (so the sweep's own nested `appendRefOps` calls do not recurse) and PERMANENTLY
        /// only once an attempt completes VERIFIED CLEAN (a full pass over the live state found zero
        /// stale bindings). Any failed or partial attempt re-arms it (with the
        /// `precommit_sweep_backoff_*` cooldown), so a later read/mutation trigger retries until clean --
        /// a single attempt burned in the post-restart error window must not leave a dead incarnation's
        /// precommit bindings protected from GC forever on a long-lived mount.
        bool needs_stale_precommit_sweep = false;
        /// Per-table retry cooldown for the stale-precommit sweep (guarded by `state_mutex`),
        /// mirroring `publish_backoff_*` below: `until` is the boottime instant before which
        /// `maybeSweepStalePrecommits` refuses to re-attempt; `ms` is the current exponential interval
        /// (0 = no failure yet, or reset by the last verified-clean sweep).
        uint64_t precommit_sweep_backoff_until_ms = 0;
        uint64_t precommit_sweep_backoff_ms = 0;
        /// Test-observability + graceful settling for the background snapshot-publish dispatch (see
        /// `maybeScheduleSnapshotPublish`): the count of in-flight publish attempts for this table, and
        /// the condvar (guarded by `state_mutex`) a test waits on via `waitForSnapshotPublishSettleForTest`.
        std::atomic<int> pending_snapshot_publishes{0};
        std::condition_variable publish_settle_cv;
        /// Per-table snapshot-publish dispatch backoff (guarded by
        /// `state_mutex`). `publish_backoff_until_ms` is the boottime instant before which
        /// `maybeScheduleSnapshotPublish` refuses to dispatch; `publish_backoff_ms` is the current
        /// exponential interval (0 = not backing off), doubled on each consecutive non-Committed publish
        /// outcome and reset to 0 on the next durable publish.
        uint64_t publish_backoff_until_ms = 0;
        uint64_t publish_backoff_ms = 0;

        std::deque<std::shared_ptr<RefMutationItem>> pending;    /// guarded by ref_queue_mutex
        bool leader_active = false;                               /// guarded by ref_queue_mutex
        /// Set before the exact `Live -> Removing` catalog CAS and retained until that life is deleted.
        /// New positive mutations check it in the same queue critical section as admission; the one
        /// terminal `DropNamespace` item is the sole exception.
        bool removal_admission_closed = false;
        std::condition_variable cv;

        /// Published after GC commits the exact catalog deletion. The exact cache pointer is detached;
        /// old holders observe the flag and remain a predecessor, never a rebindable name handle.
        std::atomic<bool> catalog_life_invalidated{false};

        /// Set true when a self-remount detaches this runtime from the cache (`quiesceRefTablesForRemount`):
        /// the fresh incarnation re-recovers each table under the new epoch on next touch, so any leader
        /// still holding THIS (now-orphaned) runtime must fail closed instead of allocating an id / applying
        /// against its stale cache once the re-armed fence re-opens the gate. Stored with release BEFORE the
        /// remount re-arms the fence, so a lane that observes `mayMutate` true also observes this flag
        /// (release/acquire through the fence) -- there is no interleaving where a stale runtime both passes
        /// the fence and reads this flag false.
        std::atomic<bool> superseded_by_remount{false};
    };

    /// Appends against an exact runtime already captured by a lifecycle operation. This is the sole
    /// path used by namespace removal after its exact `Live -> Removing` catalog transition: resolving
    /// the logical name again there would either refuse the required terminal append or, after a
    /// replacement, retarget destructive work to the successor. Ordinary mutations enter through the
    /// public name-based `appendRefOps` wrapper and can never select this path.
    RefTxnId appendRefOpsOnRuntime(
        const RootNamespace & ns, const std::shared_ptr<RefTableRuntime> & rt, MutationScope scope,
        std::function<std::vector<RefOp>(const RefTableState &)> build_ops,
        RootMutationOrigin origin, RootMutationKind kind, bool skip_stale_precommit_sweep,
        bool terminal_removal_authorized);

    /// Publishes only from the exact runtime captured by the caller. Background dispatch carries this
    /// pointer across the thread hand-off; it must never resolve the logical name again and accidentally
    /// publish a same-name successor while settling the predecessor's in-flight accounting.
    bool tryPublishSnapshotAndAdvanceCheckpointOnceOnRuntime(
        const RootNamespace & ns, const std::shared_ptr<RefTableRuntime> & rt);

    /// The logical-name cache owns no lifecycle identity. It merely points at the runtime currently
    /// admitted for that name; exact retirement/remount clears this pointer while external holders may
    /// continue using the detached predecessor and fail closed against its immutable identity.
    struct RefNameSlot
    {
        std::shared_ptr<RefTableRuntime> current;
    };
    static constexpr size_t kMaxRefBatch = 1000;
    /// Recovery retries at most this many times when an object selected by LIST vanishes before GET.
    /// A failed recovery-seal `PUT` is separate: it leaves `recovered` false and the next touch starts
    /// a fresh LIST/replay/seal attempt rather than resuming this bounded vanish-retry loop.
    static constexpr size_t kRefRecoveryMaxRestarts = 3;
    /// How many times the CAS-walk may lose the SAME dead epoch's seal slot to a straggler before it
    /// fails closed. Not an arbitrary round number: INV-1's every-attempt rule permits AT MOST ONE
    /// in-flight conditional create per (table, writer), and there is one writer per mount, so an honest
    /// run needs ONE retry. The margin covers a dying writer whose lane held several attempts across
    /// distinct incarnations; anything beyond it is a store that keeps materializing objects underneath
    /// a walk, which is a fact to report, never one to keep looping on.
    static constexpr size_t kRefRecoveryMaxSlotAttemptsPerEpoch = 64;
    /// Fixed safety margin subtracted (alongside the per-table `4 + ns.size()` overhead) from
    /// the raw `ref_snapshot_max_bytes`/`ref_removal_max_bytes` hard limits before calling `admits`.
    static constexpr uint64_t kRefAdmissionSafetyMargin = 4096;

    /// `mutable` for `confirmExactRef`, the one CONST member function that needs the lane snapshot:
    /// taking a mutex to read consistently does not make the read a mutation.
    mutable std::mutex ref_queue_mutex;
    std::map<String, RefNameSlot> ref_name_slots;
    /// Monotonic access stamp for whole-table cache LRU eviction, bumped on every table touch and
    /// recorded in `RefTableRuntime::last_touch_tick`.
    std::atomic<uint64_t> ref_table_access_tick{0};
    std::atomic<uint64_t> next_ref_runtime_id{0};
    std::atomic<uint64_t> recovery_install_count_for_test{0};
    /// Latched by `drainRefLanesForShutdown` before it
    /// snapshots `ref_name_slots`/waits on each table's queue -- every ordinary ref mutation (`appendRefOps`)
    /// checks this under the SAME `ref_queue_mutex` critical section it uses to enqueue its item, so the
    /// check-and-enqueue is atomic with the drain's snapshot-and-wait: a caller either enqueues strictly
    /// before the drain observes this table (and the drain then waits for it), or observes this flag
    /// already true and never enqueues at all. No caller can land a NEW item after the drain has decided
    /// this table is idle.
    std::atomic<bool> shutting_down{false};

    /// The id `rt`'s next transaction carries (INV-1): `RefTableState::nextTxnId` of the table's OWN
    /// state under the live writer epoch. There is no counter behind this -- the id is a pure function
    /// of the state the transaction will be applied to, which buys two properties a pool-wide counter
    /// could not:
    ///   - each namespace's ids are dense `1..T` within one epoch, so a reader holding a table's log
    ///     ids can tell a COMPLETE stream from a truncated one without consulting anything else;
    ///   - an attempt that provably sent nothing consumes nothing: the state it derived from is
    ///     unchanged, so the next caller derives the SAME id and no hole is left behind.
    ///
    /// A post-durable install failure moves the lane to `NeedsRecovery`, so this function is not called
    /// again until replay has installed that durable transaction and advanced `greatest_applied`.
    ///
    /// The epoch component is the live mount incarnation's writer epoch, not the open-time
    /// `process_epoch`: a self-remount allocates a strictly-greater durable writer_epoch, so every ref
    /// transaction stamped after the remount sorts strictly ABOVE any (dead-incarnation or twin) log
    /// still durable under an older epoch. `RefTxnId` compares epoch first, so the epoch bump alone
    /// guarantees that a new log is never inserted at or below an already durable table log id.
    ///
    /// MUST be called with `rt.state_mutex` held, and the caller must apply the transaction to the SAME
    /// state it read here: an id derived from one snapshot of a table and applied to another is not that
    /// table's successor, and the apply-side density check would (correctly) reject it.
    RefTxnId allocateRefTxnId(const RefTableRuntime & rt) const
    {
        return rt.state.nextTxnId(live_epoch_fn());
    }

    /// The CAS-owned retry controller this Pool's ref-log writer path uses for every conditional
    /// log/snapshot `PUT` and uncertain-result resolution. It is also shared by the part-manifest
    /// write and mutable freshness-meta writes. The controller is stateless per call (immutable
    /// budget/clock/sleep — the sleep fn mutates only through the test-only seam, before traffic), so
    /// concurrent lanes and builds use the one instance safely.
    std::unique_ptr<CasRequestController> ref_request_controller;

    /// Test-only hook called before a compatible append batch is carved; null in production.
    std::function<void()> ref_pre_carve_hook_for_test;

    /// Test-only fault seam fired on the CALLING thread at the instant a queue caller takes append-lane
    /// leadership -- i.e. at the FIRST allocation that builds the leader's responsibility set, the last
    /// throwing point before the baton (`leader_active`) is published. A throw here must leave the lane
    /// idle (baton un-taken, the caller's item un-enqueued), never a permanently non-idle namespace with
    /// no live leader. Null in production. See `appendRefOps`.
    std::function<void()> ref_pre_tenure_hook_for_test;

    /// Test-only hook fired at each carve/validation phase point (see `CarvePhaseForTest`); null in
    /// production.
    std::function<void(CarvePhaseForTest)> carve_hook_for_test;

    /// Test-only probe fired inside either post-durable state install, under
    /// `DENY_ALLOCATIONS_IN_SCOPE`: the ordinary committed install and wedge-resolution adoption. The
    /// exact attempt is installed before sending, so `Unresolved` needs no post-I/O object install. It is the negative
    /// control for the guard: a probe that allocates must abort a debug build, which is what proves the
    /// region is armed and actually entered -- so a future edit that adds an allocating statement there
    /// cannot pass unnoticed. It is also the only way left to reach `NeedsRecovery` from one of these
    /// otherwise non-throwing regions. A test that installs a throwing probe must therefore disarm it
    /// after the region it targets, or every later install throws too. Null in production.
    std::function<void()> install_region_probe_for_test;
    std::function<void()> append_after_runtime_capture_hook_for_test;
    std::function<void()> read_before_state_lock_hook_for_test;
    std::function<void()> readable_catalog_after_observation_hook_for_test;
    std::function<void()> namespace_presence_probe_after_first_read_hook_for_test;
    std::function<void()> namespace_presence_probe_after_terminal_proven_hook_for_test;
    std::function<void()> wedge_before_slot_occupy_hook_for_test;
    std::function<void()> snapshot_after_capture_hook_for_test;
    std::function<void()> snapshot_before_ckpt_cas_hook_for_test;

    /// Non-materializing diagnostic/cache lookup. It never observes the catalog and never creates a
    /// name slot or runtime.
    std::shared_ptr<RefTableRuntime> lookupRefTableRuntime(const RootNamespace & ns) const;

    /// Publishes or returns one runtime for an exact catalog-observed life and admitted mount-fence
    /// generation. A conflicting attached identity is a stale observation and fails closed rather than
    /// retargeting either runtime.
    std::shared_ptr<RefTableRuntime> acquireRefTableRuntime(
        const NamespaceLifeId & life, uint64_t admitted_generation);

    /// Lookup-first non-minting read acquisition. A cold name consults the catalog and materializes only
    /// an exact `Live` life; absence/`Creating`/`Removing` returns no runtime.
    std::shared_ptr<RefTableRuntime> acquireReadableRefTableRuntime(const RootNamespace & ns);

    /// Lookup-first mutation acquisition. A cold name resolves or births the catalog life before
    /// constructing its runtime.
    std::shared_ptr<RefTableRuntime> acquireMutableRefTableRuntime(const RootNamespace & ns);

    /// Common removal implementation. `expected_incarnation` is present for the decommission-only
    /// exact-life overload and is checked before every lifecycle branch, including stalled creation.
    DropNamespaceStats dropNamespaceImpl(
        const RootNamespace & ns, const std::optional<UInt128> & expected_incarnation);

    /// Lazily recovers `ns` per spec §4: catalog life resolution (first table-open only) -> `_ckpt` ->
    /// exact-key base snapshot -> ARITHMETIC tail -> seal CAS-walk -> `_ckpt` CAS -> install. It does not
    /// expose the table as recovered until every dead epoch it discovered is durably closed; concurrent
    /// callers serialize across the whole unlocked I/O window through `recovery_in_progress`.
    void ensureRefTableRecovered(const RootNamespace & ns, RefTableRuntime & rt);

    /// Stage B (spec INV-3, §3): resolves `ns`'s catalog life -- ONCE per table-open, from inside
    /// `ensureRefTableRecovered`'s transient-retry envelope, never from a per-write path (a per-write
    /// catalog GET is a protocol-step addition and is vetoed). Three cases, closing over the catalog's
    /// own three-state grammar:
    ///   - no entry at all: this call is the namespace's first-ever opener. Mints one via
    ///     `CasRefCatalog::createNamespace` under a `CreatorFence` built from `server_root_id`,
    ///     `live_epoch` and `admitted_generation`;
    ///   - an entry already `Live`/`Removing`: adopts its incarnation directly
    ///     (`NamespaceLifeId::fromCatalogEntry`) -- `Removing` is adopted exactly like `Live` because
    ///     this call only needs A life to key objects with; refusing WRITES to a `Removing` namespace
    ///     is a different mechanism's job (Task 6's read-side contract), not this resolution's;
    ///   - an entry `Creating`: if its `creator` fence is THIS mount's own (a previous attempt of this
    ///     same open landed step 1 but not steps 2/3, e.g. after a transient error), resumes
    ///     `completeCreation` directly over the observed entry; otherwise reconciles it via
    ///     `CasRefCatalog::reconcileStaleCreator` + `isCreatorFenceTerminal`, refusing retry-later while
    ///     the old creator's fence is not yet provably dead.
    /// Every `createNamespace`/`completeCreation`/`reconcileStaleCreator` outcome that writes nothing
    /// (`FencedOut`, `Superseded`, a reconciled entry, `EntryChanged`) re-reads the catalog and loops;
    /// `CreatorFenceStillLive` throws the retry-later class, which this function's caller (the transient
    /// retry loop) or a higher one re-drives. Bounded against a pathological duel between two openers;
    /// each primitive this loop calls has its OWN bounded retry against the catalog's single object, so
    /// this bound is only against THIS loop's re-read cycle.
    NamespaceLifeId resolveNamespaceLife(
        const RootNamespace & ns, uint64_t admitted_generation, uint64_t live_epoch,
        bool * lifecycle_refusal = nullptr);

    /// ONE attempt of the recovery walk, run with NO lock held (the candidate is private; nothing
    /// touches `rt` until the install). `nullopt` REQUESTS A RESTART from a fresh listing -- the two
    /// innocent explanations, a base that vanished under a checkpoint that moved and a hole a racing
    /// cleanup could account for. Everything terminal throws.
    ///
    /// `admitted_generation` is the ONE fence generation this whole recovery was admitted under: the
    /// walk presents it to every `slotOccupy` and to the `_ckpt` CAS, and the caller presents the same
    /// value once more immediately before installing.
    /// `retained_attempt` is copied under `state_mutex` before the unlocked walk. It is evidence from
    /// this runtime's admitted writer, not a second recovery authority: only the exact slot it names
    /// is compared byte-for-byte, and a successor seal remains the existing conclusive-loss case.
    /// `cancelled` is the CALLER's latch, threaded in rather than kept locally: a cancellation is
    /// reported through the retry-later class (the caller should retry, against the FRESH incarnation),
    /// so without it the transient loop reads the stop as a blip and re-drives the very work the remount
    /// just stopped -- while the barrier blocks waiting for that recovery to finish.
    std::optional<RecoveryResult> runRecoveryWalkOnce(
        const RootNamespace & ns, RefTableRuntime & rt, uint64_t admitted_generation, uint64_t live_epoch,
        const std::optional<RefAppendAttempt> & retained_attempt, std::optional<String> & hole_detail,
        bool & cancelled);

    /// The I/O-boundary poll of the walk: is this recovery still entitled to continue? Two independent
    /// facts, each of which alone disqualifies it -- a self-remount asked it to stop, or a self-remount
    /// already detached its runtime. Throws; cancellation raises the retry-later class and LATCHES
    /// through `cancelled` so the caller's transient loop does not re-drive it.
    ///
    /// The FENCE is deliberately absent: it gates the three sites that spend it (every `slotOccupy`, the
    /// `_ckpt` CAS, the install), not every read. See the definition for why.
    void checkRecoveryStillAdmitted(const RootNamespace & ns, RefTableRuntime & rt, bool & cancelled) const;

    /// Publishes a completed streaming recovery into the runtime in one atomic step: copies EVERY field
    /// `RecoveryResult` carries into `rt` and sets `recovered` LAST, so a waiter woken after this returns
    /// never observes a partially-installed table. Struct-driven so a future added publication field
    /// cannot be silently dropped from a scattered assignment list (spec §5). MUST hold `rt.state_mutex`.
    void installRecoveryResult(RefTableRuntime & rt, RecoveryResult && result);

    /// Evicts least-recently-touched, idle whole-table runtimes until the configured cache budget is met,
    /// retaining `keep_ns` even when it is the next candidate.
    void enforceRefTableCacheBudget(const RootNamespace & keep_ns);

    /// Runs the append queue leader for `ns`, completing its own item and any compatible items carved
    /// into the same batch. Exceptions are stored for waiters and do not leave the leader flag latched.
    /// Every item this leader becomes responsible for -- its own `own` plus each item a flush removes
    /// from `pending` to form a batch -- is recorded into `owned_items`, so the caller's leadership guard
    /// can complete + de-pend any that a flush leaves unfinished on an exceptional exit.
    void runRefQueueLeader(const RootNamespace & ns, const std::shared_ptr<RefTableRuntime> & rt,
                           const std::shared_ptr<RefMutationItem> & own,
                           std::vector<std::shared_ptr<RefMutationItem>> & owned_items);

    /// Validates, durably appends, and applies one compatible batch while preserving copy-before-commit
    /// and apply-after-commit ordering -- the LIVE state is still only ever advanced once the object is
    /// durable; `commitRefChunk`'s pre-`PUT` apply targets a private candidate that nothing else can
    /// observe. Every item it carves out of `pending` is appended to
    /// `owned_items` (the leader's responsibility set) at the moment it is carved. When a batch's total
    /// op count exceeds `ref_txn_max_ops`, the validation loop emits SEVERAL ref-log transactions in one
    /// tenure via `commitRefChunk` -- each a complete commit boundary.
    void flushRefBatch(const RootNamespace & ns, const std::shared_ptr<RefTableRuntime> & rt,
                       std::vector<std::shared_ptr<RefMutationItem>> & owned_items);

    enum class WedgeResolution : uint8_t
    {
        NoWedge,
        Adopted,
        Rejected,
        StillWedged,
        Corrupted,
    };

    struct WedgeResolutionResult
    {
        WedgeResolution kind = WedgeResolution::NoWedge;
        std::exception_ptr survivor_error;
    };

    /// ONE bounded resolution attempt for `rt`'s outstanding wedge (spec INV-1's every-attempt rule):
    /// at most one `slotOccupy(wedge.key, wedge.bytes, ...)` per calling flush, gated on the wedge's
    /// ORIGINAL `admitted_fence_generation` rather than the current one. There is deliberately NO
    /// background retry thread and no deadline-resetting loop: a permanently quiet wedged namespace
    /// waits for its next caller or for a remount, which is acceptable precisely because the wedged
    /// operation was never acknowledged.
    ///
    /// The conditional CREATE is what makes the rule "every attempt has its own conclusive rejection"
    /// affordable: the ref-log key is write-once, so a create either lands our exact bytes (the
    /// transaction is durable -- and it is the SAME transaction, byte for byte) or conflicts with
    /// whatever is there, which the follow-up read then names. A read alone could only ever report
    /// "absent", which is not a rejection: the earlier ambiguous attempt could still land afterwards.
    ///
    /// Post-I/O recheck: the outcome is adjudicated on an I/O result, so before ANY action follows from
    /// it (adopt, acknowledge, unwedge, fail the survivors) this re-acquires `state_mutex`, presents
    /// `admitted_fence_generation` back through `checkFenceOrThrow`, and compares the full wedge
    /// identity against what is still installed. A result that returns after a fence bump/re-arm, or
    /// after the wedge it belonged to was replaced, is INERT for this runtime.
    WedgeResolutionResult resolveWedgeOnce(
        const RootNamespace & ns, const std::shared_ptr<RefTableRuntime> & rt);

    /// Commits ONE chunk of a `flushRefBatch` tenure as a complete ref-log transaction: allocates the
    /// real transaction id, PREPARES the chunk (`prepareRefChunk` -- candidate, transaction, key, sealed
    /// bytes, complete attempt, birth contribution), durably `PUT`s the sealed bytes, installs the
    /// candidate under `state_mutex` by a no-throw swap, advances the tail counters, records the
    /// per-transaction metrics, completes exactly `chunk_survivors` with the real id (waking their
    /// waiters), and schedules snapshot publication. Preparation completes BEFORE the first durable
    /// effect of EITHER chunk shape -- an ordinary chunk's ref-log `PUT`, and a `NamespaceBirth` chunk's
    /// earlier `_ckpt` publish -- so that nothing between "durable" and "recorded" can throw (spec §A1);
    /// a preparation failure is an ordinary pre-durability rejection. For the same reason the
    /// `RefAppendAttempt` is built COMPLETE before the `PUT` -- the request reads its key and body -- so
    /// the `Unresolved` arm only has to move it into the runtime: the OTHER thing that must be recorded
    /// once the object may be durable. Arming that attempt into the runtime is NOT preparation (it
    /// mutates `RefTableRuntime`) and stays here, between preparation and the first send.
    /// Returns true when the chunk committed durably; false on any non-throwing failure (a rejected
    /// apply / DefiniteFailure / unresolved wedge / a conclusive PUT rejection / an encode failure),
    /// after having already failed `chunk_survivors` with the appropriate error. Past the durable `PUT`
    /// it does not throw at all.
    /// PRECONDITION: the caller has already released its scratch `working` copy so the post-commit
    /// overlay fold is in place (the E5 fast path).
    bool commitRefChunk(const RootNamespace & ns, const std::shared_ptr<RefTableRuntime> & rt,
                        const std::vector<RefOp> & chunk_ops,
                        const std::vector<std::shared_ptr<RefMutationItem>> & chunk_survivors);

    /// Enters the hard recovery fence after a transaction is known durable but cannot be installed.
    /// The exact attempt is discarded because replay, not another write, is now the only legal owner.
    static void requireRecovery(RefTableRuntime & rt, const RootNamespace & ns, std::string_view region) noexcept;

    /// Leadership-exit guard for `appendRefOps`: under `ref_queue_mutex`, completes every still-unfinished
    /// item this leader owned (with `flush_exception` when unwinding, or a fail-closed `LOGICAL_ERROR`
    /// otherwise), removes each owned item from `pending` so no future leader can carve it, and releases
    /// leadership (`leader_active = false` + `cv.notify_all`). On the normal path every owned item is
    /// already `done`, so only the leadership release has effect. This is the single authority that
    /// resets `leader_active` on any exit from the leader loop.
    void completeOwnedItemsAndReleaseLeadership(
        const RootNamespace & ns, const std::shared_ptr<RefTableRuntime> & rt,
        const std::vector<std::shared_ptr<RefMutationItem>> & owned_items,
        std::exception_ptr flush_exception);

    /// Schedules best-effort background publication when tail thresholds and backoff permit. The
    /// dispatch is fenced and the detached task retains the owner pin until it finishes.
    void maybeScheduleSnapshotPublish(const RootNamespace & ns, const std::shared_ptr<RefTableRuntime> & rt);

    /// Merges one contribution into `life`'s `_ckpt` (spec INV-4), presenting `admitted_generation` back
    /// through the pool's fence callback on every CAS attempt. The ledger owns no `CasMountRuntime`, so
    /// this is only the place that assembles the deadline from the ledger's own injectable boot clock
    /// and CAS budget; the algorithm itself is `publishCkpt`, shared verbatim with every other writer.
    ///
    /// THREE call sites (corrected here from "two": `runRecoveryWalkOnce`'s own sealer contribution
    /// below is a third and was missing from this count), and they contribute DISJOINT fields:
    ///   - `commitRefChunk`'s namespace-birth transaction contributes `life_epoch` -- it is the only
    ///     writer that knows it (this transaction's own writer epoch), and spec §3 has the `_ckpt`
    ///     created before the namespace becomes Live. The contribution is no longer built at the call
    ///     site: `prepareRefChunk` returns it as `PreparedRefChunk::birth_contribution` and
    ///     `commitRefChunk` passes that prepared value here. The split is deliberate -- DECIDING the
    ///     contribution is pure preparation, while THIS call is a birth chunk's first durable effect, so
    ///     the publish had to stay behind when preparation moved out;
    ///   - `tryPublishSnapshotAndAdvanceCheckpointOnce` contributes `checkpoint_snapshot_id` once the
    ///     snapshot body is durable, and contributes NOTHING about `life_epoch` (an absence, which the semantic-max
    ///     merge leaves alone) because the publisher does not know it and must never guess;
    ///   - `runRecoveryWalkOnce` contributes `last_epoch_seal` once its own CAS-walk minted or adopted
    ///     one -- it is the only writer that mints seals, so it is the only writer that can record
    ///     where the chain now ends.
    CkptPublishOutcome publishCkptContribution(const NamespaceLifeId & life, const RefCkpt & contribution,
                                               uint64_t admitted_generation,
                                               const std::function<void(uint64_t)> & check_admission);

    /// Common candidate predicate for scheduler admission and execution after capture. Caller holds
    /// `rt.state_mutex`; an epoch seal is not state-bearing and cannot be snapshotted.
    bool hasStateBearingSnapshotCandidateUnderStateLock(const RefTableRuntime & rt) const;

    /// The Live + single-in-flight-gate + backoff + tail-threshold admission decision, factored out so
    /// both the trigger (`maybeScheduleSnapshotPublish`) and the settlement re-evaluation share ONE
    /// authority. The caller MUST hold `rt.state_mutex`; on admission this increments
    /// `pending_snapshot_publishes` and returns true (the caller then dispatches). The fence check
    /// (`may_mutate`) is the caller's responsibility (it is not held under `state_mutex`).
    bool admitSnapshotPublishUnderStateLock(RefTableRuntime & rt);

    /// Launches one detached publish attempt. Assumes `pending_snapshot_publishes` was already
    /// incremented for this dispatch (by `admitSnapshotPublishUnderStateLock`). The task settles through
    /// `settleSnapshotPublish`; if the thread cannot even be constructed, the count is undone and the
    /// settle waiter notified so a leaked in-flight count never wedges shutdown/settle.
    void dispatchSnapshotPublisher(const RootNamespace & ns, const std::shared_ptr<RefTableRuntime> & rt);

    /// Runs at the end of one detached publish attempt: drops this attempt's in-flight count and, under
    /// the SAME `state_mutex` hold, re-evaluates the accumulated tail so a trigger the single-flight gate
    /// discarded during this attempt (e.g. chunks 2..N of a chunked tenure whose chunk-1 publish was
    /// in flight) is re-fired instead of lost. Re-admitting under the same lock keeps the in-flight count
    /// from transiently reaching zero across the handoff, so a settle waiter never observes a false
    /// "settled". Notifies the settle condvar only when no follow-up publish is dispatched.
    void settleSnapshotPublish(const RootNamespace & ns, const std::shared_ptr<RefTableRuntime> & rt);

    /// Advances the exponential delay after a non-durable snapshot publication outcome.
    void advancePublishBackoff(RefTableRuntime & rt);
    /// Clears the snapshot-publication delay after durable progress.
    void resetPublishBackoff(RefTableRuntime & rt);

    /// Checks whether recovery or a mutation requested stale-precommit cleanup and dispatches it when
    /// its per-table cooldown permits.
    void maybeSweepStalePrecommits(const RootNamespace & ns, const std::shared_ptr<RefTableRuntime> & rt);

    /// Performs one fenced stale-precommit sweep. A partial or failed sweep re-arms the requirement and
    /// propagates its exception; only a verified-clean pass clears it.
    void sweepStalePrecommitsNow(const RootNamespace & ns, const std::shared_ptr<RefTableRuntime> & rt);

    /// Runs the read-triggered sweep without allowing an uncertain maintenance append to fail the read.
    void sweepStalePrecommitsForRead(const RootNamespace & ns, const std::shared_ptr<RefTableRuntime> & rt);

    /// Advances the stale-precommit sweep's exponential cooldown after failure.
    void advancePrecommitSweepBackoff(RefTableRuntime & rt);
    /// Clears the stale-precommit sweep cooldown after a verified-clean pass.
    void resetPrecommitSweepBackoff(RefTableRuntime & rt);

};

}
