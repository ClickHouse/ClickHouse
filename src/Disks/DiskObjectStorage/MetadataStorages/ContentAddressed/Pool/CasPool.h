#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasEventDispatcher.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestControl.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPlainObjects.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasManifestReader.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasMountRuntime.h>
#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <cstdint>
#include <functional>
#include <future>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <unordered_map>
#include <utility>

namespace DB::Cas
{


/// Configuration supplied when opening a content-addressed pool. The fields remain flat for the
/// compatibility of existing wiring and tests, while `refLedgerConfig` and `mountConfig` project
/// the fields owned by those subsystems into typed values passed by value. The projections avoid an
/// include cycle and make it explicit that the subsystem objects do not retain a reference to this
/// configuration object.
struct PoolConfig
{
    String pool_prefix;
    UInt128 server_id{};                      /// owner token (ServerUUID) — provenance + watermark
    /// Explicit, configured identity of the layout subtree this server owns. Required + validated
    /// (clean relative path); `ServerUUID`/`server_id` is demoted to an
    /// owner token. Validated via `Cas::validateServerRootId`.
    String server_root_id;
    uint64_t blob_header_len = 256;           /// creation-time only; the pool is authoritative on reopen
    /// CAS mixed-algo pools: the
    /// NODE-LOCAL algo this Pool writes NEW content with (`Pool::writeAlgo()`). NOT durable pool
    /// state -- two live nodes may intentionally write with different (already-admitted) algos, so
    /// no single truthful pool-wide value exists. `PoolMeta::createOrValidate` accepts it with no
    /// write when it is already a member of the pool's `algos_used`; otherwise it is admitted via
    /// the CAS-union below (opt-in, `blob_hash_allow_new`) or refused (BAD_ARGUMENTS, the default --
    /// a changed config alone must never silently turn a pool mixed). Default `CityHash128` keeps
    /// every existing pool's hash byte-for-byte unchanged.
    BlobHashAlgo blob_hash_algo = BlobHashAlgo::CityHash128;
    /// Opt-in for `blob_hash_algo` to be ADMITTED into the pool's `algos_used` when it is not
    /// already a member. Consulted only on
    /// the FIRST open that would otherwise refuse -- once admitted, `algos_used` membership alone is
    /// the steady-state check and this flag is not needed again for the same algo. Default `false`
    /// (fail-closed, matching the default-refuse behavior).
    bool blob_hash_allow_new = false;
    /// Dedup cache: byte ceiling for the per-disk known-present blob-hash LRU set. 0 disables the
    /// cache (every create misses → HEAD-before-PUT only). A hint cache; correctness never depends on
    /// it (a stale hit is caught by the mandatory HEAD in putBlob).
    uint64_t deduplication_cache_bytes = 64ULL << 20;        /// 64 MiB
    /// HEAD-before-PUT: on a dedup-cache MISS, a blob whose body is >= this many bytes is written
    /// HEAD-first (a cheap HEAD avoids streaming a body that would 412). 0 disables the size trigger.
    uint64_t deduplication_head_first_min_bytes = 1ULL << 20;   /// 1 MiB
    /// Part-folder cache: byte bound for the manifest DECODE cache. The old cache
    /// was count-bounded only (16384 entries) — decoded manifests carry inline bytes, so the worst
    /// case was multi-GB. 0 disables decode caching (every read decodes fresh — diagnostic mode).
    uint64_t manifest_decode_cache_bytes = 128ULL << 20;
    /// How many superseded snapshot generations to retain. After committing
    /// generation G, generations <= G - this are pruned (bounded per round). 0 = keep ALL
    /// (debug/forensics — replay GC's in-degree view as-of a past round). Default 3 = the safety
    /// margin covering any in-flight/resuming leader (a leader more than `keep` generations behind
    /// has lost its lease; its round-commit CAS fails).
    uint64_t gc_snapshot_generations_to_keep = 3;
    /// Blob target shards for GC. Default 1 (a single shard, i.e. no fan-out). Creation-time only;
    /// the pool is authoritative on reopen. This is the BLOB-HASH-prefix reducer axis.
    uint64_t gc_shards = 1;
    /// Cursor-paced orphan part-manifest sweep. The LIST budget bounds cold-prefix enumeration
    /// per completed GC round; the delete budget separately bounds exact-token destructive work.
    uint64_t manifest_sweep_list_budget_keys = 1000;
    uint64_t manifest_sweep_delete_budget_keys = 100;
    /// Per-round blob-deletion work envelope: caps how many entries the fold's graduation
    /// (condemned -> delete_pending) and redelete (exact-token delete of a prior delete_pending row)
    /// arms move out of the durable retired pipeline in one round. Excess entries are carried
    /// unchanged in `still_retired` and retried next round (never dropped). 0 = unbounded.
    /// Default UNBOUNDED. A count cap here is not backpressure: it throttles the consumer while the
    /// producer (inserts, merges) is unaware of it, and the excess is carried in `still_retired`, which
    /// the next round reads in full -- so a round's cost grows with the debt while its useful work stays
    /// capped. Under sustained load that is a feedback loop, not a delay.
    uint64_t gc_round_graduation_budget = 0;
    uint64_t gc_round_redelete_budget = 0;
    /// Orphan-manifest sweep: caps on the expensive step the LIST/nomination budgets never covered —
    /// building a namespace's protection view (catalog-authoritative table recovery + committed-tail
    /// walk). `sweep_namespace_budget` bounds how many DISTINCT namespaces one page may build a view
    /// for; `sweep_recovery_op_budget` bounds the total ref-log GET/decode ops the committed-tail walk
    /// may spend across every namespace, cumulative for the round. Exhausting either retains every
    /// remaining candidate of the affected namespace on this page (fail-closed; retention is always
    /// safe) rather than deciding it without a complete protection view.
    uint64_t gc_round_sweep_namespace_budget = 20;
    uint64_t gc_round_sweep_recovery_op_budget = 5000;
    /// Ref-object cleanup (covered log/snapshot exact deletes) and generation-prefix wholesale delete
    /// (superseded-generation prune) caps, cumulative for the round. `prefix_wholesale_budget` is the
    /// round's shared remainder every prune `deletePrefixWholesale` call draws from, so no single call
    /// passes an unbounded `bounded_remaining`.
    uint64_t gc_round_ref_cleanup_budget = 5000;
    uint64_t gc_round_prefix_wholesale_budget = 20000;
    /// The post-CAS hand-off reclaim draws from its OWN reserve, never from `prefix_wholesale_budget`
    /// above: the prune safely under-serves and retries next round via its cursor, but the hand-off is a
    /// one-shot event with no reclaimer besides `fsck`, so a prune-heavy round must never be able to
    /// starve it to zero.
    /// Default UNBOUNDED, and this one is not a tuning choice. The hand-off is one-shot: a generation it
    /// cannot fully reclaim in its round is never revisited, because the parent-seal difference that
    /// triggers it does not recur. A cap on work that has no second chance does not defer the work, it
    /// leaks it -- the same shape as the manifest-cleanup cap that was removed outright.
    uint64_t gc_round_handoff_prefix_wholesale_budget = 0;
    /// `GcOutcomes` per-round entry cap across the redelete/spared audit log, cumulative for the round.
    /// Bounds only the audit-log write -- the settlement decision it records already happened
    /// unconditionally in the fold. 0 = unbounded.
    /// Default UNBOUNDED: nothing is retried on exhaustion because there is nothing left to retry -- the
    /// decision already happened -- so the only thing a cap drops is the audit row explaining it, and it
    /// drops exactly the rows of the busiest rounds, which are the ones an investigation needs.
    uint64_t gc_round_outcome_entry_budget = 0;
    /// Frontier probes: how many KNOWN-BUT-UNHINTED namespaces one round may walk to prove their
    /// frontier. A namespace this round's LIST hint still mentions is walked regardless (the round owes
    /// its edges anyway), and a HELD one is always walked (its hold must be retried by exact key, spec
    /// §5), so this bounds only the extra exact `GET`s the universe union introduced -- normally zero,
    /// because ordinary active namespaces normally remain hinted. Running out is NOT an error: the
    /// unprobed namespaces are simply unproven, which
    /// suppresses all destruction for the round. 0 => probe none (the exhaustion path, which tests
    /// drive directly).
    ///
    /// Default effectively unbounded, and it must be spelled as a huge number rather than `0`: unlike
    /// every other budget here, `0` means "probe nothing", not "no cap" -- the tests drive that path
    /// deliberately, so the sentinel cannot simply be redefined. Exhaustion is the worst failure shape
    /// of any budget in this struct, because it does not defer work: unprobed namespaces are unproven,
    /// and one unproven namespace suppresses ALL destruction for the round. A count that is fine for ten
    /// namespaces silently becomes a permanent GC stop for a pool with enough tables.
    /// The cost of removing the cap is round LENGTH (extra exact `GET`s, normally zero because active
    /// namespaces stay hinted) -- which is bounded by nothing today, since rounds have no time deadline.
    uint64_t gc_frontier_probe_budget = std::numeric_limits<uint64_t>::max();
    /// skip-unchanged: a GC round may DEFER
    /// (re-adopt the sealed in-degree generation instead of rebuilding it) when fewer than this many
    /// shards changed since the last fold AND no destructive decision is due. Default 1 = fold as soon
    /// as anything changed (batching off; only idle rounds defer). > 1 batches small deltas.
    uint64_t gc_fold_threshold = 1;
    /// Liveness bound for batching: force a FOLD after this many consecutive DEFER rounds even below
    /// the threshold. Inert at gc_fold_threshold == 1 (an idle defer has nothing to fold). Default 8.
    uint64_t gc_fold_max_defer_rounds = 8;
    /// Diagnostic-only threshold for a `Removing` catalog life whose terminal cleanup evidence has
    /// not appeared. Test/config struct seam only; no user-facing setting is registered.
    uint64_t gc_stuck_removal_rounds = 10;
    /// gc-rebuild: max in-memory edges per gc-shard batch during rebuildBaseline
    /// (~32 B each => default ~256 MB); each full batch folds into the next attempt number with the
    /// previous attempt's runs as priors, so memory is O(budget), never O(edges).
    uint64_t rebuild_edge_budget = 8000000;
    /// Bounded pool size for the per-hash freshness
    /// meta writes GC schedules at condemn/spare/delete (mass-DROP: a round condemning ~1M blobs would
    /// take hours sequential). Every job internally catches its own exceptions (never wedges the round;
    /// feedback_ca_gc_never_throw_on_404) and `Gc::runRegularRound` waits for the round's whole batch
    /// before the round's single gc/state CAS, so the meta writes are durable before that CAS commits.
    uint64_t gc_meta_pool_size = 16;
    bool background_watermark = false;       /// tests drive renewOnce explicitly; gates the merged heartbeat's background thread
    /// Installed on the pool before a writable mount can start its renewal thread.
    CasEventSink event_sink = {};

    /// Mount-lease TTL: how long a freshly-renewed mount lease is valid. The local
    /// write fence's monotonic deadline is `renew_time + this`, so a superseded/paused writer is fenced
    /// once `this` elapses with no successful renew. The background renewer runs every
    /// `mount_renew_period` (default ttl/3) so a healthy mount renews well before expiry.
    std::chrono::milliseconds mount_lease_ttl_ms{30000};
    std::chrono::milliseconds mount_renew_period{10000};   /// = ttl/3 by default
    bool read_only = false;                   /// observe-only open: skip the mutating capability probe; reads only

    /// Boot-time "start now, fix later": skip the access-check-class part of the capability probe
    /// (the `_probe/` read/write/delete/list round trip and the store-precondition check) while
    /// STILL opening writable — a mistyped bucket / transient DNS blip at mount should not hard-fail
    /// the disk when the operator asked to defer the access check (U#5), mirroring `checkAccess`'s
    /// `skip_access_check` gate for other disk types.
    ///
    /// These gates survive it on a writable `Pool::open`, both correctness hazards rather than
    /// preflight conveniences, so that path runs them even here. `Pool::openForDecommission` is the
    /// deliberate exception — see the assignment there.
    /// `checkSkipAccessCheckSupport` decides whether the backend may skip the battery at all — a
    /// generation-dialect (GCS) backend refuses, because only the battery proves its token-exact
    /// DELETE honours the generation precondition. `checkConditionalWriteSingleAttemptSupport`
    /// (cas-s3-timeout-retry-control) guards every conditional write this mount will ever issue
    /// against running under the disk's ~500-attempt transparent retry policy.
    bool skip_access_check = false;

    /// The CAS retry controller's budget (cas-s3-timeout-retry-control), validated against
    /// `mount_lease_ttl_ms` at writable open (`Pool::open` calls `validateCasRequestBudget`) — an
    /// inconsistent budget refuses the mount rather than silently retrying unsafely. Defaults are
    /// consistent with the default `mount_lease_ttl_ms` above; a caller that raises the lease TTL may
    /// keep these defaults, but a caller that LOWERS it must revisit this budget too.
    CasRequestBudget cas_request_budget{};

    /// The write-fence deadline clock (CLOCK_BOOTTIME milliseconds; see `MountFence`). Empty = the real
    /// boot clock (`Pool::bootMs`); injected by tests to drive the fence deadline deterministically.
    std::function<uint64_t()> boot_ms_fn = {};

    /// Test hook for open/remount waits: `Pool::waitSleep` -- the mount-claim observation loop's poll --
    /// routes through this function when set instead of a real `std::this_thread::sleep_for`, so a test
    /// observes every wait without actually blocking. Empty (the production default) sleeps for real.
    ///
    /// That poll is now the ONLY wait either path can block on. The post-reclaim materialization grace
    /// (`T_mat`) that used to sit beside it is gone: it existed so a straggler conditional `PUT` from a
    /// dying epoch would land (or exhaust its retries) before the successor started trusting its recovery
    /// LISTINGS, and recovery does not trust listings any more -- it walks the stream arithmetically and
    /// closes every dead epoch with an in-band `EpochSeal` at `{E, T+1}`, written as a conditional
    /// create. The straggler's own conditional create then LOSES against that occupied slot, whenever it
    /// arrives. Waiting for a race that the protocol already decides is not caution, it is latency.
    std::function<void(uint64_t)> wait_sleep_fn = {};   /// test hook for open/remount waits

    /// a table becomes a publish candidate once its retained
    /// tail -- every applied txn strictly above the newest published snapshot, no age filter -- exceeds
    /// either threshold (their count / the sum of their encoded bytes), or right after recovery replays
    /// a tail already above one (the mount-time trigger). Publication is background and never blocks an
    /// append (see `Pool::maybeScheduleSnapshotPublish`). The grace-age holdback this trigger used to
    /// apply is gone by design: the recovery seal decides a late-arriving predecessor write outright --
    /// its conditional create loses to the seal already occupying the slot -- so a young txn has nothing
    /// left to wait out.
    /// The count default trades write-side PUT volume against read-side cold-fold cost: every publish
    /// re-encodes and PUTs the FULL snapshot, so a low threshold under sustained load degenerates into
    /// a near-continuous full-snapshot PUT stream, while on the read side a cold fold pays one GET per
    /// log the newest snapshot does not cover -- 256 bounds that at 256 extra GETs, each far cheaper
    /// than the snapshot churn it avoids.
    uint64_t snapshot_log_count_threshold = 256;
    uint64_t snapshot_log_bytes_threshold = 1ULL << 20;   /// 1 MiB
    /// bounded per-table backoff arming a dispatch cooldown after
    /// a NON-Committed publish outcome (an S3 timeout / uncertain PUT). Without it, a saturated backend
    /// turns every ref read into a re-dispatched full-snapshot encode+PUT (the read-triggered PUT storm),
    /// since a non-Committed publish deliberately does not prune the tail (that would be data loss) and so
    /// leaves the threshold trigger latched. The interval doubles from `initial` up to `max` per
    /// consecutive failure and resets on the next durable publish; combined with the single-in-flight
    /// gate, it bounds publish dispatch to O(failures), not O(reads).
    uint64_t snapshot_publish_backoff_initial_ms = 200;
    uint64_t snapshot_publish_backoff_max_ms = 30000;
    /// Bounded per-table cooldown between FAILED stale-precommit sweep attempts (the dangling-precommit
    /// hazard). A failed/partial sweep re-arms
    /// `needs_stale_precommit_sweep` instead of consuming the once-per-mount shot (one attempt burned in
    /// the post-restart error window used to leave a dead incarnation's precommit bindings -- and the
    /// manifests they protect from the GC orphan sweep -- live forever on a long-lived mount); this
    /// cooldown keeps the retry from storming a saturated backend, exactly like the publish backoff.
    /// Doubles from `initial` up to `max` per consecutive failure; reset by a verified-clean sweep.
    uint64_t precommit_sweep_backoff_initial_ms = 200;
    uint64_t precommit_sweep_backoff_max_ms = 30000;

    /// resident-memory ceiling for the writer's
    /// whole-table ref cache (`CasRefLedger::ref_name_slots`). This implementation has no row overlay, so eviction is
    /// WHOLE-TABLE: when the summed estimated weight of cached tables exceeds this, whole tables are
    /// dropped (never rows) and the next touch re-recovers them from the durable snapshot+log objects
    /// Evicting the table drops the entire object; the next access repeats
    /// recovery"). A table with a wedged append lane, a nonempty pending queue, or any in-flight
    /// caller/publish (its un-persisted lane/queue state is not reconstructable) is never evicted, and
    /// neither is the table whose recovery just triggered the pass -- so the effective floor is one
    /// table. 0 = unbounded (eviction disabled). The estimate is the base snapshot body size plus the
    /// retained log-tail bytes; both are already tracked, so a mutation costs no extra encode.
    uint64_t ref_table_cache_bytes = 256ULL << 20;   /// 256 MiB

    /// Projection accessors: build the per-owner typed slice from
    /// the flat fields above, for BY-VALUE injection into the ref-ledger / mount-runtime components.
    /// The fields stay flat here so every external caller (wiring, tests) that sets them is unchanged;
    /// the slices are derived, not stored. `boot_ms_fn` is intentionally in `MountConfig` and reaches
    /// the ref-ledger as a ctor callback (not duplicated into `RefLedgerConfig`).
    RefLedgerConfig refLedgerConfig() const
    {
        return RefLedgerConfig{
            .server_root_id = server_root_id,
            .gc_shards = gc_shards,
            .snapshot_log_count_threshold = snapshot_log_count_threshold,
            .snapshot_log_bytes_threshold = snapshot_log_bytes_threshold,
            .snapshot_publish_backoff_initial_ms = snapshot_publish_backoff_initial_ms,
            .snapshot_publish_backoff_max_ms = snapshot_publish_backoff_max_ms,
            .precommit_sweep_backoff_initial_ms = precommit_sweep_backoff_initial_ms,
            .precommit_sweep_backoff_max_ms = precommit_sweep_backoff_max_ms,
            .ref_table_cache_bytes = ref_table_cache_bytes,
        };
    }

    MountConfig mountConfig() const
    {
        return MountConfig{
            .mount_lease_ttl_ms = mount_lease_ttl_ms,
            .mount_renew_period = mount_renew_period,
            .background_watermark = background_watermark,
            .boot_ms_fn = boot_ms_fn,
            .wait_sleep_fn = wait_sleep_fn,
        };
    }
};


struct PartWriteInfo
{
    std::optional<String> intended_ref;       /// "ns/ref" forensics for the envelope (diagnostic)
    /// The owning root namespace, set EXPLICITLY by the wiring. When present it is authoritative for
    /// the manifest's owning namespace (PartWriteTxn::manifestNamespace), so a ref that itself contains '/'
    /// (the `detached/<part>` fold) is staged in the TABLE namespace — NOT in a spurious
    /// `<ns>/detached` namespace produced by splitting intended_ref on the last '/'. Absent ⇒ fall
    /// back to splitting intended_ref on the last '/' (the diagnostic-only path used by Core tests).
    std::optional<RootNamespace> intended_namespace;
    ProvenanceOp op = ProvenanceOp::Other;
};

class PartWriteTxn;
using PartWriteTxnPtr = std::shared_ptr<PartWriteTxn>;
class Gc;
class Pool;
using PoolPtr = std::shared_ptr<Pool>;

/// One listed key `Pool::listNamespaces` could not attribute to a namespace, with the refusal message
/// that stopped it. Behind Stage B's format bump the only such key is a ref object or a namespace file
/// that names no LIFE (the un-incarnated Stage A shape), which the `Layout` parsers refuse by name.
struct UnattributableNamespaceKey
{
    String key;
    String reason;

    bool operator==(const UnattributableNamespaceKey &) const = default;
};

/// What one `Pool::listNamespaces` enumeration observed: the namespaces it attributed, and the keys it
/// could not attribute to any namespace at all.
///
/// The two halves are separate because a short `namespaces` list is not the same fact as a clean one,
/// and the enumeration is not the place to decide what the difference means. A namespace disappears
/// from `namespaces` only if EVERY key that would have named it is in `skipped`, since attribution is
/// per key -- but "only if" is not "never", and each consumer's stakes differ: a browse probe can
/// answer conservatively, while a caller that RETIRES a slot on the strength of an empty list cannot
/// treat an incomplete universe as a drained one.
struct NamespaceListing
{
    std::vector<String> namespaces;
    std::vector<UnattributableNamespaceKey> skipped;
};

/// The façade for one content-addressed pool. `open` first validates the backend's conditional-write
/// capabilities and the durable pool metadata, and refuses to mount when either check fails. A
/// writable instance owns the mount lease and write fence; the read path uses immutable manifests
/// and does not participate in GC token ownership. The façade delegates plain-object access,
/// manifest reads, ref-log mutation, and mount lifecycle to the corresponding member components.
class Pool : public std::enable_shared_from_this<Pool>
{
    /// PartWriteTxn/Gc reach the ref-log lane only through Pool's PUBLIC surface now (the ref subsystem moved
    /// to the `ref_ledger` member): PartWriteTxn's staging PUTs go through `stagingPutIfAbsent`/
    /// `stagingConditionalCreate` (which encapsulate the controller call + fence), its ref mutations
    /// through the public `appendRefOps` delegate; Gc uses the public
    /// `wedgedRefLaneCount`. No `friend` needed -- both prior friendships were removed when the
    /// ref-ledger became a member component.

public:
    /// Construct a pool after backend capabilities and durable pool metadata have been validated.
    /// For a writable configuration, `open` also claims the configured mount before returning; a
    /// read-only configuration returns an instance that performs no mutating startup operations.
    static PoolPtr open(BackendPtr backend, PoolConfig config);
    /// Admin writer mount of the VICTIM `server_root_id`, for `SYSTEM CAS DROP POOL
    /// MEMBER`. Impersonates the victim's
    /// owner uuid (`readOwnerUuid`, or -- when the owner anchor itself is missing -- recovered from a
    /// lingering mount lease) and mounts writable under `MountClaimPolicy::NoWait`: a live victim
    /// lease is an immediate `ABORTED` refusal (no wait-and-observe, no FORCE variant), unlike the
    /// bounded reclaim wait a normal `open` pays. Throws `BAD_ARGUMENTS` when there is nothing to
    /// decommission (no owner anchor and no mount lease for `victim_srid`).
    static PoolPtr openForDecommission(BackendPtr backend, PoolConfig config, const String & victim_srid);
    /// Stop the mount-renewal and remount activity, drain ref-log work, and release the owned
    /// backend-facing components in their dependency order. Destruction is also the clean-farewell
    /// path for a writable mount, so it must complete before the owning backend is released.
    ~Pool();

    /// ---- per-server watermark surface ----
    /// process_epoch: random nonzero per Pool (process). GC checks epoch EQUALITY, never ordering.
    uint64_t epoch() const { return mount_runtime.epoch(); }
    /// The durable-monotone writer_epoch allocated at writable open. On a
    /// writable Pool this is the value bridged into `process_epoch` (so the watermark + the manifest
    /// manifest ref carries it); on a read-only open the random `process_epoch` is unchanged and
    /// no durable epoch is allocated. A self-remount re-establishes this to the fresh incarnation's
    /// writer_epoch (kept equal to `liveWriterEpoch`). The epoch-aware sweep reads this value.
    uint64_t writerEpoch() const { return mount_runtime.writerEpoch(); }
    /// The GC floor: the oldest in-flight build_seq, or next_build_seq when no build is active (so a
    /// quiescent server's watermark floor advances to the next-to-be-allocated seq). Locks builds_mutex.
    uint64_t minActive();
    /// Test/assertion accessor for the next-to-allocate build_seq under the lock.
    uint64_t peekNextBuildSeq();
    /// Renew the merged heartbeat once (bump seq, refresh min_active from the live callback, stamp a
    /// fresh expires_at_ms). The build-watermark floor rides this beat. In production this is driven by
    /// the background renewer (background_watermark).
    void renewWatermarkOnce();

    /// ---- local write fence ----
    /// A purely local, in-memory check — NEVER a per-write S3 read. True iff the fence has not latched
    /// `lost` and the monotonic deadline has not passed. Permissive until armed: a Pool that has not
    /// armed the fence (the default deadline is steady_clock::time_point::max()) always allows mutations.
    bool mayMutate() const;
    /// Latch the fence to lost (once lost, stays lost). Called by the renewer on a superseded
    /// or foreign observation; the gated mutate chokepoints then fail closed.
    void tripMountLost();
    /// Refresh the write-fence deadline (a CLOCK_BOOTTIME-milliseconds instant; release).
    /// keeper renew calls this on success.
    void setMountDeadline(uint64_t deadline_boot_ms);
    /// Arm the fence at startup: set (uuid, epoch, deadline), clear `lost`.
    void armMountFence(UInt128 server_uuid, uint64_t writer_epoch, uint64_t deadline_boot_ms);
    void setArmMountFenceInterpositionHookForTest(std::function<void()> hook)
    {
        mount_runtime.setArmMountFenceInterpositionHookForTest(std::move(hook));
    }
    /// The fence clock: CLOCK_BOOTTIME in milliseconds (includes VM-suspend time, unlike
    /// CLOCK_MONOTONIC — see `MountFence`). Consults the injected `config.boot_ms_fn` if set (tests),
    /// otherwise `bootMs`.
    uint64_t bootMsNow() const;
    /// The real boot clock: CLOCK_BOOTTIME in milliseconds. Static so tests can compose it.
    static uint64_t bootMs();

    /// ---- fence-generation admission (rev.7 [C2]/[D1]; owned by `mount_runtime`) ----
    /// Bumped on every `tripMountLost`/`armMountFence`. Forwarders used directly by the S3-native
    /// staging-buffer finalize (`ContentAddressedTransaction::writeFile`) -- the durable-effect site
    /// outside `CasPlainObjects` that needs to capture-then-recheck a fence-generation token across an
    /// async, potentially long-running upload. `CasPlainObjects` reaches the same primitives via
    /// injected callbacks (see its own constructor).
    uint64_t fenceGeneration() const { return mount_runtime.fenceGeneration(); }
    /// Throws the typed transient refusal (`throwCasTransientUnavailable`) unless the fence is currently
    /// held AND still at `admitted_generation`.
    void checkFenceOrThrow(uint64_t admitted_generation) const { mount_runtime.checkFenceOrThrow(admitted_generation); }

    /// ---- pool lifecycle condition (rev.7 §1; owned by `mount_runtime`) ----
    /// Atomic read of the current lifecycle condition. Thin forwarder; safe to call from any thread.
    PoolLifecycle lifecycle() const { return mount_runtime.lifecycle(); }
    /// Whether the pool has reached one of the two fully-terminal `Vanished` values
    /// (`VanishedReplaced` / `VanishedForgotten`).
    bool isVanished() const { return mount_runtime.isVanished(); }
    /// Whether the terminal-intent latch is published — a natural `enterVanished`, OR FORGET's early
    /// (spec §5 step 1) `publishVanishedIntent`, and NEVER `IdentityLost` ([C1]). See
    /// `CasMountRuntime::vanishedIntentPublished`. The GC scheduler consults this ALONGSIDE `isVanished()`
    /// to self-exit its loops the instant the pool is (being driven) terminal, at the earliest signal.
    bool vanishedIntentPublished() const { return mount_runtime.vanishedIntentPublished(); }
    /// The store()-class lifecycle gate: throws the typed `INVALID_STATE` error, whose message names the
    /// terminal sub-state, when the pool has entered `IdentityLost` or any `Vanished` state; returns
    /// silently while `Live`/`TransientNotLive`. This is the minimal "nothing silently proceeds" hook the
    /// metadata storage's `poolAccess()` calls after its null-pool (`throwStorageNotStarted`) check. The
    /// FULL six-class operation gate — which additionally throws in the transient state and answers
    /// truth-absent on removes/enumeration — is `checkOpAdmitted`; this covers only the terminal states.
    void throwIfLifecycleTerminal() const;

    /// A non-gated, I/O-free lifecycle snapshot for `system.cas_mounts` (spec §7,
    /// Factory class). Reads only the runtime's atomics — NO backend op — so it is truthful in EVERY
    /// state, including the terminal ones the store()-class surface refuses. `detail` is the same [D5]
    /// reason text `throwIfLifecycleTerminal` throws (empty while `Live`/`TransientNotLive`), which spec §1
    /// requires appear verbatim in the snapshot; `since` is the wall-clock second the current non-`Live`
    /// state was entered (0 while `Live`). The metadata-storage layer maps `lifecycle` to the operator
    /// vocabulary and derives the enum-clean sub-state word separately (see `CasLifecycleSnapshot`).
    struct LifecycleSnapshot
    {
        PoolLifecycle lifecycle = PoolLifecycle::Live;
        String detail;
        time_t since = 0;
    };
    LifecycleSnapshot lifecycleSnapshot() const;

    /// `SYSTEM CAS FORGET` — the operator force-Vanish (spec §5). Drives THIS pool to
    /// `Vanished(forgotten)` with the fence-first protocol, node-locally, regardless of the current
    /// lifecycle (it works precisely on a NOT-live disk — a stuck transient/`IdentityLost` pool). In order:
    /// (1) publish the terminal-intent latch FIRST (so the remount loop / keeper callback bail at their
    /// next step boundary, bounding the joins below); (2) trip the local fence (the deliberate
    /// decommission act, allowed on a live disk); (3+4) stop the GC scheduler via `stop_and_join_gc` —
    /// injected because the scheduler is owned above the Pool, a no-op in contexts that run none — and stop
    /// + join the self-remount thread; (5) drain the ref lanes (bounded) and retire the keeper WITHOUT an
    /// unearned clean farewell (the lease expires by observation unless the lanes provably drained); then
    /// (6) publish `Vanished(forgotten)` carrying `reason` (the [D5] message with the operator's decommission
    /// timestamp). Idempotent: an already-`Vanished` pool returns immediately (first terminal transition
    /// wins). MUST run on the admin/query thread, never a pool (remount/GC) thread — the joins would
    /// otherwise self-deadlock (hazard C6).
    void forgetDisk(const std::function<void()> & stop_and_join_gc, const String & reason);

    /// Test seam: force the pool lifecycle condition directly to `lc` (see
    /// `CasMountRuntime::setLifecycleForTest`). Lets the operation-gate tests pin each class x state cell
    /// on a metadata-storage-owned pool without driving a full remount/erase sequence. Never used in
    /// production.
    void setLifecycleForTest(PoolLifecycle lc) { mount_runtime.setLifecycleForTest(lc); }

    /// Test seam: publish the terminal-intent latch WITHOUT settling a terminal state (spec §5 step 1 of
    /// FORGET), so a test can exercise the "FORGET intent published, state still pre-terminal" window — the
    /// step-0 remount-observer bail (M1) and the GC scheduler's earliest-signal self-exit (C1). Never used
    /// in production; FORGET reaches `publishVanishedIntent` through `forgetDisk`.
    void publishVanishedIntentForTest() { mount_runtime.publishVanishedIntent(); }

    /// ---- write side ----
    PartWriteTxnPtr beginPartWrite(PartWriteInfo info);                          /// W-HEARTBEAT durable before return
    /// Remove a build_seq from the active set; idempotent (safe from publish/abandon/dtor). Public
    /// PartWriteTxn-facing surface: a `PartWriteTxn` retires its own seq on finalize/abandon/dtor (previously reached
    /// via `friend class PartWriteTxn`, removed when the ref-ledger became a member component.
    void retireBuildSeq(uint64_t seq);

    /// Transfer a destroyed transaction's unresolved precommit-release duty to the mount. The build
    /// sequence remains active until a later mutation resolves the namespace's every-attempt wedge and
    /// proves the exact precommit absent or appends its exact removal. `noexcept`: a transaction
    /// destructor must fail closed by retaining the active build, never terminate while trying to
    /// allocate queue bookkeeping.
    void enqueueWriterCleanupDuty(
        const RootNamespace & ns, const String & ref_name, const ManifestRef & manifest, uint64_t build_seq) noexcept;

    /// ---- read side ----
    /// `audit` defaults to `Emit` so every existing caller keeps emitting `RefResolve` unchanged; see
    /// `ResolveAudit`'s doc comment (`CasRefLedger.h`) for the one `Deferred` call site.
    std::optional<Resolved> resolveRef(const RootNamespace & ns, const String & ref_name, bool allow_stale = false,
                                       ResolveAudit audit = ResolveAudit::Emit);
    /// Gate 1 of the relink confirm -- a thin forward to the ref ledger, whose declaration carries the
    /// rules (`CasRefLedger::confirmExactRef`). Read-only and object-store-I/O-free by contract.
    ConfirmAnswer confirmExactRef(const RootNamespace & ns, const String & ref_name,
                                  const ManifestRef & manifest_ref) const
    {
        return ref_ledger.confirmExactRef(ns, ref_name, manifest_ref);
    }
    /// Read the single immutable part manifest named by `id`. Derives the key via CasLayout::manifestKey,
    /// decodes the body, and fails CLOSED: a committed ref naming a missing body throws FILE_DOESNT_EXIST
    /// (INV-NO-DANGLE surfaced on the read path); a body whose `ref` ≠ id.ref (refMatchesBody) or whose
    /// `root_namespace_id` ≠ id.root_namespace (manifestNamespaceMatches) throws CORRUPTED_DATA — the
    /// ref is addressing the wrong object, or a cross-namespace dangle. Token-gated decode cache below.
    PartManifest readManifest(const ManifestId & id);
    /// Identical to `readManifest` (same mandatory HEAD, same fail-closed validation, same decode
    /// cache) but returns the SHARED immutable decode the manifest cache holds — no per-call copy.
    /// The wiring read path uses this variant.
    std::shared_ptr<const PartManifest> readManifestShared(const ManifestId & id);
    BlobLocation locate(const ManifestEntry & entry) const;       /// Blob placement only
    std::map<String, Resolved> listRefs(const RootNamespace & ns);
    /// Pure existence probe: whether any committed ref name starts with `prefix`, without
    /// materializing `listRefs`'s full map. Empty `prefix` means "any ref at all".
    bool hasAnyRefWithPrefix(const RootNamespace & ns, std::string_view prefix);
    /// Catalog-authoritative namespaces with the given logical prefix, returned unordered.
    ///
    /// The enumeration REPORTS the keys it could not attribute and DECIDES NOTHING about them: it
    /// neither aborts nor silently drops them, because the right answer differs per consumer and only
    /// the consumer knows its own stakes. See `NamespaceListing`.
    NamespaceListing listNamespaces(const String & prefix);

    /// Scoped LIST of the mirrored subtree: the distinct next-path-segment names under
    /// `roots/<prefix>` (a loose LIST used by browse only; callers re-check `listRefs`/`getFileSize`
    /// before showing an entry). Not authoritative — logical discovery uses the ref catalog. `prefix`
    /// is a server-relative or shadow-relative path ending in '/'.
    std::vector<String> listMirroredChildren(const String & prefix);

    /// ---- ref lifecycle ----
    void dropRef(const RootNamespace & ns, const String & ref_name);            /// one owner_transition removal txn
    void updateRefPublishedAt(const RootNamespace & ns, const String & ref_name,
                          std::function<void(RefPublishedAtUpdate &)> mutator);   /// one set_published_at txn
    /// The catalog transition to `Removing` happens first, then one ref-log transaction naming every
    /// owner's exact removal followed by `remove_namespace`. Performs no physical deletion: GC records
    /// folded terminal evidence and later removes the exact catalog row, while the perpetual janitor
    /// reclaims dead-life stream, checkpoint, and namespace-file bytes independently.
    DropNamespaceStats dropNamespace(const RootNamespace & ns);
    /// Decommission-only exact-life overload; never re-resolves by namespace name.
    DropNamespaceStats dropNamespace(const NamespaceLifeId & life);

    /// The catalog life this namespace's objects are keyed under, for a WRITER, and the only resolution
    /// that CREATES one: minted if the catalog names none (a namespace's first namespace file births it
    /// exactly as its first ref op would). Resolved once per table-open and cached, so this is not a
    /// per-operation catalog request.
    NamespaceLifeId namespaceLife(const RootNamespace & ns);

    /// The life a READER or a REMOVER of this namespace's files must use, or `nullopt` when it has no
    /// readable files -- a never-created namespace, one mid-creation and a dropped table all answer
    /// alike. NEVER creates a namespace: an uncataloged one is answered from a catalog-only lookup that
    /// writes nothing, so a probe or an `if_exists` unlink against a table that was never opened cannot
    /// admit an entry into the pool-wide catalog. Replaces the older "is it removed?" predicate at every
    /// namespace-file read: the life and the readability come from one observation, so a reader cannot
    /// pair one with the other's stale answer, and an unreadable namespace yields no life to read with
    /// rather than a wrong one. See `CasRefLedger`'s declaration.
    std::optional<NamespaceLifeId> namespaceFilesLifeIfReadable(const RootNamespace & ns);

    /// Thin forward to `CasRefLedger::namespaceStillLogicallyPresent`, whose declaration carries the
    /// state matrix. The sole caller is `ContentAddressedMetadataStorage::existsDirectory`'s
    /// `DirShape::TableDir` case.
    bool namespaceStillLogicallyPresent(const RootNamespace & ns);

    /// GC callback after a proved exact catalog deletion; see `CasRefLedger` for the in-place cached
    /// runtime invalidation contract.
    void invalidateRemovedCatalogLife(const NamespaceLifeId & life);

    /// Reconciles cached removal-closed ref runtimes against a complete catalog cut.
    void reconcileRefCatalogCut(const CasRefCatalog::Snapshot & catalog_cut);

    /// ==== writer ref-log append lane ====
    ///
    /// The ONE entry point every ref mutation funnels through -- Pool's own dropRef/updateRefPublishedAt
    /// above, and (as a friend) PartWriteTxn's precommitAdd/promote/abandon. This is the SOLE ref-persistence
    /// lane now: the legacy per-(ns,shard) mutable manifest format was removed once GC/sweep/fsck/inspect
    /// were rewired onto the snapshot+log ref protocol.
    ///
    /// `build_ops(state)` is invoked from the per-namespace flush leader with the table's CURRENT cached
    /// state (reflecting every earlier item of the SAME batch already applied) -- exactly the atomicity
    /// the old per-shard closure got from running inside the shard's own CAS loop. It may perform
    /// arbitrary caller-side I/O (PartWriteTxn's blob revalidation) and throw to reject ONLY this item; a
    /// LOGICAL_ERROR/ABORTED/etc it throws propagates to the item's own caller without touching any
    /// other queued item. It returns the ops this call contributes to the batch's one transaction.
    /// `scope` reuses `MutationScope` (Ref(name) may co-batch; WholeShard runs solo -- used here for
    /// `namespace_birth`, which the flush forces automatically whenever the cached state is not `Live`).
    ///
    /// Wedge semantics: at most one unresolved `PUT` per table. An
    /// `Unresolved` outcome wedges this namespace's lane -- no later id is allocated until that SAME
    /// (key, bytes) reaches a conclusive outcome. There are exactly three, and the middle one is the
    /// reason a wedge is no longer a one-way door:
    ///   it resolves DURABLE -- either an earlier attempt landed, or the bounded retry's own
    ///     conditional create lands it now -- and is applied to the cache before the next id;
    ///   a successor's `EpochSeal` occupies the key, which PROVES our bytes never landed and never can:
    ///     the wedge clears, its callers fail permanently (they were never acknowledged), and the lane
    ///     resumes only under a later writer epoch;
    ///   or the process unmounts.
    /// Every item in the failing batch receives the SAME uncertainty exception (`NETWORK_ERROR`, the
    /// retry-later class); items already wedged from an EARLIER flush are retried by the NEXT call into
    /// this namespace's queue -- at most one bounded attempt per flush, under the fence generation the
    /// wedge was ADMITTED under, never the current one.
    ///
    /// `skip_stale_precommit_sweep`:
    /// suppresses the hoisted `maybeSweepStalePrecommits` call below for THIS call only. Set ONLY by
    /// `dropNamespace`'s own removal call: that call's `build_ops` already names every current precommit
    /// binding (stale or not) for removal via `RemoveNamespace`, so the ordinary maintenance sweep is
    /// redundant there -- and, left enabled, would race it: the sweep runs FIRST (hoisted at this
    /// function's top) and reclaims any epoch-stale binding in its OWN separate transaction, so
    /// `dropNamespace`'s later `build_ops` would see it already gone and undercount
    /// `DropNamespaceStats::precommits`. No other caller passes `true` -- the sweep's behavior for
    /// ordinary writers is unchanged.
    RefTxnId appendRefOps(const RootNamespace & ns, MutationScope scope,
                         std::function<std::vector<RefOp>(const RefTableState &)> build_ops,
                         RootMutationOrigin origin, RootMutationKind kind,
                         bool skip_stale_precommit_sweep = false);

    /// the synchronous core of one publish attempt -- copies
    /// the live `RefTableState` ONCE under `state_mutex` (candidate `X` = the state's `greatest_applied`
    /// at that instant, no replay), encodes it, and `putIfAbsentControlled`s it off the lock. Returns true iff
    /// a NEW snapshot was confirmed durable this call (false covers "nothing eligible yet", "nothing
    /// new to cover", and every non-Committed outcome -- all harmless per the Failure Handling table:
    /// "Snapshot create fails: keep all logs; writer recovery remains unchanged"). Public so tests can
    /// drive one attempt deterministically without depending on the background dispatch's timing;
    /// production reaches it only through `maybeScheduleSnapshotPublish`.
    bool tryPublishSnapshotAndAdvanceCheckpointOnce(const RootNamespace & ns);

    /// ---- verbatim namespace files (format_version.txt, ...) — plain keys, never content-addressed ----
    /// Every one of them names ONE LIFE of the namespace (directive §2): the caller passes the life it
    /// already holds, and none of these issues a catalog request to obtain one.
    void putNamespaceFile(const NamespaceLifeId & life, const String & name, const String & bytes);
    std::optional<String> getNamespaceFile(const NamespaceLifeId & life, const String & name);
    std::vector<String> listNamespaceFiles(const NamespaceLifeId & life);
    /// Exact-token delete of one verbatim file (no-op when absent). Verbatim files are never
    /// content-addressed, so a mid-life delete (a pruned mutation entry, a stale tmp) must reclaim
    /// the object NOW - the reachability GC never scans them.
    void removeNamespaceFile(const NamespaceLifeId & life, const String & name);

    /// ---- plain mountpoint objects ----
    /// A loose disk file (the startup write probe; anything written outside a `@cas@` archive) is a
    /// plain object at its mirrored path `roots/<key>`. No manifest, no journal, no dedup. GC never
    /// scans these (it deletes only content and folds only registered namespaces); they are owned by
    /// their path and removed only by `removeMountpointObject`.
    void putMountpointObject(const String & key, const String & bytes);
    std::optional<String> getMountpointObject(const String & key);
    /// Existence check for a loose mountpoint object WITHOUT reading its body. Directory-safe: a HEAD
    /// routes through the backend's metadata path (a directory reports as not-an-object), so probing
    /// a directory-shaped pool path (e.g. `store`, system.remote_data_paths traversal) returns false
    /// instead of a body read that would throw "Is a directory" (EISDIR).
    bool mountpointObjectExists(const String & key);
    void removeMountpointObject(const String & key);

    /// Internal surface for PartWriteTxn (same TU family; not for the wiring):
    const PoolConfig & poolConfig() const { return config; }
    const PoolMeta & poolMeta() const { return meta; }
    const Layout & layout() const { return pool_layout; }
    Backend & backend() { return *pool_backend; }
    /// The owning `BackendPtr` itself (not just a reference into it): the decommission slot-retirement
    /// decommission step (`CasDecommission.cpp`) must keep the backend alive across `admin.reset()` -- the graceful
    /// close that stamps the mount's farewell -- to physically delete the control objects afterward. A
    /// bare `Backend &` from `backend()` would dangle the instant the owning `Pool` is destroyed.
    BackendPtr poolBackendPtr() const { return pool_backend; }

    /// Staging PUT surface for `PartWriteTxn`: both wrap the ref-ledger's retry controller
    /// AND the ref-lane fence predicate, so `PartWriteTxn` reaches neither directly (the `friend` is gone).
    /// Behavior-identical to the previously-inlined controller+fence at CasPartWriteTxn.cpp stageManifest /
    /// uploadFromSource; thin delegates to `ref_ledger`.
    CasWriteOutcome stagingPutIfAbsent(std::string_view key, std::string_view bytes, Token * out_token = nullptr);
    CasCreateResult stagingConditionalCreate(std::string_view key, const std::function<PutResult()> & attempt);
    /// Same retry/fence policy as `stagingConditionalCreate`, for a mutable If-Match overwrite.
    CasOverwriteResult stagingConditionalOverwrite(std::string_view key, std::string_view bytes, const Token & expected);
    /// Same retry/fence policy as `stagingPutIfAbsent`, for a mutable marker where an existing
    /// DIFFERENT value at the key is a normal Conflict outcome, not corruption.
    CasOverwriteResult stagingPutIfAbsentMutable(std::string_view key, std::string_view bytes);

    /// CAS mixed-algo pools:
    /// the NODE-LOCAL algo this Pool mints NEW content with (`PoolConfig::blob_hash_algo` -- never
    /// durable pool state, see the field comment). Every write-mint site uses this, never a bare
    /// `poolMeta()` field (the pool no longer records one truthful write algo).
    BlobHashAlgo writeAlgo() const { return config.blob_hash_algo; }

    /// Whether `algo` is a member of the pool's `algos_used`, per this Pool's MONOTONE in-memory
    /// cache (seeded from `algos_used` at open time, unioned by `refreshAdmittedAlgos` -- never
    /// shrinks). This is the validation-protocol fast path: a hit needs no I/O. A miss
    /// for an algo this build KNOWS about must be followed by `refreshAdmittedAlgos()` before
    /// concluding the algo is genuinely not admitted (a long-running fold can overlap a later
    /// registration by another node) -- callers at the manifest-read boundary do this.
    bool isAlgoAdmitted(BlobHashAlgo algo) const;

    /// Re-reads `_pool_meta` and unions its CURRENT `algos_used` into the in-memory admitted-algo
    /// cache (mutex-guarded; monotone -- a concurrent shrink is impossible since `algos_used` is
    /// itself append-only). Returns the refreshed cache as a sorted vector, for callers that want to
    /// render it (error messages, diagnostics). THE stale-cache-race fix: call this
    /// on every admission-check miss, not just once at open.
    std::vector<uint8_t> refreshAdmittedAlgos();

    /// The writer_epoch of the LIVE mount incarnation. Bumped by `tryRemountOnce` (self-remount
    /// after a GC fence-out) — a `PartWriteTxn` minted under an older epoch fails closed on its next step.
    uint64_t liveWriterEpoch() const { return mount_runtime.liveWriterEpoch(); }

    /// Test seam: publish a new live-incarnation writer epoch WITHOUT running a self-remount -- the
    /// epoch half of what `tryRemountOnce` does alongside its fence re-arm. Lets a test drive an epoch
    /// transition's WRITER-side effects (INV-2's `prev_epoch_seal` on the first append of the new epoch)
    /// without the claim machinery and, deliberately, without `quiesceRefTablesForRemount`, so the
    /// cached ref runtimes survive the transition and the effect under test is isolated from recovery.
    void setLiveWriterEpochForTest(uint64_t writer_epoch) { mount_runtime.setLiveWriterEpoch(writer_epoch); }

    /// Self-remount after a GC fence-out (liveness counterpart of the fence-out safety rule): the
    /// OLD incarnation may never write again (the keeper never re-mints), but a FRESH incarnation —
    /// durable writer_epoch bump + mount reclaim + re-armed write fence — is exactly what a server
    /// restart would create, so a live server may create it in place. Runs the same claim machinery as
    /// `Pool::open`. Orchestration stays here; the owned mount primitives it drives (keeper swap,
    /// epoch bump, fence re-arm) live on `mount_runtime`. Returns false (and changes nothing durable
    /// beyond the epoch bump) when the
    /// mount cannot be claimed (foreign owner / a genuinely live twin) — the caller retries. Safe to
    /// call concurrently (serialized internally); also the synchronous test seam.
    bool tryRemountOnce();

    /// Test seam: drive the (private) self-remount arm/refuse path directly — in production the
    /// keeper's on_lost callback calls `scheduleRemount`, otherwise reachable only via the background
    /// renewer's cadence. Returns true iff a recovery thread is armed after the call.
    bool scheduleRemountForTest();
    /// Test seam: how many times `scheduleRemount` has been ENTERED, counted
    /// unconditionally as its very first statement -- BEFORE the `background_watermark` early-return, so
    /// this increments even under the default `background_watermark = false` (no thread ever spawns; a
    /// test never pays for a real self-remount attempt racing this Pool's own still-live keeper, which
    /// -- confirmed while building this seam -- reliably takes 30+ seconds per call and is not something
    /// a fast unit test should be driving). Positively pins that a production call site (e.g.
    /// `reportImpossibleInterference`) actually invoked `scheduleRemount`, as opposed to merely observing
    /// `mayMutate() == false` (which `tripMountLost` alone already accounts for).
    uint64_t scheduleRemountCallCountForTest() const { return mount_runtime.scheduleRemountCallCountForTest(); }
    /// Test seam: latch `remount_shutting_down` exactly as `~Pool()` does at its top, WITHOUT tearing
    /// the Pool down, so a test can assert `scheduleRemount` refuses to spawn once teardown has begun.
    void beginShutdownForTest();


    /// Known-present blob-hash cache. A HINT only — correctness never
    /// depends on it: a hit just makes putBlob go HEAD-first, and a stale hit is caught by that HEAD.
    /// No-ops when disabled (deduplication_cache_bytes == 0). Keyed on the full `BlobRef` pair:
    /// a bare digest is never the blob identity, and the same digest value under two algos is two
    /// different objects.
    bool dedupCacheContains(const BlobRef & ref) const;
    void dedupCacheAdd(const BlobRef & ref);
    /// Test seam: retained bytes of the manifest decode cache (0 when disabled).
    size_t manifestDecodeCacheBytesForTest() const { return manifest_reader.manifestDecodeCacheBytes(); }

    /// ---- event audit (system.cas_log) ----
    /// The wiring injects a sink (CasEvent -> SystemLog row) when the log is configured; null sink
    /// (unit tests, log disabled) makes emitEvent a no-op single branch. PartWriteTxn/Gc reach this via
    /// their owning Pool. `reason`/`detail` on the event carry the decision's full rationale.
    /// Intended only for pre-open wiring or tests with no active mount thread; later installation races emitters.
    /// Every component that emits (this `Pool`, the ref ledger, the manifest reader, the mount
    /// renewer) holds a reference to `event_sink_`, so routing that ONE `std::function` through the
    /// single `event_dispatcher_` funnels every emitter into serialized, reentrancy-safe delivery
    /// (stage-1 §1, Task 2). The forwarder is installed only when a real sink is present so
    /// `hasEventSink`/`event_sink_` stays a truthful "delivery enabled" predicate and the disabled hot
    /// path still skips constructing events entirely.
    void setEventSink(CasEventSink sink)
    {
        event_dispatcher_.setSink(std::move(sink));
        if (event_dispatcher_.hasSink())
            event_sink_ = [this](CasEvent e) { event_dispatcher_.emit(std::move(e)); };
        else
            event_sink_ = {};
    }
    /// Rvalue-only: forces every call site to `std::move` its (dead-after) `CasEvent` local, so a
    /// site a future edit forgets to update is a COMPILE ERROR here rather than a silent deep copy.
    void emitEvent(CasEvent && e) const { if (event_sink_) event_sink_(std::move(e)); }
    /// Cheap predicate so query-frequency hooks can skip constructing the CasEvent (+ its detail map)
    /// entirely when the log is disabled (sink null) — a true no-op on the production hot path.
    bool hasEventSink() const noexcept { return static_cast<bool>(event_sink_); }

    /// Read the current GC round from `gc/state`. Returns 0 when `gc/state` is absent (pool
    /// never GC'd). Best-effort: its one remaining caller is `tryRemountOnce`'s MountRemount audit
    /// event, which reports round 0 on any read failure rather than let the error escalate.
    uint64_t currentGcRound() const;

private:

    /// Construct the in-memory façade from validated backend, configuration, and pool metadata.
    /// `open` performs the checks and then moves these values here so no partially validated pool is
    /// exposed to callers.
    Pool(BackendPtr backend_, PoolConfig config_, PoolMeta meta_);

    /// Mount-claim policy for `mountWritable`.
    enum class MountClaimPolicy : uint8_t
    {
        WaitForExpiry,   /// normal server open — waits out a stale self-lease
        NoWait,          /// decommission gate — a live lease is an immediate ABORTED refusal
    };

    /// The writable-mount startup tail shared by `open` and `openForDecommission`: owner claim →
    /// writer_epoch → mount claim (+fence-recovery loop) → `MountLeaseKeeper` start → watermark
    /// anchor. `our_uuid` is the identity to mount as -- `config.server_id` for a normal open, the
    /// victim's owner uuid for decommission (impersonation). `policy` changes only what happens when
    /// the mount claim does not resolve `Claimed`/`FencedSelf`: `WaitForExpiry` observes a stale-
    /// looking lease and refuses (`mountDoubleStartMessage`) only once it proves genuinely live;
    /// `NoWait` refuses immediately, with no observation wait.
    static void mountWritable(PoolPtr & store, UInt128 our_uuid, MountClaimPolicy policy);

    /// The single serialized, reentrancy-safe event funnel (Task 2). Declared BEFORE `event_sink_` so
    /// it constructs first and destructs last: the forwarder stored in `event_sink_` references it, and
    /// reverse-order destruction retires the forwarder before the dispatcher it captures.
    EventDispatcher event_dispatcher_;
    /// Null means delivery is disabled and `emitEvent` is a no-op. When a real sink is installed this
    /// holds a thin forwarder into `event_dispatcher_` (set by `setEventSink`); every other component
    /// references this member, so all emitters share the one dispatcher.
    CasEventSink event_sink_;

    /// ==== ref-ledger callbacks that stay on Pool (thin delegates onto `mount_runtime`) ==== The whole
    /// ref-log / ref-table subsystem moved to the `ref_ledger` member (Pool/CasRefLedger.h) and the mount/
    /// watermark/build-registry state to `mount_runtime` (Pool/CasMountRuntime.h); these remain here
    /// because the ledger is injected with them as callbacks (`fence_ok_fn` / `cancel_inflight_builds` /
    /// `on_impossible_interference`) at construction and they bind to `Pool`.

    /// Delegate to `mount_runtime`: the build registry (`inflight_builds`/`builds_mutex`) moved there.
    /// Cancel every in-flight build targeting `ns` once its removal transaction is durable; this
    /// cancels local builds. Injected into `ref_ledger` as the
    /// `cancel_inflight_builds` callback.
    void cancelInflightBuildsForNamespace(const RootNamespace & ns);

    /// Delegate to `mount_runtime`: the write fence moved there. pre-attempt fence check: extends
    /// `mayMutate` with the REMAINING budget check -- an attempt is not even started unless there is
    /// enough of the mount lease left for one more attempt_timeout plus the lease safety margin. Passed
    /// as `fence_ok` to every `CasRequestController` call the ref-log writer path makes.
    bool refAppendFenceOk() const;

    /// incidental-detection reaction for a foreign-interference
    /// anomaly -- a signal that arrives on an operation the writer already performs (never a dedicated
    /// probe) and that is impossible under legitimate single-writer operation once the mount lease
    /// makes `key` exclusively ours: foreign bytes observed at our own wedge key, or the wedge hard
    /// contract itself violated at new-id-allocation time. LOG_ERROR with full context, emit a
    /// `ForeignInterference` CasEvent, then fence this mount closed and arm the SAME bounded
    /// self-remount a foreign/superseded lease renewal already drives (`tripMountLost`/
    /// `scheduleRemount` -- see the keeper's `on_lost` callback). Diagnosis is strictly off the
    /// critical path: ONE background GET of `key` (best-effort, single attempt), decoded as far as its
    /// ref-log header parses, logged -- never blocking or throwing on the caller's thread. Does NOT
    /// itself throw: every call site raises its OWN `LOGICAL_ERROR` immediately after this returns, so
    /// the message can name the specific contract that broke.
    void reportImpossibleInterference(const String & key, const String & reason,
                                       const std::optional<String> & offending_ns = {});


public:
    /// Test seams: observe resident recovery/wedge state without a private-member friend hack. These
    /// observers never resolve a name, recover a table, or materialize a runtime.
    uint64_t refRecoveryRestartsForTest(const RootNamespace & ns);
    bool refLaneWedgedForTest(const RootNamespace & ns);
    /// (I1) The object key of the current wedge for `ns`, or empty when the lane is not wedged -- lets a
    /// test land a DIFFERENT object at the exact wedged key to exercise resolve-time CORRUPTED_DATA.
    String wedgedKeyForTest(const RootNamespace & ns);
    /// test seam: force this table's wedge to a synthetic value directly under `state_mutex`,
    /// bypassing every production trigger. The ONLY way to construct the provably-unreachable state the
    /// release-mode wedge-contract guard in `flushRefBatch` defends against (a wedge still present at
    /// the new-id-allocation point) -- combine with `setRefPreCarveHookForTest` to install it AFTER the
    /// top-of-flush wedge-resolution check has already run clean but BEFORE the batch is carved.
    void forceWedgeForTest(const RootNamespace & ns, uint64_t writer_epoch, uint64_t ref_sequence,
                           const String & key, const String & bytes,
                           std::optional<uint64_t> admitted_generation = std::nullopt);
    /// test seam: the fence generation the current wedge was ADMITTED under (0 when not wedged) -- the
    /// value every later retry of that wedge is gated on. See `CasRefLedger::RefAppendAttempt`.
    uint64_t wedgedAdmittedGenerationForTest(const RootNamespace & ns);
    /// test seams: this table's `prev_epoch_seal` source -- the seal that closed its previous writer
    /// epoch, `nullopt` at genesis. The setter stands in for the recovery CAS-walk that produces it.
    std::optional<RefTxnId> lastEpochSealForTest(const RootNamespace & ns);
    void setLastEpochSealForTest(const RootNamespace & ns, const std::optional<RefTxnId> & seal);
    /// Test seam: this table's append lane state.
    RefLaneState laneStateForTest(const RootNamespace & ns);

    /// Whether this resident table still owes a stale-precommit sweep (armed by recovery; re-armed by a
    /// failed attempt; cleared permanently only by a verified-clean sweep).
    bool needsStalePrecommitSweepForTest(const RootNamespace & ns);

    /// Number of ref-append lanes currently wedged (an uncertain PUT exhausted its retry budget and
    /// the lane blocks until the same key resolves durable or is conclusively rejected). Per-disk GC
    /// health for system.cas_mounts. O(live tables); takes each runtime state lock.
    size_t wedgedRefLaneCount();

    /// test seam: blocks until every background snapshot-publish attempt dispatched so far for
    /// `ns` has settled. Needed only by tests that exercise the REAL background dispatch (production
    /// concurrency); tests that just want deterministic publish-logic coverage call
    /// `tryPublishSnapshotAndAdvanceCheckpointOnce` directly instead.
    void waitForSnapshotPublishSettleForTest(const RootNamespace & ns);

    /// test seam: the count of in-flight background snapshot-publish attempts for resident `ns` (the
    /// single-in-flight gate holds this at <= 1).
    int pendingSnapshotPublishesForTest(const RootNamespace & ns);

    /// test seam: the id of the newest snapshot this resident runtime has confirmed durable (recovered
    /// or published), or `nullopt` if none.
    std::optional<RefTxnId> newestPublishedSnapshotIdForTest(const RootNamespace & ns);

    /// test seam: whether `ns` has a RECOVERED cached runtime, WITHOUT forcing a recovery to find out.
    bool refTableRecoveredForTest(const RootNamespace & ns);
    /// test seam: whether the self-remount barrier's cancellation request is visible for `ns`.
    bool refRecoveryCancelRequestedForTest(const RootNamespace & ns);

    /// The self-remount's cancel-or-join barrier over in-flight ref-table recoveries (spec §3).
    /// `tryRemountOnce` runs it immediately before quiescing the tables and re-arming the mount fence;
    /// exposed so the barrier itself can be driven directly by a test.
    void cancelRefRecoveriesAndAwaitQuiescence();

    /// test seam: count of applied txns retained above `newestPublishedSnapshotIdForTest` (the
    /// tail a snapshot candidate would replay from).
    size_t tailSinceSnapshotCountForTest(const RootNamespace & ns);
    size_t committedOverlayEntriesForTest(const RootNamespace & ns);

    /// test seam: the ledger's live precommit view for `ns` (see
    /// `CasRefLedger::livePrecommitsForTest`) -- the durable-but-unpromoted owner bindings, which is
    /// what an abandoned/aborted build must leave empty.
    std::set<std::pair<String, ManifestRef>> livePrecommitsForTest(const RootNamespace & ns);

    /// test seam: whether any writer cleanup duty is still owed (see `writerCleanupDutiesPending`) --
    /// the direct signal that a settlement failure retained its duty for retry, independent of any
    /// build-floor side effect that could have the same shape for an unrelated reason.
    bool writerCleanupDutiesPendingForTest() const { return writerCleanupDutiesPending(); }

    /// Test-only hook: called by `flushRefBatch`
    /// right before it carves a batch, i.e. AFTER the table is already recovered -- the one otherwise
    /// untestable timing window `BlockingGetBackend`-style backend tricks cannot reach, since a warm
    /// flush performs no I/O between becoming leader and carving. A test blocks here to let a second
    /// caller's item join `rt->pending` before the carve, forcing deterministic co-batching.
    void setRefPreCarveHookForTest(std::function<void()> hook) { ref_ledger.setRefPreCarveHookForTest(std::move(hook)); }

    /// Test-only: pre-tenure fault seam for the append-lane leadership acquisition; forwards to
    /// `CasRefLedger::setRefPreTenureHookForTest` (see it for the baton-safety contract).
    void setRefPreTenureHookForTest(std::function<void()> hook) { ref_ledger.setRefPreTenureHookForTest(std::move(hook)); }
    void setAppendAfterRuntimeCaptureHookForTest(std::function<void()> hook)
    {
        ref_ledger.setAppendAfterRuntimeCaptureHookForTest(std::move(hook));
    }
    void setReadBeforeStateLockHookForTest(std::function<void()> hook)
    {
        ref_ledger.setReadBeforeStateLockHookForTest(std::move(hook));
    }
    void setReadableCatalogAfterObservationHookForTest(std::function<void()> hook)
    {
        ref_ledger.setReadableCatalogAfterObservationHookForTest(std::move(hook));
    }
    void setWedgeBeforeSlotOccupyHookForTest(std::function<void()> hook)
    {
        ref_ledger.setWedgeBeforeSlotOccupyHookForTest(std::move(hook));
    }
    void setNamespacePresenceProbeAfterFirstReadHookForTest(std::function<void()> hook)
    {
        ref_ledger.setNamespacePresenceProbeAfterFirstReadHookForTest(std::move(hook));
    }
    void setNamespacePresenceProbeAfterTerminalProvenHookForTest(std::function<void()> hook)
    {
        ref_ledger.setNamespacePresenceProbeAfterTerminalProvenHookForTest(std::move(hook));
    }
    uint64_t recoveryInstallCountForTest() const { return ref_ledger.recoveryInstallCountForTest(); }

    /// Test-only: fault seam for the ref-flush two-phase carve/validation protocol; forwards to
    /// `CasRefLedger::setCarveHookForTest` (see it for the phase-point contract).
    void setCarveHookForTest(std::function<void(CasRefLedger::CarvePhaseForTest)> hook)
    {
        ref_ledger.setCarveHookForTest(std::move(hook));
    }

    /// Test-only: negative control for the post-durable install region; forwards to
    /// `CasRefLedger::setInstallRegionProbeForTest` (see it for what an allocating probe must do).
    void setInstallRegionProbeForTest(std::function<void()> probe)
    {
        ref_ledger.setInstallRegionProbeForTest(std::move(probe));
    }
    void setSnapshotAfterCaptureHookForTest(std::function<void()> hook)
    {
        ref_ledger.setSnapshotAfterCaptureHookForTest(std::move(hook));
    }
    void setSnapshotBeforeCkptCasHookForTest(std::function<void()> hook)
    {
        ref_ledger.setSnapshotBeforeCkptCasHookForTest(std::move(hook));
    }

    /// Test-only: replace the request controller's inter-attempt backoff sleep (e.g. with a no-op) —
    /// for tests that drive a persistent conditional-write fault to budget exhaustion through a fully
    /// wired Pool/disk and must not serve the production capped-exponential sleeps for real (see
    /// `CasRequestController::setSleepFnForTest`). Call before driving traffic; empty restores the
    /// real sleep.
    void setCasRetrySleepForTest(std::function<void(uint64_t)> sleep_fn);

    /// Queue depth for the ref-append-lane tests (mirrors `shardQueuePendingForTest`): how many
    /// `appendRefOps` callers are enqueued for `ns` right now.
    size_t refQueuePendingForTest(const RootNamespace & ns) { return ref_ledger.refQueuePendingForTest(ns); }

    /// Test seam: whether `ns` currently has an active append-lane leader (the baton). Mirrors
    /// `refQueuePendingForTest`; used to assert the baton is not stranded on a pre-tenure fault.
    bool refLeaderActiveForTest(const RootNamespace & ns) { return ref_ledger.refLeaderActiveForTest(ns); }

    /// Test seam: how many concurrent `ensureRefTableRecovered` callers for `ns` are
    /// PARKED right now waiting on the leader's in-flight recovery (see `RefTableRuntime::
    /// recovery_waiters_for_test`) -- lets a test `yield()`-poll for "a second caller actually reached
    /// the wait" deterministically, mirroring `refQueuePendingForTest` above.
    uint64_t refRecoveryWaitersForTest(const RootNamespace & ns) { return ref_ledger.refRecoveryWaitersForTest(ns); }

    /// cache-eviction test seams: how many whole ref tables are cached right now, and whether a
    /// specific table's runtime is currently materialized (recovered) in the cache -- a table that was
    /// evicted reports false until its next touch re-recovers it.
    size_t refTablesCachedCountForTest() { return ref_ledger.refTablesCachedCountForTest(); }
    bool refTableCachedForTest(const RootNamespace & ns) { return ref_ledger.refTableCachedForTest(ns); }
    uint64_t refTableRuntimeIdentityForTest(const RootNamespace & ns)
    {
        return ref_ledger.refTableRuntimeIdentityForTest(ns);
    }
    uint64_t refTableRuntimeAdmittedFenceGenerationForTest(const RootNamespace & ns)
    {
        return ref_ledger.refTableRuntimeAdmittedFenceGenerationForTest(ns);
    }
    std::optional<NamespaceLifeId> refTableLifeForTest(const RootNamespace & ns)
    {
        return ref_ledger.refTableLifeForTest(ns);
    }

    /// Recovery-publication inventory seams (forward to `CasRefLedger`): the seeded admission budgets,
    /// the recovered base snapshot's encoded body size and the tail-since-snapshot byte sum.
    uint64_t refSnapshotBudgetForTest(const RootNamespace & ns) { return ref_ledger.refSnapshotBudgetForTest(ns); }
    uint64_t refRemovalBudgetForTest(const RootNamespace & ns) { return ref_ledger.refRemovalBudgetForTest(ns); }
    uint64_t refBaseSnapshotBytesForTest(const RootNamespace & ns) { return ref_ledger.refBaseSnapshotBytesForTest(ns); }
    uint64_t refTailBytesSinceSnapshotForTest(const RootNamespace & ns) { return ref_ledger.refTailBytesSinceSnapshotForTest(ns); }
private:
    struct WriterCleanupDuty
    {
        String ref_name;
        ManifestRef manifest;
        uint64_t build_seq = 0;
    };

    struct WriterCleanupQueue
    {
        std::deque<std::shared_ptr<const WriterCleanupDuty>> pending;
        bool draining = false;
    };

    /// Drain `ns` before admitting its next ordinary mutation. Only one caller drains a namespace at a
    /// time; concurrent callers wait so none can overtake a cleanup whose build still holds the active
    /// watermark floor. The drain calls `ref_ledger` directly to avoid re-entering this Pool wrapper.
    void drainWriterCleanupDuties(const RootNamespace & ns);
    bool writerCleanupDutiesPending() const;

    /// The single admission seam for durable ref mutations exposed by `Pool`. Keeping drain-before-call
    /// here makes a new forwarding entry point visibly choose between servicing writer cleanup and
    /// deliberately bypassing it; the cleanup implementation itself uses `ref_ledger` directly.
    template <typename Mutation>
    decltype(auto) mutateRefsAfterWriterCleanup(const RootNamespace & ns, Mutation && mutation)
    {
        drainWriterCleanupDuties(ns);
        return std::forward<Mutation>(mutation)();
    }

    BackendPtr pool_backend;
    PoolConfig config;
    PoolMeta meta;

    mutable std::mutex writer_cleanup_mutex;
    std::condition_variable writer_cleanup_cv;
    std::map<RootNamespace, WriterCleanupQueue> writer_cleanup_queues;
    /// Sticky fail-close bit for the destructor's allocation-failure path. If a duty could not enter
    /// the queue, no mount teardown may claim a clean farewell even though the guarded map is empty.
    std::atomic<bool> writer_cleanup_queue_failed{false};

    /// CAS mixed-algo pools: monotone in-memory cache of `algos_used`, seeded from
    /// `meta.algos_used` at open. Guards `isAlgoAdmitted`/`refreshAdmittedAlgos` -- ITS OWN mutex,
    /// not `meta`'s (there is no other mutable access to `meta` post-open; this avoids taking a
    /// wider lock than the cache needs). Kept sorted (a plain vector; membership is a handful of
    /// entries, no need for a set).
    mutable std::mutex admitted_algos_mutex;
    std::vector<uint8_t> admitted_algos;

    /// Known-present cache: a bytes-bounded LRU set of blob hashes confirmed present in the pool.
    /// Value is a 1-byte presence marker; DedupWeight charges a fixed per-entry byte estimate so the
    /// configured `deduplication_cache_bytes` is an honest memory ceiling. nullptr ⇔ disabled.
    /// Marker stored for a blob hash known to be present. The value has no payload; the cache key is
    /// the complete `BlobRef`, including its hash algorithm.
    struct DedupPresent {};

    /// Fixed memory estimate used by `CacheBase` to enforce the configured byte ceiling. It is an
    /// estimate rather than an allocation measurement, but keeps cache growth bounded predictably.
    struct DedupWeight
    {
        size_t operator()(const DedupPresent &) const { return 64; }
    };
    using DeduplicationCache = CacheBase<BlobRef, DedupPresent, BlobRefHash, DedupWeight>;
    std::unique_ptr<DeduplicationCache> dedup_cache;
    Layout pool_layout;
    /// The plain-object surface (namespace files + loose mountpoint objects), extracted from Pool.
    /// Stateless over `Backend &` + `const Layout &`; declared AFTER
    /// pool_backend and pool_layout so it is constructed after (and destroyed before) both.
    CasPlainObjects plain_objects;
    /// The manifest read path + decode cache + locate, extracted from Pool.
    /// Injected with backend/layout/meta + the event-sink reference; owns the decode cache (whose
    /// synchronization is CacheBase-internal). Declared after event_sink_, pool_backend, meta, and
    /// pool_layout so it is constructed after (and destroyed before) all four.
    CasManifestReader manifest_reader;
    /// The writer ref-log / ref-table subsystem, extracted from Pool. Owns the
    /// whole-table ref cache, the append lane + wedge protocol, snapshot publication, stale-precommit
    /// sweep, cache-budget eviction, the remount/shutdown drain, and the CAS retry controller -- with
    /// the two ref mutexes. Declared AFTER event_sink_, pool_backend, meta, pool_layout, plain_objects
    /// and manifest_reader so it is constructed after (and destroyed before) every dependency it is
    /// injected with; its callbacks reach mount/watermark state now owned by `mount_runtime` (declared
    /// AFTER this member), but they capture `Pool` and run only at runtime after the Pool is fully
    /// constructed -- exactly as in the pre-3.5 layout, where the mount raw-members these callbacks reach
    /// were also declared after `ref_ledger`. `~Pool` still calls `ref_ledger.drainRefLanesForShutdown`
    /// explicitly, sequenced between `mount_runtime.stopRemountThread()` and
    /// `mount_runtime.finishTeardown()` exactly as before.
    CasRefLedger ref_ledger;
    /// The mount / write-fence / build-watermark / self-remount runtime, extracted
    /// from Pool. Owns the `MountLeaseKeeper`, the local `MountFence`, the per-server
    /// build watermark (`process_epoch` + the `builds_mutex`-guarded seq/registry) and its in-flight-build
    /// map, the live-incarnation `live_writer_epoch`, the unclean-epoch high-water-mark, and the
    /// self-remount recovery thread (with its own thread-lifecycle locks). Injected with backend/layout +
    /// the `MountConfig` slice + `server_root_id` + the event-sink reference + the pool `cas_request_budget`
    /// + a `remount_attempt` callback (== `Pool::tryRemountOnce`, which STAYS on Pool: the claim/recovery
    /// ORCHESTRATION drives these owned primitives).
    ///
    /// Declared AFTER `ref_ledger` -- preserving the pre-3.5 relative order VERBATIM (the mount raw-members
    /// this component replaces all sat after `ref_ledger`), so `mount_runtime` is destroyed FIRST and
    /// `ref_ledger` LAST. Both orders were proven equally safe -- `~Pool`
    /// quiesces both subsystems before ANY member dtor runs (stopRemountThread ->
    /// ref_ledger.drainRefLanesForShutdown -> mount_runtime.finishTeardown), and the ledger's async paths
    /// pin `Pool::shared_from_this`, so no ledger->mount callback can fire during destruction in either
    /// order. Both safe ⇒ this is a pure behavior-preserving relocation, so the ORIGINAL order is kept and
    /// NO member-order change is introduced. Declared after event_sink_, pool_backend, config and
    /// pool_layout so it is constructed after every dependency it is injected with.
    CasMountRuntime mount_runtime;

    /// Serializes `tryRemountOnce` (whose claim/recovery ORCHESTRATION stays on Pool). STAYS here with
    /// its guarded critical section: the self-remount thread-lifecycle locks + fence
    /// atomics + build registry moved to `mount_runtime`, but the top-level remount serialization guards
    /// the Pool-side orchestration, so it stays on Pool.
    std::mutex remount_mutex;

    /// The single home of the [D5] per-lifecycle reason detail (spec §1) — the human-readable text that
    /// names the ACTUAL sub-state, WITHOUT the `content-addressed pool '<id>' ` prefix. Both
    /// `throwIfLifecycleTerminal` (which prefixes it and throws) and the non-gated `lifecycleSnapshot`
    /// (which surfaces it verbatim in the system table) read it, so the error message and the introspection
    /// row can never drift. Empty for `Live`/`TransientNotLive` (no terminal detail); for
    /// `VanishedForgotten` it prefers the stored `vanishedReason()` (carrying the operator's decommission
    /// timestamp) and falls back to the static [D5] text when none was recorded (a forced-for-test state).
    String lifecycleReasonDetail(PoolLifecycle lc) const;

    /// NOTE (M-C2): the ref-log is never trimmed here — trimming needs GC's fold state
    /// (`last_folded_ref_id`, INV-JOURNAL-COVERAGE), which is GC state landing in M-C3.
};

}
