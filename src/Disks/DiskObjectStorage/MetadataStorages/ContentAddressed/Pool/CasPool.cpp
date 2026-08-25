#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasCodecUtil.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInstrumentedBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasProbe.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasSentinelProbe.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>
#include <base/scope_guard.h>
#include <fmt/format.h>
#include <algorithm>
#include <chrono>
#include <ctime>
#include <thread>
#include <unordered_set>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
    extern const int FILE_DOESNT_EXIST;
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_STATE;
}
}

namespace ProfileEvents
{
    extern const Event CASPartFolderManifestGets;
    extern const Event CASRemountHeldTransient;
    extern const Event CASRefBatchFlushes;
    extern const Event CASRefBatchedMutations;
    extern const Event CASRefBatchScopeCuts;
    extern const Event CASRefQueueWaitMicroseconds;
    extern const Event CASRefRecoveryRestarts;
    extern const Event CASRefRecoveryRetries;
    extern const Event CASRefAppendWedged;
    extern const Event CASRefAppendUnwedged;
    extern const Event CASRefAppendDefiniteFailure;
    extern const Event CASRefSweepDeferred;
    extern const Event CASRefSweepRearmed;
    extern const Event CASRefStalePrecommitsReclaimed;
    extern const Event CASRefTableEvictions;
    extern const Event CASRefSnapshotPutBytes;
    extern const Event CASRefSnapshotTailLogs;
    extern const Event CASRefSnapshotPublishDispatched;
    extern const Event CASRefSnapshotPublishBackoff;
    extern const Event CASDeduplicationCacheHits;
    extern const Event CASDeduplicationCacheMisses;
}

namespace CurrentMetrics
{
    extern const Metric CASDeduplicationCacheBytes;
    extern const Metric CASDeduplicationCacheEntries;
}

namespace DB::Cas
{

namespace
{

/// The verdict of the pool-lifecycle identity gate (step 0 of `tryRemountOnce`, spec §2). Exactly one
/// `Recover` path falls through to the existing fresh-incarnation recovery; every other verdict is
/// resolved by the gate itself (a terminal transition, or staying transient to retry).
enum class LifecycleGateVerdict : uint8_t
{
    Recover,        /// `_pool_meta` present + identity matches: proceed with the existing recovery.
    Replaced,       /// `_pool_meta` present + a FOREIGN pool_id: `Vanished(replaced)` immediately.
    IdentityLost,   /// both sentinels (`_pool_meta` + owner) authoritatively absent: fail-loud terminal.
    StayTransient,  /// a probe error, an undecodable meta, or any ambiguous observation: retry as today.
};

struct LifecycleGate
{
    LifecycleGateVerdict verdict;
    String reason;   /// human-readable detail for the WARN / typed error (only meaningful for Replaced).
};

/// Authoritative, cache-bypassing evaluation of the §2 verdict table. Reads ONLY; never claims,
/// allocates, or writes. `expected_pool_id`/`expected_blob_header_len` are the identity this Pool
/// established at open — the comparison is over those two fields ONLY ([B6]); `algos_used` and
/// `min_reader_generation` are legally mutable and are deliberately not compared (the format gate is the
/// decode itself succeeding).
LifecycleGate probePoolLifecycleGate(
    Backend & backend, const Layout & layout, const String & srid,
    UInt128 expected_pool_id, uint64_t expected_blob_header_len)
{
    const SentinelProbeResult meta_probe = probeSentinel(backend, layout.poolMetaKey());
    switch (meta_probe.outcome)
    {
        case ProbeOutcome::Present:
        {
            /// Format gate = a successful, compatible decode. A present-but-undecodable body proves
            /// neither identity nor a foreign pool_id, so it stays transient rather than being declared
            /// replaced (throw-when-uncertain).
            PoolMeta fresh;
            try
            {
                if (!meta_probe.body)
                    return {LifecycleGateVerdict::StayTransient, "_pool_meta probed Present without a body"};
                fresh = decodePoolMeta(*meta_probe.body);
            }
            catch (...)
            {
                return {LifecycleGateVerdict::StayTransient, "_pool_meta present but could not be decoded"};
            }
            if (fresh.pool_id == expected_pool_id && fresh.blob_header_len == expected_blob_header_len)
                return {LifecycleGateVerdict::Recover, {}};
            return {LifecycleGateVerdict::Replaced,
                    fmt::format("data root replaced by a foreign pool (pool_id {} != {})",
                                u128ToHex(fresh.pool_id), u128ToHex(expected_pool_id))};
        }
        case ProbeOutcome::KeyAbsent:
        {
            /// `_pool_meta` is authoritatively gone. Require the OTHER sentinel (the owner anchor) to be
            /// conclusively absent too before declaring identity lost — any surviving sentinel, or an
            /// undecidable owner probe, keeps us transient (throw-when-uncertain). rev.8: BOTH sentinels
            /// authoritatively absent ⇒ `IdentityLost` (a fail-loud terminal state), regardless of whatever
            /// else remains under the prefix. Erasure is never PROVEN by the system — only asserted by the
            /// operator's `FORGET` — so there is no prefix-emptiness leg here.
            const SentinelProbeResult owner_probe = probeSentinel(backend, layout.ownerKey(srid));
            if (owner_probe.outcome != ProbeOutcome::KeyAbsent)
                return {LifecycleGateVerdict::StayTransient,
                        "_pool_meta absent but the owner sentinel was not conclusively absent"};
            return {LifecycleGateVerdict::IdentityLost,
                    "pool sentinels (_pool_meta + owner) authoritatively absent"};
        }
        default:   /// ContainerAbsent / AccessDenied / Indeterminate — absence was never proven.
            return {LifecycleGateVerdict::StayTransient, "pool-meta probe inconclusive"};
    }
}

}

Pool::Pool(BackendPtr backend_, PoolConfig config_, PoolMeta meta_)
    : pool_backend(std::move(backend_))
    , config(std::move(config_))
    , meta(std::move(meta_))
    /// Seed the monotone admitted-algo cache from the pool state `createOrValidate` already
    /// established (fresh create, steady-state member, or a just-completed admission union) --
    /// register-before-first-write means this Pool's own `writeAlgo()` is ALWAYS a
    /// member by the time the constructor runs.
    , admitted_algos(meta.algos_used)
    /// `Layout` no longer captures a pool algo -- every blob key is built from a
    /// `BlobRef` (algo + digest) directly, so the constructor takes only the pool prefix.
    , pool_layout(config.pool_prefix)
    /// Plain-object surface component: binds to this Pool's own backend + layout (declared after
    /// both, so this reference-holding member is constructed last and destroyed first) plus two
    /// fence-generation callbacks reaching `mount_runtime` (declared AFTER `plain_objects`, hence
    /// constructed after it -- these callbacks capture `this` and are invoked only at runtime,
    /// post-construction, exactly like `ref_ledger`'s callbacks below, so referencing a
    /// not-yet-constructed sibling member through them is safe).
    , plain_objects(
          *pool_backend, pool_layout,
          [this] { return mount_runtime.fenceGeneration(); },
          [this] (uint64_t gen) { mount_runtime.checkFenceOrThrow(gen); })
    /// Manifest reader component: backend/layout/meta by reference + the event-sink reference. The
    /// sink is installed by the factory before writable mounting starts. Owns the decode cache,
    /// built from the same config bytes the Pool ctor used before.
    , manifest_reader(*pool_backend, pool_layout, meta, event_sink_, config.manifest_decode_cache_bytes)
    /// Ref-log / ref-table subsystem. Injected with backend/layout + the
    /// RefLedgerConfig slice + the event-sink reference + the pool `cas_request_budget` + the RAW mount
    /// `boot_ms_fn` (for its retry controller), plus callbacks into the mount/watermark state that lives
    /// on `mount_runtime` (reached through Pool delegates). The callbacks capture `this`; they are
    /// invoked only at runtime (post-construction), so referencing `mount_runtime` (declared AFTER
    /// `ref_ledger`, hence constructed after it) is safe -- exactly as the pre-3.5 layout referenced the
    /// mount raw-members that also followed `ref_ledger`. Declared/constructed BEFORE `mount_runtime`,
    /// preserving the original member order verbatim (see the header note).
    , ref_ledger(
          pool_backend, pool_layout, config.refLedgerConfig(), event_sink_, config.cas_request_budget,
          config.server_root_id,
          config.boot_ms_fn,
          [this] { return liveWriterEpoch(); },
          [this] { return refAppendFenceOk(); },
          [this] { return mount_runtime.fenceGeneration(); },
          [this] (uint64_t gen) { mount_runtime.checkFenceOrThrow(gen); },
          [this] { return bootMsNow(); },
          [this] { return mayMutate(); },
          [this] (const String & key, const String & reason, const std::optional<String> & offending_ns)
              { reportImpossibleInterference(key, reason, offending_ns); },
          [this] { return std::static_pointer_cast<void>(shared_from_this()); },
          [this] (const RootNamespace & ns) { cancelInflightBuildsForNamespace(ns); })
    /// Mount / write-fence / build-watermark / self-remount runtime. Injected with
    /// backend/layout + the `MountConfig` slice + `server_root_id` + the event-sink reference + the pool
    /// `cas_request_budget` + the `remount_attempt` callback (== `Pool::tryRemountOnce`, whose claim/
    /// recovery ORCHESTRATION stays on Pool). The callback captures `this`; it is invoked only at runtime
    /// (post-construction). Declared/constructed AFTER `ref_ledger`, preserving the original member order
    /// verbatim (mount destroyed first, ledger last; both orders proven safe -- see the header note).
    , mount_runtime(
          pool_backend, pool_layout, config.mountConfig(), config.server_root_id, event_sink_,
          config.cas_request_budget,
          [this] { return tryRemountOnce(); })
{
    if (config.deduplication_cache_bytes > 0)
        dedup_cache = std::make_unique<DeduplicationCache>(
            "LRU", CurrentMetrics::CASDeduplicationCacheBytes, CurrentMetrics::CASDeduplicationCacheEntries,
            config.deduplication_cache_bytes, DeduplicationCache::NO_MAX_COUNT, DeduplicationCache::DEFAULT_SIZE_RATIO);
}

bool Pool::isAlgoAdmitted(BlobHashAlgo algo) const
{
    const auto v = static_cast<uint8_t>(algo);
    std::lock_guard lock(admitted_algos_mutex);
    return std::binary_search(admitted_algos.begin(), admitted_algos.end(), v);
}

std::vector<uint8_t> Pool::refreshAdmittedAlgos()
{
    /// A direct GET+decode of `_pool_meta`, not a re-run of `createOrValidate`'s admission logic --
    /// this Pool's OWN algo is already admitted, so all this
    /// needs is the CURRENT authoritative `algos_used`, unioned into the monotone cache.
    const auto existing = pool_backend->get(pool_layout.poolMetaKey());

    std::lock_guard lock(admitted_algos_mutex);
    if (existing)
    {
        const PoolMeta fresh = decodePoolMeta(existing->bytes);
        for (uint8_t v : fresh.algos_used)
            if (!std::binary_search(admitted_algos.begin(), admitted_algos.end(), v))
            {
                admitted_algos.push_back(v);
                std::sort(admitted_algos.begin(), admitted_algos.end());
            }
    }
    return admitted_algos;
}

bool Pool::dedupCacheContains(const BlobRef & ref) const
{
    /// raw lookup counters on the presence cache itself, disabled
    /// (nullptr `dedup_cache`) means neither counter moves -- the short-circuit below never reaches the
    /// probe. `PartWriteTxn::putBlob` calls this seam up to twice on a genuine hit (once to pick the
    /// HEAD-first branch, once more just to attribute `CASBlobBodyPutAvoided` to the cache -- see
    /// CasPartWriteTxn.cpp), so `CASDeduplicationCacheHits` counts LOOKUPS, not distinct blobs or putBlob calls. A hit
    /// does not itself skip the HEAD that follows in putBlob's HEAD-first branch -- it steers the call
    /// onto that cheap branch instead of an unconditional body stream; the body PUT is what a hit
    /// actually avoids.
    if (!dedup_cache)
        return false;
    if (dedup_cache->contains(ref))
    {
        ProfileEvents::increment(ProfileEvents::CASDeduplicationCacheHits);
        return true;
    }
    ProfileEvents::increment(ProfileEvents::CASDeduplicationCacheMisses);
    return false;
}

void Pool::dedupCacheAdd(const BlobRef & ref)
{
    if (dedup_cache)
        dedup_cache->set(ref, std::make_shared<DedupPresent>());
}

/// ==== mount-runtime delegates ==== The mount lease keeper, the local write
/// fence, the per-server build watermark, the live-incarnation epoch, and the self-remount recovery
/// thread live in the `mount_runtime` member (Pool/CasMountRuntime.h); Pool keeps these thin public
/// forwarders so the wiring, PartWriteTxn, Gc, the ref-ledger callbacks, and every test call site are unchanged.
uint64_t Pool::bootMs()
{
    return CasMountRuntime::bootMs();
}

uint64_t Pool::bootMsNow() const
{
    return mount_runtime.bootMsNow();
}

bool Pool::mayMutate() const
{
    return mount_runtime.mayMutate();
}

void Pool::tripMountLost()
{
    mount_runtime.tripMountLost();
}

bool Pool::refAppendFenceOk() const
{
    return mount_runtime.refAppendFenceOk();
}

void Pool::setMountDeadline(uint64_t deadline_boot_ms)
{
    mount_runtime.setMountDeadline(deadline_boot_ms);
}

void Pool::armMountFence(UInt128 server_uuid, uint64_t writer_epoch, uint64_t deadline_boot_ms)
{
    mount_runtime.armMountFence(server_uuid, writer_epoch, deadline_boot_ms);
}

String Pool::lifecycleReasonDetail(PoolLifecycle lc) const
{
    /// The [D5] per-reason detail (spec §1) — named once here so the typed error and the introspection
    /// snapshot always agree. No `content-addressed pool '<id>' ` prefix (callers add it if they want one).
    switch (lc)
    {
        case PoolLifecycle::Live:
        case PoolLifecycle::TransientNotLive:
            return {};
        case PoolLifecycle::IdentityLost:
            return "identity lost — the pool sentinels are absent; access fails loud. Recover by restart or "
                   "SYSTEM CAS FORGET (a matching-sentinel restore does not auto-revive it).";
        case PoolLifecycle::VanishedReplaced:
            return "data root replaced by a foreign pool (pool_id mismatch) — our generation is gone; "
                   "restart re-registers the name.";
        case PoolLifecycle::VanishedForgotten:
        {
            /// The forgotten detail carries the operator's decommission TIMESTAMP, threaded through
            /// `enterVanished`'s reason by `forgetDisk` (`vanishedReason()`). A forced-for-test
            /// `VanishedForgotten` (no real FORGET ran) has no stored reason, so fall back to the static
            /// [D5] text — it still names the sub-state and keeps "erasure was NOT verified".
            const String & reason = mount_runtime.vanishedReason();
            if (!reason.empty())
                return reason;
            return "decommissioned by SYSTEM CAS FORGET — erasure was NOT verified; if this "
                   "was a mistake the data may be intact (restart re-registers the name).";
        }
    }
    return {};   /// unreachable — every `PoolLifecycle` value is handled above (`-Wswitch` enforces it).
}

void Pool::throwIfLifecycleTerminal() const
{
    /// The typed error carries the sub-state in its message so a wrong diagnosis is impossible from the
    /// first error line (spec §1 [D5]). `Live`/`TransientNotLive` proceed here — the transient class is
    /// still gated only by the write fence in this task (the full six-class gate is Task 8).
    const PoolLifecycle lc = mount_runtime.lifecycle();
    if (lc == PoolLifecycle::Live || lc == PoolLifecycle::TransientNotLive)
        return;
    throw Exception(ErrorCodes::INVALID_STATE,
        "content-addressed pool '{}' {}", config.server_root_id, lifecycleReasonDetail(lc));
}

Pool::LifecycleSnapshot Pool::lifecycleSnapshot() const
{
    /// Non-gated, I/O-free (spec §7). Read the lifecycle ONCE (acquire), then the detail/`since` it
    /// implies. `lifecycleReasonDetail` reads `vanishedReason()` only for a terminal state we have already
    /// acquire-observed here, and `since` was release-stored before the same transition — so this coherent
    /// triple never mixes a terminal state with a pre-terminal detail/timestamp.
    LifecycleSnapshot snap;
    snap.lifecycle = mount_runtime.lifecycle();
    snap.detail = lifecycleReasonDetail(snap.lifecycle);
    snap.since = mount_runtime.lifecycleSinceWallS();
    return snap;
}

PoolPtr Pool::open(BackendPtr backend, PoolConfig config)
{
    /// Wrap the pool backend once, transparently, so EVERY CA S3 op — probe, pool-meta,
    /// writer, GC, watermark — flows through the per-namespace/op ProfileEvents chokepoint. The
    /// decorator only delegates and counts; it changes no behavior (read-only opens stay write-free).
    backend = std::make_shared<InstrumentedBackend>(std::move(backend));

    /// FAIL-CLOSED: the capability probe throws NOT_IMPLEMENTED on any failed check, and
    /// PoolMeta::createOrValidate is pool-authoritative — the config constants apply only at creation.
    Layout layout(config.pool_prefix);
    bool initialize_empty_catalog = false;
    /// The probe writes and deletes throwaway keys to verify conditional-op enforcement. A read-only
    /// open must never mutate the pool it inspects; fsck only reads, so skip it. (Pool meta below is
    /// read-only when the pool already exists; a missing pool meta on a read-only backend fails closed.)
    if (!config.read_only)
    {
        /// (0) [C4][D2] Zero-write residual check FIRST — before ANY probe write. `pool_prefix` is
        /// EXCLUSIVELY CAS-owned; `createOrValidate` below may mint a fresh `_pool_meta` only over a
        /// genuinely empty prefix. The MUTATING capability battery must run AFTER this proof (it writes
        /// `_probe/` debris, which would itself make the prefix look non-empty), and the emptiness
        /// classification IGNORES structurally-valid `_probe/` debris so a normal restart after a
        /// crash-mid-battery still bootstraps cleanly. This closes the "restart poisons a
        /// partially-erased pool" hole: a missing `_pool_meta` over residual data now fails startup loud
        /// with zero writes, instead of minting a fresh identity on top of the old objects.
        switch (probePoolBootstrapResidual(*backend, layout))
        {
            case BootstrapResidual::PoolMetaPresent:
                break;   /// authoritative existing pool; its catalog is mandatory below.
            case BootstrapResidual::EmptyOrProbeOnly:
            case BootstrapResidual::CanonicalEmptyCatalogOnly:
                initialize_empty_catalog = true;
                break;   /// proven-new or canonical catalog-only pre-meta bootstrap state.
            case BootstrapResidual::ResidualWithoutMeta:
            {
                /// RECREATION QUIESCE. Reaching here means the prefix holds objects but no authoritative
                /// `_pool_meta` -- the shape a pool recreation leaves behind. Before telling the operator
                /// anything about residual data, ask the one question whose answer changes the remedy: is
                /// a writer still entitled to this prefix? Refusing an OLD-FORMAT open fences nothing --
                /// a server that mounted before the erase is still running, still holds its lease, and
                /// still has queued writes; if the operator answers the residual message below by
                /// clearing the prefix and recreating, that writer's next flush lands its old-format
                /// transactions inside the NEW pool. So a non-terminal slot fails closed with its own
                /// message: stop the writer first.
                ///
                /// This gate is the PRIMARY defence, not a nicety, because the mount fence cannot be
                /// relied on to catch a straggler afterwards. Clearing the prefix also destroys the
                /// durable writer-epoch counter, so a recreation by the SAME server uuid is handed the
                /// very `(uuid, epoch)` the survivor still holds -- and the two are then indistinguishable
                /// to the lease protocol, which reads the survivor's renewal as its own keeper adopting a
                /// refreshed body. The fence only bites when the recreating mount is DISTINGUISHABLE (a
                /// different server uuid, or a surviving epoch counter): then the survivor's next renewal
                /// finds a slot it cannot hold and its local fence latches shut.
                ///
                /// What catches the ambiguous case is INV-1, after the fact: the straggler's append lands
                /// at `{E, 1}`-relative ids in a table the recreated pool sees as empty, and the first
                /// recovery of that namespace refuses the stream as non-contiguous rather than absorbing
                /// it. That is a loud post-mortem, not prevention -- which is why the refusal here, BEFORE
                /// anything is cleared, is the one that matters.
                ///
                /// Only on this arm: `EmptyOrProbeOnly` proves there is no slot object to read (a mount
                /// lease is itself residual), and `PoolMetaPresent` is not a recreation at all -- neither
                /// pays for the scan.
                const std::vector<NonTerminalMountSlot> held = probeNonTerminalMountSlots(*backend, layout);
                if (!held.empty())
                {
                    String detail;
                    for (const NonTerminalMountSlot & slot : held)
                        detail += fmt::format("\n  server root '{}': {}", slot.server_root_id, slot.detail);
                    throw Exception(ErrorCodes::INVALID_STATE,
                        "content-addressed pool '{}' (prefix '{}'): missing _pool_meta, but {} mount "
                        "lease(s) under this prefix are still held — refusing to recreate the pool while "
                        "a writer may still be using it. Stop (or decommission) the holder(s) so their "
                        "mount slots become terminal, then retry; do NOT clear the prefix first, which "
                        "would leave the surviving writer appending into the new pool.{}",
                        config.server_root_id, config.pool_prefix, held.size(), detail);
                }
                throw Exception(ErrorCodes::INVALID_STATE,
                    "content-addressed pool '{}' (prefix '{}'): missing _pool_meta over a non-empty pool "
                    "prefix — refusing to bootstrap over residual data; recreate the pool or restore "
                    "_pool_meta. The pool prefix is exclusively CAS-owned.",
                    config.server_root_id, config.pool_prefix);
            }
            case BootstrapResidual::Indeterminate:
                throw Exception(ErrorCodes::INVALID_STATE,
                    "content-addressed pool '{}' (prefix '{}'): could not authoritatively list the pool "
                    "prefix to prove it is safe to bootstrap — refusing to create _pool_meta while "
                    "residual data cannot be ruled out (fail-closed).",
                    config.server_root_id, config.pool_prefix);
        }

        if (!config.skip_access_check)
        {
            /// Give each mount a PER-MOUNT UNIQUE probe key prefix so two servers mounting the SAME
            /// shared pool concurrently never collide on the (formerly fixed) `<pool>/_probe/token` /
            /// `<pool>/_probe/cas` keys. Without this, the loser of the `putIfAbsent` race aborts startup
            /// with PreconditionFailed (and the winner's cleanup delete can cascade into the loser). With a
            /// fresh random 128-bit id per `Pool::open`, each mounter validates conditional-op support
            /// independently. A crashed mount leaves harmless `_probe/<rand>/...` debris under the `_probe/`
            /// namespace only (never the content planes) — acceptable.
            const UInt128 probe_uid = (static_cast<UInt128>(thread_local_rng()) << 64) | thread_local_rng();
            runCapabilityProbe(*backend, config.pool_prefix + "/_probe/" + u128ToHex(probe_uid));
        }
        else
        {
            /// skip_access_check: skip the access-check-class probe I/O (store preconditions + the
            /// `_probe/` round trip, both folded into runCapabilityProbe above) but NOT the two
            /// fail-closed gates below — see `PoolConfig::skip_access_check`.
            ///
            /// First, whether this backend may skip the battery AT ALL. A generation-dialect (GCS)
            /// backend may not: the battery is the only thing that proves a token-exact DELETE
            /// actually carries its generation precondition, so skipping it here would let GC delete
            /// an incarnation it never condemned. Asking the backend keeps that policy where the
            /// dialect is known, instead of type-testing the concrete object storage from here.
            backend->checkSkipAccessCheckSupport();
            /// Then the single-attempt conditional-write gate, so a Native-mode backend with no
            /// working single-attempt client still fails closed at open instead of silently
            /// corrupting CAS state under blind retries later.
            backend->checkConditionalWriteSingleAttemptSupport();
        }
    }
    /// The catalog is mandatory for every minted pool. Make it durable before `_pool_meta`: otherwise
    /// an acknowledgement-loss or definite catalog-write failure could strand an authoritative meta
    /// that makes every later open refuse the absent catalog. The catalog-only residual proof above is
    /// the narrowly-defined retry path when this opener (or a concurrent opener) completed this step
    /// but did not reach the pool-meta create.
    if (initialize_empty_catalog)
        CasRefCatalog::initializeEmptyForNewPool(*backend, layout);
    /// `allow_mint` = writable open only: a writable `Pool::open` reaches here having just passed the
    /// zero-write residual proof above, so minting a missing `_pool_meta` is safe. A read-only/observe
    /// open never ran that proof (and there is no truly-read-only backend — `openPoolView` opens the same
    /// writable object storage and only sets `read_only`), so it must NEVER mint: an absent meta fails
    /// closed instead (spec §2 [C4][D2]).
    PoolMeta meta = PoolMeta::createOrValidate(
        *backend, layout, config.blob_header_len, config.gc_shards, config.blob_hash_algo, config.blob_hash_allow_new,
        /*allow_mint=*/!config.read_only);
    config.gc_shards = meta.gc_shards;
    const BlobHashAlgo write_algo = config.blob_hash_algo;   /// `config` is moved-from just below

    /// Private ctor: make_shared cannot reach it.
    PoolPtr store(new Pool(std::move(backend), std::move(config), std::move(meta)));
    store->setEventSink(std::move(store->config.event_sink));

    /// Register-before-first-write, belt-and-braces: `createOrValidate` above already
    /// admitted/validated the write algo, so the freshly-seeded cache must already contain it -- a
    /// violation here would mean a build/write could reach this Pool naming an algo that was never
    /// durably admitted (the invariant this whole design rests on).
    chassert(store->isAlgoAdmitted(write_algo));

    /// Per-server watermark: mint the random NONZERO `process_epoch`
    /// once per Pool (GC checks it for equality only -- a different epoch == a dead incarnation). The
    /// masking/redraw detail lives in `CasMountRuntime::mintRandomProcessEpoch`.
    store->mount_runtime.mintRandomProcessEpoch();

    /// W-ANCHOR: the per-server watermark must be durable BEFORE any object PUT. A read-only open
    /// must never mutate the pool (the probe is skipped above for the same reason), so the watermark
    /// — which rides inside the `gc/server-roots/<server_root_id>/mount` lease object — is only
    /// constructed and anchored on a writable open.
    if (!store->config.read_only)
        mountWritable(store, store->config.server_id, MountClaimPolicy::WaitForExpiry);

    return store;
}

void Pool::mountWritable(PoolPtr & store, UInt128 our_uuid, MountClaimPolicy policy)
{
    /// === Mount-safety startup protocol ===
    /// STRICT ORDER: validate id → claim owner (identity) → allocate durable writer_epoch → claim
    /// the mount lease (liveness) + arm the local write fence → anchor the watermark. owner / epoch
    /// / mount / watermark are BOOTSTRAP-CONTROL writes: they establish the very right to write and
    /// run BEFORE the write fence gates ordinary data/ref/manifest mutations. Fail closed throughout.
    /// Shared by `open` (`policy = WaitForExpiry`, `our_uuid = config.server_id`) and
    /// `openForDecommission` (`policy = NoWait`, `our_uuid` = the impersonated victim owner uuid;
    /// -- the two differ only in WHO they mount
    /// as and whether a non-`Claimed`/`FencedSelf` mount result gets `open`'s bounded observation wait
    /// or an immediate refusal.
    const String & srid = store->config.server_root_id;

    /// 1. The server_root_id is a clean relative path (mirrors the config-read validation; cheap to
    ///    re-check here so a Pool opened directly in tests is held to the same contract).
    validateServerRootId(srid);

    const ObserveRefCatalog observe_catalog = [s = store.get()]()
    {
        CasRefCatalog::Snapshot snapshot = CasRefCatalog::read(*s->pool_backend, s->pool_layout);
        snapshot.life_index.throwIfAmbiguous("CAS server-root mount safety");
        return snapshot.catalog;
    };
    /// Existing matching owner and epoch objects take fast paths that do not need an emptiness
    /// observation. Validate the mandatory authority object unconditionally before either fast path
    /// can reach a slot mutation; the callback remains available for fresh conflict rechecks below.
    (void)observe_catalog();

    /// 2. Owner anchor — IDENTITY (clock-free). A foreign uuid fails closed; an absent owner over a
    ///    non-empty subtree is CORRUPTED_DATA; a fresh empty root is claimed.
    claimOwnerOrThrow(*store->pool_backend, store->pool_layout, srid, our_uuid, observe_catalog);

    /// Wall-clock `now_ms`, hoisted above the writer_epoch allocation below: the absent-epoch
    /// branch's `DecommissionRecovery` policy needs it to judge a surviving mount's liveness before
    /// the mount-lease claim (step 4) gets its own use of the same clock.
    const auto now_ms = []() -> uint64_t
    {
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    };

    /// 3. Durable-monotone writer_epoch — CAS-bump the sticky `epoch` object. THE BRIDGE: this
    ///    durable value REPLACES the random `process_epoch` for identity, so the watermark + every
    ///    manifest ref carries it (the random mint above stays for the read-only
    ///    path, which never reaches here). The epoch-aware sweep reads this value.
    /// Mutable: a GC fence of our fresh lease during open (expiry mid-open racing a GC round) is
    /// recoverable — a fence costs an epoch, so the fence-recovery loop below re-allocates a fresh
    /// writer_epoch and re-claims (the TLA+-checked `NoPermanentWedge` invariant).
    /// `epoch_policy`: `openForDecommission`'s `NoWait` gates the absent-epoch branch's mount-probe
    /// on a TERMINAL (not live) surviving mount instead of authoritative absence (Phase C) — passed
    /// uniformly to every call below, including the fence-recovery re-allocations, where it is inert
    /// because those run with the epoch object already present.
    const EpochMintPolicy epoch_policy = (policy == MountClaimPolicy::NoWait)
        ? EpochMintPolicy::DecommissionRecovery
        : EpochMintPolicy::NormalMount;
    uint64_t writer_epoch = allocateWriterEpoch(
        *store->pool_backend, store->pool_layout, srid, epoch_policy, now_ms(), observe_catalog);
    store->mount_runtime.setProcessEpoch(writer_epoch, std::memory_order_relaxed);

    /// 4. Mount lease — LIVENESS. Decide over the current mount object using the wall-clock `now_ms`
    ///    hoisted above.
    const uint64_t ttl_ms = static_cast<uint64_t>(store->config.mount_lease_ttl_ms.count());

    /// CAS request budget: a
    /// writable mount refuses to open with a budget that could let a controlled attempt outlive the
    /// mount lease it is fenced under. Throws BAD_ARGUMENTS and aborts open on an inconsistent
    /// budget; logs the effective values once on success. The controller gates Pool's ref-mutation
    /// paths; this validates the config invariant up front, before any attempt runs.
    validateCasRequestBudget(store->config.cas_request_budget, ttl_ms,
        static_cast<uint64_t>(store->config.mount_renew_period.count()));

    /// Poll twice per renew period so a live holder's renewal is always observed within the
    /// observation window. Derived from existing config — no new knob.
    const uint64_t poll_interval_ms = std::max<uint64_t>(
        1, static_cast<uint64_t>(store->config.mount_renew_period.count()) / 2);
    /// routes through `mount_runtime.waitSleep` (which itself routes through
    /// `config.wait_sleep_fn` when a test injected one) rather than a bare `sleep_for` directly, so
    /// a test intercepting `wait_sleep_fn` observes every wait `open` can block on -- and since the
    /// post-reclaim materialization grace was retired, this observation poll is the only one left.
    const auto sleep_ms = [s = store.get()](uint64_t ms) { s->mount_runtime.waitSleep(ms); };
    /// Operator-visible log the moment startup decides to watch a stale-looking self-mount (the
    /// disk-open path blocks up to ~threshold_ms here, so a silent block would be confusing). May
    /// fire more than once per open: the observation restarts (and re-logs) every time the watched
    /// lease's write-token changes before the full threshold elapses.
    const auto on_wait_start = [&srid](const MountLease & held, uint64_t threshold_ms)
    {
        LOG_INFO(getLogger("CasPool"),
            "CAS mount '{}': a stale-looking mount lease is held by uuid={} epoch={} pid={} "
            "hostname={} (expires_at_ms={}); observing its write-token for up to ~{} ms before "
            "reclaiming. If a second server is genuinely live, its renewals will keep restarting "
            "the observation and startup will eventually abort as a live double-start.",
            srid, u128ToHex(held.server_uuid), held.writer_epoch, held.pid, held.hostname,
            held.expires_at_ms, threshold_ms);
    };

    /// Mount-slot writer audit (the "foreign writer" instrument): route every mount-slot
    /// write/conflict event through the Pool's own sink. The factory installs the configured sink
    /// before this mount protocol starts, including before any renewal thread can emit.
    /// `s` outlives the lambda: it is captured by raw pointer into the keeper, a member of
    /// `Pool` destroyed before the `Pool` itself.
    const auto emit_mount_event = [s = store.get()](CasEvent e) { s->emitEvent(std::move(e)); };

    Pool * raw = store.get();

    /// Crash-recovery (`WaitForExpiry` only): a hard-killed prior incarnation leaves a stale,
    /// unreleased mount lease. Rather than aborting, OBSERVE that lease's write-token (never its
    /// stamped `expires_at_ms` against our wall clock) until it has held stable for the full
    /// rate-bound threshold, then reclaim it; a genuinely live second server keeps renewing the
    /// token and is (after bounded restarts) reported as LiveDoubleStart. The reclaim is
    /// token-guarded (see `claimMountAwaitingExpiry`), so a live twin is never stolen from.
    /// `NoWait` skips this observation entirely (see the policy branch below).
    ///
    /// Fence-recovery loop: if the GC fences our own fresh lease while we are opening
    /// (the lease expired mid-open — e.g. a slow first beat — and a GC round fenced it), that is a
    /// RECOVERABLE state, not a wedge: a fence costs an epoch, so allocate a fresh writer_epoch and
    /// re-claim. Bounded so a pathological fence storm still fails closed. The fence can surface two
    /// ways: `claimMount` observes an already-fenced own slot (`FencedSelf`), or the keeper's adopt
    /// races a fence between its GET and CAS (`MountFencedException` from `start()`).
    /// which certificate of death (if any) justified the reclaim FINALLY adopted below
    /// (the last iteration's `claim` before `break` -- `claim` itself is loop-scoped). Read after the
    /// loop to classify (and log) an unclean reclaim.
    MountPriorState claimed_prior = MountPriorState::None;
    /// The pre-I/O boot-clock instant of the claim attempt FINALLY adopted below -- survives the
    /// `break` so the arm below can detect a claim that consumed the lease TTL and re-anchor before
    /// arming (rev.4 Phase B, round-3 finding 2).
    uint64_t claim_anchor_boot_ms = 0;
    constexpr int max_fence_recoveries = 3;
    for (int fence_recovery = 0; ; ++fence_recovery)
    {
        MountClaimResult claim;
        if (policy == MountClaimPolicy::WaitForExpiry)
        {
            claim = claimMountAwaitingExpiry(
                *store->pool_backend, store->pool_layout, srid, our_uuid, writer_epoch,
                [&now_ms]() { return now_ms(); }, [raw] { return raw->bootMsNow(); },
                ttl_ms, poll_interval_ms, sleep_ms, on_wait_start, emit_mount_event);
        }
        else
        {
            /// NoWait (decommission gate): a single unobserved attempt -- no bounded wait-and-retry
            /// for a stale-looking lease to lapse. Anything but Claimed/FencedSelf below is refused
            /// immediately.
            claim = claimMount(*store->pool_backend, store->pool_layout, srid, our_uuid, writer_epoch,
                now_ms(), ttl_ms, /*proven_dead_token=*/{}, emit_mount_event);
        }
        if (claim.kind == MountClaimResult::FencedSelf)
        {
            if (fence_recovery >= max_fence_recoveries)
                throw Exception(ErrorCodes::ABORTED,
                    "CAS mount '{}': our own mount lease was GC-fenced repeatedly during open "
                    "({} recoveries exhausted) — a fresh writer_epoch kept being fenced before we "
                    "could adopt it. This should not persist; investigate GC fence-out timing.",
                    srid, max_fence_recoveries);
            writer_epoch = allocateWriterEpoch(
                *store->pool_backend, store->pool_layout, srid, epoch_policy, now_ms(), observe_catalog);
            store->mount_runtime.setProcessEpoch(writer_epoch, std::memory_order_relaxed);
            continue;
        }
        if (claim.kind != MountClaimResult::Claimed)
        {
            if (policy == MountClaimPolicy::NoWait)
                /// No FORCE variant, no wait-and-observe: the decommission gate treats any live-looking
                /// or foreign-owner lease as an immediate refusal.
                throw Exception(ErrorCodes::ABORTED,
                    "CAS decommission '{}': pool member is alive or contended — mount lease held by "
                    "uuid={} epoch={} pid={} hostname={} (expires_at_ms={}). Refusing (no FORCE variant "
                    "exists; stop the server or wait for its lease to lapse).",
                    srid, u128ToHex(claim.body.server_uuid), claim.body.writer_epoch, claim.body.pid,
                    claim.body.hostname, claim.body.expires_at_ms);
            /// LiveDoubleStart (waited out the bound → a live twin) or ForeignOwner → fail closed
            /// with the actionable, multi-line startup error.
            throw Exception(ErrorCodes::ABORTED, "{}", mountDoubleStartMessage(srid, claim.body));
        }
        claimed_prior = claim.prior;

        /// The mount object now holds OUR live (uuid, epoch) body. `installKeeper` constructs the keeper
        /// -- which ADOPTS that very (uuid, epoch) slot rather than self-tripping the double-start guard --
        /// AND wires its `minActive` build-watermark reader, its event sink, and the fence-coupling
        /// callbacks (renew-ok refreshes the monotonic deadline; on-lost latches the fence + arms a
        /// self-remount), all captured on `mount_runtime` (see `CasMountRuntime::installKeeper`).
        /// `keeperStart` is separate so this claim orchestration can catch `MountFencedException` and
        /// retry with a fresh epoch.
        store->mount_runtime.installKeeper(our_uuid, writer_epoch, now_ms);
        claim_anchor_boot_ms = store->bootMsNow();   /// pre-I/O anchor of the claim attempt
        try
        {
            store->mount_runtime.keeperStart();
        }
        catch (const MountFencedException &)
        {
            /// The GC fenced our fresh lease between the keeper's adopt GET and CAS. Recoverable:
            /// drop this keeper, take a fresh epoch, and re-claim.
            if (fence_recovery >= max_fence_recoveries)
                throw;
            store->mount_runtime.keeperReset();
            writer_epoch = allocateWriterEpoch(
                *store->pool_backend, store->pool_layout, srid, epoch_policy, now_ms(), observe_catalog);
            store->mount_runtime.setProcessEpoch(writer_epoch, std::memory_order_relaxed);
            continue;
        }
        break;
    }

    /// A reclaim over a predecessor whose death was NOT proven clean may still have a conditional PUT
    /// from that predecessor in flight -- `Fenced` and `UncleanObserved` are exactly the two
    /// `MountPriorState`s with no such proof (`Clean`, drained farewell, and `None`, a fresh mount /
    /// same-epoch refresh with nothing to hand over, are the proven ones).
    /// An EXHAUSTIVE switch, not a positive allowlist -- a future `MountPriorState`
    /// enumerator with no proof of clean death must fail the BUILD (a missing `-Wswitch` case), never
    /// silently fall through to "clean".
    ///
    /// THIS NO LONGER WAITS. The `materialization_grace_ms` (`T_mat`) sleep that used to sit here bought
    /// one thing: time for that straggler to land (or exhaust its retries) BEFORE this incarnation began
    /// trusting its recovery LISTINGS. Recovery does not trust listings any more. It walks each ref
    /// stream arithmetically from `_ckpt` and closes every dead epoch with an in-band `EpochSeal` at
    /// `{E, T+1}`, written as a conditional create -- so the straggler's own conditional create loses to
    /// an occupied slot no matter when it arrives, and a wait can only make startup slower, never safer.
    /// What survives is the CLASSIFICATION and saying it out loud: an unclean predecessor is worth an
    /// operator-visible line, and the exhaustive switch is worth keeping as the build-time guard.
    bool unclean_reclaim = false;
    switch (claimed_prior)
    {
        case MountPriorState::None:
        case MountPriorState::Clean:
            unclean_reclaim = false;
            break;
        case MountPriorState::Fenced:
        case MountPriorState::UncleanObserved:
            unclean_reclaim = true;
            break;
    }
    if (unclean_reclaim)
    {
        LOG_INFO(getLogger("CasPool"),
            "Content-addressed mount {} follows a predecessor whose death was not proven clean "
            "(writer_epoch {}). Opening without a grace period: a still-in-flight conditional PUT from "
            "that predecessor is fenced by the recovery seal, whenever it arrives.", srid, writer_epoch);
    }

    /// Arm the local write fence: cache (uuid, epoch) and set the boottime deadline at the claim
    /// attempt's anchor + ttl (NOT `bootMsNow()` here -- arming from a post-I/O instant would authorize
    /// mutations under a deadline the durable lease never actually backs). From here ordinary ref
    /// mutations (appendRefOps) are fence-gated via mayMutate.
    const uint64_t ttl_ms_u = static_cast<uint64_t>(store->config.mount_lease_ttl_ms.count());
    if (store->bootMsNow() >= claim_anchor_boot_ms + ttl_ms_u)
    {
        /// The claim path outlived the lease TTL: its anchor can no longer authorize an armed fence (a
        /// successor may have legally started reclaiming). Re-anchor with ONE fresh conditional lease
        /// write -- it fails closed (Phase A classification) if anything took the slot meanwhile -- and
        /// arm from the new attempt's anchor (rev.4 Phase B, round-3 finding 2).
        ///
        /// The unbounded operator-configured wait this guard was written for (`T_mat`) is gone, so
        /// reaching it now means the CLAIM ITSELF -- `keeperStart`'s GET+CAS -- outran the whole lease
        /// TTL, which `validateCasRequestBudget` already refuses to configure. It stays because a stalled
        /// socket can still outlive a budget, and its recovery is one conditional write that fails closed;
        /// it is LOUD rather than fatal because a slow open under a healthy protocol is not a reason to
        /// refuse to start.
        LOG_WARNING(getLogger("CasPool"),
            "Content-addressed mount {}: the mount claim consumed the lease TTL ({} ms) before the write "
            "fence could be armed; re-writing the lease first", srid, ttl_ms_u);
        claim_anchor_boot_ms = store->bootMsNow();
        store->mount_runtime.keeperRenewOnce();
    }
    store->armMountFence(our_uuid, writer_epoch, claim_anchor_boot_ms + ttl_ms_u);
    /// Gate the background renewer with `background_watermark`: it runs only in production
    /// (`background_watermark` = context != nullptr && !read_only), never in unit tests — which
    /// drive renewOnce (or renewWatermarkOnce) explicitly and rely on the armed sub-TTL deadline,
    /// never on the loop. The keeper itself is still started above (it must claim/adopt the mount +
    /// arm the fence on every writable open); only the renewal thread is conditional. The merged
    /// heartbeat renews at `mount_renew_period` — one beat now renews the lease and the floor.
    if (store->config.background_watermark)
        store->mount_runtime.keeperStartBackground(store->config.mount_renew_period);

    store->mount_runtime.setLiveWriterEpoch(writer_epoch);
}

PoolPtr Pool::openForDecommission(BackendPtr backend, PoolConfig config, const String & victim_srid)
{
    backend = std::make_shared<InstrumentedBackend>(std::move(backend));
    validateServerRootId(victim_srid);

    config.server_root_id = victim_srid;
    config.read_only = false;
    /// The pool exists (the calling disk validated it), so no probe writes are needed. This also
    /// means the capability battery never runs here, and — unlike the writable `Pool::open` path —
    /// `checkSkipAccessCheckSupport` is deliberately NOT consulted, so a generation-dialect (GCS)
    /// backend can still be decommissioned. The fail-closed tradeoff inverts for this operation:
    /// refusing an ordinary mount costs availability and protects data, whereas refusing a
    /// decommission strands a pool with a dead replica in it and leaves the operator no way forward.
    config.skip_access_check = true;
    /// The admin claim must be RENEWED like any writable mount: the host disk may be observe-only
    /// (background_watermark=false), but an unrenewed claim (TTL ~30s) aborts any long drain midway.
    config.background_watermark = true;

    Layout layout(config.pool_prefix);

    /// Impersonate the victim: decommission acts as "the next incarnation of that server". The claim
    /// below is then EXACTLY the crash-recovery reclaim semantics (`MountClaimPolicy::NoWait`): a
    /// fenced/terminated/clean-farewell lease reclaims; a live lease refuses immediately (no bounded
    /// observation wait -- see `mountWritable`). Owner anchor absent + mount absent = nothing to
    /// decommission.
    std::optional<UInt128> victim_uuid = readOwnerUuid(*backend, layout, victim_srid);
    if (!victim_uuid)
    {
        if (const auto mount = backend->get(layout.mountKey(victim_srid)))
            victim_uuid = decodeMountLease(mount->bytes).server_uuid;   /// partial hand-cleanup: adopt from the lease
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "CAS decommission '{}': unknown pool member (no owner anchor and no mount lease). "
                "Nothing to decommission; if victim objects linger without a slot, run cas-fsck.",
                victim_srid);
    }
    config.server_id = *victim_uuid;

    backend->checkConditionalWriteSingleAttemptSupport();
    /// Decommission operates on an EXISTING pool member (an owner anchor / mount lease was just found), so
    /// `_pool_meta` must already be present. It never bootstraps: `allow_mint=false` so an absent meta
    /// (a partially-erased pool whose owner anchor survives) fails closed with INVALID_STATE rather than
    /// minting a fresh identity here (spec §2 [C4][D2]).
    PoolMeta meta = PoolMeta::createOrValidate(
        *backend, layout, config.blob_header_len, config.gc_shards, config.blob_hash_algo, config.blob_hash_allow_new,
        /*allow_mint=*/false);
    config.gc_shards = meta.gc_shards;
    const BlobHashAlgo write_algo = config.blob_hash_algo;   /// `config` is moved-from just below

    /// Private ctor: make_shared cannot reach it.
    PoolPtr store(new Pool(std::move(backend), std::move(config), std::move(meta)));
    store->setEventSink(std::move(store->config.event_sink));

    /// Register-before-first-write, belt-and-braces: same invariant `open` asserts.
    chassert(store->isAlgoAdmitted(write_algo));

    /// No random `process_epoch` mint here: `open` pays that prologue because its read-only path
    /// never reaches `mountWritable` and so needs SOME nonzero epoch, but this factory is
    /// writer-only -- `mountWritable` below unconditionally overwrites `process_epoch` with the
    /// freshly allocated durable `writer_epoch` before anything could observe the zero-initialized
    /// default.
    mountWritable(store, *victim_uuid, MountClaimPolicy::NoWait);
    return store;
}

Pool::~Pool()
{
    /// Teardown order is load-bearing and unchanged from the pre-3.5 inline sequence (only the
    /// mount/remount mechanics were relocated into `mount_runtime`):
    ///
    /// 1. Stop + join the self-remount recovery thread FIRST (it may otherwise re-create the keeper
    ///    below us). `stopRemountThread` latches `remount_shutting_down` under the thread mutex before
    ///    the join, so a keeper on_lost firing during teardown can never re-arm the thread after we join.
    mount_runtime.stopRemountThread();

    /// 2. The farewell marker the keeper's `stop()` writes is a
    /// certificate that no in-flight ref-log conditional PUT from this incarnation can land after it --
    /// a successor treats it as proof of a clean death (`MountPriorState::Clean`, no observation wait
    /// needed). Writing it without an actual drain would be a protocol-safety bug: an uncertain PUT this
    /// incarnation is still resolving could land AFTER the successor already reclaimed and started
    /// mutating. `drainRefLanesForShutdown` is the drain; bounded by one attempt's worth of budget plus
    /// the lease safety margin -- long enough for an in-flight attempt to resolve, never unbounded. It
    /// stays on `Pool` (mediating the mount↔ledger coupling), sequenced between the two mount-runtime
    /// teardown steps exactly as before.
    const bool ref_lanes_drained = ref_ledger.drainRefLanesForShutdown(
        config.cas_request_budget.attempt_timeout_ms + config.cas_request_budget.lease_safety_margin_ms);
    const bool drained = ref_lanes_drained && !writerCleanupDutiesPending();

    /// 3. Retire the merged heartbeat: `finishTeardown` runs the keeper's terminal op on a clean drain
    /// (stamping the lease already-expired + folding in the watermark farewell so a SAME-server reopen
    /// reclaims immediately) or the fail-closed no-terminal-op on an unresolved PUT, then does the
    /// belt-and-suspenders remount-thread re-join. See `CasMountRuntime::finishTeardown`.
    mount_runtime.finishTeardown(drained);
}

void Pool::forgetDisk(const std::function<void()> & stop_and_join_gc, const String & reason)
{
    /// Hazard C6: FORGET joins the self-remount thread (and, via `stop_and_join_gc`, the GC threads), so it
    /// MUST run on the admin/query thread — never a pool thread, whose join of itself would deadlock. The
    /// guard is a programming-error assertion (a self-join hangs; it never corrupts), so a chassert is the
    /// right severity, not a release fail-close.
    const ThreadName tn = getThreadName();
    chassert(tn != ThreadName::CAS_REMOUNT && tn != ThreadName::CAS_GC_SCHEDULER
             && tn != ThreadName::CAS_GC_HEARTBEAT
             && "SYSTEM CAS FORGET must not run on a CAS pool thread (self-join deadlock)");

    /// Idempotent: an already-terminal `Vanished` pool (a second FORGET, or a pool that naturally vanished
    /// as replaced) is already the terminal truth — nothing to force, and re-running the teardown
    /// would double-retire the keeper. `IdentityLost`/`TransientNotLive`/`Live` all proceed (FORGET is
    /// their escape hatch). Reading `isVanished()` here without the lock is safe: only a terminal transition
    /// sets it, terminal states are absorbing, and a natural transition that wins concurrently below merely
    /// makes our own `enterVanished` a no-op (first terminal transition wins).
    if (mount_runtime.isVanished())
        return;

    /// (1) Publish the terminal-intent latch FIRST (spec §5). The keeper callback stops arming remounts and
    /// the remount loop bails at its next step boundary, so every join below is bounded to one step + one
    /// backend timeout.
    mount_runtime.publishVanishedIntent();

    /// (2) Trip the local fence — the deliberate decommission act (allowed on a live disk). No durable-
    /// effect write admits past this point (the fence-generation gate), and a live pool moves to
    /// `TransientNotLive`, so store-class access already fails loud during the teardown window below.
    mount_runtime.tripMountLost();

    /// (3+4) Stop the GC scheduler (clears its leadership and JOINS its worker + heartbeat threads) BEFORE
    /// the Pool-side teardown, so no round writes `gc/state` under a disk we are decommissioning. Injected
    /// because the scheduler is owned above the Pool (a no-op in unit / read-only / clickhouse-disks
    /// contexts that run none). Runs OUTSIDE `remount_mutex` (spec §3 join discipline).
    if (stop_and_join_gc)
        stop_and_join_gc();

    /// (5a) Stop + join the self-remount thread. `stopRemountThread` latches the shutdown gate under the
    /// thread mutex before joining, and the thread is already bailing on the intent latch (step 1) — so the
    /// join is bounded and a keeper callback racing teardown can never re-arm it. Outside `remount_mutex`.
    mount_runtime.stopRemountThread();

    /// A remount attempt already IN FLIGHT when step 1 published the intent completes its current step
    /// before the loop bails (the "one step + one backend timeout" bound of §5), and a successful reclaim in
    /// that window re-arms the local fence (`lost = false`). Now that the remount thread is JOINED and can
    /// never run again, re-latch the fence so the terminal `mayMutate() == false` holds regardless of any
    /// such raced reclaim. Idempotent; the durable mount lease the reclaim wrote is retired by the
    /// `finishTeardown` below (it operates on whatever keeper is current — the reclaimed one).
    mount_runtime.tripMountLost();

    /// (5b) Drain the ref lanes (bounded by one attempt's budget + safety margin) to learn whether a clean
    /// farewell is EARNED — exactly the `~Pool` rule.
    const bool ref_lanes_drained = ref_ledger.drainRefLanesForShutdown(
        config.cas_request_budget.attempt_timeout_ms + config.cas_request_budget.lease_safety_margin_ms);
    const bool drained = ref_lanes_drained && !writerCleanupDutiesPending();

    /// (3+5c) Retire the merged heartbeat: a clean-release farewell ONLY if the lanes provably drained,
    /// otherwise stop background renewal with NO terminal marker so the lease expires by observation (never
    /// an unearned clean farewell). Also does the belt-and-suspenders remount rejoin. Outside `remount_mutex`.
    mount_runtime.finishTeardown(drained);

    /// The pool object OUTLIVES this FORGET (it stays registered, `Vanished(forgotten)`, until DROP/restart),
    /// so `~Pool` will re-run the same teardown. Drop the keeper now so that later teardown finds none and
    /// skips it: `MountLeaseKeeper::stop`'s terminal op is single-shot (`doTerminate` throws a `LOGICAL_ERROR`
    /// on a second call — an ASan-abort at construction), so a keeper that already terminated here must not be
    /// terminated again. `keeperReset` is safe now: every keeper-touching thread (renewal, remount) is joined.
    mount_runtime.keeperReset();

    /// (6) Publish the terminal state + WARN, under remount serialization — matching the natural-transition
    /// contract. Every pool thread is already joined, so taking `remount_mutex` here cannot self-deadlock.
    /// `reason` is the [D5] message (with the operator's decommission timestamp) that
    /// `throwIfLifecycleTerminal` surfaces to store-class callers.
    {
        std::lock_guard g(remount_mutex);
        mount_runtime.enterVanished(PoolLifecycle::VanishedForgotten, reason);
    }
}

/// The plain-object surface (namespace files + mountpoint objects) is implemented by the stateless
/// `plain_objects` component; these are thin delegates preserving the API.
void Pool::putNamespaceFile(const NamespaceLifeId & life, const String & name, const String & bytes)
{
    plain_objects.putNamespaceFile(life, name, bytes);
}

std::optional<String> Pool::getNamespaceFile(const NamespaceLifeId & life, const String & name)
{
    return plain_objects.getNamespaceFile(life, name);
}

std::vector<String> Pool::listNamespaceFiles(const NamespaceLifeId & life)
{
    return plain_objects.listNamespaceFiles(life);
}

uint64_t Pool::minActive()
{
    return mount_runtime.minActive();
}

uint64_t Pool::peekNextBuildSeq()
{
    return mount_runtime.peekNextBuildSeq();
}

bool Pool::tryRemountOnce()
{
    std::lock_guard serialize(remount_mutex);

    const String & srid = config.server_root_id;
    const UInt128 our_uuid = config.server_id;
    const auto now_ms = []() -> uint64_t
    {
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    };
    const uint64_t ttl_ms = static_cast<uint64_t>(config.mount_lease_ttl_ms.count());
    const uint64_t poll_interval_ms = std::max<uint64_t>(
        1, static_cast<uint64_t>(config.mount_renew_period.count()) / 2);

    /// Best-effort round for the MountRemount audit event only (diagnostic, never correctness-relevant):
    /// `currentGcRound` is a live `gc/state` GET, which may itself fail on the very backend trouble that
    /// is causing this remount attempt to fail — never let that escalate into an uncaught throw out of
    /// a function whose contract is "returns bool, never throws".
    const auto round_for_event = [this]() -> uint64_t
    {
        try { return currentGcRound(); } catch (...) { return 0; }
    };

    /// ==== Step 0 (rev.7 §2): pool lifecycle identity gate — BEFORE any claim/allocate/mount write ====
    /// A remount attempt means the lease is presumed lost, so first ensure we are at least transient
    /// (production reaches here already transient via `tripMountLost`; a direct/forced call may still be
    /// `Live`). Then authoritatively probe the pool sentinels and dispatch per the §2 verdict table. Only
    /// `Recover` (a present `_pool_meta` whose identity matches, in a non-`IdentityLost` state) falls
    /// through to the existing recovery below; every other verdict resolves here and returns false.
    mount_runtime.noteLeaseLost();
    /// A fully-terminal `Vanished` pool never probes/claims/writes again.
    if (mount_runtime.isVanished())
        return false;
    {
        const LifecycleGate gate = probePoolLifecycleGate(
            *pool_backend, pool_layout, config.server_root_id, meta.pool_id, meta.blob_header_len);
        switch (gate.verdict)
        {
            case LifecycleGateVerdict::Recover:
                /// [D3] no auto-revival: a matching-sentinel observation while `IdentityLost` does NOT
                /// bring the disk back — the state is terminal; only a restart recovers.
                if (mount_runtime.lifecycle() == PoolLifecycle::IdentityLost)
                    return false;
                break;   /// fall through to the existing fresh-incarnation recovery.
            case LifecycleGateVerdict::Replaced:
                /// NOT while a FORGET is in progress (spec §9 rev.8 item 7). `forgetDisk` publishes the
                /// terminal-intent latch at step 1 (`publishVanishedIntent`), then joins the remount thread;
                /// a `tryRemountOnce` already IN FLIGHT — one that passed the step-0 `isVanished()` gate
                /// BEFORE the intent was published, which that gate therefore cannot catch — could otherwise
                /// settle `Vanished(replaced)` mid-FORGET, stranding FORGET's own
                /// `enterVanished(VanishedForgotten)` (first terminal STATE transition wins) and mislabeling
                /// the operator-visible reason. The bail lives HERE, at the terminal settle, so the
                /// Recover/`armMountFence` reclaim path is untouched — its mid-FORGET fence re-arm is the
                /// SEPARATE hazard `forgetDisk`'s post-join re-trip (trip#2) guards. Post-excision this is the
                /// ONLY surviving mid-FORGET natural-terminal race (the old erasure-proof promotion is gone).
                if (mount_runtime.vanishedIntentPublished())
                    return false;
                mount_runtime.enterVanished(PoolLifecycle::VanishedReplaced, gate.reason);
                return false;
            case LifecycleGateVerdict::IdentityLost:
                /// Both sentinels authoritatively absent. Enter `IdentityLost` once (from `TransientNotLive`);
                /// a repeat probe while already `IdentityLost` is a no-op. rev.8: `IdentityLost` is a
                /// fail-loud TERMINAL state — the remount thread self-exits at its next boundary (see
                /// `CasMountRuntime::remountTerminal`), so there is no demoted observer.
                if (mount_runtime.lifecycle() != PoolLifecycle::IdentityLost)
                    mount_runtime.enterIdentityLost();
                return false;
            case LifecycleGateVerdict::StayTransient:
                /// Uncertain — remain transient and let the recovery loop retry. The probe's own reason is
                /// the only account of WHY, and it is the difference between a transient store hiccup and
                /// a pool this build can never open again: an undecodable `_pool_meta` is what a pool
                /// predating the contiguous-ref-stream format floor looks like from here (`decodePoolMeta`
                /// throws, the gate catches it), and silently retrying forever would leave that pool's
                /// operator with a fenced mount and nothing to read. Say it, and count it.
                ProfileEvents::increment(ProfileEvents::CASRemountHeldTransient);
                LOG_WARNING(getLogger("CasPool"),
                    "content-addressed pool '{}' (prefix '{}'): remount held TRANSIENT — {}. The mount "
                    "stays fenced closed and the remount loop will retry. If this repeats, the pool "
                    "metadata is unreadable to this build (a pool below its format floor must be "
                    "recreated) rather than merely unavailable.",
                    config.server_root_id, config.pool_prefix, gate.reason);
                return false;
        }
    }

    /// The same startup protocol as Pool::open steps 2-4, as a FRESH incarnation (the old one is
    /// dead by the fence-out contract and its keeper never re-mints). Open THROWS on any failure
    /// (startup is fail-closed); the remount RETURNS false instead — the recovery loop retries.
    try
    {
        const ObserveRefCatalog observe_catalog = [this]()
        {
            CasRefCatalog::Snapshot snapshot = CasRefCatalog::read(*pool_backend, pool_layout);
            snapshot.life_index.throwIfAmbiguous("CAS server-root remount safety");
            return snapshot.catalog;
        };
        /// The present-owner/present-epoch fast path would otherwise skip the observer entirely.
        /// Require the catalog before allocating a new epoch or replacing the mount slot.
        (void)observe_catalog();
        claimOwnerOrThrow(*pool_backend, pool_layout, srid, our_uuid, observe_catalog);
        const uint64_t writer_epoch = allocateWriterEpoch(
            *pool_backend, pool_layout, srid, EpochMintPolicy::NormalMount, 0, observe_catalog);

        /// Mount-slot writer audit: `this` is already fully open (setEventSink ran long ago), so
        /// unlike the initial `open`, every event fired below reaches the real sink immediately.
        const auto emit_mount_event = [this](CasEvent e) { emitEvent(std::move(e)); };

        const auto sleep_ms = [](uint64_t ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); };
        const MountClaimResult claim = claimMountAwaitingExpiry(
            *pool_backend, pool_layout, srid, our_uuid, writer_epoch,
            now_ms, [this] { return bootMsNow(); }, ttl_ms, poll_interval_ms, sleep_ms,
            [&srid](const MountLease & held, uint64_t threshold_ms)
            {
                LOG_INFO(getLogger("CasPool"),
                    "CAS self-remount '{}': observing a stale-looking mount's write-token (uuid={} "
                    "epoch={} expires_at_ms={}) for up to ~{} ms before reclaiming",
                    srid, u128ToHex(held.server_uuid), held.writer_epoch, held.expires_at_ms, threshold_ms);
            },
            emit_mount_event);
        if (claim.kind != MountClaimResult::Claimed)
        {
            LOG_WARNING(getLogger("CasPool"),
                "CAS self-remount '{}': mount not claimable ({}); will retry", srid,
                claim.kind == MountClaimResult::ForeignOwner ? "foreign owner — never taking over"
                                                             : "a live twin holds the lease");
            return false;
        }

        /// NO WAIT HERE, AND THE ASSERT IS WHAT REPLACES IT. This used to pay `materialization_grace_ms`
        /// whenever this incarnation's own ref lanes had not provably settled before the fence tripped:
        /// an unresolved (still-wedged) ref-log conditional `PUT` from the dying epoch could otherwise
        /// land after recovery began trusting its listings. Recovery no longer trusts listings — it walks
        /// arithmetically and closes every epoch below the live one with an in-band `EpochSeal` at
        /// `{E, T+1}`, so the wedged `PUT` loses its own conditional create to that seal and the wait
        /// bought nothing but latency on every fence recovery.
        ///
        /// That replacement has ONE precondition, and it is this one: the incarnation we are about to
        /// install must outrank the dying one. If the epoch did not strictly advance, the straggler's
        /// slot is not "below the live epoch", nothing seals it, and the hole the wait used to paper over
        /// reopens silently. `allocateWriterEpoch` mints from a durable monotone counter, so this holds by
        /// construction — which is exactly why it is worth asserting rather than assuming: it fails the
        /// build's own tests the moment someone reuses an epoch across a remount.
        chassert(writer_epoch > mount_runtime.liveWriterEpoch());

        /// Swap the keeper for the new incarnation. The old keeper's renewal loop already stopped on
        /// its failed renew; never run its terminal op (the slot now belongs to the new claim).
        /// `installKeeper` re-wires the SAME fence/min-active/event callbacks on `mount_runtime` as the
        /// initial `open` did -- so the granular mechanics here are the exact same primitives, in the
        /// exact same order, as `mountWritable`'s.
        if (mount_runtime.hasKeeper())
            mount_runtime.keeperStopBackground();
        mount_runtime.installKeeper(our_uuid, writer_epoch, now_ms);
        /// Pre-I/O anchor of this remount's claim attempt (mirrors `mountWritable`'s
        /// `claim_anchor_boot_ms`, captured at the identical point -- right after `installKeeper`,
        /// right before the keeper's own adopt write). No wait can land between this anchor and the arm
        /// below, so no TTL-consumed redo is needed here: the anchor alone suffices (rev.4 Phase B,
        /// round-3 finding 2; the redo lives in `mountWritable`, which keeps it for a claim that stalls).
        /// `quiesceRefTablesForRemount` below IS a wait, but a bounded one -- bounded by the same
        /// `cas_request_budget` that `validateCasRequestBudget` already guarantees fits under `ttl_ms`.
        const uint64_t remount_anchor_boot_ms = mount_runtime.bootMsNow();
        mount_runtime.keeperStart();

        /// Re-establish the ref-protocol incarnation BEFORE re-arming the fence. Order is load-bearing:
        /// `keeperStart` refreshes the lease deadline
        /// but does NOT clear `lost`, so the fence stays closed here and no append/publish can race the
        /// swap.
        /// 1. Bump the live epoch so every subsequent `allocateRefTxnId` sorts strictly above any older
        ///    (dead-incarnation or twin) durable log. Do this BEFORE `armMountFence` so there is no window
        ///    where the gate is open while the epoch is still stale. Keep `process_epoch` (the identity
        ///    accessors) equal to it.
        mount_runtime.setLiveWriterEpoch(writer_epoch);
        mount_runtime.setProcessEpoch(writer_epoch, std::memory_order_release);
        /// 2. CANCEL OR JOIN every in-flight ref-table recovery, and BLOCK here until none is left (spec
        ///    §3: "self-remount cancels or waits out recovery before rearming"). A recovery admitted under
        ///    the outgoing incarnation WRITES -- its seal CAS-walk mints epoch seals and advances the
        ///    `_ckpt` -- so it must be stopped at this boundary rather than caught one site at a time
        ///    after the incarnation has already changed underneath it. Strictly before the quiesce below
        ///    so a cancelled walk unwinds while its runtime is still attached, and strictly before the
        ///    re-arm so no old-generation write can straddle it.
        ref_ledger.cancelRecoveriesAndAwaitQuiescence();
        /// 3. Drain publishers and drop the cached tables so each re-recovers under the new epoch on next
        ///    touch (and any leader still holding an orphaned runtime fails closed). While the fence is lost.
        ref_ledger.quiesceRefTablesForRemount();
        /// 4. Re-open the gate. Anchored at the claim attempt's pre-I/O instant (`remount_anchor_boot_ms`),
        ///    never at response time -- see the comment above `keeperStart()`. From here appends allocate
        ///    ids under the new epoch and touch fresh runtimes.
        mount_runtime.armMountFence(our_uuid, writer_epoch, remount_anchor_boot_ms + ttl_ms);
        if (config.background_watermark)
            mount_runtime.keeperStartBackground(config.mount_renew_period);

        LOG_INFO(getLogger("CasPool"),
            "CAS self-remount '{}': recovered as writer_epoch {} (fresh incarnation; older builds fail closed)",
            srid, writer_epoch);
        EventEmitter{*this}.emit([&](CasEvent & e)
        {
            e.type = CasEventType::MountRemount;
            e.round = round_for_event();
            e.outcome = "ok";
            e.reason = "self-remount recovered a fresh mount incarnation after fence-out / renewal failure";
            e.detail = {{"writer_epoch", std::to_string(writer_epoch)},
                        {"server_root_id", srid}};
        });
        /// Recovery succeeded: `TransientNotLive -> Live` (never revives a terminal state — but the gate
        /// above guarantees we only reach here from a non-terminal state anyway).
        mount_runtime.noteRemounted();
        return true;
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("CasPool"), "CAS self-remount attempt failed; will retry");
        EventEmitter{*this}.emit([&](CasEvent & e)
        {
            e.type = CasEventType::MountRemount;
            e.round = round_for_event();
            e.outcome = "failed";
            e.reason = "self-remount attempt failed; the recovery loop retries with backoff";
            e.detail = {{"server_root_id", srid},
                        {"error", getCurrentExceptionMessage(/*with_stacktrace*/ false)}};
        });
        return false;
    }
}

/// The self-remount recovery thread + the merged-heartbeat renew live in `mount_runtime`
/// (Pool/CasMountRuntime.h); these are thin delegates. `mount_runtime`'s `remount_attempt` callback is
/// bound to `Pool::tryRemountOnce` (the claim/recovery orchestration that stays on Pool).
bool Pool::scheduleRemountForTest()
{
    return mount_runtime.scheduleRemountForTest();
}

void Pool::beginShutdownForTest()
{
    mount_runtime.beginShutdownForTest();
}

void Pool::renewWatermarkOnce()
{
    mount_runtime.renewWatermarkOnce();
}

void Pool::retireBuildSeq(uint64_t seq)
{
    mount_runtime.retireBuildSeq(seq);
}

void Pool::enqueueWriterCleanupDuty(
    const RootNamespace & ns, const String & ref_name, const ManifestRef & manifest, uint64_t build_seq) noexcept
{
    try
    {
        auto duty = std::make_shared<WriterCleanupDuty>(WriterCleanupDuty{
            .ref_name = ref_name,
            .manifest = manifest,
            .build_seq = build_seq,
        });
        std::lock_guard lock(writer_cleanup_mutex);
        writer_cleanup_queues[ns].pending.push_back(std::move(duty));
        writer_cleanup_cv.notify_all();
    }
    catch (...)
    {
        /// The build deliberately remains active. Advancing `min_active` after losing the only cleanup
        /// duty would make an uncertain owner grant look dead; pinning the floor until process exit is
        /// the safe failure direction, and successor recovery handles the durable remnant.
        writer_cleanup_queue_failed.store(true, std::memory_order_release);
        try
        {
            tryLogCurrentException(
                getLogger("CasPool"),
                "CAS writer cleanup duty could not be queued; retaining the build in the active watermark");
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// `noexcept` destructor path: the sticky bit above is the safety mechanism.
        }
    }
}

bool Pool::writerCleanupDutiesPending() const
{
    if (writer_cleanup_queue_failed.load(std::memory_order_acquire))
        return true;
    std::lock_guard lock(writer_cleanup_mutex);
    return std::any_of(
        writer_cleanup_queues.begin(), writer_cleanup_queues.end(),
        [](const auto & item) { return !item.second.pending.empty(); });
}

void Pool::drainWriterCleanupDuties(const RootNamespace & ns)
{
    {
        std::unique_lock lock(writer_cleanup_mutex);
        writer_cleanup_cv.wait(lock, [&]
        {
            const auto it = writer_cleanup_queues.find(ns);
            return it == writer_cleanup_queues.end() || !it->second.draining;
        });

        const auto it = writer_cleanup_queues.find(ns);
        if (it == writer_cleanup_queues.end() || it->second.pending.empty())
            return;
        it->second.draining = true;
    }

    try
    {
        while (true)
        {
            std::shared_ptr<const WriterCleanupDuty> duty;
            {
                std::lock_guard lock(writer_cleanup_mutex);
                const auto it = writer_cleanup_queues.find(ns);
                chassert(it != writer_cleanup_queues.end() && it->second.draining);
                if (it->second.pending.empty())
                {
                    writer_cleanup_queues.erase(it);
                    writer_cleanup_cv.notify_all();
                    return;
                }
                duty = it->second.pending.front();
            }

            ref_ledger.appendRefOps(
                ns,
                MutationScope::ref(duty->ref_name),
                [ref_name = duty->ref_name, manifest = duty->manifest]
                (const RefTableState & state) -> std::vector<RefOp>
                {
                    /// Absence is a conclusive settlement, not an error: the original uncertain grant
                    /// was rejected, a promote atomically consumed it, or another exact owner-removal
                    /// path already discharged it. Presence owes one exact removal.
                    if (!state.getPrecommits().contains({ref_name, manifest}))
                        return {};
                    RefOp op;
                    op.kind = RefOpKind::OwnerTransition;
                    op.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, ref_name, manifest};
                    return {op};
                },
                RootMutationOrigin::Writer,
                RootMutationKind::Abandon);

            /// Removal/absence is now durable in the same state observation. Only now may the active
            /// build floor advance beyond the manifest's build sequence.
            retireBuildSeq(duty->build_seq);

            std::lock_guard lock(writer_cleanup_mutex);
            const auto it = writer_cleanup_queues.find(ns);
            chassert(it != writer_cleanup_queues.end() && it->second.draining);
            chassert(!it->second.pending.empty() && it->second.pending.front() == duty);
            it->second.pending.pop_front();
        }
    }
    catch (...)
    {
        std::lock_guard lock(writer_cleanup_mutex);
        const auto it = writer_cleanup_queues.find(ns);
        if (it != writer_cleanup_queues.end())
            it->second.draining = false;
        writer_cleanup_cv.notify_all();
        throw;
    }
}

PartWriteTxnPtr Pool::beginPartWrite(PartWriteInfo info)
{
    /// Mint a globally-unique build id from two thread_local_rng draws (random u128).
    const UInt64 hi = thread_local_rng();
    const UInt64 lo = thread_local_rng();
    const UInt128 build_id = (static_cast<UInt128>(hi) << 64) | lo;

    /// Strictly-increasing per-process build_seq carried by the `PartWriteTxn`. The `PartWriteTxn` is
    /// added to the active set here and retired on publish/abandon/dtor, so minActive — the GC floor
    /// the Pool-owned watermark renews — tracks in-flight builds. The build registry lives on
    /// `mount_runtime`.
    const uint64_t seq = mount_runtime.allocateBuildSeq();
    bool registered = false;
    SCOPE_EXIT({ if (!registered) retireBuildSeq(seq); });

    auto build = std::make_shared<PartWriteTxn>(shared_from_this(), build_id, seq, liveWriterEpoch(), std::move(info));
    /// Register for `dropNamespace`'s post-durable build cancellation. weak_ptr:
    /// the wiring owns the returned shared_ptr; `retireBuildSeq` (publish/abandon/dtor) removes the entry.
    mount_runtime.registerInflightBuild(seq, build);
    registered = true;
    return build;
}

/// The manifest read path (readManifest / readManifestShared / locate) + its decode cache live in
/// the `manifest_reader` component (Pool/CasManifestReader.h); these are thin delegates.
std::shared_ptr<const PartManifest> Pool::readManifestShared(const ManifestId & id)
{
    return manifest_reader.readManifestShared(id);
}

PartManifest Pool::readManifest(const ManifestId & id)
{
    return manifest_reader.readManifest(id);
}

BlobLocation Pool::locate(const ManifestEntry & entry) const
{
    return manifest_reader.locate(entry);
}

namespace
{
/// a tolerant, read-only peek at the
/// `cas_ref_log` TEXT object (codecs-v3 phase 3) WITHOUT `decodeRefLogTxn`'s expected-value cross-check
/// -- the whole point of this diagnostic is that the body is NOT expected to match this key's identity.
/// It `openObject`s the stored `.zst`, skips the header line, and reads `ns`/`we`/`rs` off the meta
/// line (`we`/`rs` are decimal u64 strings). Never validates the header `v`, never reads past the meta
/// line (the ops are irrelevant to identifying the writer), and swallows any truncation/garbage: this
/// is a background diagnostic only, never a decode anything else depends on.
struct ForeignRefLogHeaderPeek
{
    String ns;
    uint64_t writer_epoch = 0;
    uint64_t ref_sequence = 0;
};

std::optional<ForeignRefLogHeaderPeek> peekForeignRefLogHeader(const String & bytes)
{
    try
    {
        const String text = openObject(FormatId::RefLog, bytes);
        ReadBufferFromMemory in(text.data(), text.size());
        const uint64_t line_cap = traitsFor(FormatId::RefLog).line_cap;
        readLine(in, line_cap, "cas_ref_log");   /// header line -- skip
        const String meta = readLine(in, line_cap, "cas_ref_log");
        ReadBufferFromMemory m(meta.data(), meta.size());
        JsonObjectReader r(m, KeyStrictness::Tolerant, "cas_ref_log");
        ForeignRefLogHeaderPeek peek;
        bool saw_ns = false;
        bool saw_we = false;
        bool saw_rs = false;
        String key;
        while (r.nextKey(key))
        {
            if (key == "ns") { peek.ns = r.readString(); saw_ns = true; }
            else if (key == "we") { peek.writer_epoch = r.readU64String(); saw_we = true; }
            else if (key == "rs") { peek.ref_sequence = r.readU64String(); saw_rs = true; }
            else r.skipUnknown(key);
        }
        if (!saw_ns || !saw_we || !saw_rs)
            return std::nullopt;
        return peek;
    }
    catch (...)
    {
        return std::nullopt;
    }
}
}

void Pool::reportImpossibleInterference(const String & key, const String & reason,
                                          const std::optional<String> & offending_ns)
{
    LOG_ERROR(getLogger("CasPool"),
        "CAS anomaly policy: impossible foreign interference for server_root '{}' (namespace='{}', key='{}'): "
        "{} -- fencing this mount closed and scheduling a remount",
        config.server_root_id, offending_ns.value_or(String{}), key, reason);

    EventEmitter{*this}.emit([&](CasEvent & e)
    {
        e.type = CasEventType::ForeignInterference;
        if (offending_ns)
            e.namespace_ = *offending_ns;
        e.reason = reason;
        e.detail = {{"key", key}, {"server_root_id", config.server_root_id}};
    });

    /// Incidental-only detection has a fail-closed reaction -- the SAME on_lost
    /// mechanics a foreign/superseded lease renewal already drives (the keeper's `setFenceCallbacks`
    /// lambda). The fence + self-remount now live on `mount_runtime`.
    mount_runtime.tripMountLost();
    mount_runtime.scheduleRemount();

    /// Diagnosis off the critical path: a background task may spend a FEW
    /// requests -- never the caller's thread, and never blocking this call's own return.
    /// `shared_from_this()` keeps the Pool alive for the thread's lifetime (mirrors
    /// `maybeScheduleSnapshotPublish`'s dispatch).
    auto self = shared_from_this();
    try
    {
        ThreadFromGlobalPool([self, key]
        {
            setThreadName(ThreadName::CAS_ANOMALY_DIAG);
            try
            {
                const auto got = self->pool_backend->get(key);
                if (!got)
                {
                    LOG_ERROR(getLogger("CasPool"),
                        "CAS anomaly diagnostics: the offending object at '{}' had already vanished by the "
                        "time the background diagnostic GET ran", key);
                    return;
                }
                if (const auto peek = peekForeignRefLogHeader(got->bytes))
                    LOG_ERROR(getLogger("CasPool"),
                        "CAS anomaly diagnostics: offending object at '{}' ({} bytes) decodes as a ref-log "
                        "header: namespace='{}', writer_epoch={}, ref_sequence={}",
                        key, got->bytes.size(), peek->ns, peek->writer_epoch, peek->ref_sequence);
                else
                    LOG_ERROR(getLogger("CasPool"),
                        "CAS anomaly diagnostics: offending object at '{}' ({} bytes) does not decode as a "
                        "ref-log header -- raw and unidentified", key, got->bytes.size());
            }
            catch (...)
            {
                tryLogCurrentException(getLogger("CasPool"),
                    "CAS anomaly diagnostics: background GET failed for '" + key + "'");
            }
        }).detach();
    }
    catch (...)
    {
        /// Pool exhaustion: best-effort diagnostics must never block the caller's own fail-closed throw.
        tryLogCurrentException(getLogger("CasPool"), "CAS anomaly diagnostics dispatch failed to launch for '" + key + "'");
    }
}

uint64_t Pool::currentGcRound() const
{
    /// Read `gc/state` once (no CAS loop — a point-in-time read is sufficient; a concurrent
    /// GC advance only makes the returned round larger, which is strictly more conservative for the
    /// `precommitAdd` self-floor). Returns 0 when absent (pool never GC'd — no round to floor to).
    const auto state_bytes = pool_backend->get(pool_layout.gcStateKey());
    if (!state_bytes)
        return 0;
    return decodeGcState(state_bytes->bytes).round;
}

void Pool::removeNamespaceFile(const NamespaceLifeId & life, const String & name)
{
    plain_objects.removeNamespaceFile(life, name);
}

void Pool::putMountpointObject(const String & key, const String & bytes)
{
    plain_objects.putMountpointObject(key, bytes);
}

std::optional<String> Pool::getMountpointObject(const String & key)
{
    return plain_objects.getMountpointObject(key);
}

bool Pool::mountpointObjectExists(const String & key)
{
    return plain_objects.mountpointObjectExists(key);
}

void Pool::removeMountpointObject(const String & key)
{
    plain_objects.removeMountpointObject(key);
}

NamespaceListing Pool::listNamespaces(const String & prefix)
{
    /// The catalog is the logical namespace authority. Physical stream/state keys expose only an
    /// opaque life id and therefore cannot mint a namespace during discovery.
    std::unordered_set<String> found;
    std::vector<UnattributableNamespaceKey> skipped;
    const CasRefCatalog::Snapshot cut = CasRefCatalog::read(*pool_backend, pool_layout);
    for (const CatalogEntry & entry : cut.catalog.entries)
    {
        try
        {
            if (const auto life = cut.life_index.resolve(entry.incarnation);
                life && life->ns.string().starts_with(prefix))
                found.insert(life->ns.string());
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::CORRUPTED_DATA)
                throw;
            skipped.push_back(UnattributableNamespaceKey{
                pool_layout.refCatalogKey() + "#" + renderIncarnation(entry.incarnation), e.message()});
        }
    }

    return NamespaceListing{{found.begin(), found.end()}, std::move(skipped)};
}

std::vector<String> Pool::listMirroredChildren(const String & prefix)
{
    /// Namespace children come from the catalog. `roots/` is still listed for loose mountpoint files,
    /// whose logical paths retain path identity.
    std::unordered_set<String> children;
    const CasRefCatalog::Snapshot cut = CasRefCatalog::read(*pool_backend, pool_layout);
    for (const CatalogEntry & entry : cut.catalog.entries)
    {
        if (!entry.ns.string().starts_with(prefix))
            continue;
        const std::string_view rest(entry.ns.string().data() + prefix.size(), entry.ns.string().size() - prefix.size());
        const size_t slash = rest.find('/');
        const std::string_view segment = slash == std::string_view::npos ? rest : rest.substr(0, slash);
        if (!segment.empty())
            children.emplace(segment);
    }

    const String roots_full = pool_layout.rootsPrefix() + prefix;
    {
        String cursor;
        while (true)
        {
            ListPage page = pool_backend->list(roots_full, cursor, /*limit*/ 1000);
            for (const ListedKey & listed : page.keys)
            {
                const String & key = listed.key;
                if (!key.starts_with(roots_full))
                    continue;
                const std::string_view rest(key.data() + roots_full.size(), key.size() - roots_full.size());
                const size_t slash = rest.find('/');
                const std::string_view seg = slash == std::string_view::npos ? rest : rest.substr(0, slash);
                if (!seg.empty())
                    children.emplace(seg);
            }
            if (page.next_cursor.empty())
                break;
            cursor = page.next_cursor;
        }
    }
    return {children.begin(), children.end()};
}


/// ==== ref-ledger delegates ==== The ref-log / ref-table subsystem lives in the
/// `ref_ledger` member (Pool/CasRefLedger.h); Pool keeps these thin public forwarders so the wiring,
/// PartWriteTxn, Gc, and every test call site is unchanged.

void Pool::setCasRetrySleepForTest(std::function<void(uint64_t)> sleep_fn)
{
    ref_ledger.setCasRetrySleepForTest(std::move(sleep_fn));
}

std::optional<Resolved> Pool::resolveRef(const RootNamespace & ns, const String & ref_name, bool allow_stale, ResolveAudit audit)
{
    return ref_ledger.resolveRef(ns, ref_name, allow_stale, audit);
}

std::map<String, Resolved> Pool::listRefs(const RootNamespace & ns)
{
    return ref_ledger.listRefs(ns);
}

bool Pool::hasAnyRefWithPrefix(const RootNamespace & ns, std::string_view prefix)
{
    return ref_ledger.hasAnyRefWithPrefix(ns, prefix);
}

void Pool::dropRef(const RootNamespace & ns, const String & ref_name)
{
    mutateRefsAfterWriterCleanup(ns, [&]
    {
        ref_ledger.dropRef(ns, ref_name);
    });
}

void Pool::updateRefPublishedAt(const RootNamespace & ns, const String & ref_name,
                             std::function<void(RefPublishedAtUpdate &)> mutator)
{
    mutateRefsAfterWriterCleanup(ns, [&]
    {
        ref_ledger.updateRefPublishedAt(ns, ref_name, std::move(mutator));
    });
}

DropNamespaceStats Pool::dropNamespace(const RootNamespace & ns)
{
    return mutateRefsAfterWriterCleanup(ns, [&]
    {
        return ref_ledger.dropNamespace(ns);
    });
}

DropNamespaceStats Pool::dropNamespace(const NamespaceLifeId & life)
{
    return mutateRefsAfterWriterCleanup(life.ns, [&]
    {
        return ref_ledger.dropNamespace(life);
    });
}

NamespaceLifeId Pool::namespaceLife(const RootNamespace & ns)
{
    return ref_ledger.namespaceLife(ns);
}

std::optional<NamespaceLifeId> Pool::namespaceFilesLifeIfReadable(const RootNamespace & ns)
{
    return ref_ledger.namespaceFilesLifeIfReadable(ns);
}

bool Pool::namespaceStillLogicallyPresent(const RootNamespace & ns)
{
    return ref_ledger.namespaceStillLogicallyPresent(ns);
}

void Pool::invalidateRemovedCatalogLife(const NamespaceLifeId & life)
{
    ref_ledger.invalidateRemovedCatalogLife(life);
}

void Pool::reconcileRefCatalogCut(const CasRefCatalog::Snapshot & catalog_cut)
{
    ref_ledger.reconcileCatalogCut(catalog_cut);
}

RefTxnId Pool::appendRefOps(const RootNamespace & ns, MutationScope scope,
                             std::function<std::vector<RefOp>(const RefTableState &)> build_ops,
                             RootMutationOrigin origin, RootMutationKind kind,
                             bool skip_stale_precommit_sweep)
{
    return mutateRefsAfterWriterCleanup(ns, [&]
    {
        return ref_ledger.appendRefOps(
            ns, std::move(scope), std::move(build_ops), origin, kind, skip_stale_precommit_sweep);
    });
}

bool Pool::tryPublishSnapshotAndAdvanceCheckpointOnce(const RootNamespace & ns)
{
    return mutateRefsAfterWriterCleanup(ns, [&]
    {
        return ref_ledger.tryPublishSnapshotAndAdvanceCheckpointOnce(ns);
    });
}

size_t Pool::wedgedRefLaneCount()
{
    return ref_ledger.wedgedRefLaneCount();
}

CasWriteOutcome Pool::stagingPutIfAbsent(std::string_view key, std::string_view bytes, Token * out_token)
{
    return ref_ledger.stagingPutIfAbsent(key, bytes, out_token);
}

CasCreateResult Pool::stagingConditionalCreate(std::string_view key, const std::function<PutResult()> & attempt)
{
    return ref_ledger.stagingConditionalCreate(key, attempt);
}

CasOverwriteResult Pool::stagingConditionalOverwrite(std::string_view key, std::string_view bytes, const Token & expected)
{
    return ref_ledger.stagingConditionalOverwrite(key, bytes, expected);
}

CasOverwriteResult Pool::stagingPutIfAbsentMutable(std::string_view key, std::string_view bytes)
{
    return ref_ledger.stagingPutIfAbsentMutable(key, bytes);
}

void Pool::cancelInflightBuildsForNamespace(const RootNamespace & ns)
{
    /// Delegate to `mount_runtime`. Invoked by
    /// `ref_ledger` through the `cancel_inflight_builds` callback once its removal transaction is durable,
    /// so that in-flight local builds targeting the removed namespace are cancelled.
    mount_runtime.cancelInflightBuildsForNamespace(ns);
}

uint64_t Pool::refRecoveryRestartsForTest(const RootNamespace & ns)
{
    return ref_ledger.refRecoveryRestartsForTest(ns);
}

bool Pool::refLaneWedgedForTest(const RootNamespace & ns)
{
    return ref_ledger.refLaneWedgedForTest(ns);
}

String Pool::wedgedKeyForTest(const RootNamespace & ns)
{
    return ref_ledger.wedgedKeyForTest(ns);
}

void Pool::forceWedgeForTest(const RootNamespace & ns, uint64_t writer_epoch, uint64_t ref_sequence,
                              const String & key, const String & bytes,
                              std::optional<uint64_t> admitted_generation)
{
    ref_ledger.forceWedgeForTest(ns, writer_epoch, ref_sequence, key, bytes, admitted_generation);
}

uint64_t Pool::wedgedAdmittedGenerationForTest(const RootNamespace & ns)
{
    return ref_ledger.wedgedAdmittedGenerationForTest(ns);
}

std::optional<RefTxnId> Pool::lastEpochSealForTest(const RootNamespace & ns)
{
    return ref_ledger.lastEpochSealForTest(ns);
}

void Pool::setLastEpochSealForTest(const RootNamespace & ns, const std::optional<RefTxnId> & seal)
{
    ref_ledger.setLastEpochSealForTest(ns, seal);
}

RefLaneState Pool::laneStateForTest(const RootNamespace & ns)
{
    return ref_ledger.laneStateForTest(ns);
}

bool Pool::needsStalePrecommitSweepForTest(const RootNamespace & ns)
{
    return ref_ledger.needsStalePrecommitSweepForTest(ns);
}

void Pool::waitForSnapshotPublishSettleForTest(const RootNamespace & ns)
{
    ref_ledger.waitForSnapshotPublishSettleForTest(ns);
}

int Pool::pendingSnapshotPublishesForTest(const RootNamespace & ns)
{
    return ref_ledger.pendingSnapshotPublishesForTest(ns);
}

std::optional<RefTxnId> Pool::newestPublishedSnapshotIdForTest(const RootNamespace & ns)
{
    return ref_ledger.newestPublishedSnapshotIdForTest(ns);
}

bool Pool::refTableRecoveredForTest(const RootNamespace & ns)
{
    return ref_ledger.refTableRecoveredForTest(ns);
}

bool Pool::refRecoveryCancelRequestedForTest(const RootNamespace & ns)
{
    return ref_ledger.refRecoveryCancelRequestedForTest(ns);
}

void Pool::cancelRefRecoveriesAndAwaitQuiescence()
{
    ref_ledger.cancelRecoveriesAndAwaitQuiescence();
}

size_t Pool::tailSinceSnapshotCountForTest(const RootNamespace & ns)
{
    return ref_ledger.tailSinceSnapshotCountForTest(ns);
}

size_t Pool::committedOverlayEntriesForTest(const RootNamespace & ns)
{
    return ref_ledger.committedOverlayEntriesForTest(ns);
}

std::set<std::pair<String, ManifestRef>> Pool::livePrecommitsForTest(const RootNamespace & ns)
{
    return ref_ledger.livePrecommitsForTest(ns);
}

}
