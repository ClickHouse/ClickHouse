#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefSnapshotFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <base/scope_guard.h>

#include <Poco/Exception.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>
#include <utility>

/// Task 10: the writer's ref persistence on the snapshot+log protocol. Covers the plan's Task 10
/// failing-test list: empty+birth recovery; snapshot+tail recovery; recovery restart on a vanished
/// object (converging on a newer snapshot); the append lane's wedge semantics (blocks the same table,
/// leaves other tables free, applies a later-observed-durable append before unwedging); invalid batch
/// entries failing in isolation; and the S3 request-cost contract (one create for a warm isolated
/// mutation, one create shared by a compatible batch).

namespace DB::ErrorCodes
{
extern const int ABORTED;
extern const int FILE_DOESNT_EXIST;
extern const int CORRUPTED_DATA;
extern const int INVALID_STATE;
extern const int LOGICAL_ERROR;
extern const int NETWORK_ERROR;
extern const int S3_ERROR;
}

namespace ProfileEvents
{
extern const Event CASRefSweepDeferred;
extern const Event CASRefSweepRearmed;
extern const Event CASRefStalePrecommitsReclaimed;
extern const Event CASRefSnapshotPutBytes;
extern const Event CASRefSnapshotTailLogs;
extern const Event CASRefSnapshotPublishDispatched;
extern const Event CASRefSnapshotPublishBackoff;
extern const Event CASConditionalWriteFenceLostPostWrite;
extern const Event CASRefRecoveryEpochSealed;
extern const Event CASRefRecoveryRetries;
}

using namespace DB::Cas;
using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::committedRow;
using DB::Cas::tests::expectThrowsCode;
using DB::Cas::tests::minimalLiveSnapshot;
using DB::Cas::tests::namespaceBirthOp;
using DB::Cas::tests::publishCommittedOps;
using DB::Cas::tests::runRegularRoundReclaiming;
using DB::Cas::tests::writeRefSnapshotRaw;
using DB::Cas::tests::writeSealAt;

namespace
{

/// The operation deadline every SINGLE-ATTEMPT fixture in this file uses, and the reason it is
/// deliberately NOT `attempt_timeout_ms`.
///
/// Those fixtures exist to make one injected ambiguous response conclusive, and `max_attempts = 1`
/// alone achieves that: with retries allowed the controller would resolve-before-reissue and report a
/// definite outcome instead. The deadline contributes nothing to that -- but setting it EQUAL to the
/// attempt timeout collapses the controller's pre-send gate into a race. The deadline is captured as
/// `now + operation_deadline_ms` and the gate asks `now + attempt_timeout_ms > deadline`
/// (`CasRequestControl.cpp`), so equal values reduce it to `now_2 > now_1`: ONE elapsed millisecond
/// between the two clock reads refuses the operation with NOTHING SENT, the injected fault is never
/// reached, and the product correctly does not wedge -- flipping every wedge expectation downstream.
///
/// That is not hypothetical. It took down
/// `CASRefWriterStalePrecommitSweep.BoundedBatchesAndInterruptionResumeAcrossMounts` on 5 of 6 sanitizer
/// CI runs (fixed in `8f9e63c7a19`), `CASRefInstallSafety.UncertainPrecommitKeepsItsCleanupOwnerAndItsBody`
/// under parallel-build load, and `CASRefWriterAppendLane.WedgedLaneBlocksSameTableWhileOtherTableProceeds`
/// in a full-binary ASan run -- the last one with the mechanism named verbatim in the thrown message
/// ("refused BEFORE any request was sent ... the operation deadline rejected before the first request").
///
/// A wide deadline keeps the request always actually sent, so what the test observes is the fault it
/// injected rather than the machine it ran on. A fixture that genuinely wants the pre-send REFUSAL
/// must drive it deterministically with a frozen clock (see `gtest_cas_ref_install_safety.cpp`'s
/// `openPoolFenceControlled`), never by racing the wall clock.
constexpr uint64_t kSingleAttemptDeadlineMs = 5000;

/// A `CasEvent` sink safe to hand to `Pool::setEventSink`: the emit runs on whatever thread the pool's
/// background syncer happens to be on, and the test reads the accumulated events afterward from the
/// main test thread with no other ordering between the two -- a bare `std::vector` there is a real data
/// race (the class this file's four `setEventSink` call sites all had, hidden because a debug/ASan build
/// doesn't reliably catch an unsynchronized push_back/iterator-read pair on a small vector). `add` takes
/// the lock only around the push; `snapshot` copies out under the lock and returns, so a caller iterating
/// the result never holds the mutex across anything that could call back into the pool (which an
/// event-sink callback legitimately can, on other seams in this file).
class SynchronizedEventLog
{
public:
    void add(const CasEvent & e)
    {
        std::lock_guard lock(mutex);
        events.push_back(e);
    }
    std::vector<CasEvent> snapshot() const
    {
        std::lock_guard lock(mutex);
        return events;
    }
private:
    mutable std::mutex mutex;
    std::vector<CasEvent> events;
};

PoolPtr openPool(const BackendPtr & backend, CasRequestBudget budget = {})
{
    /// Recovery tests seed ref-log/snapshot residue before opening; a pool with such residue always has a
    /// `_pool_meta` in production, so establish it first (Task 7's zero-write bootstrap check refuses to
    /// mint a fresh identity over residual data — see `seedPoolMetaForRestart`). Idempotent, and a no-op
    /// for the fresh-open tests that seed nothing (the subsequent open validates the just-created meta).
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    return Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test", .cas_request_budget = budget});
}

/// Task 11: like `openPool`, but the caller supplies (and owns) the rest of the config -- snapshot
/// thresholds, grace age, a fake `boot_ms_fn`, etc. `pool_prefix`/`server_root_id` are pinned so every
/// test in this file addresses the same pool shape.
PoolPtr openPoolWithConfig(const BackendPtr & backend, PoolConfig config)
{
    config.pool_prefix = "p";
    config.server_root_id = "test";
    DB::Cas::tests::seedPoolMetaForRestart(*backend);   /// see `openPool` above
    return Pool::open(backend, std::move(config));
}

/// Mirrors gtest_cas_part_write.cpp's startBuildFor/publishOneBlobPart, minus the blob (an empty-entry
/// manifest is a legal, blob-free part -- the ref-writer tests only care about ref/manifest identity).
///
/// Stage B (Task 4-C): pin `ns` to the sentinel before the first real touch -- ONE choke point for
/// every test in this file, since every real-path setup here funnels through `startBuildFor` (directly,
/// or via `publishEmptyPart` below). Many of this file's tests separately compute an expected key via
/// `DB::Cas::tests::fixture::fixtureLife(ns)` for fault injection/verification; without this the real
/// production birth mints a random incarnation and those computed keys land nowhere real.
PartWriteTxnPtr startBuildFor(const PoolPtr & s, const RootNamespace & ns, const String & ref)
{
    DB::Cas::tests::casAdmitRecoverableEntry(s->backend(), s->layout(), ns, s->liveWriterEpoch());
    PartWriteInfo info;
    info.intended_namespace = ns;
    info.intended_ref = ns.string() + "/" + ref;
    return s->beginPartWrite(info);
}

ManifestId publishEmptyPart(const PoolPtr & s, const RootNamespace & ns, const String & ref)
{
    auto build = startBuildFor(s, ns, ref);
    const ManifestId id = build->stageManifest({});
    build->precommitAdd(ns, ref, id);
    build->promote(ns, ref, build->buildId(), id);
    return id;
}

void publishWithProductionBirth(const PoolPtr & store, const RootNamespace & ns, const String & ref)
{
    PartWriteInfo info;
    info.intended_namespace = ns;
    info.intended_ref = ns.string() + "/" + ref;
    auto build = store->beginPartWrite(info);
    const ManifestId id = build->stageManifest({});
    build->precommitAdd(ns, ref, id);
    build->promote(ns, ref, build->buildId(), id);
}

CatalogEntry catalogEntryOrThrow(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    const RefCatalog catalog = CasRefCatalog::read(backend, layout).catalog;
    const auto it = std::find_if(catalog.entries.begin(), catalog.entries.end(), [&](const CatalogEntry & entry)
    {
        return entry.ns == ns;
    });
    if (it == catalog.entries.end())
        throw std::runtime_error("catalog entry missing from test fixture");
    return *it;
}

CatalogEntry replaceCatalogLifeForRuntimeRace(
    Backend & backend, const Layout & layout, const CatalogEntry & predecessor, UInt128 successor_incarnation)
{
    const CasRefCatalog::Snapshot before_delete = CasRefCatalog::read(backend, layout);
    RefCatalog without_predecessor = before_delete.catalog;
    std::erase_if(without_predecessor.entries, [&](const CatalogEntry & entry)
    {
        return entry.ns == predecessor.ns && entry.incarnation == predecessor.incarnation;
    });
    if (backend.casPut(layout.refCatalogKey(), encodeRefCatalog(without_predecessor), before_delete.token).outcome
        != CasOutcome::Committed)
        throw std::runtime_error("test failed to retire exact predecessor catalog life");

    CatalogEntry successor{
        .ns = predecessor.ns,
        .state = NsState::Live,
        .incarnation = successor_incarnation,
        .creator = std::nullopt};
    const CasRefCatalog::Snapshot after_delete = CasRefCatalog::read(backend, layout);
    RefCatalog reborn = after_delete.catalog;
    reborn.entries.push_back(successor);
    if (backend.casPut(layout.refCatalogKey(), encodeRefCatalog(reborn), after_delete.token).outcome
        != CasOutcome::Committed)
        throw std::runtime_error("test failed to publish successor catalog life");
    return successor;
}

std::optional<RefTxnId> listGreatestLogIdForTest(
    Backend & backend, const Layout & layout, const RootNamespace & ns);

std::optional<RefTxnId> listGreatestLogIdForLifeForTest(
    Backend & backend, const Layout & layout, const NamespaceLifeId & life)
{
    std::optional<RefTxnId> greatest;
    String cursor;
    for (;;)
    {
        const ListPage page = backend.list(layout.namespaceStreamPrefix(life), cursor, 1000);
        for (const ListedKey & listed : page.keys)
        {
            const auto parsed = layout.parseRefObjectKey(listed.key);
            if (parsed && parsed->life_id == life.incarnation && parsed->kind == RefObjectKind::Log
                && (!greatest || *greatest < parsed->txn_id))
                greatest = parsed->txn_id;
        }
        if (page.next_cursor.empty())
            return greatest;
        cursor = page.next_cursor;
    }
}

struct CompletedRemovingFixture
{
    CatalogEntry predecessor;
    uint64_t writer_epoch = 0;
    uint64_t runtime_identity = 0;
};

CompletedRemovingFixture prepareResidentRemovalForDrain(
    const PoolPtr & store, Backend & backend, const RootNamespace & ns, Gc & gc)
{
    publishWithProductionBirth(store, ns, "predecessor");
    const CatalogEntry predecessor = catalogEntryOrThrow(backend, store->layout(), ns);
    const uint64_t writer_epoch = store->liveWriterEpoch();
    const uint64_t runtime_identity = store->refTableRuntimeIdentityForTest(ns);

    if (runRegularRoundReclaiming(gc).deferred)
        throw std::runtime_error("fixture publish unexpectedly deferred");
    store->dropNamespace(ns);
    const CatalogEntry removing = catalogEntryOrThrow(backend, store->layout(), ns);
    if (removing.state != NsState::Removing || removing.incarnation != predecessor.incarnation)
        throw std::runtime_error("fixture removal did not publish the expected exact Removing row");
    if (runRegularRoundReclaiming(gc).deferred)
        throw std::runtime_error("fixture terminal fold unexpectedly deferred");

    const GcState state = decodeGcState(backend.get(store->layout().gcStateKey())->bytes);
    const CasFoldSeal seal = decodeFoldSeal(
        backend.get(store->layout().foldSealKey(state.snap_generation, state.snap_attempt))->bytes);
    const auto row = seal.ref_lives.find(predecessor.incarnation);
    if (row == seal.ref_lives.end() || !row->second.cleanup_evidence)
        throw std::runtime_error("fixture terminal fold produced no cleanup evidence");
    return {predecessor, writer_epoch, runtime_identity};
}

ManifestRef manifestRef(uint64_t epoch, uint64_t seq, uint32_t ordinal)
{
    return ManifestRef{epoch, seq, ordinal};
}

/// Task 11: an INDEPENDENT ground truth for "cache-replay equivalence" tests -- lists every `_log/`
/// key under `ns` directly off the backend (ignoring any snapshot), decodes and replays them in id
/// order via the SAME shared state machine the writer uses, and returns the resulting state. A
/// published snapshot's bytes must equal `encodeRefTableSnapshot(snapshotOf(replay-through-X, ns))`
/// for this oracle's replay truncated at `X`.
RefTableState independentFullReplayForTest(Backend & backend, const Layout & layout, const RootNamespace & ns,
                                            std::optional<RefTxnId> up_to = std::nullopt)
{
    std::vector<RefTxnId> ids;
    String cursor;
    for (;;)
    {
        const ListPage page = backend.list(layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)), cursor, 1000);
        for (const ListedKey & lk : page.keys)
        {
            const auto parsed = layout.parseRefObjectKey(lk.key);
            if (parsed && parsed->life_id == DB::Cas::tests::fixture::fixtureLife(ns).incarnation && parsed->kind == RefObjectKind::Log
                && (!up_to || !(*up_to < parsed->txn_id)))
                ids.push_back(parsed->txn_id);
        }
        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }
    std::sort(ids.begin(), ids.end());

    RefTableState state;
    for (const RefTxnId & id : ids)
    {
        const auto got = backend.get(layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), id));
        applyRefLogTxn(state, decodeRefLogTxn(openObject(FormatId::RefLog, got->bytes), ns.string(), id));
    }
    return state;
}

/// The greatest `_snap/<id>.proto` key currently present for `ns`, found via a fresh LIST (independent
/// of the Pool's own cached bookkeeping).
std::optional<RefTxnId> listGreatestSnapshotIdForTest(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    std::optional<RefTxnId> greatest;
    String cursor;
    for (;;)
    {
        const ListPage page = backend.list(layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)), cursor, 1000);
        for (const ListedKey & lk : page.keys)
        {
            const auto parsed = layout.parseRefObjectKey(lk.key);
            if (parsed && parsed->life_id == DB::Cas::tests::fixture::fixtureLife(ns).incarnation && parsed->kind == RefObjectKind::Snap
                && (!greatest || *greatest < parsed->txn_id))
                greatest = parsed->txn_id;
        }
        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }
    return greatest;
}

/// A backend that can (a) force one `get()` on a chosen exact key to return absent exactly once
/// (simulating an object vanishing after recovery sampled its exact checkpoint, with an optional side effect
/// fired at that exact moment -- e.g. publishing a covering newer snapshot, mirroring a concurrent GC
/// cleanup+republish race), and (b) force `putIfAbsent` on keys matching a chosen substring to throw an
/// ambiguous (Unresolved-classified) exception a bounded number of times, optionally still capturing
/// the (key, bytes) so a test can later "deliver" it -- simulating a request whose RESPONSE was lost
/// even though the write eventually landed server-side.
class RefWriterTestBackend : public CountingBackend
{
public:
    RefWriterTestBackend()
    {
        DB::Cas::tests::seedPoolMetaForRestart(*this);
    }

    using CountingBackend::get;
    using CountingBackend::getStream;
    using CountingBackend::putIfAbsent;
    using CountingBackend::putOverwrite;
    using CountingBackend::casPut;

    void clearRequestJournal()
    {
        std::lock_guard lock(request_journal_mutex);
        request_journal.clear();
    }

    void recordRequestJournalEvent(String event)
    {
        std::lock_guard lock(request_journal_mutex);
        request_journal.push_back(std::move(event));
    }

    std::vector<String> requestJournal() const
    {
        std::lock_guard lock(request_journal_mutex);
        return request_journal;
    }

    std::set<String> vanish_once_keys;
    std::function<void()> on_vanish_fire;

    enum class CatalogCasFault : uint8_t
    {
        None,
        CommitThenThrow,
        OtherWriterReplacement,
    };
    CatalogCasFault catalog_cas_fault = CatalogCasFault::None;
    String catalog_fault_key;
    String catalog_replacement_bytes;
    int catalog_resolution_get_fault_count = 0;
    bool catalog_cas_fault_fired = false;
    /// Fail one selected catalog GET after allowing an exact number of earlier catalog GETs through.
    /// This reaches the removal lane's post-close observation without faulting its initial discovery.
    int catalog_gets_before_fault = -1;
    int catalog_get_fault_count = 0;

    String fault_key_substr;
    int fault_count = 0;
    /// Let the first `fault_skip` matching PUTs through untouched before `fault_count` starts faulting.
    /// Needed now that recovery's in-band epoch seal (INV-2) shares the `_log/` prefix with every other
    /// write under a namespace: a test that wants to fault something LATER in the same prefix (e.g. the
    /// stale-precommit sweep's removal chunk) must skip past recovery's own seal writes first. Same
    /// seam as `ChunkFaultBackend::fault_skip` in `cas_test_helpers.h`.
    int fault_skip = 0;
    std::optional<std::pair<String, String>> pending_delayed_write;

    /// (I1) On a matching `putIfAbsent`, a FOREIGN writer lands a DIFFERENT object at the exact key and
    /// then this attempt's response is lost -- so the controller's resolve-before-reissue GET observes
    /// different bytes and must raise CORRUPTED_DATA (a proven conflict, never a retry signal).
    /// By default the foreign object is the attempt's own bytes plus a trailing marker -- UNDECODABLE
    /// for zstd-framed objects (the frame size no longer matches), which is exactly right for tests
    /// that pin fail-closed handling of a corrupt object. Tests that instead need a VALID foreign
    /// object (e.g. a real cross-process seal to be adopted on retry) set `corrupt_foreign_bytes`.
    String corrupt_key_substr;
    int corrupt_count = 0;
    String corrupt_foreign_bytes;

    String ckpt_conflict_key;
    size_t ckpt_conflict_count = 0;
    String ckpt_get_hook_key;
    std::function<void()> ckpt_get_hook;

    /// Force a stream `LIST` to throw a transient object-store error (S3_ERROR) a bounded number of
    /// times. Recovery must not consume this injection; callers that intentionally enumerate still do.
    int list_fault_count = 0;

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        if (list_fault_count > 0)
        {
            --list_fault_count;
            throw DB::Exception(DB::ErrorCodes::S3_ERROR, "RefWriterTestBackend: simulated transient LIST failure");
        }
        return CountingBackend::list(prefix, cursor, limit);
    }

    std::optional<GetResult> get(const String & key, Range range) override
    {
        recordRequestJournalEvent("GET " + key);
        if (key == ckpt_get_hook_key && ckpt_get_hook)
        {
            auto hook = std::exchange(ckpt_get_hook, nullptr);
            hook();
        }
        if (key == catalog_fault_key && catalog_get_fault_count > 0 && catalog_gets_before_fault >= 0)
        {
            if (catalog_gets_before_fault == 0)
            {
                --catalog_get_fault_count;
                throw std::runtime_error("RefWriterTestBackend: simulated catalog admission read failure");
            }
            --catalog_gets_before_fault;
        }
        if (catalog_cas_fault_fired && key == catalog_fault_key && catalog_resolution_get_fault_count > 0)
        {
            --catalog_resolution_get_fault_count;
            throw std::runtime_error("RefWriterTestBackend: simulated catalog resolution read failure");
        }
        const auto it = vanish_once_keys.find(key);
        if (it != vanish_once_keys.end())
        {
            vanish_once_keys.erase(it);
            if (on_vanish_fire)
            {
                auto fire = std::move(on_vanish_fire);
                on_vanish_fire = nullptr;
                fire();
            }
            return std::nullopt;
        }
        return CountingBackend::get(key, range);
    }

    CasResult casPut(
        const String & key, const String & bytes, const std::optional<Token> & expected,
        const ObjectMeta & meta) override
    {
        recordRequestJournalEvent("CAS " + key);
        if (key == ckpt_conflict_key && ckpt_conflict_count > 0)
        {
            --ckpt_conflict_count;
            return {CasOutcome::Conflict, {}};
        }
        if (key == catalog_fault_key && catalog_cas_fault != CatalogCasFault::None)
        {
            const CatalogCasFault fault = std::exchange(catalog_cas_fault, CatalogCasFault::None);
            catalog_cas_fault_fired = true;
            if (fault == CatalogCasFault::CommitThenThrow)
            {
                const CasResult result = CountingBackend::casPut(key, bytes, expected, meta);
                if (result.outcome != CasOutcome::Committed)
                    return result;
                throw Poco::TimeoutException(
                    "RefWriterTestBackend: catalog CAS committed but its response was lost");
            }

            CasResult replacement = CountingBackend::casPut(
                key, catalog_replacement_bytes, expected, meta);
            if (replacement.outcome != CasOutcome::Committed)
                return replacement;
            return {CasOutcome::Conflict, {}};
        }
        return CountingBackend::casPut(key, bytes, expected, meta);
    }

    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        recordRequestJournalEvent("PUT " + key);
        if (corrupt_count > 0 && !corrupt_key_substr.empty() && key.find(corrupt_key_substr) != String::npos)
        {
            --corrupt_count;
            /// A foreign writer lands a DIFFERENT object at this exact key; then our own response is lost.
            CountingBackend::putIfAbsent(
                key, corrupt_foreign_bytes.empty() ? bytes + String("\x01_FOREIGN_DIFFERENT") : corrupt_foreign_bytes);
            throw Poco::TimeoutException("RefWriterTestBackend: a foreign different object landed; response lost");
        }
        if (!fault_key_substr.empty() && key.find(fault_key_substr) != String::npos)
        {
            if (fault_skip > 0)
            {
                --fault_skip;
            }
            else if (fault_count > 0)
            {
                --fault_count;
                pending_delayed_write = {key, bytes};
                throw Poco::TimeoutException("RefWriterTestBackend: simulated ambiguous result (response lost)");
            }
        }
        {
            std::unique_lock lk(block_mutex);
            bool block_this = false;
            if (block_armed && key.find(block_substr) != String::npos)
            {
                if (!block_first_match_only)
                    block_this = true;                 /// block EVERY matching put (the original mode)
                else if (blocked_key.empty())
                {
                    blocked_key = key;                 /// first match: capture and block exactly this key
                    block_this = true;
                }
                else if (key == blocked_key)
                    block_this = true;                 /// the SAME captured key retried: keep blocking it
                /// a DIFFERENT matching key under first-match-only mode falls through unblocked
            }
            /// (I1) Independent per-key blocking: every matching key parks on its OWN release, unlike
            /// `block_armed` above (one shared gate released all-at-once). Lets a test park two DISTINCT
            /// `_snap/<id>` PUTs concurrently and release them in a chosen order.
            if (independent_block_armed && key.contains(independent_block_substr))
            {
                independent_blocked_keys.insert(key);
                block_cv.notify_all();
                block_cv.wait(lk, [&] { return independent_released_keys.contains(key); });
            }
            if (block_this)
            {
                block_entered = true;
                block_cv.notify_all();
                block_cv.wait(lk, [&] { return !block_armed; });
                /// fix-round F3-1a (CRITICAL, unlock-throw race harness): on release, behave like
                /// `corrupt_key_substr` above instead of proceeding normally -- a foreign writer landed
                /// DIFFERENT bytes at this exact key while we were parked, so our own attempt is a
                /// PROVEN conflict once `putIfAbsentControlled`'s resolve-before-reissue GETs it. Lets a
                /// test make the recovery seal's PUT throw CORRUPTED_DATA from INSIDE the unlocked
                /// window, deterministically, instead of merely returning a non-Committed outcome.
                if (block_throw_corrupted_on_release)
                {
                    lk.unlock();
                    CountingBackend::putIfAbsent(key, bytes + String("\x01_FOREIGN_DIFFERENT"));
                    {
                        std::lock_guard g(block_mutex);
                        block_call_completed = true;
                    }
                    block_cv.notify_all();
                    throw Poco::TimeoutException(
                        "RefWriterTestBackend: a foreign different object landed on release; response lost");
                }
            }
        }
        const PutResult r = CountingBackend::putIfAbsent(key, bytes, meta);
        {
            std::lock_guard g(block_mutex);
            block_call_completed = true;
        }
        block_cv.notify_all();
        return r;
    }
    /// See `putIfAbsent`'s `block_this` branch. Set before spawning any thread that could race
    /// `putIfAbsent`, like `corrupt_key_substr`/`fault_key_substr` above -- not itself lock-protected.
    bool block_throw_corrupted_on_release = false;

    /// "Deliver" the earlier ambiguous write: the request DID eventually land server-side, the caller
    /// just never saw the ack. No-op if no fault has fired since the last delivery.
    void materializePendingDelayedWrite()
    {
        if (pending_delayed_write)
        {
            CountingBackend::putIfAbsent(pending_delayed_write->first, pending_delayed_write->second);
            pending_delayed_write.reset();
        }
    }

    /// Task 11: blocks EVERY `putIfAbsent()` whose key contains `armed_block_substr` until
    /// `releaseBlock()` is called, notifying `awaitBlockEntered()` the first time one is reached. Used
    /// to prove snapshot publication never holds up an unrelated concurrent append.
    void armPutBlock(const String & substr)
    {
        std::lock_guard g(block_mutex);
        block_substr = substr;
        block_armed = true;
        block_entered = false;
        block_call_completed = false;
        block_first_match_only = false;
        blocked_key.clear();
    }

    /// Task 11 (monotonic-adoption harness): block ONLY the FIRST `putIfAbsent` whose key contains
    /// `substr`, capturing that exact key; every LATER put -- including a DIFFERENT `_snap/<id>` key --
    /// proceeds unblocked. Lets a test pin one in-flight publish's PUT mid-flight while a second,
    /// higher-id publish runs to completion, deterministically forcing the out-of-order overlap.
    void armPutBlockFirstMatchOnly(const String & substr)
    {
        std::lock_guard g(block_mutex);
        block_substr = substr;
        block_armed = true;
        block_entered = false;
        block_call_completed = false;
        block_first_match_only = true;
        blocked_key.clear();
    }
    void awaitBlockEntered()
    {
        std::unique_lock lk(block_mutex);
        block_cv.wait(lk, [&] { return block_entered; });
    }
    void releaseBlock()
    {
        {
            std::lock_guard g(block_mutex);
            block_armed = false;
        }
        block_cv.notify_all();
    }
    /// Blocks until the PREVIOUSLY-blocked `putIfAbsent` call has actually RETURNED (not merely been
    /// unblocked) -- i.e. its underlying `CountingBackend::putIfAbsent` has completed. Deterministic,
    /// sleep-free way to observe a detached background caller's own work finishing when the TEST no
    /// longer holds anything (e.g. a Pool handle) that call would otherwise let it wait on.
    void awaitBlockedCallCompleted()
    {
        std::unique_lock lk(block_mutex);
        block_cv.wait(lk, [&] { return block_call_completed; });
    }

    /// (I1 regression harness) Arms independent per-key blocking for every `putIfAbsent` matching
    /// `substr`: unlike `armPutBlock`/`armPutBlockFirstMatchOnly` (one shared release gate), each
    /// blocked key parks on ITS OWN release (`releaseKey`), so two distinct `_snap/<id>` PUTs can be
    /// parked concurrently -- both past their capture point, neither yet adopted -- and released in a
    /// chosen order. Needed to construct the small-candidate-adopts-before-a-larger-one-already-in-flight
    /// ordering that exercises `clampedCounterSub`'s actual clamp branch.
    void armPutBlockIndependently(const String & substr)
    {
        std::lock_guard g(block_mutex);
        independent_block_substr = substr;
        independent_block_armed = true;
        independent_blocked_keys.clear();
        independent_released_keys.clear();
    }
    /// Blocks until at least `n` distinct matching keys are currently parked.
    void awaitAtLeastNKeysBlocked(size_t n)
    {
        std::unique_lock lk(block_mutex);
        block_cv.wait(lk, [&] { return independent_blocked_keys.size() >= n; });
    }
    /// A snapshot of the keys currently parked under independent blocking.
    std::set<String> blockedKeysSnapshot()
    {
        std::lock_guard g(block_mutex);
        return independent_blocked_keys;
    }
    /// Releases exactly the given key; every OTHER independently-blocked key stays parked.
    void releaseKey(const String & key)
    {
        {
            std::lock_guard g(block_mutex);
            independent_released_keys.insert(key);
        }
        block_cv.notify_all();
    }

private:
    mutable std::mutex request_journal_mutex;
    std::vector<String> request_journal;
    std::mutex block_mutex;
    std::condition_variable block_cv;
    String block_substr;
    bool block_armed = false;
    bool block_entered = false;
    bool block_call_completed = false;
    bool block_first_match_only = false;
    String blocked_key;
    String independent_block_substr;
    bool independent_block_armed = false;
    std::set<String> independent_blocked_keys;
    std::set<String> independent_released_keys;
};

}

/// ===================================================================================
/// Recovery (spec §Recovery / exact checkpoint grounding)
/// ===================================================================================

TEST(CASRefWriterRecovery, EmptyNamespaceRecoversToEmptyState)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/never_touched"};

    EXPECT_TRUE(store->listRefs(ns).empty());
    EXPECT_FALSE(store->resolveRef(ns, "anything").has_value());
    EXPECT_EQ(store->refRecoveryRestartsForTest(ns), 0u);
}

TEST(CASRefWriterNonMinting, ListRefsOnAbsentNamespaceDoesNotMutateCatalog)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/list_absent_non_minting"};
    const auto catalog_before = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(catalog_before);
    backend->resetCounts();

    EXPECT_TRUE(store->listRefs(ns).empty());

    EXPECT_EQ(backend->putCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->putOverwriteCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->casPutCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->deleteCount(layout.refCatalogKey()), 0u);
    const auto catalog_after = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(catalog_after);
    EXPECT_EQ(catalog_after->bytes, catalog_before->bytes);
    EXPECT_EQ(catalog_after->token, catalog_before->token);
}

TEST(CASRefWriterRuntimeIdentity, ColdReadRejectsCatalogLifeReplacedWithoutLocalInvalidation)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/cold-read-catalog-aba"};
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, layout, ns, store->liveWriterEpoch());
    const CatalogEntry predecessor = catalogEntryOrThrow(*backend, layout, ns);
    ASSERT_EQ(store->refTableRuntimeIdentityForTest(ns), 0u);

    std::mutex mutex;
    std::condition_variable cv;
    bool paused = false;
    bool resume = false;
    store->setReadableCatalogAfterObservationHookForTest([&]
    {
        std::unique_lock lock(mutex);
        paused = true;
        cv.notify_all();
        cv.wait(lock, [&] { return resume; });
    });

    std::exception_ptr stale_error;
    std::thread stale_reader([&]
    {
        try
        {
            (void)store->listRefs(ns);
        }
        catch (...)
        {
            stale_error = std::current_exception();
        }
    });
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return paused; });
    }

    const CatalogEntry successor
        = replaceCatalogLifeForRuntimeRace(*backend, layout, predecessor, UInt128{0xabc002});
    const NamespaceLifeId successor_life
        = NamespaceLifeId::fromCatalogEntry(successor.ns, successor.incarnation);
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(successor_life), encodeRefCkpt(RefCkpt{
        .life_epoch = store->liveWriterEpoch(),
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);
    {
        std::lock_guard lock(mutex);
        resume = true;
    }
    cv.notify_all();
    stale_reader.join();
    store->setReadableCatalogAfterObservationHookForTest(nullptr);

    EXPECT_TRUE(stale_error) << "the stale catalog life was published instead of refused";
    EXPECT_EQ(store->refTableRuntimeIdentityForTest(ns), 0u);
    EXPECT_NO_THROW((void)store->listRefs(ns));
    ASSERT_TRUE(store->refTableLifeForTest(ns));
    EXPECT_EQ(*store->refTableLifeForTest(ns), successor_life);
}

TEST(CASRefWriterRuntimeIdentity, ColdReadRejectsReplacementByExternalPoolActor)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    PoolConfig external_config{.pool_prefix = "p", .server_root_id = "external-runtime-race"};
    auto external_store = Pool::open(backend, std::move(external_config));
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/external-catalog-runtime-publication"};
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, layout, ns, store->liveWriterEpoch());
    const CatalogEntry predecessor = catalogEntryOrThrow(*backend, layout, ns);
    ASSERT_EQ(store->refTableRuntimeIdentityForTest(ns), 0u);

    CatalogEntry successor;
    store->setReadableCatalogAfterObservationHookForTest([&]
    {
        successor = replaceCatalogLifeForRuntimeRace(
            external_store->backend(), external_store->layout(), predecessor, UInt128{0xabc003});
        const NamespaceLifeId successor_life
            = NamespaceLifeId::fromCatalogEntry(successor.ns, successor.incarnation);
        if (external_store->backend().putIfAbsent(
                external_store->layout().refCkptKey(successor_life),
                encodeRefCkpt(RefCkpt{
                    .life_epoch = external_store->liveWriterEpoch(),
                    .checkpoint_snapshot_id = std::nullopt,
                    .last_epoch_seal = std::nullopt})).outcome != PutOutcome::Done)
            throw std::runtime_error("test failed to publish external successor checkpoint");
    });

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)store->listRefs(ns); });
    store->setReadableCatalogAfterObservationHookForTest(nullptr);

    EXPECT_EQ(store->refTableRuntimeIdentityForTest(ns), 0u);
    EXPECT_NO_THROW((void)store->listRefs(ns));
    ASSERT_TRUE(store->refTableLifeForTest(ns));
    EXPECT_EQ(*store->refTableLifeForTest(ns),
        NamespaceLifeId::fromCatalogEntry(successor.ns, successor.incarnation));
}

/// The cold-reader revalidation is about THIS namespace's row, not whole-catalog stillness: an
/// unrelated namespace admitted between the two observations must not refuse the admission (that
/// refusal starved cold admissions under a parallel workload sharing one pool), while the target's
/// own row staying identical still publishes the runtime against the observed life.
TEST(CASRefWriterRuntimeIdentity, ColdReadAdmitsThroughUnrelatedCatalogMutationBetweenObservations)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/unrelated-catalog-runtime-publication"};
    const RootNamespace unrelated{"srv1/unrelated-catalog-row"};
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, layout, ns, store->liveWriterEpoch());
    ASSERT_EQ(store->refTableRuntimeIdentityForTest(ns), 0u);

    store->setReadableCatalogAfterObservationHookForTest([&]
    {
        DB::Cas::tests::fixture::admitLive(*backend, layout, unrelated);
    });

    EXPECT_NO_THROW((void)store->listRefs(ns));
    store->setReadableCatalogAfterObservationHookForTest(nullptr);

    EXPECT_NE(store->refTableRuntimeIdentityForTest(ns), 0u);
}

/// The per-row narrowing must not skip the second cut's ambiguity validation: an ALIASING incarnation
/// admitted between the two observations (another namespace stealing this life's incarnation --
/// physical life-owned keys use only the incarnation) leaves the target's own row byte-identical yet
/// must still refuse the admission. The whole-catalog comparison refused this implicitly; the
/// narrowed check must refuse it explicitly.
TEST(CASRefWriterRuntimeIdentity, ColdReadRejectsAliasingIncarnationAdmittedBetweenObservations)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/aliasing-incarnation-target"};
    const RootNamespace alias{"srv1/aliasing-incarnation-thief"};
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, layout, ns, store->liveWriterEpoch());
    const CatalogEntry target_row = catalogEntryOrThrow(*backend, layout, ns);
    ASSERT_EQ(store->refTableRuntimeIdentityForTest(ns), 0u);

    store->setReadableCatalogAfterObservationHookForTest([&]
    {
        CatalogEntry thief;
        thief.ns = alias;
        thief.state = NsState::Creating;
        thief.incarnation = target_row.incarnation;
        thief.creator = CreatorFence{
            .server_root_id = "srv1", .writer_epoch = store->liveWriterEpoch(), .fence_generation = 1};
        CasRefCatalog::casAdmitEntry(*backend, layout, 1, thief);
    });

    EXPECT_THROW((void)store->listRefs(ns), DB::Exception);
    store->setReadableCatalogAfterObservationHookForTest(nullptr);

    EXPECT_EQ(store->refTableRuntimeIdentityForTest(ns), 0u);
}

TEST(CASRefWriterRuntimeIdentity, WarmReadableRuntimeDoesNotReadCatalog)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/warm-runtime-zero-catalog-get"};
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, layout, ns, store->liveWriterEpoch());

    EXPECT_NO_THROW((void)store->listRefs(ns));
    ASSERT_NE(store->refTableRuntimeIdentityForTest(ns), 0u);
    backend->resetCounts();

    EXPECT_NO_THROW((void)store->listRefs(ns));
    EXPECT_EQ(backend->getCount(layout.refCatalogKey()), 0u);
}

/// `DROP DETACHED PART` reaches this point lookup for a part that may already be absent. Its probe
/// must not turn a missing table namespace into a new catalog life.
TEST(CASRefWriterNonMinting, ResolveRefOnAbsentNamespaceDoesNotMutateCatalog)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/resolve_absent_non_minting"};
    const auto catalog_before = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(catalog_before);
    backend->resetCounts();

    EXPECT_FALSE(store->resolveRef(ns, "detached_part").has_value());

    EXPECT_EQ(backend->putCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->putOverwriteCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->casPutCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->deleteCount(layout.refCatalogKey()), 0u);
    const auto catalog_after = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(catalog_after);
    EXPECT_EQ(catalog_after->bytes, catalog_before->bytes);
    EXPECT_EQ(catalog_after->token, catalog_before->token);
}

TEST(CASRefWriterNonMinting, DropNamespaceOnAbsentNamespaceDoesNotMutateCatalog)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/drop_absent_non_minting"};
    const auto catalog_before = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(catalog_before);
    backend->resetCounts();

    store->dropNamespace(ns);

    EXPECT_EQ(backend->putCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->putOverwriteCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->casPutCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->deleteCount(layout.refCatalogKey()), 0u);
    const auto catalog_after = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(catalog_after);
    EXPECT_EQ(catalog_after->bytes, catalog_before->bytes);
    EXPECT_EQ(catalog_after->token, catalog_before->token);
}

/// A table born by a log tail alone (no snapshot yet): `namespace_birth` with nothing else is a legal
/// Live-but-empty table.
TEST(CASRefWriterRecovery, BirthOnlyLogNoSnapshotRecoversToEmptyLiveTable)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/birth_only"};

    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{ns.string(), RefTxnId{1, 1}, {namespaceBirthOp()}, std::nullopt});
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);

    auto store = openPool(backend);
    EXPECT_TRUE(store->listRefs(ns).empty());
}

/// Empty base + birth log recovery (spec unit test list): birth and the first precommit->promote span
/// TWO separate log transactions with no snapshot at all.
TEST(CASRefWriterRecovery, BirthPlusPrecommitPromoteAcrossTwoLogsNoSnapshot)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/birth_then_promote"};
    const ManifestRef m1 = manifestRef(1, 1, 1);

    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{ns.string(), RefTxnId{1, 1},
        {namespaceBirthOp(), publishCommittedOps("part_1", m1)[0]}, std::nullopt});
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{ns.string(), RefTxnId{1, 2},
        {publishCommittedOps("part_1", m1)[1]}, std::nullopt});
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);

    auto store = openPool(backend);
    const auto resolved = store->resolveRef(ns, "part_1");
    ASSERT_TRUE(resolved.has_value());
    EXPECT_EQ(resolved->manifest_id.ref, m1);
    EXPECT_EQ(resolved->manifest_id.root_namespace, ns);

    const auto refs = store->listRefs(ns);
    ASSERT_EQ(refs.size(), 1u);
    EXPECT_TRUE(refs.contains("part_1"));
}

TEST(CASRefWriterRecovery, TerminalGapBelowCheckpointFrontierIsCorruptionNotSameLifeRebirth)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/writer_terminal_gap"};

    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        ns.string(), RefTxnId{1, 1}, {namespaceBirthOp()}, std::nullopt});
    RefOp remove;
    remove.kind = RefOpKind::RemoveNamespace;
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        ns.string(), RefTxnId{1, 2}, {std::move(remove)}, std::nullopt});
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        ns.string(), RefTxnId{2, 1}, {namespaceBirthOp()}, std::nullopt});
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{2, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{1, 2},
    });

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    const String next_log_key = layout.refLogKey(life, RefTxnId{2, 2});
    auto store = openPool(backend);
    const uint64_t installs_before = store->recoveryInstallCountForTest();

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { (void)store->listRefs(ns); });
    EXPECT_FALSE(store->refTableRecoveredForTest(ns));
    EXPECT_EQ(store->recoveryInstallCountForTest(), installs_before);

    EXPECT_ANY_THROW((void)publishEmptyPart(store, ns, "must_not_allocate"));
    EXPECT_EQ(backend->putCount(next_log_key), 0u)
        << "an unrecovered malformed life must not allocate the next writer position";
}

/// Latest snapshot plus tail recovery (spec unit test list): a snapshot covering ref "a", a tail that
/// drops "a" and publishes "b", and a STALE log at/below the snapshot id that must be ignored (its
/// content, if replayed, would corrupt the result -- proving the "ignore log keys at or below the
/// selected snapshot" rule).
TEST(CASRefWriterRecovery, SnapshotPlusTailRecovery)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/snap_tail"};
    const ManifestRef ma = manifestRef(1, 1, 1);
    const ManifestRef mb = manifestRef(1, 2, 1);

    /// A stale log BELOW the snapshot id would, if wrongly replayed, try to add "a" a second time
    /// (the snapshot already contains it) and throw -- proving it must be ignored, not merely benign.
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{ns.string(), RefTxnId{1, 3},
        {namespaceBirthOp(), publishCommittedOps("a", ma)[0], publishCommittedOps("a", ma)[1]}, std::nullopt});
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = ns.string(),
        .txn_id = RefTxnId{1, 5},
        .ops = publishCommittedOps("a", ma),
        .prev_epoch_seal = std::nullopt});
    writeRefSnapshotRaw(*backend, layout, minimalLiveSnapshot(ns.string(), RefTxnId{1, 5}, {committedRow("a", ma)}));

    std::vector<RefOp> tail_ops;
    tail_ops.push_back([&] { RefOp op; op.kind = RefOpKind::OwnerTransition;
        op.old_binding = RefOwnerBinding{RefOwnerKind::Committed, "a", ma}; return op; }());
    tail_ops.push_back(publishCommittedOps("b", mb)[0]);
    tail_ops.push_back(publishCommittedOps("b", mb)[1]);
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{ns.string(), RefTxnId{1, 6}, tail_ops, std::nullopt});
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 6},
        .checkpoint_snapshot_id = RefTxnId{1, 5},
        .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);

    backend->resetCounts();
    auto store = openPool(backend);
    EXPECT_FALSE(store->resolveRef(ns, "a").has_value());
    const auto b = store->resolveRef(ns, "b");
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(b->manifest_id.ref, mb);
    EXPECT_EQ(store->listRefs(ns).size(), 1u);
    EXPECT_EQ(backend->getCount(layout.refLogKey(life, RefTxnId{1, 5})), 1u)
        << "recovery must validate the selected base's retained ordinary log";
    EXPECT_EQ(backend->getCount(layout.refSnapshotKey(life, RefTxnId{1, 5})), 1u)
        << "the fixture must reach and decode the selected base snapshot";
}

/// Restart-on-vanish (spec §Recovery): the checkpoint-named snapshot vanishes during its exact GET
/// while concurrent cleanup publishes a newer checkpoint base. Recovery must restart from the newer
/// exact checkpoint, not treat the vanish as corruption.
TEST(CASRefWriterRecovery, RestartOnVanishConvergesOnNewerSnapshot)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/vanish_race"};
    const ManifestRef ma = manifestRef(1, 1, 1);
    const ManifestRef mb = manifestRef(1, 2, 1);

    /// Stage B (Task 4-C): pin `ns` to the sentinel before the raw snapshot below -- `store->resolveRef`
    /// further down is a real production read that triggers `resolveNamespaceLife`, which for an
    /// UNADMITTED namespace mints a fresh RANDOM incarnation rather than adopting the sentinel the raw
    /// fixture writes at.
    DB::Cas::tests::fixture::admitLive(*backend, layout, ns);
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    const RefTxnId snap_x{1, 10};
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = ns.string(),
        .txn_id = snap_x,
        .ops = publishCommittedOps("a", ma),
        .prev_epoch_seal = std::nullopt});
    writeRefSnapshotRaw(*backend, layout, minimalLiveSnapshot(ns.string(), snap_x, {committedRow("a", ma)}));
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 10},
        .checkpoint_snapshot_id = snap_x,
        .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);
    backend->vanish_once_keys.insert(layout.refSnapshotKey(life, snap_x));
    bool vanish_fired = false;
    backend->on_vanish_fire = [&]
    {
        vanish_fired = true;
        const RefTxnId snap_y{1, 20};
        DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
            .ns = ns.string(),
            .txn_id = snap_y,
            .ops = publishCommittedOps("b", mb),
            .prev_epoch_seal = std::nullopt});
        writeRefSnapshotRaw(*backend, layout, minimalLiveSnapshot(ns.string(), snap_y, {committedRow("b", mb)}));
        const auto before = backend->get(layout.refCkptKey(life));
        ASSERT_TRUE(before);
        ASSERT_EQ(backend->casPut(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
            .life_epoch = 1,
            .committed_through = RefTxnId{1, 20},
            .checkpoint_snapshot_id = snap_y,
            .last_epoch_seal = std::nullopt}), before->token).outcome, CasOutcome::Committed);
    };

    backend->resetCounts();
    auto store = openPool(backend);
    const auto b = store->resolveRef(ns, "b");
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(b->manifest_id.ref, mb);
    EXPECT_FALSE(store->resolveRef(ns, "a").has_value()) << "must converge on snapshot Y, not a mix of X and Y";
    EXPECT_EQ(store->refRecoveryRestartsForTest(ns), 1u);
    EXPECT_TRUE(vanish_fired) << "the fixture must reach the old snapshot GET and fire the replacement hook";
    EXPECT_FALSE(backend->vanish_once_keys.contains(layout.refSnapshotKey(life, snap_x)));
    EXPECT_EQ(backend->getCount(layout.refLogKey(life, snap_x)), 1u);
    EXPECT_EQ(backend->getCount(layout.refLogKey(life, RefTxnId{1, 20})), 1u);
}

/// A DIFFERENT valid object at the exact snapshot key (not merely absent) is corruption, never a
/// restart signal -- pins the boundary between "vanished" (restart) and "corrupt" (fail closed).
TEST(CASRefWriterRecovery, DifferentBytesAtSelectedSnapshotIsCorruptionNotRestart)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/corrupt_snap"};
    const RefTxnId snap_x{1, 10};

    /// A structurally-valid snapshot BODY, but for a DIFFERENT namespace, placed under `ns`'s own key
    /// (a copy-under-the-wrong-prefix scenario) -- decodeRefTableSnapshot's key/body cross-check must
    /// reject it, never treat it as a restart signal.
    /// Stage B (Task 4-C): pin `ns` to the sentinel before the raw write below -- `store->resolveRef`
    /// further down is a real production read that would otherwise mint a fresh RANDOM incarnation
    /// for this unadmitted namespace instead of adopting the sentinel the raw fixture writes at.
    DB::Cas::tests::fixture::admitLive(*backend, layout, ns);
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, layout, ns);
    const RootNamespace other_ns{"srv1/other"};
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = ns.string(),
        .txn_id = snap_x,
        .ops = publishCommittedOps("anchor", manifestRef(1, 10, 1)),
        .prev_epoch_seal = std::nullopt});
    DB::Cas::RefTableSnapshot foreign;
    foreign.ns = other_ns.string();
    foreign.snapshot_id = snap_x;
    const String snapshot_key = layout.refSnapshotKey(life, snap_x);
    ASSERT_EQ(backend->putIfAbsent(snapshot_key,
        DB::Cas::sealObject(DB::Cas::FormatId::RefSnapshot, DB::Cas::encodeRefTableSnapshot(foreign))).outcome,
        PutOutcome::Done);
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = snap_x,
        .checkpoint_snapshot_id = snap_x,
        .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);

    auto store = openPool(backend);
    backend->resetCounts();
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->resolveRef(ns, "anything"); });
    EXPECT_EQ(backend->getCount(layout.refLogKey(life, snap_x)), 1u)
        << "the matching ordinary log must be validated before the selected snapshot";
    EXPECT_EQ(backend->getCount(snapshot_key), 1u)
        << "the corruption must come from decoding the required checkpoint snapshot";
}

/// ===================================================================================
/// Append lane: request cost + batching (spec §Common Mutation Path / §Local Batching Queue)
/// ===================================================================================

TEST(CASRefWriterAppendLane, CommittedChunkPublishesFrontierBeforeInstallAndAck)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/warm"};
    publishEmptyPart(store, ns, "part_1");
    publishEmptyPart(store, ns, "part_2");
    ASSERT_TRUE(store->resolveRef(ns, "part_1").has_value());
    ASSERT_TRUE(store->resolveRef(ns, "part_2").has_value());

    const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(*backend, store->layout(), ns).value();
    const String log_prefix = store->layout().namespaceStreamPrefix(life) + "_log/";
    const String ckpt_key = store->layout().refCkptKey(life);
    const auto ckpt_before = readCkpt(*backend, store->layout(), life);
    ASSERT_TRUE(ckpt_before);
    ASSERT_TRUE(ckpt_before->ckpt.committed_through);
    const RefTxnId expected_frontier{
        ckpt_before->ckpt.committed_through->writer_epoch,
        ckpt_before->ckpt.committed_through->ref_sequence + 1};
    backend->clearRequestJournal();
    const uint64_t list_before = backend->listTotal();
    const uint64_t put_before = backend->putTotal();
    const uint64_t ckpt_get_before = backend->getCount(ckpt_key);
    const uint64_t ckpt_cas_before = backend->casPutCount(ckpt_key);

    std::mutex mutex;
    std::condition_variable cv;
    bool pre_carve_entered = false;
    bool post_install_entered = false;
    bool release_post_install = false;
    bool follower_returned = false;
    store->setRefPreCarveHookForTest([&]
    {
        std::unique_lock lock(mutex);
        if (pre_carve_entered)
            return;
        pre_carve_entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return store->refQueuePendingForTest(ns) >= 2; });
    });
    store->setCarveHookForTest([&](CasRefLedger::CarvePhaseForTest phase)
    {
        if (phase != CasRefLedger::CarvePhaseForTest::PostInstallPreAck)
            return;
        backend->recordRequestJournalEvent("INSTALL");
        std::unique_lock lock(mutex);
        post_install_entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release_post_install; });
    });

    std::exception_ptr leader_error;
    std::exception_ptr follower_error;
    std::thread leader([&]
    {
        try
        {
            store->dropRef(ns, "part_1");
        }
        catch (...)
        {
            leader_error = std::current_exception();
        }
    });
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return pre_carve_entered; });
    }
    std::thread follower([&]
    {
        try
        {
            store->dropRef(ns, "part_2");
            backend->recordRequestJournalEvent("FOLLOWER ACK");
            {
                std::lock_guard lock(mutex);
                follower_returned = true;
            }
            cv.notify_all();
        }
        catch (...)
        {
            follower_error = std::current_exception();
        }
    });
    while (store->refQueuePendingForTest(ns) < 2)
        std::this_thread::yield();
    cv.notify_all();
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return post_install_entered; });
    }

    bool follower_returned_before_release = false;
    {
        std::lock_guard lock(mutex);
        follower_returned_before_release = follower_returned;
    }
    std::exception_ptr observation_error;
    bool part_1_visible = true;
    bool part_2_visible = true;
    try
    {
        part_1_visible = store->resolveRef(ns, "part_1").has_value();
        part_2_visible = store->resolveRef(ns, "part_2").has_value();
    }
    catch (...)
    {
        observation_error = std::current_exception();
    }
    {
        std::lock_guard lock(mutex);
        release_post_install = true;
    }
    cv.notify_all();
    leader.join();
    follower.join();
    store->setRefPreCarveHookForTest(nullptr);
    store->setCarveHookForTest(nullptr);

    EXPECT_FALSE(follower_returned_before_release)
        << "a co-batched waiter returned before the installed transaction was acknowledged";
    EXPECT_FALSE(observation_error);
    EXPECT_FALSE(part_1_visible);
    EXPECT_FALSE(part_2_visible)
        << "both co-batched mutations must be visible before either waiter can return success";
    EXPECT_FALSE(leader_error);
    EXPECT_FALSE(follower_error);
    EXPECT_EQ(backend->listTotal(), list_before) << "a warm mutation performs no LIST";
    EXPECT_EQ(backend->putTotal(), put_before + 1) << "exactly one body PUT with create-if-absent";
    EXPECT_EQ(backend->getCount(ckpt_key), ckpt_get_before + 1)
        << "one committed chunk pays exactly one checkpoint GET";
    EXPECT_EQ(backend->casPutCount(ckpt_key), ckpt_cas_before + 1)
        << "one committed chunk pays exactly one checkpoint CAS";

    const std::vector<String> journal = backend->requestJournal();
    ASSERT_EQ(journal.size(), 5u);
    EXPECT_EQ(journal[0].find("PUT " + log_prefix), 0u) << journal[0];
    EXPECT_EQ(journal[1], "GET " + ckpt_key);
    EXPECT_EQ(journal[2], "CAS " + ckpt_key);
    EXPECT_EQ(journal[3], "INSTALL");
    EXPECT_EQ(journal[4], "FOLLOWER ACK");

    const auto durable_ckpt = readCkpt(*backend, store->layout(), life);
    ASSERT_TRUE(durable_ckpt);
    EXPECT_EQ(durable_ckpt->ckpt.committed_through, expected_frontier);
}

TEST(CASRefWriterAppendLane, CheckpointConflictAfterLogCommitRequiresRecoveryWithoutInstall)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/frontier-conflict"};
    publishEmptyPart(store, ns, "x");
    const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(*backend, store->layout(), ns).value();
    const String ckpt_key = store->layout().refCkptKey(life);
    const auto before = readCkpt(*backend, store->layout(), life);
    ASSERT_TRUE(before);
    ASSERT_TRUE(before->ckpt.committed_through);
    const RefTxnId candidate{before->ckpt.committed_through->writer_epoch,
                             before->ckpt.committed_through->ref_sequence + 1};
    const size_t tail_before = store->tailSinceSnapshotCountForTest(ns);

    backend->ckpt_conflict_key = ckpt_key;
    backend->ckpt_conflict_count = 100;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });

    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), tail_before)
        << "the durable log was not installed or acknowledged";
    const auto after = readCkpt(*backend, store->layout(), life);
    ASSERT_TRUE(after);
    EXPECT_EQ(after->ckpt.committed_through, before->ckpt.committed_through);
    EXPECT_TRUE(backend->get(store->layout().refLogKey(life, candidate)))
        << "the log PUT committed before checkpoint publication failed";
    EXPECT_FALSE(backend->get(store->layout().refLogKey(
        life, RefTxnId{candidate.writer_epoch, candidate.ref_sequence + 1})))
        << "no later id may be allocated above an unfrontiered durable transaction";
}

TEST(CASRefWriterAppendLane, FenceMovementAtCheckpointPublicationRequiresRecoveryWithoutInstall)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/frontier-fenced"};
    publishEmptyPart(store, ns, "x");
    const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(*backend, store->layout(), ns).value();
    const String ckpt_key = store->layout().refCkptKey(life);
    const auto before = readCkpt(*backend, store->layout(), life);
    ASSERT_TRUE(before);
    ASSERT_TRUE(before->ckpt.committed_through);
    const RefTxnId candidate{before->ckpt.committed_through->writer_epoch,
                             before->ckpt.committed_through->ref_sequence + 1};
    const size_t tail_before = store->tailSinceSnapshotCountForTest(ns);

    backend->ckpt_get_hook_key = ckpt_key;
    backend->ckpt_get_hook = [&] { store->tripMountLost(); };
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });

    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), tail_before)
        << "the fenced frontier attempt must not install or acknowledge the durable log";
    const auto after = readCkpt(*backend, store->layout(), life);
    ASSERT_TRUE(after);
    EXPECT_EQ(after->ckpt.committed_through, before->ckpt.committed_through);
    EXPECT_TRUE(backend->get(store->layout().refLogKey(life, candidate)));
    EXPECT_FALSE(backend->get(store->layout().refLogKey(
        life, RefTxnId{candidate.writer_epoch, candidate.ref_sequence + 1})));
}

/// Phase 3 (reftable-cow-map materialization): each of these N
/// publishes is its own isolated (unbatched) flush touching exactly one NEW ref -- if
/// `flushRefBatch` did not materialize `rt->state.committed` after installing each flush's
/// transaction, the overlay would grow by ~1 entry per flush and this would read back ~N,
/// defeating the whole point of the COW map for a long-running table.
TEST(CASRefWriterAppendLane, MaterializeKeepsOverlaySmallAcrossManyIsolatedFlushes)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/cowmap"};

    constexpr int kRefs = 20;
    for (int i = 0; i < kRefs; ++i)
        publishEmptyPart(store, ns, "ref" + std::to_string(i));

    EXPECT_LE(store->committedOverlayEntriesForTest(ns), 1u);
    EXPECT_EQ(store->listRefs(ns).size(), static_cast<size_t>(kRefs));   /// sanity: all N really committed
}

/// `B` compatible queued mutations share one create (spec §Writer Budget).
TEST(CASRefWriterAppendLane, CompatibleMutationsShareOneCreate)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/cobatch"};
    publishEmptyPart(store, ns, "a");
    publishEmptyPart(store, ns, "b");
    ASSERT_TRUE(store->resolveRef(ns, "a").has_value());
    ASSERT_TRUE(store->resolveRef(ns, "b").has_value());

    std::mutex m;
    std::condition_variable cv;
    bool entered = false;
    store->setRefPreCarveHookForTest([&]
    {
        std::unique_lock lk(m);
        if (entered)
            return;   /// only the leader's own first carve blocks; a second flush (if any) proceeds
        entered = true;
        cv.notify_all();
        cv.wait(lk, [&] { return store->refQueuePendingForTest(ns) >= 2; });
    });

    const uint64_t put_before = backend->putTotal();
    std::thread t_a([&] { store->dropRef(ns, "a"); });
    {
        std::unique_lock lk(m);
        cv.wait(lk, [&] { return entered; });
    }
    std::thread t_b([&] { store->dropRef(ns, "b"); });
    while (store->refQueuePendingForTest(ns) < 2)
        std::this_thread::yield();
    cv.notify_all();   /// wakes the pre-carve hook's own wait once its predicate (>=2 pending) holds
    t_a.join();
    t_b.join();
    store->setRefPreCarveHookForTest(nullptr);

    EXPECT_EQ(backend->putTotal(), put_before + 1) << "both drops must land in ONE created log object";
    EXPECT_FALSE(store->resolveRef(ns, "a").has_value());
    EXPECT_FALSE(store->resolveRef(ns, "b").has_value());
}

/// An invalid queued request returns its own exception without entering the transaction; the
/// co-batched neighbor still lands, in the SAME one create.
TEST(CASRefWriterAppendLane, InvalidBatchEntryGetsOwnExceptionBatchSurvives)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/invalid_entry"};
    publishEmptyPart(store, ns, "good");

    std::mutex m;
    std::condition_variable cv;
    bool entered = false;
    store->setRefPreCarveHookForTest([&]
    {
        std::unique_lock lk(m);
        if (entered)
            return;
        entered = true;
        cv.notify_all();
        cv.wait(lk, [&] { return store->refQueuePendingForTest(ns) >= 2; });
    });

    const uint64_t put_before = backend->putTotal();
    std::exception_ptr bad_error;
    std::thread t_bad([&]
    {
        try { store->dropRef(ns, "does_not_exist"); }
        catch (...) { bad_error = std::current_exception(); }
    });
    {
        std::unique_lock lk(m);
        cv.wait(lk, [&] { return entered; });
    }
    std::thread t_good([&] { store->dropRef(ns, "good"); });
    while (store->refQueuePendingForTest(ns) < 2)
        std::this_thread::yield();
    cv.notify_all();
    t_bad.join();
    t_good.join();
    store->setRefPreCarveHookForTest(nullptr);

    ASSERT_TRUE(bad_error != nullptr) << "the invalid item's OWN caller must receive its exception";
    expectThrowsCode(DB::ErrorCodes::FILE_DOESNT_EXIST, [&] { std::rethrow_exception(bad_error); });
    EXPECT_EQ(backend->putTotal(), put_before + 1) << "the survivor's own transaction still costs one create";
    EXPECT_FALSE(store->resolveRef(ns, "good").has_value()) << "the innocent co-batched drop must land";
}

/// ===================================================================================
/// Append lane: wedge semantics (spec §Writer-Side Linearization)
/// ===================================================================================

TEST(CASRefWriterAppendLane, WedgedLaneBlocksSameTableWhileOtherTableProceeds)
{
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = kSingleAttemptDeadlineMs;
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend, budget);
    const Layout & layout = store->layout();
    const RootNamespace ns_a{"srv1/wedge_a"};
    const RootNamespace ns_b{"srv1/wedge_b"};
    publishEmptyPart(store, ns_a, "x");
    publishEmptyPart(store, ns_a, "x_second");
    publishEmptyPart(store, ns_b, "y");

    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns_a)) + "_log/";
    backend->fault_count = 1;

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns_a, "x"); });
    EXPECT_TRUE(store->refLaneWedgedForTest(ns_a));

    /// A different table proceeds normally while ns_a stays wedged.
    EXPECT_NO_THROW(store->dropRef(ns_b, "y"));
    EXPECT_FALSE(store->resolveRef(ns_b, "y").has_value());

    /// Retrying ns_a does not allocate a later id -- it re-attempts the SAME one. The wedge's key was
    /// never actually written (the fault never wrote through), and under the every-attempt rule the
    /// retry is a conditional CREATE of the same bytes rather than a bare read: it lands, which makes
    /// the wedged transaction durable and adopts it. That is the point of the rule -- a bare read could
    /// only ever report "absent", which is not a rejection, and the lane would stay wedged forever over
    /// a key nothing had written. See `gtest_cas_ref_wedge_every_attempt.cpp` for the full rule.
    EXPECT_NO_THROW(store->dropRef(ns_a, "x_second"));
    EXPECT_FALSE(store->refLaneWedgedForTest(ns_a)) << "the retry's own create resolves the lane";
    EXPECT_FALSE(store->resolveRef(ns_a, "x").has_value()) << "the wedged drop was adopted on resolution";
}

TEST(CASRefWriterAppendLane, WedgedAppendObservedDurableAppliesBeforeNextId)
{
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = kSingleAttemptDeadlineMs;
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend, budget);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/wedge_unwedge"};
    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");

    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->fault_count = 1;

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));
    ASSERT_TRUE(store->resolveRef(ns, "x").has_value()) << "not yet applied while wedged";

    /// The earlier request eventually lands server-side; the caller just never saw the ack.
    backend->materializePendingDelayedWrite();

    /// A later mutation on the SAME table first resolves the wedge (applying "drop x" to cache) BEFORE
    /// allocating its own next id (which drops "y").
    EXPECT_NO_THROW(store->dropRef(ns, "y"));

    EXPECT_FALSE(store->refLaneWedgedForTest(ns));
    EXPECT_FALSE(store->resolveRef(ns, "x").has_value()) << "the wedged drop was applied on resolution";
    EXPECT_FALSE(store->resolveRef(ns, "y").has_value()) << "the next mutation committed normally afterward";
}

/// Wedge tail-counter accounting across the three states (xhigh review, item F): an UNRESOLVED wedge
/// applied nothing, so it must NOT bump the applied-above-snapshot tail counters; a RESOLVED wedge is a
/// commit like any other and MUST bump them (exactly once) alongside the ordinary commit that resolves
/// it; and the resolution must fold its applied overlay in place (no residual committed overlay). Under
/// the default 256-log / 1 MiB snapshot thresholds this handful of txns never triggers a publish, so the
/// tail counter is a stable running count.
TEST(CASRefWriterAppendLane, WedgeResolutionJoinsTailCountersAndFoldsOverlay)
{
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = kSingleAttemptDeadlineMs;
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend, budget);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/wedge_tail"};
    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");

    const size_t tail_after_setup = store->tailSinceSnapshotCountForTest(ns);

    /// Wedge the lane: the single-attempt budget turns the ambiguous log PUT into an Unresolved outcome.
    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->fault_count = 1;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));
    ASSERT_TRUE(store->resolveRef(ns, "x").has_value()) << "not applied while merely wedged";
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), tail_after_setup)
        << "an UNRESOLVED wedge applied nothing and must not join the tail counters";

    /// The wedged PUT actually landed server-side; a later mutation resolves the wedge (applying drop x)
    /// before committing its own drop y.
    backend->materializePendingDelayedWrite();
    EXPECT_NO_THROW(store->dropRef(ns, "y"));

    EXPECT_FALSE(store->refLaneWedgedForTest(ns));
    EXPECT_FALSE(store->resolveRef(ns, "x").has_value()) << "the wedged drop was applied on resolution";
    EXPECT_FALSE(store->resolveRef(ns, "y").has_value());
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), tail_after_setup + 2)
        << "the RESOLVED wedge (drop x) and the ordinary commit (drop y) must each bump the tail once";
    EXPECT_EQ(store->committedOverlayEntriesForTest(ns), 0u)
        << "both the wedge resolution and the ordinary commit fold their overlay in place at install";
}

/// B3: `Pool::wedgedRefLaneCount()` (the accessor `CasGcScheduler::gcHealth()` reads for
/// `system.cas_mounts.wedged_namespace_count`) must count EXACTLY the tables with a live
/// wedge -- neither a cached-but-healthy table nor an unrelated table's own successful mutation may move
/// it, and it must track the wedge's full lifecycle (0 -> 1 -> 0), not just a one-shot snapshot.
TEST(CASRefWriterAppendLane, WedgedRefLaneCountTracksExactlyTheWedgedTableThroughItsLifecycle)
{
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = kSingleAttemptDeadlineMs;
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend, budget);
    const Layout & layout = store->layout();
    const RootNamespace ns_a{"srv1/wedge_count_a"};
    const RootNamespace ns_b{"srv1/wedge_count_b"};
    publishEmptyPart(store, ns_a, "x");
    publishEmptyPart(store, ns_a, "y");
    publishEmptyPart(store, ns_b, "p");
    ASSERT_EQ(store->wedgedRefLaneCount(), 0u) << "both tables cached and healthy before the fault";

    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns_a)) + "_log/";
    backend->fault_count = 1;

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns_a, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns_a));
    EXPECT_EQ(store->wedgedRefLaneCount(), 1u);

    /// ns_b's own mutation succeeds and must not be swept into the count.
    EXPECT_NO_THROW(store->dropRef(ns_b, "p"));
    EXPECT_EQ(store->wedgedRefLaneCount(), 1u) << "an unrelated table's successful mutation must not move the count";

    /// The earlier request eventually lands server-side; resolving ns_a's wedge on its next mutation
    /// drops the count back to zero.
    backend->materializePendingDelayedWrite();
    EXPECT_NO_THROW(store->dropRef(ns_a, "y"));
    EXPECT_FALSE(store->refLaneWedgedForTest(ns_a));
    EXPECT_EQ(store->wedgedRefLaneCount(), 0u);
}

/// ===================================================================================
/// I1: a CORRUPTED_DATA from the retry controller (resolve-before-reissue observed a DIFFERENT object at
/// the exact key) must be surfaced LOUDLY to the caller and never hang the table's append queue. The
/// unfixed code let the throw propagate through the leader loop with `leader_active` still true, so every
/// queued and future caller for that table blocked forever in `cv.wait`.
/// ===================================================================================

/// Append-site CORRUPTED_DATA: the offending caller gets the error, the lane is NOT wedged (a proven
/// different-object conflict is conclusive, not uncertain), and no caller HANGS -- the queue's leader
/// bookkeeping is restored, proven by a bounded wait on both a same-table and an independent-table
/// append.
///
/// The reaction is now the mount's, not the table's [review I5]: a foreign object at a key that
/// mount-lease exclusivity says is exclusively ours contradicts the exclusivity itself, so the append
/// site routes through `reportImpossibleInterference` exactly as the wedge-resolve site does -- fence
/// closed, remount scheduled. Before this task it failed closed and stayed closed, blocking the table
/// until somebody remounted by hand. So there are two separate scopes to keep straight, and this test
/// pins both:
///   the FENCE is mount-wide -- while it is closed EVERY lane is refused, including untouched ones;
///   the DAMAGE is per-namespace -- a real remount replaces both immutable runtimes, then recovery of
///   the damaged stream still refuses while the unrelated table commits normally.
TEST(CASRefWriterAppendLane, I1AppendCorruptionSurfacesAndFencesTheMountForRemount)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/i1_append"};
    const RootNamespace other{"srv1/i1_other"};
    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, other, "z");

    /// The next `_log` PUT for `ns` has a foreign different object land at its key; resolve-before-reissue
    /// then observes the mismatch and raises CORRUPTED_DATA.
    backend->corrupt_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->corrupt_count = 1;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "x"); });
    EXPECT_FALSE(store->refLaneWedgedForTest(ns)) << "a proven different-object conflict must not wedge the lane";
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted);
    EXPECT_FALSE(store->mayMutate()) << "the impossible-interference reaction must fence this mount closed";
    EXPECT_EQ(store->scheduleRemountCallCountForTest(), 1u)
        << "the append site must schedule the remount that re-derives this table from the durable log";

    /// Mount-wide while fenced -- and, crucially, PROMPT: a real cv hang would time out this wait, which
    /// is the regression this test was written for.
    auto fenced = std::async(std::launch::async, [&] { store->dropRef(other, "z"); });
    ASSERT_EQ(fenced.wait_for(std::chrono::seconds(10)), std::future_status::ready)
        << "the independent-table append hung -- the queue's leader bookkeeping was not restored";
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { fenced.get(); });

    /// Drive the scheduled production recovery boundary. A direct fence re-arm is intentionally NOT a
    /// substitute anymore: immutable runtimes retain the generation that admitted them and cannot be
    /// rebound to the new one.
    const String mount_key = layout.mountKey("test");
    const auto mount = backend->get(mount_key);
    ASSERT_TRUE(mount);
    MountLease fenced_mount = decodeMountLease(mount->bytes);
    fenced_mount.gc_fenced = true;
    fenced_mount.seq += 1;
    ASSERT_EQ(backend->putOverwrite(mount_key, encodeMountLease(fenced_mount), mount->token).outcome,
        PutOutcome::Done);
    ASSERT_TRUE(store->tryRemountOnce());

    auto same = std::async(std::launch::async, [&] { store->dropRef(ns, "x"); });
    ASSERT_EQ(same.wait_for(std::chrono::seconds(10)), std::future_status::ready)
        << "the same-table append hung -- the queue's leader bookkeeping was not restored";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { same.get(); });

    auto indep = std::async(std::launch::async, [&] { store->dropRef(other, "z"); });
    ASSERT_EQ(indep.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    indep.get();
    EXPECT_FALSE(store->resolveRef(other, "z").has_value())
        << "an unrelated table's stream is independent and must be entirely unaffected by the damage";
}

/// Wedge-resolve-site foreign interference: a wedged lane whose key a foreign writer overwrote must
/// surface the anomaly to the triggering caller and fault the lane, without hanging.
/// rev.6 Task 11 (spec §anomaly-policy): under the mount-lease exclusivity model this is no longer a
/// possible protocol outcome (the wedged key is exclusively ours) -- it routes through
/// `reportImpossibleInterference`, which fences the mount and schedules a remount.
///
/// It surfaces as `CORRUPTED_DATA`. It was `LOGICAL_ERROR` between rev.6 and the every-attempt rule,
/// and that had a cost this test used to carry: `LOGICAL_ERROR` ABORTS the process in debug/sanitizer
/// builds, so the whole test had to be release-only with a death-test twin standing in elsewhere.
/// Storage-controlled input must never be able to abort the server, so the arm now reports the
/// occupant for what it is -- corruption -- and one test covers every build.
TEST(CASRefWriterAppendLane, I1WedgeResolveCorruptionSurfacesAndFaultsLane)
{
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = kSingleAttemptDeadlineMs;
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend, budget);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/i1_wedge"};
    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");

    /// Wedge the lane with an ambiguous PUT that never landed.
    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->fault_count = 1;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));

    /// A foreign writer lands a DIFFERENT object at the exact wedged key; the next append's wedge resolve
    /// observes the mismatch and must raise `CORRUPTED_DATA` to that caller while faulting the lane.
    const String wedged_key = store->wedgedKeyForTest(ns);
    ASSERT_FALSE(wedged_key.empty());
    ASSERT_EQ(backend->putIfAbsent(wedged_key, "a-different-object").outcome, PutOutcome::Done);

    auto fut = std::async(std::launch::async, [&]
    {
        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "y"); });
    });
    ASSERT_EQ(fut.wait_for(std::chrono::seconds(10)), std::future_status::ready)
        << "the wedge-resolve anomaly hung the queue instead of surfacing to the caller";
    fut.get();
    EXPECT_FALSE(store->refLaneWedgedForTest(ns));
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted)
        << "foreign interference is a terminal lane verdict";

    /// The queue's leader bookkeeping was restored, so a SUBSEQUENT same-table caller does not hang: it
    /// observes the terminal state and returns promptly (a real cv hang would time out this bounded
    /// wait). This is the leg the unfixed code left blocked forever.
    auto fut2 = std::async(std::launch::async, [&]
    {
        try
        {
            store->dropRef(ns, "y");
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// The anomaly is expected here; this future only verifies that the caller does not hang.
        }
    });
    ASSERT_EQ(fut2.wait_for(std::chrono::seconds(10)), std::future_status::ready)
        << "a later same-table append hung -- the leader bookkeeping was not restored after the anomaly";
    fut2.get();
}

/// ===================================================================================
/// rev.6 Task 11: wedge hard contract + anomaly policy (spec §anomaly-policy)
/// ===================================================================================

/// Foreign bytes at a wedge key (see `I1WedgeResolveCorruptionSurfacesAndFaultsLane` above for the
/// hang-freedom coverage) must ALSO trip the local write fence closed and audit a `ForeignInterference`
/// event -- the full anomaly-policy reaction, not just the throw. It runs in every build now that the
/// arm reports `CORRUPTED_DATA` instead of the process-aborting `LOGICAL_ERROR`; the death twin that
/// used to stand in for debug/sanitizer builds went with it.
TEST(CASAnomalyPolicy, ForeignBytesAtWedgeKeyTripFenceAndRemount)
{
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = kSingleAttemptDeadlineMs;
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<RefWriterTestBackend>();
    SynchronizedEventLog seen;   /// declared BEFORE the Pool so it outlives the background syncer's emits (ASan 2026-07-09)
    auto store = openPool(backend, budget);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/anomaly_wedge"};
    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");

    store->setEventSink([&](const CasEvent & e) { seen.add(e); });

    /// Wedge the lane with an ambiguous PUT that never landed.
    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->fault_count = 1;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));
    ASSERT_TRUE(store->mayMutate()) << "the fence must not be tripped yet -- only an ordinary Unresolved wedge so far";
    ASSERT_EQ(store->scheduleRemountCallCountForTest(), 0u) << "no remount must have been scheduled yet by the ordinary wedge alone";

    /// Out-of-band, a foreign writer lands DIFFERENT bytes at the exact wedged key.
    const String wedged_key = store->wedgedKeyForTest(ns);
    ASSERT_FALSE(wedged_key.empty());
    ASSERT_EQ(backend->putIfAbsent(wedged_key, "a-different-object").outcome, PutOutcome::Done);

    /// The next append's wedge resolve observes the mismatch: CORRUPTED_DATA, the fence trips closed,
    /// and a ForeignInterference event is audited.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "y"); });

    EXPECT_FALSE(store->refLaneWedgedForTest(ns));
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted)
        << "foreign interference must fault the lane";
    EXPECT_FALSE(store->mayMutate()) << "the local write fence must trip closed on the anomaly";
    /// Positively pins that `reportImpossibleInterference` called `scheduleRemount` (not just
    /// `tripMountLost`, which alone already accounts for `mayMutate() == false` above). Counted at
    /// `scheduleRemount`'s own entry regardless of `background_watermark` -- see that accessor's
    /// comment for why this test deliberately does NOT enable `background_watermark` to observe a real
    /// spawned thread: doing so was tried and makes the store's self-remount attempt race its own
    /// still-live keeper for 30+ seconds per call (confirmed while building this test), which is not
    /// something a fast unit test should be driving.
    EXPECT_EQ(store->scheduleRemountCallCountForTest(), 1u)
        << "reportImpossibleInterference must have called scheduleRemount exactly once";

    const std::vector<CasEvent> observed = seen.snapshot();
    const auto has_event = std::any_of(observed.begin(), observed.end(),
        [](const CasEvent & e) { return e.type == CasEventType::ForeignInterference; });
    EXPECT_TRUE(has_event) << "a ForeignInterference CasEvent must be audited";
}

/// An impossible non-`Ready` state at new-id allocation must refuse before minting an id, fault the
/// lane, and trigger the anomaly policy. The synthetic wedge is injected after the top-of-flush
/// resolver gate, so it represents an internal lifecycle contradiction rather than a normal wedge.
TEST(CASAnomalyPolicy, NonReadyAtNewIdAllocationFaultsAndFailsClosed)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    SynchronizedEventLog seen;   /// declared BEFORE the Pool so it outlives the background syncer's emits (ASan 2026-07-09)
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/wedge_contract"};
    publishEmptyPart(store, ns, "x");

    store->setEventSink([&](const CasEvent & e) { seen.add(e); });

    store->setRefPreCarveHookForTest([&]
    {
        store->forceWedgeForTest(ns, /*writer_epoch*/ 1, /*ref_sequence*/ 1, "bogus/_log/key", "bogus-bytes");
    });

    /// Ground truth: no NEW `_log` object may appear -- the guard must refuse BEFORE any id is minted
    /// or PUT attempted.
    auto countLogObjects = [&]
    {
        size_t n = 0;
        String cursor;
        for (;;)
        {
            const ListPage page = backend->list(layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)), cursor, 1000);
            for (const ListedKey & lk : page.keys)
            {
                const auto parsed = layout.parseRefObjectKey(lk.key);
                if (parsed && parsed->life_id == DB::Cas::tests::fixture::fixtureLife(ns).incarnation && parsed->kind == RefObjectKind::Log)
                    ++n;
            }
            if (page.next_cursor.empty())
                break;
            cursor = page.next_cursor;
        }
        return n;
    };
    const size_t log_objects_before = countLogObjects();

    ASSERT_TRUE(store->mayMutate()) << "the fence must be armed BEFORE the wedge-contract violation, or the guard would trivially pass for the wrong reason";
    ASSERT_EQ(store->scheduleRemountCallCountForTest(), 0u) << "no remount must have been scheduled yet";

    /// BACKLOG `{#lane-terminal-reported-as-retryable}`: `Faulted` is a TERMINAL lane state, the same
    /// one every OTHER `Faulted` arm in `commitRefChunk` reports as `CORRUPTED_DATA` -- reporting it as
    /// `NETWORK_ERROR`/retry-later would tell the caller a state the lane can never leave on its own is
    /// transient.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->dropRef(ns, "x"); });

    EXPECT_EQ(countLogObjects(), log_objects_before) << "the release guard must refuse before allocating/PUTting a new _log object";
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Faulted)
        << "the invariant violation must have one explicit terminal state";
    EXPECT_FALSE(store->mayMutate()) << "the local write fence must trip closed on the wedge-contract violation";
    /// See the sibling test's comment on why this checks the call-count seam (never `background_watermark`
    /// + a real thread -- that combination makes the store's self-remount race its own still-live keeper).
    EXPECT_EQ(store->scheduleRemountCallCountForTest(), 1u)
        << "reportImpossibleInterference must have called scheduleRemount exactly once";

    const std::vector<CasEvent> observed = seen.snapshot();
    const auto has_event = std::any_of(observed.begin(), observed.end(),
        [](const CasEvent & e) { return e.type == CasEventType::ForeignInterference; });
    EXPECT_TRUE(has_event) << "a ForeignInterference CasEvent must be audited";
}

/// I3: a conditional write whose attempt classified Committed but whose FINAL post-write fence check
/// failed (the mount fence was lost after the write may have landed) is counted separately, not folded
/// into the generic Unresolved classifier (spec §Late Predecessor PUT best-effort diagnostic).
TEST(CASRequestControllerFenceLoss, I3PostWriteFenceLossIsCounted)
{
    using ProfileEvents::global_counters;
    auto backend = std::make_shared<CountingBackend>();
    CasRequestBudget budget;
    budget.max_attempts = 3;
    CasRequestController ctrl(backend, budget, [] { return static_cast<uint64_t>(0); });   // fixed clock

    /// `fence_ok` holds for the pre-attempt check, then is lost by the post-write check.
    int calls = 0;
    auto fence_ok = [&calls] { return ++calls <= 1; };

    const auto before = global_counters[ProfileEvents::CASConditionalWriteFenceLostPostWrite].load();
    const CasWriteOutcome outcome = ctrl.putIfAbsentControlled("k", "v", fence_ok);
    EXPECT_EQ(outcome, CasWriteOutcome::Unresolved) << "a post-write fence loss must never be reported as Committed";
    EXPECT_EQ(global_counters[ProfileEvents::CASConditionalWriteFenceLostPostWrite].load(), before + 1);
}

/// Task B (stageManifest rides the controller): a Committed return surfaces the committed
/// incarnation's token — from the attempt's own PutResult, and equally from a resolve that proves an
/// earlier ambiguous attempt landed — so audit emitters (`PartWriteTxn::stageManifest`'s `ManifestPut`
/// event) keep their token without a follow-up HEAD.
TEST(CASRequestController, CommittedSurfacesTokenFromPutAndFromResolve)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequestController ctrl(backend, CasRequestBudget{}, [] { return static_cast<uint64_t>(0); });
    const auto fence_ok = [] { return true; };

    Token direct_token;
    ASSERT_EQ(ctrl.putIfAbsentControlled("k1", "v1", fence_ok, &direct_token), CasWriteOutcome::Committed);
    EXPECT_EQ(direct_token, backend->head("k1").token) << "the direct-commit token is the PutResult's";

    /// k2 already holds the IDENTICAL bytes (an earlier ambiguous attempt that landed): the attempt's
    /// PreconditionFailed collapses to Unresolved and the resolve GET proves Committed — the token must
    /// be the observed incarnation's, and no second incarnation is ever created.
    const Token pre_existing = backend->putIfAbsent("k2", "v2").token;
    Token resolved_token;
    ASSERT_EQ(ctrl.putIfAbsentControlled("k2", "v2", fence_ok, &resolved_token), CasWriteOutcome::Committed);
    EXPECT_EQ(resolved_token, pre_existing) << "the resolve-commit token is the observed incarnation's";
}

/// ===================================================================================
/// Task 13: whole-table ref-cache eviction (spec §Byte, Memory, And CPU Budget)
/// ===================================================================================

/// A tiny cache budget forces WHOLE-TABLE eviction: publishing to several tables in turn keeps only the
/// most-recently-touched one resident, and an evicted table re-recovers its exact committed state on the
/// next touch (spec §Startup And Recovery: "Evicting the table drops the entire object; the next access
/// repeats recovery").
TEST(CASRefTableCacheEviction, WholeTableEvictionUnderBudgetReRecovers)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPoolWithConfig(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test", .ref_table_cache_bytes = 1});
    const RootNamespace ns_a{"srv1/evict_a"};
    const RootNamespace ns_b{"srv1/evict_b"};
    const RootNamespace ns_c{"srv1/evict_c"};

    publishEmptyPart(store, ns_a, "x");
    publishEmptyPart(store, ns_b, "y");
    publishEmptyPart(store, ns_c, "z");

    /// A 1-byte budget is below one table's weight, so each new table evicts the prior idle ones: only
    /// the last-touched table stays resident (the just-recovered table is never evicted).
    EXPECT_EQ(store->refTablesCachedCountForTest(), 1u);
    EXPECT_TRUE(store->refTableCachedForTest(ns_c));
    EXPECT_FALSE(store->refTableCachedForTest(ns_a));
    EXPECT_FALSE(store->refTableCachedForTest(ns_b));

    /// The evicted table re-recovers its exact committed state on next touch.
    const auto resolved = store->resolveRef(ns_a, "x");
    ASSERT_TRUE(resolved.has_value()) << "an evicted table must re-recover its committed ref";
    /// That touch, in turn, evicted the previously-resident table under the same budget.
    EXPECT_TRUE(store->refTableCachedForTest(ns_a));
    EXPECT_FALSE(store->refTableCachedForTest(ns_c));
}

/// A zero budget disables eviction entirely: every touched table stays resident.
TEST(CASRefTableCacheEviction, ZeroBudgetDisablesEviction)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPoolWithConfig(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test", .ref_table_cache_bytes = 0});
    for (const String & n : {String("srv1/keep_a"), String("srv1/keep_b"), String("srv1/keep_c")})
        publishEmptyPart(store, RootNamespace{n}, "x");
    EXPECT_EQ(store->refTablesCachedCountForTest(), 3u);
}

/// A table with a WEDGED append lane is never evicted, even when idle and over budget: its uncertain
/// in-flight PUT is not reconstructable from the durable objects (spec §Writer-Side Linearization), so
/// re-recovery must not be allowed to drop and re-materialize it (which could re-allocate an id).
TEST(CASRefTableCacheEviction, WedgedTableIsNeverEvicted)
{
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = kSingleAttemptDeadlineMs;
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPoolWithConfig(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .cas_request_budget = budget, .ref_table_cache_bytes = 1});
    const Layout & layout = store->layout();
    const RootNamespace ns_w{"srv1/wedged"};
    publishEmptyPart(store, ns_w, "x");

    /// Wedge ns_w's append lane with one ambiguous (Unresolved) PUT that exhausts the single-attempt budget.
    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns_w)) + "_log/";
    backend->fault_count = 1;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns_w, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns_w));

    /// Pressure the cache with other tables. ns_w is idle and over the 1-byte budget, but its wedged lane
    /// makes it non-evictable, so its wedge state survives (a fresh runtime would report no wedge).
    publishEmptyPart(store, RootNamespace{"srv1/other_a"}, "y");
    publishEmptyPart(store, RootNamespace{"srv1/other_b"}, "z");

    EXPECT_TRUE(store->refTableCachedForTest(ns_w)) << "a wedged table must never be evicted";
    EXPECT_TRUE(store->refLaneWedgedForTest(ns_w)) << "and its wedge state survives";
}

/// ===================================================================================
/// Task 11: snapshot publication (spec §writer-snapshot-publication)
/// ===================================================================================

/// The count threshold fires a background publish covering the whole retained tail; its bytes must
/// equal an INDEPENDENT oracle's replay of the same logs through the published id (cache-replay
/// equivalence), and the retained tail must be fully pruned afterward (spec: "Publication is
/// background and never blocks an append").
TEST(CASRefWriterSnapshotPublish, ThresholdTriggerPublishesCacheReplayEquivalentBytes)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    PoolConfig config;
    config.snapshot_log_count_threshold = 3;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    auto store = openPoolWithConfig(backend, config);
    const RootNamespace ns{"srv1/threshold_publish"};

    publishEmptyPart(store, ns, "a");   /// tail: 2 (birth+add, promote)
    publishEmptyPart(store, ns, "b");   /// tail: 3, then 4 (4 > 3 -> dispatches ONE background publish)

    store->waitForSnapshotPublishSettleForTest(ns);

    const auto snap_id = listGreatestSnapshotIdForTest(*backend, layout, ns);
    ASSERT_TRUE(snap_id.has_value()) << "the threshold trigger must have published a snapshot";
    EXPECT_TRUE(store->newestPublishedSnapshotIdForTest(ns) == snap_id);
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), 0u) << "a snapshot covering everything prunes the whole tail";

    const auto got = backend->get(layout.refSnapshotKey(DB::Cas::tests::fixture::fixtureLife(ns), *snap_id));
    ASSERT_TRUE(got.has_value());

    /// The independent oracle: replay every `_log/` object directly, ignoring the snapshot entirely.
    const RefTableState oracle = independentFullReplayForTest(*backend, layout, ns, snap_id);
    const String expected_bytes = encodeRefTableSnapshot(snapshotOf(oracle, ns.string()));
    EXPECT_EQ(openObject(FormatId::RefSnapshot, got->bytes), expected_bytes)
        << "published snapshot bytes must equal replay(logs through X)";
}

/// A publisher owns the runtime it captured, not the logical name. If that exact life is deleted and
/// the name is reborn while snapshot bytes are still only local, the old attempt must become inert: in
/// particular it must not recreate the predecessor's `_snap` or `_ckpt` after the GC retired them.
TEST(CASRefWriterSnapshotPublish, CapturedPredecessorCannotPublishAfterSameNameRebirth)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    PoolConfig config;
    config.gc_fold_threshold = 1;
    config.ref_table_cache_bytes = 0;
    config.snapshot_log_count_threshold = 1ULL << 40;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    auto store = openPoolWithConfig(backend, config);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/publisher-predecessor-rebirth"};

    publishWithProductionBirth(store, ns, "predecessor");
    const CatalogEntry predecessor = catalogEntryOrThrow(*backend, layout, ns);
    const NamespaceLifeId predecessor_life
        = NamespaceLifeId::fromCatalogEntry(ns, predecessor.incarnation);
    const auto predecessor_snapshot
        = listGreatestLogIdForLifeForTest(*backend, layout, predecessor_life);
    ASSERT_TRUE(predecessor_snapshot);

    Gc gc(store, UInt128{105});
    ASSERT_FALSE(runRegularRoundReclaiming(gc).deferred);

    std::mutex mutex;
    std::condition_variable cv;
    bool captured = false;
    bool release = false;
    store->setSnapshotAfterCaptureHookForTest([&]
    {
        std::unique_lock lock(mutex);
        captured = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release; });
    });

    auto publisher = std::async(std::launch::async, [&]() -> bool
    {
        return store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns);
    });
    SCOPE_EXIT({
        {
            std::lock_guard lock(mutex);
            release = true;
        }
        cv.notify_all();
    });
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), [&] { return captured; }));
    }

    EXPECT_NO_THROW(store->dropNamespace(ns));
    EXPECT_FALSE(runRegularRoundReclaiming(gc).deferred);
    EXPECT_TRUE(runRegularRoundReclaiming(gc).deferred);
    const RefCatalog after_removal = CasRefCatalog::read(*backend, layout).catalog;
    EXPECT_TRUE(std::none_of(after_removal.entries.begin(), after_removal.entries.end(),
        [&](const CatalogEntry & entry) { return entry.ns == ns; }));

    publishWithProductionBirth(store, ns, "successor");
    const uint64_t successor_runtime = store->refTableRuntimeIdentityForTest(ns);
    const CatalogEntry successor = catalogEntryOrThrow(*backend, layout, ns);
    EXPECT_NE(successor.incarnation, predecessor.incarnation);
    const auto predecessor_ckpt_before_resume = backend->get(layout.refCkptKey(predecessor_life));
    ASSERT_TRUE(predecessor_ckpt_before_resume)
        << "the removal protocol leaves this checkpoint as janitor-owned predecessor debris";

    {
        std::lock_guard lock(mutex);
        release = true;
    }
    cv.notify_all();
    ASSERT_EQ(publisher.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    EXPECT_FALSE(publisher.get());
    store->setSnapshotAfterCaptureHookForTest(nullptr);

    EXPECT_FALSE(backend->get(layout.refSnapshotKey(predecessor_life, *predecessor_snapshot)))
        << "a stale publisher recreated the retired predecessor snapshot";
    const auto predecessor_ckpt_after_resume = backend->get(layout.refCkptKey(predecessor_life));
    ASSERT_TRUE(predecessor_ckpt_after_resume);
    EXPECT_EQ(predecessor_ckpt_after_resume->token, predecessor_ckpt_before_resume->token)
        << "a stale publisher replaced the retired predecessor checkpoint";
    EXPECT_EQ(predecessor_ckpt_after_resume->bytes, predecessor_ckpt_before_resume->bytes)
        << "a stale publisher changed the retired predecessor checkpoint";
    EXPECT_EQ(store->refTableRuntimeIdentityForTest(ns), successor_runtime);
    EXPECT_TRUE(store->resolveRef(ns, "successor"));
}

/// The runtime admission check belongs inside every retrying `_ckpt` CAS attempt, not merely before
/// calling the checkpoint helper. Retirement in the body-PUT/checkpoint gap leaves the already-written
/// snapshot as harmless debris but must not advance or recreate the predecessor checkpoint.
TEST(CASRefWriterSnapshotPublish, RetiredPredecessorCannotAdvanceCkptAfterSnapshotPut)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    PoolConfig config;
    config.gc_fold_threshold = 1;
    config.ref_table_cache_bytes = 0;
    config.snapshot_log_count_threshold = 1ULL << 40;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    auto store = openPoolWithConfig(backend, config);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/publisher-predecessor-ckpt-race"};

    publishWithProductionBirth(store, ns, "predecessor");
    const CatalogEntry predecessor = catalogEntryOrThrow(*backend, layout, ns);
    const NamespaceLifeId predecessor_life
        = NamespaceLifeId::fromCatalogEntry(ns, predecessor.incarnation);
    const auto candidate_id = listGreatestLogIdForLifeForTest(*backend, layout, predecessor_life);
    ASSERT_TRUE(candidate_id);

    Gc gc(store, UInt128{106});
    ASSERT_FALSE(runRegularRoundReclaiming(gc).deferred);

    std::mutex mutex;
    std::condition_variable cv;
    bool before_ckpt_cas = false;
    bool release = false;
    store->setSnapshotBeforeCkptCasHookForTest([&]
    {
        std::unique_lock lock(mutex);
        before_ckpt_cas = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release; });
    });

    auto publisher = std::async(std::launch::async, [&]() -> bool
    {
        return store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns);
    });
    SCOPE_EXIT({
        {
            std::lock_guard lock(mutex);
            release = true;
        }
        cv.notify_all();
    });
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), [&] { return before_ckpt_cas; }));
    }
    EXPECT_TRUE(backend->get(layout.refSnapshotKey(predecessor_life, *candidate_id)))
        << "the hook must run after the snapshot body PUT and immediately before `_ckpt` admission";

    EXPECT_NO_THROW(store->dropNamespace(ns));
    EXPECT_FALSE(runRegularRoundReclaiming(gc).deferred);
    EXPECT_TRUE(runRegularRoundReclaiming(gc).deferred);
    const RefCatalog after_removal = CasRefCatalog::read(*backend, layout).catalog;
    EXPECT_TRUE(std::none_of(after_removal.entries.begin(), after_removal.entries.end(),
        [&](const CatalogEntry & entry) { return entry.ns == ns; }));

    publishWithProductionBirth(store, ns, "successor");
    const uint64_t successor_runtime = store->refTableRuntimeIdentityForTest(ns);
    const CatalogEntry successor = catalogEntryOrThrow(*backend, layout, ns);
    EXPECT_NE(successor.incarnation, predecessor.incarnation);
    const auto predecessor_ckpt_before_resume = backend->get(layout.refCkptKey(predecessor_life));
    ASSERT_TRUE(predecessor_ckpt_before_resume);

    {
        std::lock_guard lock(mutex);
        release = true;
    }
    cv.notify_all();
    ASSERT_EQ(publisher.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    EXPECT_FALSE(publisher.get());
    store->setSnapshotBeforeCkptCasHookForTest(nullptr);

    const auto predecessor_ckpt_after_resume = backend->get(layout.refCkptKey(predecessor_life));
    ASSERT_TRUE(predecessor_ckpt_after_resume);
    EXPECT_EQ(predecessor_ckpt_after_resume->token, predecessor_ckpt_before_resume->token);
    EXPECT_EQ(predecessor_ckpt_after_resume->bytes, predecessor_ckpt_before_resume->bytes);
    EXPECT_EQ(store->refTableRuntimeIdentityForTest(ns), successor_runtime);
    EXPECT_TRUE(store->resolveRef(ns, "successor"));
}

/// A read that already owns the predecessor runtime does not consult the name slot again after a
/// same-name successor is published. Because removal applies the terminal state before retirement, a
/// reader paused immediately before its state lock resumes with `NotFound`, never successor data.
TEST(CASRefWriterRuntimeIdentity, CapturedReaderCannotRetargetSameNameSuccessor)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    PoolConfig config;
    config.gc_fold_threshold = 1;
    config.ref_table_cache_bytes = 0;
    config.snapshot_log_count_threshold = 1ULL << 40;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    auto store = openPoolWithConfig(backend, config);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/captured-reader-rebirth"};

    publishWithProductionBirth(store, ns, "shared");
    ASSERT_TRUE(store->resolveRef(ns, "shared"));
    const CatalogEntry predecessor = catalogEntryOrThrow(*backend, layout, ns);
    Gc gc(store, UInt128{107});
    ASSERT_FALSE(runRegularRoundReclaiming(gc).deferred);

    std::mutex mutex;
    std::condition_variable cv;
    bool captured = false;
    bool release = false;
    store->setReadBeforeStateLockHookForTest([&]
    {
        std::unique_lock lock(mutex);
        if (captured)
            return;
        captured = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release; });
    });
    auto reader = std::async(std::launch::async, [&]
    {
        return store->resolveRef(ns, "shared");
    });
    SCOPE_EXIT({
        {
            std::lock_guard lock(mutex);
            release = true;
        }
        cv.notify_all();
    });
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), [&] { return captured; }));
    }

    EXPECT_NO_THROW(store->dropNamespace(ns));
    EXPECT_FALSE(runRegularRoundReclaiming(gc).deferred);
    EXPECT_TRUE(runRegularRoundReclaiming(gc).deferred);
    publishWithProductionBirth(store, ns, "shared");
    const CatalogEntry successor = catalogEntryOrThrow(*backend, layout, ns);
    EXPECT_NE(successor.incarnation, predecessor.incarnation);
    const uint64_t successor_runtime = store->refTableRuntimeIdentityForTest(ns);
    const auto successor_ref = store->resolveRef(ns, "shared");
    ASSERT_TRUE(successor_ref);

    {
        std::lock_guard lock(mutex);
        release = true;
    }
    cv.notify_all();
    ASSERT_EQ(reader.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    EXPECT_FALSE(reader.get()) << "the captured predecessor reader retargeted through the name slot";
    store->setReadBeforeStateLockHookForTest(nullptr);

    EXPECT_EQ(store->refTableRuntimeIdentityForTest(ns), successor_runtime);
    const auto successor_after = store->resolveRef(ns, "shared");
    ASSERT_TRUE(successor_after);
    EXPECT_EQ(successor_after->manifest_id.ref, successor_ref->manifest_id.ref);
}

/// Ordinary append admission also owns the runtime it captured. If removal and rebirth complete before
/// enqueue, the predecessor's closed lane returns retry-later; it cannot enqueue into or mutate the
/// successor even when the successor deliberately reuses the same logical ref name.
TEST(CASRefWriterRuntimeIdentity, CapturedAppendCannotEnqueueIntoSameNameSuccessor)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    PoolConfig config;
    config.gc_fold_threshold = 1;
    config.ref_table_cache_bytes = 0;
    config.snapshot_log_count_threshold = 1ULL << 40;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    auto store = openPoolWithConfig(backend, config);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/captured-append-rebirth"};

    publishWithProductionBirth(store, ns, "shared");
    const CatalogEntry predecessor = catalogEntryOrThrow(*backend, layout, ns);
    Gc gc(store, UInt128{108});
    ASSERT_FALSE(runRegularRoundReclaiming(gc).deferred);

    std::mutex mutex;
    std::condition_variable cv;
    bool captured = false;
    bool release = false;
    store->setAppendAfterRuntimeCaptureHookForTest([&]
    {
        std::unique_lock lock(mutex);
        if (captured)
            return;
        captured = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release; });
    });
    auto append = std::async(std::launch::async, [&]
    {
        store->dropRef(ns, "shared");
    });
    SCOPE_EXIT({
        {
            std::lock_guard lock(mutex);
            release = true;
        }
        cv.notify_all();
    });
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), [&] { return captured; }));
    }

    EXPECT_NO_THROW(store->dropNamespace(ns));
    EXPECT_FALSE(runRegularRoundReclaiming(gc).deferred);
    EXPECT_TRUE(runRegularRoundReclaiming(gc).deferred);
    publishWithProductionBirth(store, ns, "shared");
    const CatalogEntry successor = catalogEntryOrThrow(*backend, layout, ns);
    EXPECT_NE(successor.incarnation, predecessor.incarnation);
    const uint64_t successor_runtime = store->refTableRuntimeIdentityForTest(ns);
    const auto successor_ref = store->resolveRef(ns, "shared");
    ASSERT_TRUE(successor_ref);

    {
        std::lock_guard lock(mutex);
        release = true;
    }
    cv.notify_all();
    ASSERT_EQ(append.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { append.get(); });
    store->setAppendAfterRuntimeCaptureHookForTest(nullptr);

    EXPECT_EQ(store->refTableRuntimeIdentityForTest(ns), successor_runtime);
    const auto successor_after = store->resolveRef(ns, "shared");
    ASSERT_TRUE(successor_after);
    EXPECT_EQ(successor_after->manifest_id.ref, successor_ref->manifest_id.ref);
}

/// Exact retirement is pointer/key scoped. A delayed notification for the predecessor may arrive after
/// its same-name successor is already attached; it must not erase or poison that successor slot.
TEST(CASRefWriterRuntimeIdentity, LatePredecessorInvalidationLeavesSuccessorAttached)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    PoolConfig config;
    config.gc_fold_threshold = 1;
    config.ref_table_cache_bytes = 0;
    auto store = openPoolWithConfig(backend, config);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/late-predecessor-invalidation"};

    publishWithProductionBirth(store, ns, "predecessor");
    const CatalogEntry predecessor = catalogEntryOrThrow(*backend, layout, ns);
    const NamespaceLifeId predecessor_life
        = NamespaceLifeId::fromCatalogEntry(ns, predecessor.incarnation);
    Gc gc(store, UInt128{109});
    ASSERT_FALSE(runRegularRoundReclaiming(gc).deferred);
    store->dropNamespace(ns);
    ASSERT_FALSE(runRegularRoundReclaiming(gc).deferred);
    ASSERT_TRUE(runRegularRoundReclaiming(gc).deferred);

    publishWithProductionBirth(store, ns, "successor");
    const CatalogEntry successor = catalogEntryOrThrow(*backend, layout, ns);
    ASSERT_NE(successor.incarnation, predecessor.incarnation);
    const NamespaceLifeId successor_life
        = NamespaceLifeId::fromCatalogEntry(ns, successor.incarnation);
    const uint64_t successor_runtime = store->refTableRuntimeIdentityForTest(ns);
    const auto successor_ref = store->resolveRef(ns, "successor");
    ASSERT_TRUE(successor_ref);

    store->invalidateRemovedCatalogLife(predecessor_life);

    EXPECT_EQ(store->refTableRuntimeIdentityForTest(ns), successor_runtime);
    EXPECT_EQ(store->refTableLifeForTest(ns), successor_life);
    const auto successor_after = store->resolveRef(ns, "successor");
    ASSERT_TRUE(successor_after);
    EXPECT_EQ(successor_after->manifest_id.ref, successor_ref->manifest_id.ref);
}

/// Task 13 (spec §implementation-impact): a threshold snapshot publish increments the writer-side
/// observability counters -- snapshot PUT bytes and the tail-logs-compacted count
/// (logs-per-table-after-snapshot). Before/after deltas prove both sites fire.
TEST(CASRefWriterSnapshotPublish, PublishIncrementsSnapshotCounters)
{
    using ProfileEvents::global_counters;
    const auto bytes_before = global_counters[ProfileEvents::CASRefSnapshotPutBytes].load();
    const auto logs_before  = global_counters[ProfileEvents::CASRefSnapshotTailLogs].load();

    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    PoolConfig config;
    config.snapshot_log_count_threshold = 3;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    auto store = openPoolWithConfig(backend, config);
    const RootNamespace ns{"srv1/counter_publish"};

    publishEmptyPart(store, ns, "a");
    publishEmptyPart(store, ns, "b");
    store->waitForSnapshotPublishSettleForTest(ns);

    ASSERT_TRUE(listGreatestSnapshotIdForTest(*backend, layout, ns).has_value())
        << "the threshold trigger must have published a snapshot";
    EXPECT_GT(global_counters[ProfileEvents::CASRefSnapshotPutBytes].load(), bytes_before);
    EXPECT_GT(global_counters[ProfileEvents::CASRefSnapshotTailLogs].load(), logs_before);
}

/// A fresh mount that recovers a large PRE-EXISTING tail (left by a predecessor whose own thresholds
/// never fired) retains that tail as trigger debt. Recovery ends at a terminal epoch seal, which is not
/// snapshot-serializable; one ordinary successor makes the inherited over-threshold tail publishable.
/// The single successor alone is below the threshold, so the dispatch still proves the mount-time tail
/// was retained rather than forgotten during recovery.
TEST(CASRefWriterSnapshotPublish, MountTimeRecoveredLargeTailPublishesAfterOrdinarySuccessor)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/mount_time_publish"};

    {
        /// Predecessor: default (high) thresholds, so nothing publishes yet. 3 parts -> 6 tail entries.
        auto predecessor = openPool(backend);
        publishEmptyPart(predecessor, ns, "a");
        publishEmptyPart(predecessor, ns, "b");
        publishEmptyPart(predecessor, ns, "c");
    }   /// mount released; the tail is durable but nothing has published it

    PoolConfig config;
    config.snapshot_log_count_threshold = 3;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    auto successor = openPoolWithConfig(backend, config);

    /// A mere read triggers recovery. The recovered tail is already above threshold, but its greatest
    /// applied record is the terminal seal, so there is deliberately no snapshot candidate yet.
    EXPECT_EQ(successor->listRefs(ns).size(), 3u);
    successor->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_GT(successor->tailSinceSnapshotCountForTest(ns), config.snapshot_log_count_threshold)
        << "the mount must retain the predecessor's large uncovered tail";
    EXPECT_FALSE(listGreatestSnapshotIdForTest(*backend, layout, ns).has_value())
        << "a terminal recovery seal is not snapshot-serializable";

    /// One ordinary transaction above the seal reopens the candidate. It cannot cross the threshold
    /// by itself; publication therefore depends on the recovered mount-time tail asserted above.
    successor->dropRef(ns, "c");
    successor->waitForSnapshotPublishSettleForTest(ns);

    const auto snap_id = listGreatestSnapshotIdForTest(*backend, layout, ns);
    ASSERT_TRUE(snap_id.has_value())
        << "the ordinary successor must make the inherited mount-time tail publishable";
    EXPECT_EQ(successor->tailSinceSnapshotCountForTest(ns), 0u);

    const auto got = backend->get(layout.refSnapshotKey(DB::Cas::tests::fixture::fixtureLife(ns), *snap_id));
    ASSERT_TRUE(got.has_value());
    const RefTableState oracle = independentFullReplayForTest(*backend, layout, ns, snap_id);
    EXPECT_EQ(openObject(FormatId::RefSnapshot, got->bytes), encodeRefTableSnapshot(snapshotOf(oracle, ns.string())));
}

/// ===================================================================================
/// rev.6 Task 10 (spec §publish-from-live): the grace-window machinery
/// (`snapshot_min_log_age_ms`, the tail-replay-from-`snapshot_base_state` copy-once path,
/// `CasRefLatePredecessorObserved`) is DELETED. The Task 8 recovery-seal plus the Task 6
/// recovery seal already makes a late-arriving predecessor write born-covered for every
/// observer by the time this writer could ever see it, so a young committed txn has nothing left to
/// wait out -- it is immediately publish-eligible, with no time manipulation anywhere below.
/// ===================================================================================

/// A just-committed txn is covered by a publish forced immediately afterward -- no fake clock, no
/// aging, no waiting: the OLD grace-window code would have published nothing here at all.
TEST(CASRefWriterPublishFromLive, YoungTxnIsCoveredImmediately)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/publish_from_live_young"};
    auto store = openPool(backend);

    /// Setup: birth the namespace and add a precommit (not the txn under test).
    auto build = startBuildFor(store, ns, "a");
    const ManifestId id = build->stageManifest({});
    build->precommitAdd(ns, "a", id);

    /// The ONE committed txn under test.
    build->promote(ns, "a", build->buildId(), id);

    ASSERT_TRUE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns))
        << "publish-from-live: a just-committed txn is immediately coverable, with no grace window";
    const auto snap_id = listGreatestSnapshotIdForTest(*backend, layout, ns);
    ASSERT_TRUE(snap_id.has_value());
    const auto got = backend->get(layout.refSnapshotKey(DB::Cas::tests::fixture::fixtureLife(ns), *snap_id));
    ASSERT_TRUE(got.has_value());
    const RefTableSnapshot snap = decodeRefTableSnapshot(openObject(FormatId::RefSnapshot, got->bytes), ns.string(), *snap_id);
    ASSERT_EQ(snap.committed.size(), 1u);
    EXPECT_EQ(snap.committed.front().ref_name, "a")
        << "the published snapshot body contains the just-promoted row";
}

/// The count trigger fires purely off the tail counters -- no aging involved -- even under a boot
/// clock that never advances (the old code REQUIRED aging past `snapshot_min_log_age_ms` to fire).
TEST(CASRefWriterSnapshotPublish, TriggerFiresOnCountAboveThresholdWithoutAging)
{
    using ProfileEvents::global_counters;
    auto backend = std::make_shared<RefWriterTestBackend>();
    const RootNamespace ns{"srv1/publish_from_live_trigger"};
    uint64_t fake_now = 1'000'000;   /// frozen: never advances

    PoolConfig config;
    config.snapshot_log_count_threshold = 3;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    config.boot_ms_fn = [&fake_now] { return fake_now; };
    config.mount_lease_ttl_ms = std::chrono::milliseconds(10'000'000);
    auto store = openPoolWithConfig(backend, config);

    const auto before = global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load();
    publishEmptyPart(store, ns, "a");   /// tail: 2
    publishEmptyPart(store, ns, "b");   /// tail: 4 > 3 -> dispatches, clock frozen throughout
    store->waitForSnapshotPublishSettleForTest(ns);

    EXPECT_GT(global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load(), before)
        << "the count trigger must fire without any aging, even under a frozen clock";
}

/// Adoption subtracts EXACTLY the counters captured at copy time, not whatever the counters read at
/// adoption time: while a publish's PUT is in flight (captured count/bytes fixed), more commits land
/// on the live counters. After adoption, the counters must equal precisely the amount appended AFTER
/// the copy -- not zero (would drop the new txns from the next publish trigger) and not negative/
/// wrapped (an unsigned underflow).
TEST(CASRefWriterSnapshotPublish, AdoptionSubtractsCapturedCountersUnderConcurrentAppends)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const RootNamespace ns{"srv1/publish_from_live_adoption"};
    auto store = openPool(backend);

    publishEmptyPart(store, ns, "a");
    ASSERT_EQ(store->tailSinceSnapshotCountForTest(ns), 2u);

    backend->armPutBlock("_snap/");
    std::thread publisher([&] { store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns); });
    backend->awaitBlockEntered();   /// the candidate (count=2) is captured; the PUT is now in flight, no lock held

    publishEmptyPart(store, ns, "b");   /// +2 more commits land WHILE the publish's PUT is in flight
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), 4u);

    backend->releaseBlock();
    publisher.join();

    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), 2u)
        << "adoption must subtract only the CAPTURED count (2), leaving exactly the 2 txns appended "
           "after the copy";
}

/// Publication must never block a concurrent append on the SAME table (spec: "Publication is
/// background and never blocks an append"): while a dispatched background publish is stuck mid-PUT, an
/// ordinary mutation on the table must still complete promptly (a real deadlock would hang this test).
TEST(CASRefWriterSnapshotPublish, PublicationNeverBlocksConcurrentAppend)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/publish_no_block"};
    PoolConfig config;
    config.snapshot_log_count_threshold = 3;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    auto store = openPoolWithConfig(backend, config);

    backend->armPutBlock("_snap/");

    publishEmptyPart(store, ns, "a");
    publishEmptyPart(store, ns, "b");   /// tail reaches 4 (> 3) -> dispatches a background publish

    backend->awaitBlockEntered();   /// the dispatched attempt is now stuck mid-PUT on the snapshot key

    /// An unrelated mutation on the SAME table must complete without waiting for the stuck publish.
    EXPECT_NO_THROW(store->dropRef(ns, "a"));
    EXPECT_FALSE(store->resolveRef(ns, "a").has_value());

    backend->releaseBlock();
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_TRUE(listGreatestSnapshotIdForTest(*backend, layout, ns).has_value());
}

/// Review caution (T10 review): a dispatched background publish must never outlive the Pool object
/// it operates on -- `maybeScheduleSnapshotPublish` captures `shared_from_this()` BY VALUE into the
/// dispatch lambda specifically to guarantee this (the classic "background thread references a
/// dangling owner" shutdown segfault, avoided here since a shared_ptr copy keeps the object alive for
/// as long as the thread holds it, regardless of what every OTHER holder does). Proves it directly:
/// blocks a dispatched publish mid-PUT, drops the TEST's own (only) Pool handle while still blocked,
/// and confirms via a `weak_ptr` that the Pool demonstrably survives on the blocked thread's own
/// reference alone. Then unblocks it with no live Pool handle anywhere in this test any more -- a
/// dangling-pointer crash here would abort the whole test binary, the strongest possible signal for
/// this specific hazard.
TEST(CASRefWriterSnapshotPublish, PublishThreadOutlivesDroppedPoolHandleWithoutCrashing)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const RootNamespace ns{"srv1/publish_outlives_store"};
    PoolConfig config;
    config.snapshot_log_count_threshold = 3;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    auto store = openPoolWithConfig(backend, config);

    backend->armPutBlock("_snap/");
    publishEmptyPart(store, ns, "a");
    publishEmptyPart(store, ns, "b");   /// tail reaches 4 (> 3) -> dispatches a background publish
    backend->awaitBlockEntered();       /// stuck mid-PUT, holding its OWN shared_ptr<Pool> copy

    std::weak_ptr<Pool> weak_store = store;
    store.reset();   /// drop the ONLY Pool handle this test holds
    EXPECT_FALSE(weak_store.expired())
        << "the blocked background thread's own shared_ptr copy must keep the Pool alive";

    backend->releaseBlock();
    /// Deterministic, sleep-free: waits for the blocked call to actually RETURN (not merely unblock),
    /// entirely through the backend -- this test holds no Pool handle to wait on any more.
    backend->awaitBlockedCallCompleted();
}

/// Review (T11) — CRITICAL: publishes are NOT serialized, so two overlapping attempts can finish out of
/// order. An OLDER-candidate publish that lands its `_snap` PUT AFTER a newer one already adopted must
/// NOT regress `newest_snapshot_id` back to its older id, and its (monotonically-skipped) adoption must
/// NOT touch the tail counters a newer attempt already reset -- either would drop the txns committed in
/// between, so the NEXT published snapshot would silently omit committed transactions and recovery
/// would lose refs. Deterministic, sleep-free: the fake backend blocks publish #1's PUT (capturing
/// exactly its key) while a higher-id publish #2 runs to completion, then unblocks #1.
TEST(CASRefWriterSnapshotPublish, ConcurrentOutOfOrderPublishDoesNotRegressBaseNorDropCommittedTxns)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/concurrent_publish_monotonic"};
    PoolConfig config;
    /// High thresholds: NO automatic background dispatch -- we drive
    /// `tryPublishSnapshotAndAdvanceCheckpointOnce` directly for full determinism.
    config.snapshot_log_count_threshold = 1ULL << 40;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    auto store = openPoolWithConfig(backend, config);

    publishEmptyPart(store, ns, "a");   /// tail: 2 (birth+add, promote)
    publishEmptyPart(store, ns, "b");   /// tail: 4 -- greatest_applied is publish #1's candidate

    /// Block ONLY publish #1's own `_snap` PUT (its exact key is captured on first match); a later,
    /// different `_snap/<id>` key proceeds unblocked.
    backend->armPutBlockFirstMatchOnly("_snap/");

    std::thread publisher1([&] { store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns); });
    backend->awaitBlockEntered();   /// #1 is parked mid-PUT on `_snap/<older>`, holding no lock

    /// While #1 is parked, commit more txns and run publish #2 to COMPLETION: it PUTs a strictly higher
    /// `_snap/<newer>` (unblocked) and adopts it, resetting the tail counters through its own candidate.
    publishEmptyPart(store, ns, "c");
    ASSERT_TRUE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));
    const auto newest_after_2 = store->newestPublishedSnapshotIdForTest(ns);
    ASSERT_TRUE(newest_after_2.has_value());
    ASSERT_EQ(store->tailSinceSnapshotCountForTest(ns), 0u) << "publish #2 covers everything committed so far";

    /// Release #1: on the BUGGY code it now adopts its OLDER candidate, regressing newest below #2
    /// and/or double-subtracting from the counters #2 already reset. The monotonic guard must skip
    /// that adoption entirely -- both the `newest_snapshot_id` write and the counter subtraction.
    backend->releaseBlock();
    publisher1.join();

    const auto newest_after_1 = store->newestPublishedSnapshotIdForTest(ns);
    ASSERT_TRUE(newest_after_1.has_value());
    EXPECT_FALSE(*newest_after_1 < *newest_after_2)
        << "a late-finishing OLDER publish must not regress newest_snapshot_id below the adopted newer one";
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), 0u)
        << "publish #1's skipped (monotonically-superseded) adoption must not subtract from counters "
           "publish #2 already reset -- an unguarded subtraction here would corrupt or underflow them";

    /// Independent proof no committed txn was lost: the NEXT publish's bytes must equal a full log replay.
    /// A regressed base would omit the txns committed while publish #1 was parked.
    publishEmptyPart(store, ns, "d");
    ASSERT_TRUE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));
    const auto snap_id = listGreatestSnapshotIdForTest(*backend, layout, ns);
    ASSERT_TRUE(snap_id.has_value());
    const auto got = backend->get(layout.refSnapshotKey(DB::Cas::tests::fixture::fixtureLife(ns), *snap_id));
    ASSERT_TRUE(got.has_value());
    const RefTableState oracle = independentFullReplayForTest(*backend, layout, ns, snap_id);
    EXPECT_EQ(openObject(FormatId::RefSnapshot, got->bytes), encodeRefTableSnapshot(snapshotOf(oracle, ns.string())))
        << "published snapshot bytes must equal replay(all logs through X) -- a regressed base drops txns";
}

/// (I1, review of commit 9093482176a) `clampedCounterSub`'s actual clamp-to-zero branch -- the exact
/// hazard it exists for -- was previously unpinned: `AdoptionSubtractsCapturedCountersUnderConcurrentAppends`
/// subtracts from a counter that never goes below the captured amount (no clamp needed), and in
/// `ConcurrentOutOfOrderPublish...` above the SMALLER candidate is the one parked, so its adoption is
/// skipped entirely by the T11 monotonic guard BEFORE it would ever reach the subtraction -- the clamp
/// is never exercised either way. This test forces the one ordering the guard does NOT catch: the
/// SMALLER candidate adopts (and subtracts) FIRST, then a LARGER candidate -- captured earlier, while
/// the counter still held the region the smaller one just subtracted -- adopts second. Its captured
/// count therefore double-counts that already-subtracted region, and `clampedCounterSub` must clamp
/// to zero rather than wrap a `uint64_t` to ~`UINT64_MAX` (which would permanently re-latch the C4
/// storm trigger in a release build -- no `chassert` to catch it). Deterministic, sleep-free: two
/// `_snap` PUTs are parked independently (both past their own capture, neither yet adopted) via
/// `armPutBlockIndependently`, then released in the specific order that reproduces the hazard.
TEST(CASRefWriterSnapshotPublish, ClampedCounterSubClampsInsteadOfUnderflowingOnOutOfOrderAdoption)
{
    using ProfileEvents::global_counters;
    auto backend = std::make_shared<RefWriterTestBackend>();
    const RootNamespace ns{"srv1/clamp_out_of_order"};
    PoolConfig config;
    /// High thresholds: NO automatic background dispatch -- we drive
    /// `tryPublishSnapshotAndAdvanceCheckpointOnce` directly for full determinism.
    config.snapshot_log_count_threshold = 1ULL << 40;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    auto store = openPoolWithConfig(backend, config);

    publishEmptyPart(store, ns, "a");   /// tail: 2 -- publisher A's (smaller) candidate

    backend->armPutBlockIndependently("_snap/");

    std::thread publisher_a([&] { store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns); });
    backend->awaitAtLeastNKeysBlocked(1);   /// A has captured (candidate=2 txns, count=2) and parked mid-PUT
    const String key_a = *backend->blockedKeysSnapshot().begin();

    publishEmptyPart(store, ns, "b");   /// tail: 4 -- publisher B's (larger) candidate, captured BELOW

    std::thread publisher_b([&] { store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns); });
    backend->awaitAtLeastNKeysBlocked(2);   /// B has ALSO captured (candidate=4 txns, count=4) and parked
    const auto blocked = backend->blockedKeysSnapshot();
    ASSERT_EQ(blocked.size(), 2u) << "both publishers must be parked past their own capture before either adopts";
    String key_b;
    for (const auto & k : blocked)
        if (k != key_a)
            key_b = k;
    ASSERT_FALSE(key_b.empty());

    /// Release the SMALLER candidate first: its monotonic guard passes (newest is still unset), so it
    /// adopts -- newest becomes A's candidate, and the count drops from the live 4 to 2 (4 - captured_A=2).
    backend->releaseKey(key_a);
    publisher_a.join();
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), 2u)
        << "publisher A (smaller candidate) adopts first and subtracts its own captured count safely";

    /// Release the LARGER candidate: its monotonic guard ALSO passes (newest=A's candidate < B's
    /// candidate), so it reaches the subtraction with `captured_count_B == 4` -- but the live counter
    /// is now only 2 (A's adoption already removed the overlapping region). A plain `fetch_sub` here
    /// would wrap to ~UINT64_MAX; `clampedCounterSub` must clamp to 0 instead.
    backend->releaseKey(key_b);
    publisher_b.join();
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), 0u)
        << "clampedCounterSub must clamp to 0, not underflow/wrap, when B's captured count (4) already "
           "includes the region A's earlier adoption already subtracted";

    /// A wrapped counter would read as ~UINT64_MAX, permanently latching `over_threshold` (the C4
    /// storm regression). With the huge threshold configured above, a dispatch firing here can ONLY
    /// mean the counter is corrupted -- a clamped counter of 0 never crosses it.
    const auto dispatched_before = global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load();
    for (int i = 0; i < 5; ++i)
        store->resolveRef(ns, "a");
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load(), dispatched_before)
        << "a correctly-clamped counter must never latch the threshold trigger";
}

/// ===================================================================================
/// C4: bound the read-triggered snapshot-publish dispatch (spec §writer-snapshot-publication). A
/// fold-heavy reader must not turn every ref read into a re-dispatched full-snapshot encode+PUT: an
/// in-flight gate admits at most one publish per table, and a non-Committed outcome arms a bounded
/// per-table backoff instead of re-triggering on the next read. The unfixed code dispatched a new
/// publish on every trigger and never backed off, producing the soak's 46 GB/hr `_snap` PUT storm.
/// ===================================================================================

/// Under a saturated backend (every `_snap` PUT is Unresolved), the read path must NOT re-dispatch a
/// publish on each read: the failure arms the backoff, and while it holds no read re-dispatches.
TEST(CASRefWriterSnapshotPublish, C4LatchBoundedUnderSustainedNonCommittedPublish)
{
    using ProfileEvents::global_counters;
    auto backend = std::make_shared<RefWriterTestBackend>();
    const RootNamespace ns{"srv1/c4_latch"};

    CasRequestBudget budget;   /// one attempt per publish so a failure is a single PUT
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = kSingleAttemptDeadlineMs;
    budget.lease_safety_margin_ms = 100;

    uint64_t fake_now = 1'000'000;
    PoolConfig config;
    config.snapshot_log_count_threshold = 1;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    config.snapshot_publish_backoff_initial_ms = 5000;   /// the frozen clock keeps the backoff armed
    config.mount_lease_ttl_ms = std::chrono::milliseconds(10'000'000);
    config.boot_ms_fn = [&fake_now] { return fake_now; };
    config.cas_request_budget = budget;
    auto store = openPoolWithConfig(backend, config);

    /// Every `_snap` PUT throws Unresolved (backend saturated), from the very first publish attempt.
    backend->fault_key_substr = "_snap/";
    backend->fault_count = 100000;

    publishEmptyPart(store, ns, "a");   /// crosses the threshold -> one dispatch -> fails -> backoff armed
    store->waitForSnapshotPublishSettleForTest(ns);

    const auto dispatched_before = global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load();
    for (int i = 0; i < 30; ++i)
    {
        store->resolveRef(ns, "a");
        store->waitForSnapshotPublishSettleForTest(ns);
    }
    EXPECT_EQ(global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load(), dispatched_before)
        << "reads within the backoff window must not re-dispatch a publish (the storm latch is broken)";
}

/// Recovery can leave the runtime at an epoch seal with a tail already above the threshold. The seal
/// is not snapshot-serializable, so admission itself must reject it: letting execution reject it would
/// make settlement immediately dispatch another identical background attempt. A later ordinary record
/// must re-enable the same scheduler.
TEST(CASRefWriterSnapshotPublish, RecoveredSealAboveThresholdDoesNotRedispatchUntilOrdinarySuccessor)
{
    using ProfileEvents::global_counters;
    auto backend = std::make_shared<RefWriterTestBackend>();
    const RootNamespace ns{"srv1/recovered_seal_no_storm"};

    {
        PoolConfig predecessor_config;
        predecessor_config.snapshot_log_count_threshold = 1ULL << 40;
        predecessor_config.snapshot_log_bytes_threshold = 1ULL << 40;
        auto predecessor = openPoolWithConfig(backend, predecessor_config);
        DB::Cas::tests::fixture::admitLive(*backend, predecessor->layout(), ns);
        const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(*backend, predecessor->layout(), ns);
        ASSERT_EQ(backend->putIfAbsent(predecessor->layout().refCkptKey(life), encodeRefCkpt(RefCkpt{
            .life_epoch = predecessor->liveWriterEpoch(),
            .committed_through = std::nullopt,
            .checkpoint_snapshot_id = std::nullopt,
            .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);
        publishEmptyPart(predecessor, ns, "before_seal");
        const auto before = backend->get(predecessor->layout().refCkptKey(life));
        ASSERT_TRUE(before);
        const RefCkpt before_seal = decodeRefCkpt(before->bytes);
        ASSERT_TRUE(before_seal.committed_through);
        const RefTxnId seal_id{before_seal.committed_through->writer_epoch,
                               before_seal.committed_through->ref_sequence + 1};
        writeSealAt(*backend, predecessor->layout(), ns, seal_id);

        RefCkpt recovered_seal = before_seal;
        recovered_seal.committed_through = seal_id;
        recovered_seal.last_epoch_seal = seal_id;
        ASSERT_EQ(backend->casPut(predecessor->layout().refCkptKey(life), encodeRefCkpt(recovered_seal), before->token).outcome,
                  CasOutcome::Committed);
    }

    PoolConfig successor_config;
    successor_config.snapshot_log_count_threshold = 0;
    successor_config.snapshot_log_bytes_threshold = 1ULL << 40;
    auto successor = openPoolWithConfig(backend, successor_config);

    EXPECT_TRUE(successor->resolveRef(ns, "before_seal").has_value());
    successor->waitForSnapshotPublishSettleForTest(ns);
    const auto dispatched_at_seal = global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load();
    for (int i = 0; i < 5; ++i)
        EXPECT_TRUE(successor->resolveRef(ns, "before_seal").has_value());
    successor->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load(), dispatched_at_seal)
        << "a recovered seal must not dispatch or re-dispatch an unpublishable snapshot candidate";

    /// One ordinary append transaction above the recovered seal must reopen the scheduler. `dropRef`
    /// is exactly one ordinary ref-log append, unlike `publishEmptyPart`'s two-phase part publication.
    successor->dropRef(ns, "before_seal");
    successor->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load(), dispatched_at_seal + 1)
        << "an ordinary successor above the seal must make the threshold candidate publishable again";
}

/// While one background publish is in flight (blocked mid-PUT), further reads must NOT dispatch a
/// second: the single-in-flight gate holds `pending_snapshot_publishes` at one per table.
TEST(CASRefWriterSnapshotPublish, C4InFlightGateAdmitsAtMostOne)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const RootNamespace ns{"srv1/c4_gate"};
    PoolConfig config;
    config.snapshot_log_count_threshold = 0;   /// any nonempty tail triggers
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    auto store = openPoolWithConfig(backend, config);

    publishEmptyPart(store, ns, "a");
    store->waitForSnapshotPublishSettleForTest(ns);   /// drain the setup publishes; tail is compacted

    /// Block the first `_snap` PUT so one publisher parks in flight.
    backend->armPutBlockFirstMatchOnly("_snap/");
    std::thread mutator([&] { store->dropRef(ns, "a"); });   /// its detached publisher blocks mid-PUT
    backend->awaitBlockEntered();

    /// Many more reads while it is blocked must not admit a second publisher.
    for (int i = 0; i < 20; ++i)
        store->resolveRef(ns, "a");
    EXPECT_EQ(store->pendingSnapshotPublishesForTest(ns), 1)
        << "the in-flight gate must hold background publishes to at most one per table";

    backend->releaseBlock();
    mutator.join();
    store->waitForSnapshotPublishSettleForTest(ns);
}

/// A non-Committed publish defers the next dispatch by the backoff, then a read past the backoff
/// deadline dispatches exactly one retry that publishes a durable snapshot (freshness preserved).
TEST(CASRefWriterSnapshotPublish, C4BackoffDefersThenRetriesAndPublishes)
{
    using ProfileEvents::global_counters;
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/c4_backoff"};

    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = kSingleAttemptDeadlineMs;
    budget.lease_safety_margin_ms = 100;

    uint64_t fake_now = 1'000'000;
    PoolConfig config;
    config.snapshot_log_count_threshold = 1;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    config.snapshot_publish_backoff_initial_ms = 1000;
    config.mount_lease_ttl_ms = std::chrono::milliseconds(10'000'000);
    config.boot_ms_fn = [&fake_now] { return fake_now; };
    config.cas_request_budget = budget;
    auto store = openPoolWithConfig(backend, config);

    /// Fail ONLY the first `_snap` PUT (arms the backoff); later PUTs succeed.
    backend->fault_key_substr = "_snap/";
    backend->fault_count = 1;

    publishEmptyPart(store, ns, "a");   /// dispatch -> publish fails -> backoff armed
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_FALSE(listGreatestSnapshotIdForTest(*backend, layout, ns).has_value());

    /// A read within the backoff window (frozen clock) must not re-dispatch.
    const auto d1 = global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load();
    store->resolveRef(ns, "a");
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load(), d1)
        << "a read within the backoff window must not re-dispatch";
    EXPECT_FALSE(listGreatestSnapshotIdForTest(*backend, layout, ns).has_value());

    /// Advance past the backoff: exactly one retry is dispatched and it publishes.
    fake_now += 2000;
    store->resolveRef(ns, "a");
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load(), d1 + 1)
        << "after the backoff elapses exactly one retry is dispatched";
    EXPECT_TRUE(listGreatestSnapshotIdForTest(*backend, layout, ns).has_value())
        << "the retry publishes a durable snapshot (freshness preserved)";
}

/// ===================================================================================
/// rev.6 Task 10 (spec §publish-from-live): the tail counters count ONLY applied txns strictly above
/// `newest_snapshot_id` -- incremented per commit, subtracted (clamped) exactly by adoption. This
/// pins that a successful publish's adoption RESETS the counters rather than merely reducing them: a
/// buggy "subtract a fixed prune count" scheme could let the table's already-covered history keep
/// contributing to the trigger forever.
/// ===================================================================================

/// After a successful publish adopts `newest_snapshot_id`, the trigger arithmetic must restart from
/// zero above it, not keep counting the table's already-covered history: 4 covered + 2 fresh entries
/// must read as 2 (below a 3 threshold), never as 6.
TEST(CASRefWriterSnapshotPublish, TriggerIgnoresEntriesCoveredByNewestSnapshot)
{
    using ProfileEvents::global_counters;
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/trigger_covered"};
    PoolConfig config;
    config.snapshot_log_count_threshold = 3;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    auto store = openPoolWithConfig(backend, config);

    const auto d0 = global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load();

    /// Drive ONE successful publish: 4 entries (4 > 3).
    publishEmptyPart(store, ns, "a");
    publishEmptyPart(store, ns, "b");
    store->waitForSnapshotPublishSettleForTest(ns);
    ASSERT_EQ(global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load(), d0 + 1);
    const auto first_snap = listGreatestSnapshotIdForTest(*backend, layout, ns);
    ASSERT_TRUE(first_snap.has_value());
    EXPECT_TRUE(store->newestPublishedSnapshotIdForTest(ns) == first_snap);
    ASSERT_EQ(store->tailSinceSnapshotCountForTest(ns), 0u);

    /// 2 fresh entries: 2 <= 3, while the covered history (4 entries at/below the snapshot) would push
    /// a covered-counting trigger to 6 > 3. Must not dispatch.
    publishEmptyPart(store, ns, "c");
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load(), d0 + 1)
        << "entries covered by the newest snapshot must not count toward the trigger";
    EXPECT_TRUE(listGreatestSnapshotIdForTest(*backend, layout, ns) == first_snap);
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), 2u);

    /// Crossing the threshold with the fresh tail alone (4 > 3) dispatches exactly one more publish,
    /// and it covers the whole uncovered tail.
    publishEmptyPart(store, ns, "d");
    store->waitForSnapshotPublishSettleForTest(ns);
    EXPECT_EQ(global_counters[ProfileEvents::CASRefSnapshotPublishDispatched].load(), d0 + 2);
    const auto second_snap = listGreatestSnapshotIdForTest(*backend, layout, ns);
    ASSERT_TRUE(second_snap.has_value());
    EXPECT_TRUE(*first_snap < *second_snap);
    EXPECT_EQ(store->tailSinceSnapshotCountForTest(ns), 0u);
}

/// ===================================================================================
/// Task 11: successor stale-precommit cleanup (spec §Clean Up Old Precommits)
/// ===================================================================================

/// A predecessor's dangling (never-promoted) precommits are swept by the successor mount's first touch
/// of the table; a precommit the SUCCESSOR itself adds under its OWN (current) epoch must survive.
TEST(CASRefWriterStalePrecommitSweep, SweepsOnlyStaleEpochPrecommitsKeepsCurrentEpoch)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const RootNamespace ns{"srv1/precommit_sweep_basic"};

    {
        /// A predecessor writer leaves THREE precommits dangling (a crash before promote).
        auto predecessor = openPool(backend);
        for (const String & name : {"stale_a", "stale_b", "stale_c"})
        {
            auto build = startBuildFor(predecessor, ns, name);
            const ManifestId id = build->stageManifest({});
            build->precommitAdd(ns, name, id);
            /// no promote -- left dangling, as a crashed build would leave it
        }
    }   /// predecessor destroyed: its mount lease is released

    /// The successor allocates a strictly higher durable writer_epoch; its own FRESH precommit must
    /// survive the sweep its very first touch of the table triggers.
    auto successor = openPool(backend);
    auto build = startBuildFor(successor, ns, "fresh_x");
    const ManifestId fresh_id = build->stageManifest({});
    build->precommitAdd(ns, "fresh_x", fresh_id);   /// this call's own appendRefOps hoists the sweep first

    const RefTableState replayed = independentFullReplayForTest(*backend, successor->layout(), ns);
    EXPECT_EQ(replayed.getLifecycle(), RefLifecycle::Live);
    EXPECT_TRUE(replayed.getCommitted().empty());
    ASSERT_EQ(replayed.getPrecommits().size(), 1u);
    EXPECT_EQ(replayed.getPrecommits().begin()->first, "fresh_x");
    EXPECT_EQ(replayed.getPrecommits().begin()->second, fresh_id.ref);
}

/// The sweep chunks its removal to `ref_txn_max_ops` stale precommits per transaction (spec
/// §Clean Up Old Precommits), and an interruption (an uncertain PUT, wedging the lane) leaves the
/// remainder harmlessly for a LATER mount's own fresh recovery to finish -- "each chunk re-reads the
/// LIVE state, so a partial sweep just leaves fewer stale bindings for the next chunk (a later retry
/// on this mount, or the next mount's recovery) to find." (Same-mount retry is pinned separately by
/// `FailedSweepRearmsAndRetriesUntilClean`.)
TEST(CASRefWriterStalePrecommitSweep, BoundedBatchesAndInterruptionResumeAcrossMounts)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/precommit_sweep_bounded"};
    /// Derived from `ref_txn_max_ops` (not a literal) so a future cap change cannot silently drop this
    /// back to a single removal chunk: still > the cap, forcing at least two removal chunks.
    constexpr int kTotalStale = static_cast<int>(ref_txn_max_ops) + 200;

    uint64_t e1 = 0;
    {
        auto predecessor = openPool(backend);
        e1 = predecessor->writerEpoch();
    }   /// predecessor released; only its epoch is needed -- the stale precommits are seeded raw below

    /// Seed kTotalStale precommits directly (bypassing any Pool) under the predecessor's epoch,
    /// spread over two raw log objects (each within the per-transaction op ENCODE cap) so recovery
    /// costs only two GETs, not kTotalStale of them.
    {
        std::vector<RefOp> ops1;
        ops1.push_back(namespaceBirthOp());
        for (int i = 0; i < 700; ++i)
        {
            RefOp op;
            op.kind = RefOpKind::OwnerTransition;
            op.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "stale_" + std::to_string(i), manifestRef(e1, static_cast<uint64_t>(i) + 1, 1)};
            ops1.push_back(op);
        }
        DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{ns.string(), RefTxnId{e1, 1}, ops1, std::nullopt});

        std::vector<RefOp> ops2;
        for (int i = 700; i < kTotalStale; ++i)
        {
            RefOp op;
            op.kind = RefOpKind::OwnerTransition;
            op.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "stale_" + std::to_string(i), manifestRef(e1, static_cast<uint64_t>(i) + 1, 1)};
            ops2.push_back(op);
        }
        DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{ns.string(), RefTxnId{e1, 2}, ops2, std::nullopt});
    }
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = e1,
        .committed_through = RefTxnId{e1, 2},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    /// The successor: a tight retry budget so ONE simulated ambiguous response wedges rather than
    /// transparently retries away. `8f9e63c7a19` widened `kSingleAttemptDeadlineMs` off a zero-width
    /// race (equal attempt/operation deadlines), but it still measures the capture-to-gate window --
    /// encoding the removal chunk (up to `ref_txn_max_ops` ops) -- against the REAL wall clock, so it
    /// recurred (3 of 3 sanitizer lanes) once that encode step got slow enough on its own, independent
    /// of scheduler contention: msan in particular. `ref_request_controller` reads its clock through
    /// the same injectable seam as the mount fence (`CasRefLedger`'s `controller_boot_ms_fn` is the
    /// pool's `boot_ms_fn`), so freeze it here instead of racing it -- the fault-injecting PUT below
    /// still reaches the backend synchronously; only the deadline arithmetic stops moving.
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = kSingleAttemptDeadlineMs;
    budget.lease_safety_margin_ms = 100;
    PoolConfig config;
    config.cas_request_budget = budget;
    config.boot_ms_fn = [] { return uint64_t{0}; };
    auto successor = openPoolWithConfig(backend, config);

    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    /// The successor's own recovery runs first and mints one in-band seal for the dead predecessor
    /// epoch `e1` (its durable ids are `{e1,1}` and `{e1,2}`, so the seal lands at `{e1,3}`) -- that PUT
    /// shares this same `_log/` prefix, so it would eat the fault before the sweep ever gets a chance.
    /// Skip it and land the fault on the sweep's FIRST removal chunk's PUT, as intended.
    backend->fault_skip = 1;
    backend->fault_count = 1;   /// hits exactly the sweep's FIRST removal chunk's PUT

    /// The sweep is piggybacked on this mount's very first touch; its (uncertain) failure is INSULATED
    /// from the read (resolveRef/listRefs call `sweepStalePrecommitsForRead`, not
    /// `maybeSweepStalePrecommits` directly): the read itself still succeeds, the failure is counted.
    const uint64_t deferred_before = ProfileEvents::global_counters[ProfileEvents::CASRefSweepDeferred].load();
    EXPECT_NO_THROW(successor->listRefs(ns));
    const uint64_t deferred_after = ProfileEvents::global_counters[ProfileEvents::CASRefSweepDeferred].load();
    EXPECT_EQ(deferred_after, deferred_before + 1)
        << "the read-only caller must observe (and count) the deferred sweep failure, not throw";
    EXPECT_TRUE(successor->refLaneWedgedForTest(ns));

    /// The first chunk's request actually landed server-side; the caller just never saw the ack.
    backend->materializePendingDelayedWrite();
    successor.reset();   /// abandoned mid-sweep WITHOUT ever resolving its own wedge in-memory

    /// A THIRD mount (successor-of-the-successor): fresh recovery replays the two raw seed logs PLUS the
    /// first chunk's now-durable removal, sees `needs_stale_precommit_sweep` armed again, and finishes
    /// the remaining stale precommits in exactly one further chunk (<= 1000 remain). `successor` was
    /// abandoned mid-wedge above -- Task 5's drain fails closed on an unresolved PUT, so no clean
    /// farewell was written -> this reclaim is `MountPriorState::UncleanObserved` (rev.6 Task 4), which
    /// pays a real ~36.5s token-stability observation wait here. Inject a fake `boot_ms_fn` +
    /// `wait_sleep_fn` (mirroring `CASMountOpenWaits.UncleanOpenPaysOnlyTheObservationWindow`) so it
    /// resolves instantly.
    uint64_t resumer_fake_boot = 0;
    PoolConfig resumer_config;
    resumer_config.boot_ms_fn = [&resumer_fake_boot] { return resumer_fake_boot; };
    resumer_config.wait_sleep_fn = [&resumer_fake_boot](uint64_t ms) { resumer_fake_boot += ms; };
    auto resumer = openPoolWithConfig(backend, resumer_config);
    EXPECT_NO_THROW(resumer->listRefs(ns));

    const RefTableState final_state = independentFullReplayForTest(*backend, layout, ns);
    EXPECT_EQ(final_state.getLifecycle(), RefLifecycle::Live);
    EXPECT_TRUE(final_state.getPrecommits().empty()) << "every stale precommit must eventually be swept";

    /// Bounded batches: exactly THREE NEW `_log/` objects (epoch > e1) were needed -- never kTotalStale
    /// individual removals, and one more than before INV-2 went in-band. In order: `{e1+1,1}` the
    /// successor's own (delayed-delivered) FIRST removal chunk; `{e1+1,2}` the epoch seal that closes
    /// the successor's own epoch once IT becomes dead in turn -- minted by `resumer`'s recovery, since
    /// `successor` was abandoned mid-sweep without a clean farewell and never sealed itself; and
    /// `{e1+2,1}` the resumer's own SECOND removal chunk, finishing the remaining stale precommits.
    size_t new_log_objects = 0;
    {
        String cursor;
        for (;;)
        {
            const ListPage page = backend->list(layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)), cursor, 1000);
            for (const ListedKey & lk : page.keys)
            {
                const auto parsed = layout.parseRefObjectKey(lk.key);
                if (parsed && parsed->life_id == DB::Cas::tests::fixture::fixtureLife(ns).incarnation
                    && parsed->kind == RefObjectKind::Log && parsed->txn_id.writer_epoch != e1)
                    ++new_log_objects;
            }
            if (page.next_cursor.empty())
                break;
            cursor = page.next_cursor;
        }
    }
    EXPECT_EQ(new_log_objects, 3u);
}

/// S13 regression fix (triage `.superpowers/sdd/s13-triage-report.md`, run 20260713T172032_S13_seed42):
/// a FAILED sweep attempt must NOT consume the once-per-mount shot. The failure re-arms
/// `needs_stale_precommit_sweep` (with a bounded backoff, so a saturated backend is not stormed), the
/// read that piggybacked the sweep still succeeds (existing `CASRefSweepDeferred` contract), and a later
/// trigger -- here a mutation -- retries until a pass completes verified clean, clearing the flag
/// permanently. Each reclaimed binding is audited: one `precommit_reclaim` CA-log event + one
/// `CASRefStalePrecommitsReclaimed` increment, exactly per binding.
TEST(CASRefWriterStalePrecommitSweep, FailedSweepRearmsAndRetriesUntilClean)
{
    using ProfileEvents::global_counters;
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/precommit_sweep_retry"};

    /// One shared injected clock for both incarnations. The successor's wait hook below advances this
    /// same clock, so both mount observation and the later sweep-backoff deadline are deterministic.
    uint64_t fake_now = 1'000'000;
    size_t mount_wait_calls = 0;
    const auto fake_clock = [&fake_now] { return fake_now; };

    {
        /// A predecessor writer leaves THREE precommits dangling (a crash before promote).
        PoolConfig pred_config;
        pred_config.mount_lease_ttl_ms = std::chrono::milliseconds(10'000'000);
        pred_config.boot_ms_fn = fake_clock;
        auto predecessor = openPoolWithConfig(backend, pred_config);
        std::vector<PartWriteTxnPtr> predecessor_builds;
        for (const String & name : {"stale_a", "stale_b", "stale_c"})
        {
            auto build = startBuildFor(predecessor, ns, name);
            const ManifestId id = build->stageManifest({});
            build->precommitAdd(ns, name, id);
            /// no promote -- left dangling, as a crashed build would leave it
            predecessor_builds.push_back(std::move(build));
        }
    }   /// all three cleanup duties remain pending; predecessor publishes no clean farewell

    /// The successor: a tight retry budget so ONE simulated ambiguous response wedges rather than
    /// transparently retries away (mirrors the wedge-semantics tests in this file exactly).
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = kSingleAttemptDeadlineMs;
    budget.lease_safety_margin_ms = 100;
    PoolConfig config;
    config.cas_request_budget = budget;
    config.mount_lease_ttl_ms = std::chrono::milliseconds(10'000'000);
    config.boot_ms_fn = fake_clock;
    config.wait_sleep_fn = [&fake_now, &mount_wait_calls](uint64_t ms)
    {
        ++mount_wait_calls;
        fake_now += ms;
    };
    SynchronizedEventLog seen;   /// declared BEFORE the Pool so it outlives the background syncer's emits (ASan 2026-07-09)
    auto successor = openPoolWithConfig(backend, config);
    EXPECT_GT(mount_wait_calls, 0u)
        << "the unclean predecessor must exercise the injected mount-observation wait";

    successor->setEventSink([&](const CasEvent & e) { seen.add(e); });

    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    /// The successor's own recovery runs first and mints one in-band seal for the predecessor's now-dead
    /// epoch (its three precommits are its only durable ids, so the seal takes the very next slot) --
    /// that PUT shares this same `_log/` prefix, so it would eat the fault before the sweep gets a turn.
    /// Skip it and land the fault on the sweep's FIRST removal chunk's PUT, as intended.
    backend->fault_skip = 1;
    backend->fault_count = 1;   /// hits exactly the sweep's FIRST removal chunk's PUT

    /// FIRST trigger (read path): the sweep's removal PUT is uncertain -> the lane wedges; the read
    /// itself still succeeds and counts the deferral (existing contract) -- but the shot must NOT be
    /// consumed: the flag is re-armed for a later trigger.
    const uint64_t deferred_before = global_counters[ProfileEvents::CASRefSweepDeferred].load();
    const uint64_t rearmed_before = global_counters[ProfileEvents::CASRefSweepRearmed].load();
    const uint64_t reclaimed_before = global_counters[ProfileEvents::CASRefStalePrecommitsReclaimed].load();
    EXPECT_NO_THROW(successor->listRefs(ns));
    EXPECT_EQ(global_counters[ProfileEvents::CASRefSweepDeferred].load(), deferred_before + 1);
    EXPECT_TRUE(successor->refLaneWedgedForTest(ns));
    EXPECT_TRUE(successor->needsStalePrecommitSweepForTest(ns))
        << "a failed sweep must re-arm needs_stale_precommit_sweep, not consume the once-per-mount shot";
    EXPECT_EQ(global_counters[ProfileEvents::CASRefSweepRearmed].load(), rearmed_before + 1);

    /// Within the backoff window (the injected clock has not advanced) a read must NOT re-attempt --
    /// the bounded-backoff storm latch: no new deferral, flag still armed.
    EXPECT_NO_THROW(successor->listRefs(ns));
    EXPECT_EQ(global_counters[ProfileEvents::CASRefSweepDeferred].load(), deferred_before + 1)
        << "within the backoff window the sweep must not re-attempt (PUT-storm latch)";
    EXPECT_TRUE(successor->needsStalePrecommitSweepForTest(ns));

    /// The lost response later lands server-side; past the backoff deadline the NEXT trigger (a
    /// mutation this time) retries: the lane resolves its wedge (the first chunk's removals become
    /// durable and applied), the re-pass verifies clean, and the flag clears permanently.
    backend->materializePendingDelayedWrite();
    fake_now += 60'000;   /// beyond any armed backoff (initial 200 ms, max 30 s)
    EXPECT_NO_THROW(publishEmptyPart(successor, ns, "fresh"));
    EXPECT_FALSE(successor->refLaneWedgedForTest(ns));
    EXPECT_FALSE(successor->needsStalePrecommitSweepForTest(ns))
        << "a verified-clean sweep clears the flag permanently";

    /// Ground truth: every stale binding reclaimed; the successor's own committed work intact.
    const RefTableState final_state = independentFullReplayForTest(*backend, layout, ns);
    EXPECT_EQ(final_state.getLifecycle(), RefLifecycle::Live);
    EXPECT_TRUE(final_state.getPrecommits().empty());
    EXPECT_TRUE(final_state.getCommitted().contains("fresh"));

    /// Audit (INTROSPECTION-1): exactly ONE `precommit_reclaim` event per reclaimed stale binding --
    /// this is what makes the S13 card's "abandoned precommits reclaimed" counter falsifiable.
    std::vector<String> reclaimed_refs;
    for (const CasEvent & e : seen.snapshot())
        if (e.type == CasEventType::PrecommitReclaim)
            reclaimed_refs.push_back(e.ref_name);
    std::sort(reclaimed_refs.begin(), reclaimed_refs.end());
    EXPECT_EQ(reclaimed_refs, (std::vector<String>{"stale_a", "stale_b", "stale_c"}));
    EXPECT_EQ(global_counters[ProfileEvents::CASRefStalePrecommitsReclaimed].load(), reclaimed_before + 3);
}

/// Verified-clean semantics: a sweep that finds NOTHING stale clears the flag on its very first pass
/// and emits no reclaim event (so "no abandons" and "reclaim broken" stay distinguishable in the
/// audit log).
TEST(CASRefWriterStalePrecommitSweep, VerifiedCleanSweepClearsFlagWithoutEvents)
{
    using ProfileEvents::global_counters;
    auto backend = std::make_shared<RefWriterTestBackend>();
    const RootNamespace ns{"srv1/precommit_sweep_clean"};

    {
        auto predecessor = openPool(backend);
        publishEmptyPart(predecessor, ns, "committed_x");   /// committed work only; nothing dangles
    }

    SynchronizedEventLog seen;   /// declared BEFORE the Pool so it outlives the background syncer's emits (ASan 2026-07-09)
    auto successor = openPool(backend);
    successor->setEventSink([&](const CasEvent & e) { seen.add(e); });

    const uint64_t deferred_before = ProfileEvents::global_counters[ProfileEvents::CASRefSweepDeferred].load();
    const uint64_t reclaimed_before = global_counters[ProfileEvents::CASRefStalePrecommitsReclaimed].load();
    EXPECT_NO_THROW(successor->listRefs(ns));
    EXPECT_FALSE(successor->needsStalePrecommitSweepForTest(ns))
        << "a clean first pass IS the verified-clean sweep: the flag clears without any removal";
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRefSweepDeferred].load(), deferred_before);
    EXPECT_EQ(global_counters[ProfileEvents::CASRefStalePrecommitsReclaimed].load(), reclaimed_before);
    const std::vector<CasEvent> observed = seen.snapshot();
    EXPECT_EQ(std::count_if(observed.begin(), observed.end(),
        [](const CasEvent & e) { return e.type == CasEventType::PrecommitReclaim; }), 0);
}

/// ===================================================================================
/// C1: self-remount establishes a fresh ref-protocol incarnation (spec §Startup And Recovery /
/// §write-fence). A self-remount bumps the durable writer_epoch, so every ref transaction it stamps
/// afterward sorts strictly above any log a dead-incarnation or same-uuid twin left durable under an
/// older epoch, and it drops its stale in-memory cache so the next touch re-recovers under the new
/// epoch. The unfixed code kept the open-time `process_epoch` and the cached tables across the fence-out.
/// ===================================================================================

namespace
{

/// Fence out the mount lease so `tryRemountOnce` reclaims a fresh incarnation (mirrors
/// gtest_cas_pool.cpp's fenceOutMount, without its ASSERT_ macros so it can run outside a fixture).
void fenceOutRefMount(Backend & backend, const String & mount_key)
{
    const auto got = backend.get(mount_key);
    MountLease m = decodeMountLease(got->bytes);
    m.gc_fenced = true;
    m.seq += 1;
    backend.putOverwrite(mount_key, encodeMountLease(m), got->token);
}

/// The greatest `_log/<id>` transaction id currently present for `ns` (independent of any Pool cache).
std::optional<RefTxnId> listGreatestLogIdForTest(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    std::optional<RefTxnId> greatest;
    String cursor;
    for (;;)
    {
        const ListPage page = backend.list(layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)), cursor, 1000);
        for (const ListedKey & lk : page.keys)
        {
            const auto parsed = layout.parseRefObjectKey(lk.key);
            if (parsed && parsed->life_id == DB::Cas::tests::fixture::fixtureLife(ns).incarnation && parsed->kind == RefObjectKind::Log
                && (!greatest || *greatest < parsed->txn_id))
                greatest = parsed->txn_id;
        }
        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }
    return greatest;
}

/// Seed a same-uuid TWIN incarnation that bumped the durable writer_epoch and durably DROPPED `ref_name`
/// (its committed binding `old_ref`) at `{twin_epoch, 1}` -- an id that sorts strictly above every log a
/// Pool wrote under its own (lower) open-time epoch. Returns the twin's epoch.
/// `prev_epoch_seal` is NOT optional decoration here. The twin's drop is sequence 1 of a new epoch, so
/// INV-2's grammar requires it to name the seal that closed the epoch below -- and the reader enforces
/// that, so a twin seeded without it describes a stream with an uncertified epoch boundary, which is
/// exactly what recovery must refuse. The link names the id the recovering pool's own CAS-walk will mint
/// for the dead epoch: one past that epoch's greatest durable id, which is what `seal_of_previous_epoch`
/// derives by listing rather than hard-coding, so the fixture cannot drift from the walk's arithmetic.
uint64_t seedTwinDrop(Backend & backend, const Layout & layout, const RootNamespace & ns,
                      const String & ref_name, const ManifestRef & old_ref)
{
    uint64_t greatest_in_previous_epoch = 0;
    uint64_t previous_epoch = 0;
    forEachListedKey(backend, layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)), [&](const ListedKey & lk)
    {
        const auto parsed = layout.parseRefObjectKey(lk.key);
        if (!parsed || parsed->kind != RefObjectKind::Log)
            return;
        if (parsed->txn_id.writer_epoch > previous_epoch
            || (parsed->txn_id.writer_epoch == previous_epoch && parsed->txn_id.ref_sequence > greatest_in_previous_epoch))
        {
            previous_epoch = parsed->txn_id.writer_epoch;
            greatest_in_previous_epoch = parsed->txn_id.ref_sequence;
        }
    }, 1000);

    const uint64_t twin_epoch = allocateWriterEpoch(backend, layout, "test", EpochMintPolicy::NormalMount, 0, [] { return RefCatalog{}; });
    RefLogTxn twin;
    twin.ns = ns.string();
    twin.txn_id = RefTxnId{twin_epoch, 1};
    twin.prev_epoch_seal = RefTxnId{previous_epoch, greatest_in_previous_epoch + 1};
    RefOp drop;
    drop.kind = RefOpKind::OwnerTransition;
    drop.old_binding = RefOwnerBinding{RefOwnerKind::Committed, ref_name, old_ref};
    twin.ops = {drop};
    DB::Cas::tests::fixture::writeRefLogRaw(backend, layout, twin);
    return twin_epoch;
}

}

/// A fence-loss generation is a rejection marker, not a runtime admission token. If remount then loses
/// to a foreign owner, neither a warm name nor a never-seen name may select/materialize a runtime under
/// that intermediate generation; the predecessor remains only as a detached diagnostic object.
TEST(CASRefWriterRemount, FailedRemountPublishesNoRuntimeUnderFenceLossGeneration)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace existing{"srv1/failed-remount-existing"};
    const RootNamespace never_seen{"srv1/failed-remount-never-seen"};

    publishWithProductionBirth(store, existing, "a");
    const uint64_t predecessor_runtime = store->refTableRuntimeIdentityForTest(existing);
    const uint64_t predecessor_generation
        = store->refTableRuntimeAdmittedFenceGenerationForTest(existing);
    const size_t cached_before = store->refTablesCachedCountForTest();
    ASSERT_NE(predecessor_runtime, 0u);
    ASSERT_EQ(predecessor_generation, store->fenceGeneration());

    const String mount_key = layout.mountKey("test");
    const auto got = backend->get(mount_key);
    ASSERT_TRUE(got);
    MountLease foreign = decodeMountLease(got->bytes);
    foreign.server_uuid = foreign.server_uuid + UInt128{1};
    foreign.seq += 1;
    ASSERT_EQ(backend->putOverwrite(mount_key, encodeMountLease(foreign), got->token).outcome,
        PutOutcome::Done);

    store->tripMountLost();
    const uint64_t rejected_generation = store->fenceGeneration();
    ASSERT_NE(rejected_generation, predecessor_generation);
    EXPECT_FALSE(store->tryRemountOnce());
    EXPECT_FALSE(store->mayMutate());

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)store->resolveRef(existing, "a"); });
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)store->resolveRef(never_seen, "a"); });
    EXPECT_EQ(store->refTablesCachedCountForTest(), cached_before);
    EXPECT_EQ(store->refTableRuntimeIdentityForTest(existing), predecessor_runtime);
    EXPECT_EQ(store->refTableRuntimeAdmittedFenceGenerationForTest(existing), predecessor_generation);
    EXPECT_EQ(store->refTableRuntimeIdentityForTest(never_seen), 0u);
    EXPECT_NE(store->refTableRuntimeAdmittedFenceGenerationForTest(existing), rejected_generation);

    /// Make the foreign occupant terminal before teardown; it remains foreign and is never taken over.
    fenceOutRefMount(*backend, mount_key);
}

/// C1/N1 (stale cache): a warm table whose committed ref a twin durably dropped must re-recover to the
/// twin's view after a self-remount. The unfixed code kept the stale cache and still resolved the ref.
TEST(CASRefWriterRemount, ReRecoversStaleCacheToTwinDrop)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/remount_twin_view"};

    const ManifestId a_id = publishEmptyPart(store, ns, "a");
    ASSERT_TRUE(store->resolveRef(ns, "a").has_value());
    const uint64_t e1 = store->liveWriterEpoch();
    const uint64_t predecessor_runtime = store->refTableRuntimeIdentityForTest(ns);
    const NamespaceLifeId predecessor_life = *store->refTableLifeForTest(ns);
    const uint64_t predecessor_generation = store->refTableRuntimeAdmittedFenceGenerationForTest(ns);

    /// A same-uuid twin bumped the durable epoch and durably dropped "a"; this Pool's warm cache never
    /// observed it.
    const uint64_t twin_epoch = seedTwinDrop(*backend, layout, ns, "a", a_id.ref);
    ASSERT_GT(twin_epoch, e1);
    ASSERT_TRUE(store->resolveRef(ns, "a").has_value()) << "precondition: the warm cache is stale";

    fenceOutRefMount(*backend, layout.mountKey("test"));
    ASSERT_TRUE(store->tryRemountOnce());
    EXPECT_GT(store->liveWriterEpoch(), twin_epoch);

    /// The remount dropped the stale runtime: the next read re-recovers from the durable objects and
    /// adopts the twin's drop -- "a" is gone.
    EXPECT_FALSE(store->resolveRef(ns, "a").has_value())
        << "a self-remount must re-recover the table under the new epoch, adopting the twin's drop";
    EXPECT_NE(store->refTableRuntimeIdentityForTest(ns), predecessor_runtime);
    EXPECT_EQ(store->refTableLifeForTest(ns), predecessor_life);
    EXPECT_NE(store->refTableRuntimeAdmittedFenceGenerationForTest(ns), predecessor_generation);
    EXPECT_EQ(store->refTableRuntimeAdmittedFenceGenerationForTest(ns), store->fenceGeneration());
}

/// C1/N2 (epoch routing + ordering): a post-remount append must stamp its log with the fresh
/// incarnation's live epoch, landing strictly above a twin's durable log (the pagination premise
/// "a new log is never inserted at or below an already durable table log id"). The unfixed code stamped
/// the stale open-time epoch, which sorts BELOW a higher-epoch twin log.
TEST(CASRefWriterRemount, PostRemountAppendCarriesLiveEpochSortingAboveTwinLogs)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/remount_epoch_order"};

    const ManifestId a_id = publishEmptyPart(store, ns, "a");
    const uint64_t e1 = store->liveWriterEpoch();
    const uint64_t twin_epoch = seedTwinDrop(*backend, layout, ns, "a", a_id.ref);
    ASSERT_GT(twin_epoch, e1);

    fenceOutRefMount(*backend, layout.mountKey("test"));
    ASSERT_TRUE(store->tryRemountOnce());
    const uint64_t e2 = store->liveWriterEpoch();
    ASSERT_GT(e2, twin_epoch);

    publishEmptyPart(store, ns, "b");
    const auto greatest = listGreatestLogIdForTest(*backend, layout, ns);
    ASSERT_TRUE(greatest.has_value());
    EXPECT_EQ(greatest->writer_epoch, e2)
        << "the newest ref log must carry the fresh incarnation's epoch and be the greatest id";
    EXPECT_GT(*greatest, (RefTxnId{twin_epoch, 1}))
        << "the post-remount append must sort strictly above the twin's log";
}

/// C1 (wedge disposition): a wedged append lane's runtime (and its wedge) is dropped on a self-remount,
/// the next touch re-recovers a clean lane, and appends resume without hanging. The unfixed code kept the
/// wedged runtime cached across the remount. The drop is a plain cache detach and needs to certify
/// nothing: the undecided PUT the wedge describes is settled by the seal the next recovery writes into
/// its slot -- see `quiesceRefTablesForRemount`'s doc comment (`CasPool.h`).
TEST(CASRefWriterRemount, DiscardsWedgeAndLaneRemainsUsable)
{
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = kSingleAttemptDeadlineMs;
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<RefWriterTestBackend>();
    /// The self-remount below blocks on nothing (see
    /// `CASRemountWaits.UnresolvedWedgeRemountPaysNoWaitEither`, `gtest_cas_pool.cpp`); the injected
    /// `boot_ms_fn`/`wait_sleep_fn` keep this test off the real clock anyway.
    uint64_t fake_boot = 0;
    PoolConfig config;
    config.cas_request_budget = budget;
    config.boot_ms_fn = [&fake_boot] { return fake_boot; };
    config.wait_sleep_fn = [&fake_boot](uint64_t ms) { fake_boot += ms; };
    auto store = openPoolWithConfig(backend, config);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/remount_wedge"};
    publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "y");

    /// Wedge the lane with an ambiguous PUT that never landed server-side.
    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->fault_count = 1;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));

    fenceOutRefMount(*backend, layout.mountKey("test"));
    ASSERT_TRUE(store->tryRemountOnce());

    EXPECT_FALSE(store->refLaneWedgedForTest(ns))
        << "a self-remount discards the in-memory wedge with the detached runtime";

    /// The lane is usable, not hung: a fresh append completes and carries the live epoch.
    EXPECT_NO_THROW(store->dropRef(ns, "y"));
    EXPECT_FALSE(store->resolveRef(ns, "y").has_value());
    const auto greatest = listGreatestLogIdForTest(*backend, layout, ns);
    ASSERT_TRUE(greatest.has_value());
    EXPECT_EQ(greatest->writer_epoch, store->liveWriterEpoch());
}

/// C1 residual: a flush leader that passed the top-of-flush gate BEFORE a self-remount and stalled
/// mid-flush (here parked at the pre-carve hook, post-top-gate / pre-allocate) across the whole
/// fence-loss + remount window must NOT, on resume, allocate an id and PUT a transaction validated
/// against its now-stale detached cache. The pre-allocate `superseded_by_remount` re-check fails it
/// closed: the caller gets the failure and no backend `_log` object is created.
TEST(CASRefWriterRemount, SupersededLeaderMidFlushFailsClosedCreatesNoObject)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    /// This test parks a flush leader (`leader_active` stays true) across the ENTIRE `tryRemountOnce`
    /// call below by construction (`release` is only set AFTER `tryRemountOnce` returns). Keep the
    /// request budget at the file's usual tiny-wedge-test values so every bounded wait on that path
    /// stays well under a second.
    CasRequestBudget budget;
    budget.attempt_timeout_ms = 100;
    budget.lease_safety_margin_ms = 100;
    uint64_t fake_boot = 0;
    PoolConfig config;
    config.cas_request_budget = budget;
    config.boot_ms_fn = [&fake_boot] { return fake_boot; };
    config.wait_sleep_fn = [&fake_boot](uint64_t ms) { fake_boot += ms; };
    auto store = openPoolWithConfig(backend, config);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/remount_midflush"};
    publishEmptyPart(store, ns, "x");
    const auto greatest_before = listGreatestLogIdForTest(*backend, layout, ns);
    ASSERT_TRUE(greatest_before.has_value());

    /// Park the next flush leader at the pre-carve hook (post-top-gate, pre-allocate). Fires once.
    std::mutex m;
    std::condition_variable cv;
    bool entered = false;
    bool release = false;
    std::atomic<bool> hook_fired{false};
    store->setRefPreCarveHookForTest([&]
    {
        if (hook_fired.exchange(true))
            return;
        std::unique_lock<std::mutex> lk(m);
        entered = true;
        cv.notify_all();
        cv.wait(lk, [&] { return release; });
    });

    auto fut = std::async(std::launch::async, [&]() -> std::string
    {
        try { store->dropRef(ns, "x"); return "committed"; }
        catch (const DB::Exception & e) { return e.message(); }
    });
    { std::unique_lock<std::mutex> lk(m); cv.wait(lk, [&] { return entered; }); }   /// leader parked mid-flush

    /// The remount completes while the leader is parked (the quiesce does not wait for leaders); it marks
    /// the table superseded and re-arms the fence.
    fenceOutRefMount(*backend, layout.mountKey("test"));
    ASSERT_TRUE(store->tryRemountOnce());

    /// Unpark: the leader resumes, re-checks the flag before allocating, and fails closed.
    { std::lock_guard<std::mutex> lk(m); release = true; }
    cv.notify_all();

    ASSERT_EQ(fut.wait_for(std::chrono::seconds(10)), std::future_status::ready)
        << "the superseded leader hung instead of failing closed";
    const std::string result = fut.get();
    EXPECT_NE(result.find("superseded by a self-remount"), std::string::npos)
        << "expected a superseded fail-closed, got: " << result;

    /// No new ref-log object was created: the greatest durable log id is unchanged.
    const auto greatest_after = listGreatestLogIdForTest(*backend, layout, ns);
    ASSERT_TRUE(greatest_after.has_value());
    EXPECT_EQ(*greatest_after, *greatest_before)
        << "a superseded leader must allocate no id and PUT no object";
}

/// ===================================================================================
/// Task 11: namespace removal (spec §Namespace Removal)
/// ===================================================================================

/// A cached writer paused after its ordinary gates must re-check the exact catalog life immediately
/// before id allocation. A concurrent `Live -> Removing` transition therefore admits no late owner.
TEST(CASRefWriterNamespaceRemoval, CachedPositiveWriterCannotAppendAfterRemovingIsPublished)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/removing_blocks_cached_writer"};
    publishEmptyPart(store, ns, "existing");

    const CasRefCatalog::Snapshot before = CasRefCatalog::read(*backend, layout);
    const auto observed = std::find_if(before.catalog.entries.begin(), before.catalog.entries.end(),
        [&](const CatalogEntry & entry) { return entry.ns == ns; });
    ASSERT_NE(observed, before.catalog.entries.end());
    ASSERT_EQ(observed->state, NsState::Live);
    const CatalogEntry & exact_live = *observed;
    const auto greatest_before = listGreatestLogIdForTest(*backend, layout, ns);
    ASSERT_TRUE(greatest_before);

    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false;
    bool release = false;
    std::atomic<bool> hook_fired{false};
    store->setRefPreCarveHookForTest([&]
    {
        if (hook_fired.exchange(true))
            return;
        std::unique_lock lock(mutex);
        entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release; });
    });

    auto writer = std::async(std::launch::async, [&]() -> String
    {
        try
        {
            publishEmptyPart(store, ns, "late");
            return "committed";
        }
        catch (const DB::Exception & e)
        {
            return e.message();
        }
    });

    bool writer_parked = false;
    {
        std::unique_lock lock(mutex);
        writer_parked = cv.wait_for(lock, std::chrono::seconds(10), [&] { return entered; });
    }
    if (writer_parked)
    {
        CasRefCatalog::casUpdate(*backend, layout, [&](const RefCatalog & current)
        {
            RefCatalog next = current;
            const auto it = std::find(next.entries.begin(), next.entries.end(), exact_live);
            if (it == next.entries.end())
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "exact Live row changed during test transition");
            it->state = NsState::Removing;
            it->removal_started_round = 0;
            return next;
        });
    }
    {
        std::lock_guard lock(mutex);
        release = true;
    }
    cv.notify_all();

    EXPECT_TRUE(writer_parked) << "cached writer did not reach the deterministic pre-carve seam";
    ASSERT_EQ(writer.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    const String result = writer.get();
    store->setRefPreCarveHookForTest(nullptr);

    EXPECT_NE(result, "committed") << "cached positive ownership appended after `Removing` became visible";
    EXPECT_FALSE(store->resolveRef(ns, "late"));
    EXPECT_EQ(listGreatestLogIdForTest(*backend, layout, ns), greatest_before);
}

/// dropNamespace's ONE body transaction names an exact removal for every committed ref AND every
/// dangling precommit, with `remove_namespace` as the FINAL op -- never any other shape.
TEST(CASRefWriterNamespaceRemoval, TxnNamesEveryOwnerThenRemoveNamespace)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/remove_shape"};

    publishEmptyPart(store, ns, "committed_1");
    publishEmptyPart(store, ns, "committed_2");
    /// One precommit left dangling (never promoted) so the removal txn must ALSO name it.
    auto build = startBuildFor(store, ns, "dangling");
    const ManifestId dangling_id = build->stageManifest({});
    build->precommitAdd(ns, "dangling", dangling_id);

    store->dropNamespace(ns);

    /// The newest `_log/` object for `ns` is the removal transaction.
    std::optional<RefTxnId> newest_log;
    {
        String cursor;
        for (;;)
        {
            const ListPage page = backend->list(layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)), cursor, 1000);
            for (const ListedKey & lk : page.keys)
            {
                const auto parsed = layout.parseRefObjectKey(lk.key);
                if (parsed && parsed->life_id == DB::Cas::tests::fixture::fixtureLife(ns).incarnation && parsed->kind == RefObjectKind::Log
                    && (!newest_log || *newest_log < parsed->txn_id))
                    newest_log = parsed->txn_id;
            }
            if (page.next_cursor.empty())
                break;
            cursor = page.next_cursor;
        }
    }
    ASSERT_TRUE(newest_log.has_value());
    const auto got = backend->get(layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), *newest_log));
    ASSERT_TRUE(got.has_value());
    const RefLogTxn removal_txn = decodeRefLogTxn(openObject(FormatId::RefLog, got->bytes), ns.string(), *newest_log);

    ASSERT_FALSE(removal_txn.ops.empty());
    EXPECT_EQ(removal_txn.ops.back().kind, RefOpKind::RemoveNamespace);
    size_t owner_removals = 0;
    for (size_t i = 0; i + 1 < removal_txn.ops.size(); ++i)
    {
        const RefOp & op = removal_txn.ops[i];
        EXPECT_EQ(op.kind, RefOpKind::OwnerTransition);
        EXPECT_TRUE(op.old_binding.has_value());
        EXPECT_FALSE(op.new_binding.has_value());
        ++owner_removals;
    }
    EXPECT_EQ(owner_removals, 3u) << "2 committed + 1 dangling precommit";
}

/// The terminal transaction is the only durable removal record. Generation 7 never publishes a
/// terminal `Removed` snapshot; the ordinary cleanup/janitor paths own old immutable stream debris.
TEST(CASRefWriterNamespaceRemoval, RemovalPublishesTerminalLogWithoutTerminalSnapshot)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/remove_snapshot"};

    publishEmptyPart(store, ns, "a");
    const auto snapshot_before = store->newestPublishedSnapshotIdForTest(ns);
    store->dropNamespace(ns);

    EXPECT_EQ(store->newestPublishedSnapshotIdForTest(ns), snapshot_before)
        << "removal must not publish a terminal snapshot";
    EXPECT_GT(store->tailSinceSnapshotCountForTest(ns), 0u)
        << "the terminal transaction remains ordinary immutable stream work until GC folds it";

    size_t terminal_logs = 0;
    for (const ListedKey & listed : backend->list(layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)), "", 1000).keys)
    {
        const auto parsed = layout.parseRefObjectKey(listed.key);
        if (!parsed || parsed->kind != RefObjectKind::Log)
            continue;
        const auto got = backend->get(listed.key);
        ASSERT_TRUE(got.has_value());
        const RefLogTxn txn = decodeRefLogTxn(openObject(FormatId::RefLog, got->bytes), ns.string(), parsed->txn_id);
        if (!txn.ops.empty() && txn.ops.back().kind == RefOpKind::RemoveNamespace)
            ++terminal_logs;
    }
    EXPECT_EQ(terminal_logs, 1u);
}

/// Review fix (prerequisite to this task's dropNamespace rewiring): `flushRefBatch`'s per-item
/// validation previously previewed each op as its OWN single-op trial transaction, so a
/// whole-transaction-shape rule ("remove_namespace must be the FINAL op") trivially passed on every
/// singleton slice regardless of an item's REAL combined shape -- a malformed item would only have
/// been caught by the post-persist apply, AFTER its transaction object was already durable (bricking
/// the table on every future recovery and permanently wedging this table's lane). Drives
/// `appendRefOps` directly with a deliberately malformed multi-op item (remove_namespace not last) to
/// prove the whole-item shape check now rejects it BEFORE any backend object is created.
TEST(CASRefWriterNamespaceRemoval, MalformedShapeWithRemoveNamespaceNotFinalRejectedBeforeAnyCreate)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/malformed_shape"};
    publishEmptyPart(store, ns, "a");   /// births the table so the malformed item isn't ALSO rejected
                                        /// for the unrelated reason "namespace_birth was needed first"

    const uint64_t put_before = backend->putTotal();
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        store->appendRefOps(ns, MutationScope::wholeShard(),
            [](const RefTableState &) -> std::vector<RefOp>
            {
                RefOp remove_ns_1;
                remove_ns_1.kind = RefOpKind::RemoveNamespace;
                RefOp remove_ns_2;
                remove_ns_2.kind = RefOpKind::RemoveNamespace;
                return {remove_ns_1, remove_ns_2};   /// remove_namespace NOT the final op -- malformed
            },
            /// Deliberately mislabel the malformed terminal as an ordinary mutation: this bypasses
            /// the public removal-capability preflight and proves the txn-wide shape check itself
            /// rejects the object before the later capability check or any backend mutation.
            RootMutationOrigin::Writer, RootMutationKind::Publish);
    });

    EXPECT_EQ(backend->putTotal(), put_before) << "the malformed shape must be rejected before any object is created";
    ASSERT_TRUE(store->resolveRef(ns, "a").has_value()) << "the malformed attempt left no trace on the table";
}

/// A caller cannot turn the generic append surface into a second namespace-removal capability, even
/// when it disguises terminal operations as an ordinary mutation kind. Only `dropNamespace` may carry
/// the exact runtime ownership established by the durable `Live -> Removing` transition.
TEST(CASRefWriterNamespaceRemoval, GenericAppendCannotWriteTerminalWhileCatalogIsLive)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/unauthorized_terminal"};
    publishEmptyPart(store, ns, "owned");
    const CatalogEntry live = catalogEntryOrThrow(*backend, layout, ns);
    ASSERT_EQ(live.state, NsState::Live);
    const auto greatest_before = listGreatestLogIdForLifeForTest(
        *backend, layout, NamespaceLifeId::fromCatalogEntry(ns, live.incarnation));
    ASSERT_TRUE(greatest_before);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        store->appendRefOps(ns, MutationScope::wholeShard(),
            [](const RefTableState & state) -> std::vector<RefOp>
            {
                std::vector<RefOp> ops;
                for (const auto [ref_name, row] : state.getCommitted())
                {
                    RefOp remove_owner;
                    remove_owner.kind = RefOpKind::OwnerTransition;
                    remove_owner.old_binding = RefOwnerBinding{
                        RefOwnerKind::Committed, ref_name, row.manifest_ref};
                    ops.push_back(std::move(remove_owner));
                }
                RefOp terminal;
                terminal.kind = RefOpKind::RemoveNamespace;
                ops.push_back(terminal);
                return ops;
            },
            RootMutationOrigin::Writer, RootMutationKind::Publish,
            /*skip_stale_precommit_sweep=*/true);
    });

    EXPECT_EQ(catalogEntryOrThrow(*backend, layout, ns), live);
    EXPECT_EQ(listGreatestLogIdForLifeForTest(
        *backend, layout, NamespaceLifeId::fromCatalogEntry(ns, live.incarnation)), greatest_before)
        << "an unauthorized terminal must allocate no id and create no ref-log object";
    EXPECT_TRUE(store->resolveRef(ns, "owned"));
}

/// The public generic surface must reject the terminal-capable operation kind before resolving or
/// creating a life. Otherwise an absent name can acquire a catalog row and checkpoint before the
/// internal terminal capability check rejects the actual operations.
TEST(CASRefWriterNamespaceRemoval, GenericTerminalOnAbsentNamePerformsZeroDurableMutation)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/absent_unauthorized_terminal"};
    const CasRefCatalog::Snapshot catalog_before = CasRefCatalog::read(*backend, store->layout());

    backend->resetCounts();
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        store->appendRefOps(ns, MutationScope::wholeShard(),
            [](const RefTableState &) -> std::vector<RefOp>
            {
                RefOp terminal;
                terminal.kind = RefOpKind::RemoveNamespace;
                return {terminal};
            },
            RootMutationOrigin::Writer, RootMutationKind::DropNamespace,
            /*skip_stale_precommit_sweep=*/true);
    });

    EXPECT_EQ(backend->putTotal(), 0u);
    EXPECT_EQ(backend->putOverwriteTotal(), 0u);
    EXPECT_EQ(backend->casPutTotal(), 0u);
    EXPECT_EQ(backend->deleteTotal(), 0u);
    const CasRefCatalog::Snapshot catalog_after = CasRefCatalog::read(*backend, store->layout());
    EXPECT_EQ(catalog_after.token, catalog_before.token);
    EXPECT_EQ(catalog_after.catalog, catalog_before.catalog);
    EXPECT_FALSE(store->refTableLifeForTest(ns));
}

/// A namespace file births a catalog life and checkpoint without necessarily creating a ref stream.
/// Removing that table must still publish terminal evidence and let GC retire the catalog row.
TEST(CASRefWriterNamespaceRemoval, CatalogedNamespaceFilesOnlyLifeCompletesRemoval)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    PoolConfig config;
    config.gc_fold_threshold = 1;
    config.gc_fold_max_defer_rounds = 0;
    auto store = openPoolWithConfig(backend, config);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/files_only"};
    const NamespaceLifeId life = store->namespaceLife(ns);
    store->putNamespaceFile(life, "format_version.txt", "1\n");
    ASSERT_TRUE(backend->list(layout.namespaceStreamPrefix(life), "", 100).keys.empty());
    ASSERT_EQ(catalogEntryOrThrow(*backend, layout, ns).state, NsState::Live);

    EXPECT_NO_THROW(store->dropNamespace(ns));
    ASSERT_EQ(catalogEntryOrThrow(*backend, layout, ns).state, NsState::Removing);

    const ListPage terminal_page = backend->list(layout.namespaceStreamPrefix(life), "", 100);
    ASSERT_EQ(terminal_page.keys.size(), 1u);
    const auto parsed = layout.parseRefObjectKey(terminal_page.keys.front().key);
    ASSERT_TRUE(parsed);
    const auto terminal_body = backend->get(terminal_page.keys.front().key);
    ASSERT_TRUE(terminal_body);
    const RefLogTxn terminal = decodeRefLogTxn(
        openObject(FormatId::RefLog, terminal_body->bytes), ns.string(), parsed->txn_id);
    ASSERT_EQ(terminal.ops.size(), 2u);
    EXPECT_EQ(terminal.ops[0].kind, RefOpKind::NamespaceBirth);
    EXPECT_EQ(terminal.ops[1].kind, RefOpKind::RemoveNamespace);

    Gc gc(store, UInt128{181});
    ASSERT_FALSE(runRegularRoundReclaiming(gc).deferred);
    (void)runRegularRoundReclaiming(gc);
    const RefCatalog after = CasRefCatalog::read(*backend, layout).catalog;
    EXPECT_TRUE(std::none_of(after.entries.begin(), after.entries.end(), [&](const CatalogEntry & entry)
    {
        return entry.ns == ns;
    }));
}

/// If the first catalog read after closing the positive lane fails, the catch-side authoritative read
/// is still allowed to prove the exact original `Live` row and reopen admission.
TEST(CASRefWriterNamespaceRemoval, PredurableCatalogReadFailureReopensExactLiveLane)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/predurable_read_failure"};
    publishEmptyPart(store, ns, "owned");
    const CatalogEntry live = catalogEntryOrThrow(*backend, layout, ns);

    backend->catalog_fault_key = layout.refCatalogKey();
    backend->catalog_gets_before_fault = 1;   /// initial discovery succeeds; post-close observation fails
    backend->catalog_get_fault_count = 1;
    EXPECT_THROW(store->dropNamespace(ns), std::runtime_error);
    EXPECT_EQ(catalogEntryOrThrow(*backend, layout, ns), live);

    EXPECT_NO_THROW(store->updateRefPublishedAt(ns, "owned", [](RefPublishedAtUpdate & update)
    {
        update.published_at_ms = 17;
    })) << "a fresh exact Live observation must reopen the lane after a pre-durable failure";
    EXPECT_EQ(catalogEntryOrThrow(*backend, layout, ns).state, NsState::Live);
}

/// spec §Namespace Removal (writer, line 666): "After the transaction is durable, it applies the same
/// operations to memory, cancels local builds, and rejects further ordinary mutations." An in-flight
/// build for the removed namespace must be cancelled once (and only once) the removal is durable: its
/// next operation throws (ABORTED, from requireAlive) rather than promoting a fresh committed ref into
/// the just-removed namespace.
TEST(CASRefWriterNamespaceRemoval, DropNamespaceCancelsInFlightBuildAndNextOpThrows)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/remove_cancels_build"};

    publishEmptyPart(store, ns, "committed");   /// births the table + one committed ref

    /// An in-flight build for ns: staged + precommit-added, never promoted.
    auto build = startBuildFor(store, ns, "inflight");
    const ManifestId id = build->stageManifest({});
    build->precommitAdd(ns, "inflight", id);

    store->dropNamespace(ns);

    /// The build is cancelled: EVERY subsequent operation fails fast at `requireAlive` with
    /// NETWORK_ERROR (fix #37 phase 2's CAS write-retry-later reroute). `stageManifest` is the
    /// discriminator -- it has NO namespace-lifecycle gate, so an UN-cancelled build would happily
    /// execute it (staging more debris into a dead namespace); only cancellation stops it.
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { build->stageManifest({}); });
    /// And it certainly cannot promote a fresh committed ref into the removed namespace (the important
    /// invariant -- though the old WPromote "precommit removed" guard also blocked this, less directly).
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { build->promote(ns, "inflight", build->buildId(), id); });

    /// The cancelled build did not recreate anything in the removed namespace.
    EXPECT_FALSE(store->resolveRef(ns, "inflight").has_value());
    EXPECT_FALSE(store->resolveRef(ns, "committed").has_value()) << "the whole namespace was removed";
}

/// The catalog transition precedes the terminal append. If that append is unresolved, the namespace
/// remains `Removing`, positive ownership is refused, and a retry of the same removal resolves the wedge.
TEST(CASRefWriterNamespaceRemoval, RemovalAppendFailureLeavesRemovingAndRetryCompletes)
{
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = kSingleAttemptDeadlineMs;
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend, budget);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/remove_fault_keeps_build"};

    publishEmptyPart(store, ns, "committed");

    auto build = startBuildFor(store, ns, "inflight");
    const ManifestId id = build->stageManifest({});
    build->precommitAdd(ns, "inflight", id);

    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->fault_count = 1;

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropNamespace(ns); });

    EXPECT_EQ(catalogEntryOrThrow(*backend, layout, ns).state, NsState::Removing);
    EXPECT_FALSE(store->resolveRef(ns, "committed"))
        << "a fresh name lookup must not expose a catalog-Removing life";
    /// The build was NOT cancelled: a non-append operation (`stageManifest` -- it never touches the now
    /// wedged ref-append lane) still succeeds; it would throw ABORTED had the build been cancelled.
    EXPECT_NO_THROW(build->stageManifest({}));
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        build->promote(ns, "inflight", build->buildId(), id);
    });

    EXPECT_NO_THROW(store->dropNamespace(ns));
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { build->stageManifest({}); });
}

/// `namespaceStillLogicallyPresent` must stay `true` for the entire window between the catalog's
/// durable `Live -> Removing` transition and the terminal `remove_namespace` append actually landing --
/// the crash-shaped case the fix exists for. Reuses the injected stream-write fault shape from
/// `RemovalAppendFailureLeavesRemovingAndRetryCompletes`.
TEST(CASRefWriterNamespaceRemoval, PresenceProbeStaysTrueThroughRemovingUntilTerminalRetrySucceeds)
{
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = kSingleAttemptDeadlineMs;
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend, budget);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/presence_removing_no_terminal"};

    publishEmptyPart(store, ns, "committed");
    EXPECT_TRUE(store->namespaceStillLogicallyPresent(ns));

    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->fault_count = 1;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropNamespace(ns); });

    ASSERT_EQ(catalogEntryOrThrow(*backend, layout, ns).state, NsState::Removing);
    EXPECT_TRUE(store->namespaceStillLogicallyPresent(ns))
        << "the catalog transitioned but the terminal append never landed -- cleanup is unproven";

    EXPECT_NO_THROW(store->dropNamespace(ns));
    EXPECT_FALSE(store->namespaceStillLogicallyPresent(ns))
        << "the retried removal's terminal is now durable";
}

/// A `Creating` row is conservative in both directions -- present, and removal refuses to cancel it
/// while its creator fence cannot be proven dead, then succeeds once a terminal certificate (here, a
/// GC-fenced lease for the same server root) makes the fence provably terminal.
TEST(CASRefWriterNamespaceRemoval, PresenceProbeCreatingIsPresentAndRemovalWaitsForCreatorFenceTerminality)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/presence_still_creating"};

    CatalogEntry entry;
    entry.ns = ns;
    entry.state = NsState::Creating;
    entry.incarnation = UInt128(99);
    entry.creator = CreatorFence{.server_root_id = "srv1", .writer_epoch = store->liveWriterEpoch(), .fence_generation = 1};
    CasRefCatalog::casAdmitEntry(*backend, layout, 1, entry);

    EXPECT_TRUE(store->namespaceStillLogicallyPresent(ns));

    /// The creator fence names an unmounted server root: `isCreatorFenceTerminal` cannot certify it
    /// dead (absence proves nothing), so removal fails closed rather than cancelling a `Creating` row a
    /// live writer might still publish into.
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropNamespace(ns); });
    EXPECT_EQ(catalogEntryOrThrow(*backend, layout, ns).state, NsState::Creating);
    EXPECT_TRUE(store->namespaceStillLogicallyPresent(ns));

    /// Publish a GC-fenced lease for the SAME server root -- one of `isCreatorFenceTerminal`'s accepted
    /// certificates -- and removal now cancels the row outright (no `Removing` transition for a
    /// namespace that never reached `Live`).
    MountLease dead;
    dead.writer_epoch = store->liveWriterEpoch();
    dead.gc_fenced = true;
    dead.seq = 1;
    backend->putIfAbsent(layout.mountKey("srv1"), encodeMountLease(dead));

    EXPECT_NO_THROW(store->dropNamespace(ns));
    EXPECT_FALSE(store->namespaceStillLogicallyPresent(ns));
}

/// A "no catalog row" observation must never be turned into `false` by a race. Pausing the probe
/// right after its first catalog read and admitting a fresh `Creating` row before it resumes must
/// answer present -- the second read sees the born row; a stale absent answer is the one forbidden
/// outcome. A namespace absent from both reads legitimately settles on absent.
TEST(CASRefWriterNamespaceRemoval, PresenceProbeNoRowObservationRevalidatesRatherThanRacingToAbsent)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace racing{"srv1/presence_no_row_races_birth"};
    const RootNamespace stable{"srv1/presence_no_row_stays_absent"};

    std::mutex mutex;
    std::condition_variable cv;
    bool paused = false;
    bool resume = false;
    store->setNamespacePresenceProbeAfterFirstReadHookForTest([&]
    {
        std::unique_lock lock(mutex);
        paused = true;
        cv.notify_all();
        cv.wait(lock, [&] { return resume; });
    });

    std::exception_ptr raced_error;
    bool raced_answer = false;
    std::thread racer([&]
    {
        try
        {
            raced_answer = store->namespaceStillLogicallyPresent(racing);
        }
        catch (...)
        {
            raced_error = std::current_exception();
        }
    });
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return paused; });
    }

    CatalogEntry born;
    born.ns = racing;
    born.state = NsState::Creating;
    born.incarnation = UInt128(1234);
    born.creator = CreatorFence{.server_root_id = "srv1", .writer_epoch = store->liveWriterEpoch(), .fence_generation = 1};
    CasRefCatalog::casAdmitEntry(*backend, layout, 1, born);

    {
        std::lock_guard lock(mutex);
        resume = true;
    }
    cv.notify_all();
    racer.join();
    store->setNamespacePresenceProbeAfterFirstReadHookForTest(nullptr);

    if (raced_error)
        std::rethrow_exception(raced_error);
    EXPECT_TRUE(raced_answer) << "a namespace born after the first read must never resolve to a stale absent";

    /// Negative control: an unraced, genuinely absent namespace settles on `false`.
    EXPECT_FALSE(store->namespaceStillLogicallyPresent(stable));
}

/// The starvation regression: proving THIS row absent must not require the WHOLE catalog to hold
/// still. An unrelated namespace admitted between the probe's two reads changes the catalog token and
/// content, yet the target -- absent from both reads -- settles on `false` instead of a retry storm
/// (observed live as a 193/194 retry-later loop under a parallel workload sharing one pool).
TEST(CASRefWriterNamespaceRemoval, PresenceProbeIgnoresUnrelatedCatalogChurnBetweenReads)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const Layout & layout = store->layout();
    const RootNamespace target{"srv1/presence_churn_target_stays_absent"};
    const RootNamespace unrelated{"srv1/presence_churn_unrelated_born"};

    std::mutex mutex;
    std::condition_variable cv;
    bool paused = false;
    bool resume = false;
    store->setNamespacePresenceProbeAfterFirstReadHookForTest([&]
    {
        std::unique_lock lock(mutex);
        paused = true;
        cv.notify_all();
        cv.wait(lock, [&] { return resume; });
    });

    std::exception_ptr probe_error;
    bool answer = true;
    std::thread prober([&]
    {
        try
        {
            answer = store->namespaceStillLogicallyPresent(target);
        }
        catch (...)
        {
            probe_error = std::current_exception();
        }
    });
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return paused; });
    }

    CatalogEntry born;
    born.ns = unrelated;
    born.state = NsState::Creating;
    born.incarnation = UInt128(5678);
    born.creator = CreatorFence{.server_root_id = "srv1", .writer_epoch = store->liveWriterEpoch(), .fence_generation = 1};
    CasRefCatalog::casAdmitEntry(*backend, layout, 1, born);

    {
        std::lock_guard lock(mutex);
        resume = true;
    }
    cv.notify_all();
    prober.join();
    store->setNamespacePresenceProbeAfterFirstReadHookForTest(nullptr);

    if (probe_error)
        std::rethrow_exception(probe_error);
    EXPECT_FALSE(answer) << "unrelated churn between the two reads must not force a retry or a wrong present";
}

/// Every unreadable or ambiguous observation must throw, never answer `false`. Covers a catalog `GET`
/// failure on the probe's very first read, and a lost mount fence discovered mid-probe. Not covered
/// here: a missing checkpoint for a `Removing` row, and an ambiguous incarnation -- both would need a
/// raw-catalog-write test helper this suite does not currently expose.
TEST(CASRefWriterNamespaceRemoval, PresenceProbeCatalogReadFailurePropagatesRatherThanAnsweringAbsent)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/presence_catalog_read_fault"};

    backend->catalog_fault_key = store->layout().refCatalogKey();
    backend->catalog_gets_before_fault = 0;
    backend->catalog_get_fault_count = 1;
    EXPECT_THROW((void)store->namespaceStillLogicallyPresent(ns), std::runtime_error);
}

TEST(CASRefWriterNamespaceRemoval, PresenceProbeFenceLossPropagatesRatherThanAnsweringAbsent)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/presence_fence_loss"};
    publishEmptyPart(store, ns, "x");

    const String mount_key = store->layout().mountKey("test");
    const auto got = backend->get(mount_key);
    ASSERT_TRUE(got);
    MountLease foreign = decodeMountLease(got->bytes);
    foreign.server_uuid = foreign.server_uuid + UInt128{1};
    foreign.seq += 1;
    ASSERT_EQ(backend->putOverwrite(mount_key, encodeMountLease(foreign), got->token).outcome, PutOutcome::Done);
    store->tripMountLost();

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)store->namespaceStillLogicallyPresent(ns); });
}

/// Pin all three facade states against each other on one namespace -- `Live` (present, content
/// readable), incomplete `Removing` (present, content deliberately unreadable), and terminal `Removing`
/// (absent, immediately, no GC).
TEST(CASRefWriterNamespaceRemoval, PresenceProbeFacadeConsistencyAcrossRemovalLifecycle)
{
    CasRequestBudget budget;
    budget.max_attempts = 1;
    budget.attempt_timeout_ms = 100;
    budget.operation_deadline_ms = kSingleAttemptDeadlineMs;
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend, budget);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/presence_facade_consistency"};

    publishEmptyPart(store, ns, "committed");
    EXPECT_TRUE(store->namespaceStillLogicallyPresent(ns));
    EXPECT_FALSE(store->listRefs(ns).empty());

    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->fault_count = 1;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropNamespace(ns); });

    EXPECT_TRUE(store->namespaceStillLogicallyPresent(ns))
        << "present for cleanup, even though content below is about to prove unreadable";
    EXPECT_FALSE(store->namespaceFilesLifeIfReadable(ns).has_value());
    EXPECT_FALSE(store->resolveRef(ns, "committed").has_value());

    EXPECT_NO_THROW(store->dropNamespace(ns));
    EXPECT_FALSE(store->namespaceStillLogicallyPresent(ns));
    EXPECT_FALSE(store->namespaceFilesLifeIfReadable(ns).has_value());
}

/// Fix-verify review finding: `namespaceStillLogicallyPresent`'s `Removing` branch proved the OBSERVED
/// incarnation's terminal and returned `false` for the name without re-checking whether the catalog had
/// moved on since. Proving one incarnation terminal is not proof the CURRENT logical namespace is
/// absent: GC can delete the now-terminal row and a successor can be born under the same name while the
/// probe's own recovery call (real I/O, no upper bound) is still in flight. Drives that exact
/// interleaving deterministically via the terminal-proven hook: pause right after the predecessor's
/// terminal is proven, drain GC to actually delete its row, birth a successor under the same name, then
/// resume and require `true` (present) -- never the stale `false` the unfixed probe would answer.
TEST(CASRefWriterNamespaceRemoval, PresenceProbeRevalidatesAfterTerminalProvenRatherThanRacingToStaleAbsent)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    PoolConfig config;
    config.gc_fold_threshold = 1;
    config.gc_fold_max_defer_rounds = 8;
    config.ref_table_cache_bytes = 0;
    auto store = openPoolWithConfig(backend, config);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/presence_terminal_revalidate"};
    const UInt128 gc_id = hexToU128("00000000000000000000000000000001");

    const auto catalog_entry = [&]() -> std::optional<CatalogEntry>
    {
        const RefCatalog catalog = CasRefCatalog::read(*backend, layout).catalog;
        const auto it = std::find_if(catalog.entries.begin(), catalog.entries.end(), [&](const CatalogEntry & entry)
        {
            return entry.ns == ns;
        });
        return it == catalog.entries.end() ? std::nullopt : std::optional<CatalogEntry>{*it};
    };

    /// `publishEmptyPart` pins its catalog entry to `fixture::fixtureLife(ns)`, a life derived from
    /// `ns` alone -- the SAME value every time for the same name, which is exactly wrong for a test
    /// whose whole point is that the successor's incarnation must differ from the predecessor's.
    /// `publishWithProductionBirth` goes through the real birth path (`resolveNamespaceLife`'s random
    /// mint), so both incarnations below are independently random.
    publishWithProductionBirth(store, ns, "predecessor");
    const std::optional<CatalogEntry> predecessor = catalog_entry();
    ASSERT_TRUE(predecessor.has_value());

    Gc gc(store, gc_id);
    store->dropNamespace(ns);   /// terminal durable; catalog row still Removing until GC deletes it

    /// Pause the probe right after it proves the predecessor's terminal, before its revalidation read.
    std::mutex mutex;
    std::condition_variable cv;
    bool paused = false;
    bool resume = false;
    store->setNamespacePresenceProbeAfterTerminalProvenHookForTest([&]
    {
        std::unique_lock lock(mutex);
        paused = true;
        cv.notify_all();
        cv.wait(lock, [&] { return resume; });
    });

    std::optional<bool> probe_result;
    std::exception_ptr probe_error;
    std::thread prober([&]
    {
        try
        {
            probe_result = store->namespaceStillLogicallyPresent(ns);
        }
        catch (...)
        {
            probe_error = std::current_exception();
        }
    });
    /// A fatal assertion below (predecessor/successor state, GC round shape) must not skip joining
    /// `prober` -- it is still blocked on `cv` at that point, and destructing a joinable `std::thread`
    /// calls `std::terminate`, aborting the whole binary and hiding every test queued after this one.
    bool prober_joined = false;
    SCOPE_EXIT({
        if (!prober_joined)
        {
            {
                std::lock_guard lock(mutex);
                resume = true;
            }
            cv.notify_all();
            prober.join();
            store->setNamespacePresenceProbeAfterTerminalProvenHookForTest(nullptr);
        }
    });
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return paused; });
    }

    /// While the probe is paused: drain GC to actually delete the predecessor's catalog row (fold the
    /// terminal, then a drain-only round to adopt the evidence and delete the row -- same two-round
    /// shape `SameNameSameWriterEpochRebirth...` uses), then birth a successor under the SAME name. The
    /// row must be gone before a fresh creation is admitted at all, so this also proves the row really
    /// was deleted, not merely that the test raced ahead of production's own invariants.
    ASSERT_FALSE(runRegularRoundReclaiming(gc).deferred) << "the terminal delta must fold";
    ASSERT_TRUE(runRegularRoundReclaiming(gc).deferred) << "the drain-only round must adopt the evidence seal";
    ASSERT_FALSE(catalog_entry().has_value()) << "control: the predecessor's row is really gone before rebirth";
    publishWithProductionBirth(store, ns, "successor");
    const std::optional<CatalogEntry> successor = catalog_entry();
    ASSERT_TRUE(successor.has_value());
    ASSERT_NE(successor->incarnation, predecessor->incarnation);

    {
        std::lock_guard lock(mutex);
        resume = true;
    }
    cv.notify_all();
    prober.join();
    store->setNamespacePresenceProbeAfterTerminalProvenHookForTest(nullptr);
    prober_joined = true;

    ASSERT_FALSE(probe_error) << "a successor born under the same name must never surface as an error";
    ASSERT_TRUE(probe_result.has_value());
    EXPECT_TRUE(*probe_result)
        << "the predecessor's proven terminal must not answer false once a successor occupies its name";
}

/// `StorageJoin`/`StorageSet::truncate` call `disk->removeRecursive(path)` then `disk->createDirectories
/// (path)`. `createDirectories` is a CAS no-op (`ContentAddressedTransaction::createDirectory` only
/// checks write admission, it never touches the catalog), so the actual re-mint happens lazily on the
/// FIRST subsequent write, which resolves through `namespaceLife` exactly like this test does directly.
/// Right after `TRUNCATE` the catalog row is still `Removing` -- GC has not yet folded and deleted it --
/// so that first write throws a typed retry-later error rather than silently wedging or corrupting
/// anything: the same self-healing window the presence-probe revalidation above depends on. Before the
/// `existsDirectory` fix, the directory never reported as present in the first place, so `TRUNCATE`
/// silently skipped `removeRecursive` entirely and the table kept its OLD contents -- a different,
/// quieter wrong answer than this one, not a newly introduced break.
TEST(CASRefWriterNamespaceRemoval, FilesOnlyNamespaceTruncateThrowsRetryLaterUntilGcReclaimsThenRebirths)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    PoolConfig config;
    config.gc_fold_threshold = 1;
    config.gc_fold_max_defer_rounds = 8;
    config.ref_table_cache_bytes = 0;
    auto store = openPoolWithConfig(backend, config);
    const RootNamespace ns{"srv1/truncate_retry_then_rebirth"};
    const UInt128 gc_id = hexToU128("00000000000000000000000000000002");

    /// Birth a files-only namespace: `StorageJoin`/`StorageSet` never publish a MergeTree part, their
    /// table root only ever carries plain table files (`putNamespaceFile`'s shape, not a manifest ref).
    const NamespaceLifeId predecessor_life = store->namespaceLife(ns);
    store->putNamespaceFile(predecessor_life, "data.bin", "predecessor-contents");

    Gc gc(store, gc_id);
    store->dropNamespace(ns);   /// the TRUNCATE-shaped removeRecursive: terminal durable, row still Removing

    /// The very next write CAS would attempt (the `createDirectories` no-op already ran; this is the
    /// first real write) must not be told the namespace is gone, and must not silently mint into a
    /// row still occupied by the predecessor -- it throws retry-later.
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)store->namespaceLife(ns); });

    /// Drain GC (fold the terminal, then a drain-only round to delete the now-evidenced row) -- same
    /// two-round shape the presence-probe revalidation test above uses.
    ASSERT_FALSE(runRegularRoundReclaiming(gc).deferred) << "the terminal delta must fold";
    ASSERT_TRUE(runRegularRoundReclaiming(gc).deferred) << "the drain-only round must adopt the evidence seal";

    /// Self-healed: the same logical name now mints a fresh incarnation and accepts writes again,
    /// exactly what a retried `INSERT` (or a retried `TRUNCATE`) after the CAS write's retry-later gets.
    const NamespaceLifeId successor_life = store->namespaceLife(ns);
    EXPECT_NE(successor_life.incarnation, predecessor_life.incarnation);
    store->putNamespaceFile(successor_life, "data.bin", "successor-contents");
    const auto successor_contents = store->getNamespaceFile(successor_life, "data.bin");
    ASSERT_TRUE(successor_contents.has_value());
    EXPECT_EQ(*successor_contents, "successor-contents");
}

/// Cancellation is namespace-scoped: dropping namespace N must not cancel an in-flight build targeting a
/// DIFFERENT namespace M -- that build promotes normally.
TEST(CASRefWriterNamespaceRemoval, DropNamespaceDoesNotCancelBuildsInOtherNamespaces)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns_dropped{"srv1/remove_me"};
    const RootNamespace ns_other{"srv1/keep_me"};

    publishEmptyPart(store, ns_dropped, "x");

    /// An in-flight build in a DIFFERENT namespace.
    auto other_build = startBuildFor(store, ns_other, "y");
    const ManifestId id = other_build->stageManifest({});
    other_build->precommitAdd(ns_other, "y", id);

    store->dropNamespace(ns_dropped);

    /// The other namespace's build is untouched: it promotes successfully and its ref resolves.
    EXPECT_NO_THROW(other_build->promote(ns_other, "y", other_build->buildId(), id));
    EXPECT_TRUE(store->resolveRef(ns_other, "y").has_value());
}

/// A writer-side create/resolution cannot reuse the predecessor while its catalog row is `Removing`.
/// Both the resident-runtime and fresh-runtime paths return typed retry-later without a durable write.
TEST(CASRefWriterNamespaceRemoval, CreateAgainstRemovingRetriesWithoutMutation)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const RootNamespace ns{"srv1/create-while-removing"};

    {
        auto store = openPool(backend);
        publishEmptyPart(store, ns, "predecessor");
        store->dropNamespace(ns);

        backend->resetCounts();
        expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)store->namespaceLife(ns); });
        EXPECT_EQ(backend->putTotal(), 0u);
        EXPECT_EQ(backend->putOverwriteTotal(), 0u);
        EXPECT_EQ(backend->casPutTotal(), 0u);
    }

    auto fresh_store = openPool(backend);
    backend->resetCounts();
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)fresh_store->namespaceLife(ns); });
    EXPECT_EQ(backend->putTotal(), 0u);
    EXPECT_EQ(backend->putOverwriteTotal(), 0u);
    EXPECT_EQ(backend->casPutTotal(), 0u);
}

/// Same-name rebirth must not inherit the predecessor's physical life or folded cursor even when the
/// writer mount and its per-name runtime stay resident throughout the complete real removal sequence.
TEST(CASRefWriterNamespaceRemoval, SameNameSameWriterEpochRebirthInvalidatesResidentLifeAndStartsAtZeroCoverage)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    PoolConfig config;
    config.gc_fold_threshold = 1;
    config.gc_fold_max_defer_rounds = 8;
    config.ref_table_cache_bytes = 0;   /// unbounded: no eviction can explain a fresh resolution
    auto store = openPoolWithConfig(backend, config);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/same-name-rebirth"};
    const UInt128 gc_id = hexToU128("00000000000000000000000000000001");

    const auto publish_without_fixture_admission = [&](const String & ref)
    {
        PartWriteInfo info;
        info.intended_namespace = ns;
        info.intended_ref = ns.string() + "/" + ref;
        auto build = store->beginPartWrite(info);
        const ManifestId id = build->stageManifest({});
        build->precommitAdd(ns, ref, id);
        build->promote(ns, ref, build->buildId(), id);
    };
    const auto catalog_entry = [&]() -> std::optional<CatalogEntry>
    {
        const RefCatalog catalog = CasRefCatalog::read(*backend, layout).catalog;
        const auto it = std::find_if(catalog.entries.begin(), catalog.entries.end(), [&](const CatalogEntry & entry)
        {
            return entry.ns == ns;
        });
        return it == catalog.entries.end() ? std::nullopt : std::optional<CatalogEntry>{*it};
    };

    publish_without_fixture_admission("predecessor");
    const CatalogEntry predecessor = *catalog_entry();
    const uint64_t writer_epoch = store->liveWriterEpoch();
    ASSERT_EQ(store->refTableLifeForTest(ns)->incarnation, predecessor.incarnation);
    const uint64_t runtime_identity = store->refTableRuntimeIdentityForTest(ns);
    ASSERT_NE(runtime_identity, 0u);

    Gc gc(store, gc_id);
    ASSERT_FALSE(runRegularRoundReclaiming(gc).deferred);
    GcState state = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    CasFoldSeal seal = decodeFoldSeal(backend->get(layout.foldSealKey(state.snap_generation, state.snap_attempt))->bytes);
    const auto predecessor_row = seal.ref_lives.find(predecessor.incarnation);
    ASSERT_NE(predecessor_row, seal.ref_lives.end());
    ASSERT_NE(predecessor_row->second.coverage.last_folded_ref_id, RefTxnId{});

    const uint64_t puts_before_drop = backend->putTotal();
    store->dropNamespace(ns);
    ASSERT_GT(backend->putTotal(), puts_before_drop)
        << "control: the real removal call returned after durably writing its terminal artifacts";
    std::optional<RefTxnId> terminal_id;
    for (const ListedKey & listed : backend->list(
        layout.namespaceStreamPrefix(NamespaceLifeId::fromCatalogEntry(ns, predecessor.incarnation)),
        "", 1000).keys)
    {
        const auto parsed = layout.parseRefObjectKey(listed.key);
        if (parsed && parsed->kind == RefObjectKind::Log
            && (!terminal_id || *terminal_id < parsed->txn_id))
            terminal_id = parsed->txn_id;
    }
    ASSERT_TRUE(terminal_id.has_value());
    const auto terminal_body = backend->get(layout.refLogKey(
        NamespaceLifeId::fromCatalogEntry(ns, predecessor.incarnation), *terminal_id));
    ASSERT_TRUE(terminal_body.has_value());
    const RefLogTxn terminal = decodeRefLogTxn(
        openObject(FormatId::RefLog, terminal_body->bytes), ns.string(), *terminal_id);
    ASSERT_FALSE(terminal.ops.empty());
    ASSERT_EQ(terminal.ops.back().kind, RefOpKind::RemoveNamespace)
        << "control: the newest old-life log is the production terminal record";
    const std::optional<CatalogEntry> removing = catalog_entry();
    ASSERT_TRUE(removing.has_value());
    ASSERT_EQ(removing->state, NsState::Removing)
        << "the real terminal returned durable, but its catalog row stayed Live";
    ASSERT_EQ(removing->incarnation, predecessor.incarnation);
    ASSERT_EQ(store->refTableRuntimeIdentityForTest(ns), runtime_identity)
        << "the removal path must invalidate the resident runtime's life, not pass through eviction";

    ASSERT_FALSE(runRegularRoundReclaiming(gc).deferred) << "the terminal delta must fold";
    state = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    seal = decodeFoldSeal(backend->get(layout.foldSealKey(state.snap_generation, state.snap_attempt))->bytes);
    ASSERT_TRUE(seal.ref_lives.at(predecessor.incarnation).cleanup_evidence.has_value());

    const RoundReport drain = runRegularRoundReclaiming(gc);
    ASSERT_TRUE(drain.deferred) << "the drain-only idle invocation must leave the evidence seal adopted";
    ASSERT_FALSE(catalog_entry().has_value());
    ASSERT_EQ(store->refTableRuntimeIdentityForTest(ns), 0u)
        << "catalog deletion must detach the predecessor from the name slot";

    publish_without_fixture_admission("successor");
    const CatalogEntry successor = *catalog_entry();
    ASSERT_EQ(successor.ns, predecessor.ns) << "the exact same logical namespace must be reused";
    ASSERT_NE(successor.incarnation, predecessor.incarnation);
    ASSERT_EQ(store->liveWriterEpoch(), writer_epoch) << "rebirth must use the same mounted writer epoch";
    ASSERT_NE(store->refTableRuntimeIdentityForTest(ns), runtime_identity)
        << "rebirth must publish a distinct successor runtime, not reset the predecessor";
    ASSERT_EQ(store->refTableLifeForTest(ns)->incarnation, successor.incarnation);

    const NamespaceLifeId successor_life = NamespaceLifeId::fromCatalogEntry(ns, successor.incarnation);
    const ListPage successor_stream = backend->list(layout.namespaceStreamPrefix(successor_life), "", 1000);
    ASSERT_FALSE(successor_stream.keys.empty()) << "the real successor writer produced foldable stream work";
    std::vector<GcPhaseRecord> successor_phases;
    gc.setPhaseSink([&](const GcPhaseRecord & phase) { successor_phases.push_back(phase); });
    const RoundReport successor_round = runRegularRoundReclaiming(gc);
    const auto decision = std::find_if(successor_phases.begin(), successor_phases.end(), [](const GcPhaseRecord & phase)
    {
        return phase.phase == "defer_decision";
    });
    ASSERT_NE(decision, successor_phases.end());
    ASSERT_FALSE(successor_round.deferred)
        << "successor stream keys=" << successor_stream.keys.size()
        << ", changed_shards=" << decision->metrics.at("changed_shards")
        << ", dead_life_debris=" << decision->metrics.at("dead_life_debris");
    state = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    seal = decodeFoldSeal(backend->get(layout.foldSealKey(state.snap_generation, state.snap_attempt))->bytes);
    EXPECT_FALSE(seal.ref_lives.contains(predecessor.incarnation));
    const auto successor_row = seal.ref_lives.find(successor.incarnation);
    ASSERT_NE(successor_row, seal.ref_lives.end());
    EXPECT_EQ(successor_row->second.coverage.last_folded_ref_id.writer_epoch, writer_epoch);
    EXPECT_EQ(successor_row->second.coverage.last_folded_ref_id.ref_sequence, 2u)
        << "the successor starts at its own birth+publish stream, not the predecessor's cursor";
    for (const String & key : backend->touchedKeys())
        EXPECT_EQ(key.find("/_cleanup/"), String::npos) << key;
}

/// Losing the response to an erase that committed must not strand the same resident writer runtime
/// behind its old removal-admission gate. A complete resolution read proves the exact old row absent,
/// so the same name can be born immediately under a fresh incarnation without inheriting coverage.
TEST(CASRefWriterNamespaceRemoval, CommitThenThrowEraseResolvesAndRebindsResidentRuntimeImmediately)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    PoolConfig config;
    config.gc_fold_threshold = 1;
    config.ref_table_cache_bytes = 0;
    auto store = openPoolWithConfig(backend, config);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/removal-erase-lost-response"};
    Gc gc(store, UInt128{101});
    const CompletedRemovingFixture ready = prepareResidentRemovalForDrain(store, *backend, ns, gc);

    backend->catalog_fault_key = layout.refCatalogKey();
    backend->catalog_cas_fault = RefWriterTestBackend::CatalogCasFault::CommitThenThrow;
    EXPECT_NO_THROW((void)runRegularRoundReclaiming(gc));

    const RefCatalog after_erase = CasRefCatalog::read(*backend, layout).catalog;
    EXPECT_TRUE(std::none_of(after_erase.entries.begin(), after_erase.entries.end(), [&](const CatalogEntry & entry)
    {
        return entry.ns == ns && entry.incarnation == ready.predecessor.incarnation;
    }));
    EXPECT_EQ(store->refTableRuntimeIdentityForTest(ns), 0u);

    EXPECT_NO_THROW(publishWithProductionBirth(store, ns, "successor"));
    const CatalogEntry successor = catalogEntryOrThrow(*backend, layout, ns);
    EXPECT_NE(successor.incarnation, ready.predecessor.incarnation);
    EXPECT_EQ(store->liveWriterEpoch(), ready.writer_epoch);
    EXPECT_NE(store->refTableRuntimeIdentityForTest(ns), ready.runtime_identity);

    ASSERT_FALSE(runRegularRoundReclaiming(gc).deferred);
    const GcState state = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    const CasFoldSeal seal = decodeFoldSeal(
        backend->get(layout.foldSealKey(state.snap_generation, state.snap_attempt))->bytes);
    EXPECT_FALSE(seal.ref_lives.contains(ready.predecessor.incarnation));
    ASSERT_TRUE(seal.ref_lives.contains(successor.incarnation));
    EXPECT_EQ(seal.ref_lives.at(successor.incarnation).coverage.last_folded_ref_id,
        (RefTxnId{ready.writer_epoch, 2}));
}

/// If another actor wins the erase race by replacing the exact old row, `EntryChanged` still proves
/// the predecessor life dead and must invalidate its resident runtime.
TEST(CASRefWriterNamespaceRemoval, OtherWinnerReplacementInvalidatesExactPredecessorLife)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    PoolConfig config;
    config.gc_fold_threshold = 1;
    config.ref_table_cache_bytes = 0;
    auto store = openPoolWithConfig(backend, config);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/removal-other-winner-replacement"};
    Gc gc(store, UInt128{102});
    const CompletedRemovingFixture ready = prepareResidentRemovalForDrain(store, *backend, ns, gc);

    const CatalogEntry replacement{
        .ns = ns,
        .state = NsState::Live,
        .incarnation = UInt128{0xfeed},
        .creator = std::nullopt};
    ASSERT_NE(replacement.incarnation, ready.predecessor.incarnation);
    const NamespaceLifeId replacement_life = NamespaceLifeId::fromCatalogEntry(ns, replacement.incarnation);
    ASSERT_EQ(backend->putIfAbsent(layout.refCkptKey(replacement_life), encodeRefCkpt(RefCkpt{
        .life_epoch = ready.writer_epoch,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt})).outcome, PutOutcome::Done);
    backend->catalog_fault_key = layout.refCatalogKey();
    backend->catalog_replacement_bytes = encodeRefCatalog(RefCatalog{.entries = {replacement}});
    backend->catalog_cas_fault = RefWriterTestBackend::CatalogCasFault::OtherWriterReplacement;

    EXPECT_NO_THROW((void)runRegularRoundReclaiming(gc));
    EXPECT_EQ(store->namespaceLife(ns), replacement_life);
    EXPECT_NE(store->refTableRuntimeIdentityForTest(ns), ready.runtime_identity);
}

/// Failure to read the catalog while resolving a lost erase response is not success. A later fresh
/// name lookup nevertheless observes the old exact row absent and reconciles the resident runtime.
TEST(CASRefWriterNamespaceRemoval, LaterNameLookupReconcilesAfterEraseResolutionReadFailure)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    PoolConfig config;
    config.gc_fold_threshold = 1;
    config.ref_table_cache_bytes = 0;
    auto store = openPoolWithConfig(backend, config);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/removal-resolution-read-failure-lookup"};
    Gc gc(store, UInt128{103});
    const CompletedRemovingFixture ready = prepareResidentRemovalForDrain(store, *backend, ns, gc);

    backend->catalog_fault_key = layout.refCatalogKey();
    backend->catalog_cas_fault = RefWriterTestBackend::CatalogCasFault::CommitThenThrow;
    backend->catalog_resolution_get_fault_count = 1;
    EXPECT_THROW((void)runRegularRoundReclaiming(gc), std::runtime_error);
    EXPECT_TRUE(CasRefCatalog::read(*backend, layout).catalog.entries.empty());

    std::optional<NamespaceLifeId> successor;
    EXPECT_NO_THROW(successor = store->namespaceLife(ns));
    ASSERT_TRUE(successor.has_value());
    EXPECT_NE(successor->incarnation, ready.predecessor.incarnation);
    EXPECT_NE(store->refTableRuntimeIdentityForTest(ns), ready.runtime_identity);
}

/// The normal post-LIST catalog cut is also a reconciliation point. It repairs a missed local
/// invalidation before any later writer touches the name.
TEST(CASRefWriterNamespaceRemoval, PostListCatalogCutReconcilesMissedEraseInvalidation)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    PoolConfig config;
    config.gc_fold_threshold = 1;
    config.ref_table_cache_bytes = 0;
    auto store = openPoolWithConfig(backend, config);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv1/removal-resolution-read-failure-post-list"};
    Gc gc(store, UInt128{104});
    const CompletedRemovingFixture ready = prepareResidentRemovalForDrain(store, *backend, ns, gc);

    backend->catalog_fault_key = layout.refCatalogKey();
    backend->catalog_cas_fault = RefWriterTestBackend::CatalogCasFault::CommitThenThrow;
    backend->catalog_resolution_get_fault_count = 1;
    EXPECT_THROW((void)runRegularRoundReclaiming(gc), std::runtime_error);
    EXPECT_TRUE(CasRefCatalog::read(*backend, layout).catalog.entries.empty());

    EXPECT_NO_THROW((void)runRegularRoundReclaiming(gc));
    EXPECT_NO_THROW(publishWithProductionBirth(store, ns, "successor"));
    EXPECT_NE(catalogEntryOrThrow(*backend, layout, ns).incarnation, ready.predecessor.incarnation);
    EXPECT_NE(store->refTableRuntimeIdentityForTest(ns), ready.runtime_identity);
}

/// ===================================================================================
/// Task 11: namespace birth / the recreation gate (spec §Namespace Birth)
/// ===================================================================================

/// The writer assignment site may pin an already-`Live` catalog life, but recovering that empty life
/// performs no catalog or stream mutation. It must install the exact incarnation from the observed row.
TEST(CASRefWriterNamespaceBirth, ExistingLiveCatalogRowPinsExactLifeWithoutMutation)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/existing-live-assignment"};
    CasRefCatalog::casAdmitEntry(*backend, store->layout(), 1, CatalogEntry{
        .ns = ns, .state = NsState::Live, .incarnation = UInt128{41}});
    DB::Cas::tests::writeRecoverableCkptForRawFixture(*backend, store->layout(), ns, RefCkpt{
        .life_epoch = store->liveWriterEpoch(),
        .committed_through = std::nullopt,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    backend->resetCounts();
    const NamespaceLifeId life = store->namespaceLife(ns);
    EXPECT_EQ(life.incarnation, UInt128{41});
    ASSERT_TRUE(store->refTableLifeForTest(ns));
    EXPECT_EQ(store->refTableLifeForTest(ns)->incarnation, UInt128{41});
    EXPECT_EQ(backend->putTotal(), 0u);
    EXPECT_EQ(backend->putOverwriteTotal(), 0u);
    EXPECT_EQ(backend->casPutTotal(), 0u);
}

/// A read of a never-born name may observe the catalog, but it must not allocate the local name slot
/// or a life runtime. Otherwise arbitrary read traffic can fill the cache with identity-less runtimes,
/// and a later birth has to mutate one of those objects into a different identity.
TEST(CASRefWriterNamespaceBirth, NeverBornReadAllocatesNoRuntime)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/never-born-read-no-runtime"};

    ASSERT_EQ(store->refTablesCachedCountForTest(), 0u);
    EXPECT_FALSE(store->resolveRef(ns, "missing").has_value());
    EXPECT_EQ(store->refTablesCachedCountForTest(), 0u)
        << "catalog absence must be decided before a runtime is constructed";
    EXPECT_EQ(store->refTableRuntimeIdentityForTest(ns), 0u);
}

/// A never-born namespace follows the ordinary catalog-first birth path.
TEST(CASRefWriterNamespaceBirth, BirthFromNeverBornUsesOrdinaryPath)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/virgin"};

    EXPECT_NO_THROW(publishEmptyPart(store, ns, "first"));
    EXPECT_TRUE(store->resolveRef(ns, "first").has_value());
}

/// Coverage gap (Task 13a): the "one op per ref name per batch" cut in `flushRefBatch` (the `seen_refs`
/// guard, `CASRefBatchScopeCuts`) had no test after the shard-lane `CasShardQueue.SameRefMutations
/// SplitAcrossFlushes` was retired. Two payload mutations of the SAME committed ref, made co-pending by
/// the pre-carve hook (mirrors `CompatibleMutationsShareOneCreate`), must NOT co-batch: per-request undo
/// validates each op against the pre-batch state, so the batch carries at most one op per ref name and
/// the two flush as two separate `_log` objects.
TEST(CASRefWriterAppendLane, SameRefMutationsSplitAcrossFlushes)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/samerefsplit"};
    publishEmptyPart(store, ns, "a");
    ASSERT_TRUE(store->resolveRef(ns, "a").has_value());

    std::mutex m;
    std::condition_variable cv;
    bool entered = false;
    store->setRefPreCarveHookForTest([&]
    {
        std::unique_lock lk(m);
        if (entered)
            return;   /// only the leader's own first carve blocks; the second flush proceeds
        entered = true;
        cv.notify_all();
        cv.wait(lk, [&] { return store->refQueuePendingForTest(ns) >= 2; });
    });

    const uint64_t put_before = backend->putTotal();
    std::thread t_a([&] { store->updateRefPublishedAt(ns, "a", [](RefPublishedAtUpdate & r) { r.published_at_ms = 1; }); });
    {
        std::unique_lock lk(m);
        cv.wait(lk, [&] { return entered; });
    }
    std::thread t_b([&] { store->updateRefPublishedAt(ns, "a", [](RefPublishedAtUpdate & r) { r.published_at_ms = 2; }); });
    while (store->refQueuePendingForTest(ns) < 2)
        std::this_thread::yield();
    cv.notify_all();
    t_a.join();
    t_b.join();
    store->setRefPreCarveHookForTest(nullptr);

    EXPECT_EQ(backend->putTotal(), put_before + 2) << "same-ref mutations must flush as two separate logs";
    /// Neither mutation was lost or corrupted -- the ref still resolves with one of the two writes.
    const auto resolved = store->resolveRef(ns, "a");
    ASSERT_TRUE(resolved.has_value());
    EXPECT_TRUE(resolved->published_at_ms == 1 || resolved->published_at_ms == 2);
}

/// ===================================================================================
/// rev.6 Task 8: the recovery seal (spec §recovery-seal / §seal-id / §seal-soundness). At an UNCLEAN
/// mount, `ensureRefTableRecovered` must close every dead epoch it discovers with an immediate
/// snapshot -- published at the UPPER BOUND of the dead-epoch region, `{liveWriterEpoch() - 1,
/// UINT64_MAX}` -- BEFORE the table is exposed as recovered, so no late predecessor PUT from any dead
/// epoch can ever surface to a cold fold or a fresh recovery.
/// ===================================================================================

namespace
{

/// Seeds crash-style predecessor debris for the seal tests: two DEAD epochs (1 and 2) of durable logs
/// under `ns` -- epoch 1 births ref "a", epoch 2 adds ref "b" -- with no snapshot, and burns the
/// durable epoch counter to exactly 2 so a subsequent `Pool::open` allocates epoch 3 (both dead
/// epochs land strictly below the fresh writer's own, as `dead_region_nonempty` requires).
void seedSealFixtureDeadEpochs(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    allocateWriterEpoch(backend, layout, "test", EpochMintPolicy::NormalMount, 0, [] { return RefCatalog{}; });   /// burns epoch 1
    allocateWriterEpoch(backend, layout, "test", EpochMintPolicy::NormalMount, 0, [] { return RefCatalog{}; });   /// burns epoch 2

    RefLogTxn birth;
    birth.ns = ns.string();
    birth.txn_id = RefTxnId{1, 1};
    birth.ops = {namespaceBirthOp(), publishCommittedOps("a", manifestRef(1, 1, 1))[0],
                 publishCommittedOps("a", manifestRef(1, 1, 1))[1]};
    DB::Cas::tests::fixture::writeRefLogRaw(backend, layout, birth);

    RefLogTxn mut;
    mut.ns = ns.string();
    mut.txn_id = RefTxnId{2, 1};
    /// Sequence 1 of a new epoch names the seal that closed the one below -- `{1,2}`, the slot right
    /// after epoch 1's only durable id, which is where the recovering mount's CAS-walk puts it. The seal
    /// OBJECT is deliberately not seeded: this fixture's subject is a recovery that has to mint it.
    mut.prev_epoch_seal = RefTxnId{1, 2};
    mut.ops = {publishCommittedOps("b", manifestRef(2, 1, 1))[0],
               publishCommittedOps("b", manifestRef(2, 1, 1))[1]};
    DB::Cas::tests::fixture::writeRefLogRaw(backend, layout, mut);
    DB::Cas::tests::writeRecoverableCkptForRawFixture(backend, layout, ns, RefCkpt{
        /// The namespace was born in epoch 1 and only `{1,1}` is fronted initially. Recovery must mint
        /// the missing required seal `{1,2}` before it may adopt the already durable `{2,1}` successor.
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });
}

/// Plants a same-uuid, UNCLEAN (crash-style, no farewell) predecessor mount lease at `epoch`: a bare
/// `claimMount` followed by a GC fence-out -- mirrors `CASMountOpenWaits.FencedPriorReclaimsWithoutAnyWait`. A fenced
/// prior is an immediate certificate of death (`claimMountAwaitingExpiry` reclaims it on its FIRST
/// attempt, no observation polling), so a fake-clocked successor `Pool::open` above it becomes
/// unclean deterministically, without any real sleep.
void seedUncleanPredecessorMount(Backend & backend, const Layout & layout, uint64_t epoch)
{
    claimMount(backend, layout, "test", UInt128(1), epoch, /*now_ms=*/1000, /*ttl_ms=*/500);
    fenceOutRefMount(backend, layout.mountKey("test"));
}

/// The budget every seal test's successor `Pool::open` uses: a 500ms lease TTL needs a scaled-down
/// budget (RFC cas-s3-timeout-retry-control §required-timeout-model: attempt_timeout + safety_margin <
/// lease TTL) -- mirrors `CASMountOpenWaits.FencedPriorReclaimsWithoutAnyWait` exactly.
CasRequestBudget sealTestTinyBudget()
{
    return CasRequestBudget{
        .attempt_timeout_ms = 50, .operation_deadline_ms = kSingleAttemptDeadlineMs, .max_attempts = 1,
        .lease_safety_margin_ms = 50};
}

}

/// The `RefWriterRecoverySeal` suite is RETIRED with the sentinel seal it pinned, and the replacement is
/// `gtest_cas_ref_recovery_cas_walk.cpp` (`CASRefRecoveryCasWalk`), which covers the same duties against
/// the in-band mechanism: a dead epoch closed at `{E, T+1}`, a concurrent recoverer's seal adopted, a
/// straggler adopted and re-sealed at the new `T+1`, chained seals across burned epochs, and genesis.
///
/// Three of its properties changed MEANING rather than mechanism, and a reader looking for them here
/// should know where they went:
///
///   - "a clean boundary does not seal" is GONE as a rule. Sealing is now decided by `epoch < live_epoch`
///     alone, never by how the predecessor died: the seal is the chain link that makes a MISSING epoch
///     detectable, and a chain with holes in it wherever a mount shut down cleanly is not a chain.
///   - "a late log below the seal is invisible to recovery" is replaced by something stronger, and the
///     replacement is what makes the detector unnecessary: the seal occupies the ghost's own log key, so
///     a late PUT is REFUSED by the store instead of landing somewhere a reader must learn to ignore.
///   - the `sealed_from` inventory assertions are gone with the field; the chain link recovery installs
///     is `last_epoch_seal`, asserted in the new suite and in `CASRecoveryStreaming`'s inventory test.
///
/// The fixtures above (`seedSealFixtureDeadEpochs`, `seedUncleanPredecessorMount`, `sealTestTinyBudget`)
/// are KEPT: the recovery-retry suite below drives the same dead-epoch shape.

/// ===================================================================================
/// Layer 1 of the stuck-table-load fix: `ensureRefTableRecovered` retries a whole recovery attempt
/// after a TRANSIENT object-store NETWORK_ERROR (bounded by `recovery_retry_budget_ms`), instead of
/// failing the table's async load permanently. Non-transient errors and the terminal vanish-race
/// brake still fail fast.
/// ===================================================================================

TEST(CASRefWriterRecoveryRetry, TransientSealFailureIsRetriedThenSucceeds)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/retry_ok"};

    seedSealFixtureDeadEpochs(*backend, layout, ns);
    seedUncleanPredecessorMount(*backend, layout, /*epoch=*/2);

    uint64_t fake_now = 1'000'000;

    PoolConfig config;
    config.server_id = UInt128(1);
    config.mount_lease_ttl_ms = std::chrono::milliseconds(500);
    config.cas_request_budget = sealTestTinyBudget();
    config.cas_request_budget.recovery_retry_budget_ms = 120000;
    config.cas_request_budget.recovery_retry_initial_backoff_ms = 1000;
    config.cas_request_budget.recovery_retry_max_backoff_ms = 30000;
    config.boot_ms_fn = [&fake_now] { return fake_now; };
    config.wait_sleep_fn = [](uint64_t) {};
    auto store = openPoolWithConfig(backend, config);
    ASSERT_TRUE(store);
    ASSERT_EQ(store->liveWriterEpoch(), 3u);

    /// No-op backoff and a frozen clock: retries run until the transient faults are exhausted, and the
    /// frozen clock keeps the mount fence alive across them (advancing it past the tiny lease TTL would
    /// drop the fence and abort recovery -- exercising the fence path, which is the budget test's job).
    store->setCasRetrySleepForTest([](uint64_t) {});

    /// Fail the epoch seal's conditional create twice with a transient (timeout) error; the third
    /// attempt lands. The seal is a LOG transaction at `{2,2}` -- the slot after the dead epoch's last
    /// durable id -- because INV-2 closes an epoch in-band, at the key a straggler would have taken.
    const RefTxnId seal_id{2, 2};
    backend->fault_key_substr = layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), seal_id);
    backend->fault_count = 2;

    using ProfileEvents::global_counters;
    const auto retries_before = global_counters[ProfileEvents::CASRefRecoveryRetries].load();
    const auto sealed_before = global_counters[ProfileEvents::CASRefRecoveryEpochSealed].load();

    EXPECT_EQ(store->listRefs(ns).size(), 2u) << "recovery must succeed after retrying past the faults";

    EXPECT_EQ(global_counters[ProfileEvents::CASRefRecoveryRetries].load(), retries_before + 2);
    /// TWO dead epochs (1 and 2) are closed by this walk, and a whole attempt is re-driven per transient
    /// failure -- so the seals of the epochs a failed attempt already closed are ADOPTED on the retry
    /// rather than minted again. Exactly two are minted in total.
    EXPECT_EQ(global_counters[ProfileEvents::CASRefRecoveryEpochSealed].load(), sealed_before + 2);
}

TEST(CASRefWriterRecoveryRetry, RecoveryDoesNotEnumerateItsStream)
{
    /// A recovery stream LIST used to be a transient failure leg. The checkpoint now names both the
    /// base and frontier, so the same injected failures must remain untouched while recovery seals.
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/retry_list"};

    seedSealFixtureDeadEpochs(*backend, layout, ns);
    seedUncleanPredecessorMount(*backend, layout, /*epoch=*/2);

    uint64_t fake_now = 1'000'000;

    PoolConfig config;
    config.server_id = UInt128(1);
    config.mount_lease_ttl_ms = std::chrono::milliseconds(500);
    config.cas_request_budget = sealTestTinyBudget();
    config.cas_request_budget.recovery_retry_budget_ms = 120000;
    config.boot_ms_fn = [&fake_now] { return fake_now; };
    config.wait_sleep_fn = [](uint64_t) {};
    auto store = openPoolWithConfig(backend, config);
    ASSERT_TRUE(store);
    ASSERT_EQ(store->liveWriterEpoch(), 3u);

    store->setCasRetrySleepForTest([](uint64_t) {});

    /// If recovery ever reintroduces a stream LIST, this injection turns the attempt into a retry and
    /// consumes the counter. `namespaceFilesLifeIfReadable` reaches writer recovery without performing
    /// the unrelated user-facing `listRefs` enumeration.
    backend->list_fault_count = 2;

    using ProfileEvents::global_counters;
    const auto retries_before = global_counters[ProfileEvents::CASRefRecoveryRetries].load();
    const auto sealed_before = global_counters[ProfileEvents::CASRefRecoveryEpochSealed].load();

    ASSERT_TRUE(store->namespaceFilesLifeIfReadable(ns));
    EXPECT_EQ(backend->list_fault_count, 2);
    EXPECT_EQ(global_counters[ProfileEvents::CASRefRecoveryRetries].load(), retries_before);
    EXPECT_EQ(global_counters[ProfileEvents::CASRefRecoveryEpochSealed].load(), sealed_before + 2)
        << "two dead epochs (1 and 2) are closed without enumerating their stream";
}

TEST(CASRefWriterRecoveryRetry, TransientFailureLongerThanBudgetPropagates)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/retry_budget"};

    seedSealFixtureDeadEpochs(*backend, layout, ns);
    seedUncleanPredecessorMount(*backend, layout, /*epoch=*/2);

    uint64_t fake_now = 1'000'000;

    PoolConfig config;
    config.server_id = UInt128(1);
    /// Lease TTL >> the recovery budget so the CLOCK-advancing backoff below trips the budget check,
    /// not the mount fence -- this test specifically exercises the budget-exhaustion path.
    config.mount_lease_ttl_ms = std::chrono::milliseconds(600000);
    config.cas_request_budget = sealTestTinyBudget();
    config.cas_request_budget.recovery_retry_budget_ms = 5000;   /// small, deterministic
    config.cas_request_budget.recovery_retry_initial_backoff_ms = 1000;
    config.cas_request_budget.recovery_retry_max_backoff_ms = 30000;
    config.boot_ms_fn = [&fake_now] { return fake_now; };
    config.wait_sleep_fn = [](uint64_t) {};
    auto store = openPoolWithConfig(backend, config);
    ASSERT_TRUE(store);
    store->setCasRetrySleepForTest([&fake_now](uint64_t ms) { fake_now += ms; });

    /// The seal is an in-band LOG transaction at the slot after the dead epoch's last durable id, not a
    /// snapshot at a synthetic id: epoch 1 closes at `{1,2}`, which is the FIRST write the walk attempts.
    const RefTxnId seal_id{1, 2};
    backend->fault_key_substr = layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), seal_id);
    backend->fault_count = 1000;   /// never stops failing within the budget

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->listRefs(ns); });
}

TEST(CASRefWriterRecoveryRetry, NonNetworkErrorIsNotRetried)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/retry_fatal"};

    seedSealFixtureDeadEpochs(*backend, layout, ns);
    seedUncleanPredecessorMount(*backend, layout, /*epoch=*/2);

    PoolConfig config;
    config.server_id = UInt128(1);
    config.mount_lease_ttl_ms = std::chrono::milliseconds(500);
    config.cas_request_budget = sealTestTinyBudget();
    config.wait_sleep_fn = [](uint64_t) {};
    auto store = openPoolWithConfig(backend, config);
    ASSERT_TRUE(store);

    size_t sleep_calls = 0;
    store->setCasRetrySleepForTest([&sleep_calls](uint64_t) { ++sleep_calls; });

    /// A foreign writer lands DIFFERENT valid bytes at the seal key; resolve-before-reissue then throws
    /// CORRUPTED_DATA (a real cross-process seal conflict), which must NOT be retried.
    const RefTxnId seal_id{1, 2};
    backend->corrupt_key_substr = layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), seal_id);
    backend->corrupt_count = 1;

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->listRefs(ns); });
    EXPECT_EQ(backend->corrupt_count, 0)
        << "the test must reach the injected foreign seal conflict, not fail on fixture validation";
    EXPECT_EQ(sleep_calls, 0u) << "a non-transient error must fail fast with zero backoff sleeps";
}

TEST(CASRefWriterRecoveryRetry, VanishBrakeStaysTerminalNotRetried)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/retry_vanish"};
    const ManifestRef ma = manifestRef(1, 1, 1);

    /// Stage B (Task 4-C): pin `ns` to the sentinel before the raw snapshot below -- `store->listRefs`
    /// further down is a real production read that would otherwise mint a fresh RANDOM incarnation for
    /// this unadmitted namespace instead of adopting the sentinel the raw fixture writes at.
    DB::Cas::tests::fixture::admitLive(*backend, layout, ns);
    const RefTxnId snap_x{1, 10};
    std::vector<RefOp> base_ops{namespaceBirthOp()};
    const auto publish_a = publishCommittedOps("a", ma);
    base_ops.insert(base_ops.end(), publish_a.begin(), publish_a.end());
    DB::Cas::tests::fixture::writeRefLogRaw(*backend, layout, RefLogTxn{ns.string(), snap_x, std::move(base_ops), std::nullopt});
    writeRefSnapshotRaw(*backend, layout, minimalLiveSnapshot(ns.string(), snap_x, {committedRow("a", ma)}));
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = snap_x,
        .checkpoint_snapshot_id = snap_x,
        .last_epoch_seal = std::nullopt,
    });

    auto store = openPool(backend);

    size_t sleep_calls = 0;
    store->setCasRetrySleepForTest([&sleep_calls](uint64_t) { ++sleep_calls; });

    /// A checkpoint-named snapshot belongs to the caller's immutable authority cut. If that exact
    /// object is absent, recovery must report corruption immediately; it must neither reinterpret a
    /// transient disappearance as a new authority cut nor enter the outer transient-retry loop.
    const String vkey = layout.refSnapshotKey(DB::Cas::tests::fixture::fixtureLife(ns), snap_x);
    backend->vanish_once_keys.insert(vkey);

    using ProfileEvents::global_counters;
    const auto retries_before = global_counters[ProfileEvents::CASRefRecoveryRetries].load();

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { store->listRefs(ns); });

    EXPECT_FALSE(backend->vanish_once_keys.contains(vkey))
        << "the test must reach the checkpoint-named snapshot GET, not fail on earlier fixture validation";
    EXPECT_EQ(global_counters[ProfileEvents::CASRefRecoveryRetries].load(), retries_before)
        << "missing immutable checkpoint authority is terminal; the outer transient-retry loop must NOT re-drive it";
    EXPECT_EQ(sleep_calls, 0u) << "no backoff sleep for missing immutable checkpoint authority";
}

TEST(CASRefWriterRecoveryRetry, ThrowingBackoffSleepDoesNotWedgeRecovery)
{
    /// If the backoff sleep itself throws (e.g. a clock syscall failure), the retry loop must re-acquire
    /// state_mutex before unwinding so the SCOPE_EXIT that clears `recovery_in_progress` runs LOCKED --
    /// otherwise a later touch would hang forever on the never-cleared flag. This drives that path and
    /// then proves a second touch can still recover (the lane is not wedged).
    auto backend = std::make_shared<RefWriterTestBackend>();
    const Layout layout("p");
    const RootNamespace ns{"srv1/retry_sleep_throw"};

    seedSealFixtureDeadEpochs(*backend, layout, ns);
    seedUncleanPredecessorMount(*backend, layout, /*epoch=*/2);

    PoolConfig config;
    config.server_id = UInt128(1);
    config.mount_lease_ttl_ms = std::chrono::milliseconds(500);
    config.cas_request_budget = sealTestTinyBudget();
    config.cas_request_budget.recovery_retry_budget_ms = 120000;
    config.wait_sleep_fn = [](uint64_t) {};
    auto store = openPoolWithConfig(backend, config);
    ASSERT_TRUE(store);

    /// First touch: the seal PUT fails transiently -> the loop enters backoff -> the sleep THROWS.
    bool sleep_should_throw = true;
    store->setCasRetrySleepForTest([&sleep_should_throw](uint64_t)
    {
        if (sleep_should_throw)
            throw std::runtime_error("injected backoff-sleep failure");
    });
    const RefTxnId seal_id{1, 2};
    backend->fault_key_substr = layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), seal_id);
    backend->fault_count = 1;

    EXPECT_ANY_THROW(store->listRefs(ns));   /// the sleep failure propagates

    /// The lane must NOT be wedged: with the fault now spent and the sleep no longer throwing, a second
    /// touch recovers cleanly. If recovery_in_progress had leaked (SCOPE_EXIT run unlocked / not run), a
    /// concurrent-safe second recovery would deadlock or mis-behave.
    sleep_should_throw = false;
    EXPECT_EQ(store->listRefs(ns).size(), 2u) << "a second touch must recover; the retry lane is not wedged";
}

/// ===================================================================================
/// Task 16: `hasAnyRefWithPrefix` -- pure existence probe, same recovery preamble as `listRefs` but
/// without materializing the full ref map (an early-exit scan).
/// ===================================================================================

TEST(CASRefWriterListRefs, HasAnyRefWithPrefixMatchesListRefsEmptiness)
{
    auto backend = std::make_shared<RefWriterTestBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/prefix_probe"};
    const RootNamespace empty_ns{"srv1/prefix_probe_empty"};

    EXPECT_FALSE(store->hasAnyRefWithPrefix(empty_ns, "")) << "a never-touched namespace has no refs";

    publishEmptyPart(store, ns, "all_1_1_0");
    publishEmptyPart(store, ns, "detached-x");

    EXPECT_TRUE(store->hasAnyRefWithPrefix(ns, "")) << "empty prefix means \"any ref at all\"";
    EXPECT_TRUE(store->hasAnyRefWithPrefix(ns, "detached-"));
    EXPECT_FALSE(store->hasAnyRefWithPrefix(ns, "moving-")) << "no ref carries this prefix";

    store->dropNamespace(ns);
    EXPECT_FALSE(store->hasAnyRefWithPrefix(ns, "")) << "a tombstoned namespace has no committed refs";
}
