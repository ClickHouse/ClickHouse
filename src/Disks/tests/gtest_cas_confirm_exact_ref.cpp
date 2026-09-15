#include <gtest/gtest.h>

#include "config.h"

#include <base/defines.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedExchange.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>

#include <Poco/Exception.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <optional>
#include <string_view>
#include <thread>
#include <vector>

/// Task 10 (spec §confirm-primitive, "Gate 1 -- exact-token identity under a lane snapshot"): the
/// ledger-side half of the publish-then-confirm relink handoff.
///
/// `confirmExactRef` is a GATE, and the only direction in which it may fail is `Unknown`. A `Yes`
/// authorizes a remote receiver to promote a manifest it staged from this writer's blobs, so a `Yes`
/// produced from a stale, lagging or partially-recovered view is a live-blob-deletion bug, not a
/// missed optimization. Every test below therefore pins one of the six snapshot rules by constructing
/// the exact state in which a naive "look the row up and compare" implementation would answer `Yes`
/// (or `No`) and asserting `Unknown` instead.
///
/// Two properties are contract, not detail, and are asserted as such:
///   - ZERO object-store I/O. A cold, evicted or recovering table answers `Unknown`; it must not
///     recover from storage to answer, and it must not even MATERIALIZE a runtime -- a read-only
///     interserver query must never be able to make this writer do work.
///   - The snapshot spans BOTH lane mutexes, so an append admitted concurrently is ordered strictly
///     after it: there is no window in which the confirm says `Yes` while a mutation OF THAT REF is
///     already admitted. A queued or in-flight mutation of another SINGLE ref does not refuse -- rule 3
///     reads each item's `MutationScope`, and refusing for the whole table starved two replicas of each
///     other under load on a slow control plane. A `WholeShard`-scoped mutation still refuses every ref.
///
/// The suite name is prefixed `Cas` so it is covered by the `Cas*` unit-test gate filter.

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int MEMORY_LIMIT_EXCEEDED;
extern const int LOGICAL_ERROR;
}

namespace ProfileEvents
{
    extern const Event CASRelinkConfirmRefusedRefMutationInFlight;
    extern const Event CASRelinkConfirmRefusedLaneWedged;
    extern const Event CASRelinkConfirmRefusedLaneBroken;
    extern const Event CASRelinkConfirmRefusedMountCannotSpeak;
}

using namespace DB::Cas;
using DB::Cas::tests::CountingBackend;

namespace
{

/// A `CountingBackend` with two recovery-side seams: a one-shot NON-transient exact GET failure (which
/// leaves the namespace runtime resident but UNRECOVERED, because recovery fails closed), and a
/// blocking exact GET (which parks a caller inside `ensureRefTableRecovered` with
/// `recovery_in_progress` set). The failure is deliberately `CORRUPTED_DATA`:
/// `isTransientRecoveryError` does not list it, so recovery fails fast instead of burning its retry
/// budget.
/// The engine reissues an unresolved write until its OWN retry window closes, and that window is
/// measured on a clock the engine reads. Both seams here share one counter -- the sleep the engine
/// performs is what advances the clock -- so a fault that stays armed ends the call at its deadline
/// with no real time passing. Installed on the whole pool, because the ref-lane write, its settling
/// read and the recovery retry loop all pace through the same seam. The pool owns the closures and the
/// closures own the clock, so it outlives everything that can still read it.
class VirtualRetryClock
{
public:
    static std::shared_ptr<VirtualRetryClock> installOn(const PoolPtr & store)
    {
        auto clock = std::make_shared<VirtualRetryClock>();
        store->setCasRequestNowFnForTest([clock] { return clock->nowMs(); });
        store->setCasRetrySleepForTest([clock](uint64_t ms) { clock->advance(ms); });
        return clock;
    }

    uint64_t nowMs() const
    {
        std::lock_guard lock(mutex);
        return now_ms;
    }
    size_t pauseCount() const
    {
        std::lock_guard lock(mutex);
        return pauses;
    }
    uint64_t longestPause() const
    {
        std::lock_guard lock(mutex);
        return longest_pause;
    }

    void advance(uint64_t ms)
    {
        std::lock_guard lock(mutex);
        /// Plus one millisecond, because full jitter can draw a ZERO pause: a clock that does not move
        /// would leave the loop reissuing for ever against a fault that never clears.
        now_ms += ms + 1;
        ++pauses;
        longest_pause = std::max(longest_pause, ms);
    }

private:
    mutable std::mutex mutex;
    uint64_t now_ms = 0;
    size_t pauses = 0;
    uint64_t longest_pause = 0;
};

/// `ChunkFaultBackend` COUNTS its faults, and a count can no longer make one conclusive: the write
/// engine settles every ambiguity by an exact read and then REISSUES, so a fault that runs out
/// mid-call is answered by the next attempt instead of by the call's own deadline -- which is the
/// whole difference between a wedge and a commit.
class LatchedChunkFaultBackend : public DB::Cas::tests::ChunkFaultBackend
{
public:
    bool latched = false;

    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value,
                                             DB::Cas::TransportAccess & access) override
    {
        if (latched && mode != Mode::None && fault_skip == 0 && !expected_value && !fault_substr.empty()
            && key.find(fault_substr) != String::npos)
            fault_count = 1;
        return ChunkFaultBackend::write(key, bytes, expected_value, access);
    }

    void disarm()
    {
        latched = false;
        mode = Mode::None;
        fault_count = 0;
        fault_skip = 0;
        fail_read_once_key.clear();
    }
};

class RecoveryLatchBackend : public CountingBackend
{
public:
    /// Set before the driving call; consumed by the first matching recovery read.
    String fail_get_once_key;

    std::optional<DB::Cas::Backend::Raw> read(const String & key, DB::Cas::TransportAccess & access) override
    {
        if (!fail_get_once_key.empty() && key == fail_get_once_key)
        {
            fail_get_once_key.clear();
            throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
                "RecoveryLatchBackend: simulated non-transient exact read failure");
        }
        {
            std::unique_lock lk(m);
            if (!block_key.empty() && key == block_key)
            {
                entered = true;
                cv.notify_all();
                /// Bounded (20s) so a wiring bug bounds the wait instead of hanging the suite.
                cv.wait_for(lk, std::chrono::seconds(20), [&] { return block_key.empty(); });
            }
        }
        return CountingBackend::read(key, access);
    }

    void armBlockedGet(const String & key)
    {
        std::lock_guard lk(m);
        block_key = key;
        entered = false;
    }
    void awaitBlockedGet()
    {
        std::unique_lock lk(m);
        cv.wait_for(lk, std::chrono::seconds(20), [&] { return entered; });
        ASSERT_TRUE(entered) << "the recovery GET never parked -- the in-progress window was not exercised";
    }
    void releaseBlockedGet()
    {
        {
            std::lock_guard lk(m);
            block_key.clear();
        }
        cv.notify_all();
    }

private:
    std::mutex m;
    std::condition_variable cv;
    String block_key;
    bool entered = false;
};

/// Parks the append lane's leader in the pre-carve window -- BEFORE it takes `ref_queue_mutex`, before
/// any `PUT`, and therefore with the table's apply-state still `Clean`. That is what isolates the
/// quiescence rule: the only thing wrong with the table while parked is that an append is in flight.
struct LeaderLatch
{
    std::mutex m;
    std::condition_variable cv;
    bool entered = false;
    bool released = false;

    void arm(const PoolPtr & store)
    {
        store->setRefPreCarveHookForTest([this]
        {
            std::unique_lock lk(m);
            if (entered)
                return;   /// only the FIRST carve parks; retries proceed straight through
            entered = true;
            cv.notify_all();
            /// Bounded (20s): a staging bug must bound the wait, not block the whole suite.
            cv.wait_for(lk, std::chrono::seconds(20), [this] { return released; });
        });
    }
    void awaitEntered()
    {
        std::unique_lock lk(m);
        cv.wait_for(lk, std::chrono::seconds(20), [this] { return entered; });
        ASSERT_TRUE(entered) << "the append lane's leader never reached the pre-carve window";
    }
    void release()
    {
        {
            std::lock_guard lk(m);
            released = true;
        }
        cv.notify_all();
    }
};

/// Rendezvous for the co-batching pre-carve hook of the chunked-flush case (same shape as
/// `gtest_cas_ref_chunked_flush.cpp`'s `CaseSync`).
struct CaseSync
{
    std::mutex m;
    std::condition_variable cv;
    bool entered = false;
};

/// `num_pairs` add-then-remove precommit op pairs on ONE ref, each pair naming a distinct manifest,
/// so an item scoped `MutationScope::ref(ref)` names exactly the ref its ops mutate (the flush
/// validates that). Every pair is undone immediately, so the LIVE state stays ~empty and validating
/// thousands of ops stays linear -- it is the OP COUNT, not the resident state, that drives the chunk
/// split under test.
std::vector<RefOp> precommitAddRemovePairs(const String & ref, size_t num_pairs, uint64_t manifest_epoch)
{
    std::vector<RefOp> ops;
    ops.reserve(num_pairs * 2);
    for (size_t i = 0; i < num_pairs; ++i)
    {
        const ManifestRef manifest{manifest_epoch, i + 1, 1};
        RefOp add;
        add.kind = RefOpKind::OwnerTransition;
        add.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, ref, manifest};
        ops.push_back(std::move(add));
        RefOp remove;
        remove.kind = RefOpKind::OwnerTransition;
        remove.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, ref, manifest};
        ops.push_back(std::move(remove));
    }
    return ops;
}

PoolPtr openPool(const BackendPtr & backend)
{
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    return Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

PoolPtr openPoolWithConfig(const BackendPtr & backend, PoolConfig config)
{
    config.pool_prefix = "p";
    config.server_root_id = "test";
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    return Pool::open(backend, std::move(config));
}

/// A legal blob-free part: an empty-entry manifest is enough to drive a real precommit+promote pair
/// through the append lane and leave a committed ref behind. Returns the committed `ManifestId`.
ManifestId publishEmptyPart(const PoolPtr & s, const RootNamespace & ns, const String & ref,
                            bool allow_repoint = false)
{
    PartWriteInfo info;
    info.intended_namespace = ns;
    info.intended_ref = ns.string() + "/" + ref;
    auto build = s->beginPartWrite(info);
    const ManifestId id = build->stageManifest({});
    build->precommitAdd(ns, ref, id);
    build->promote(ns, ref, build->buildId(), id, allow_repoint);
    return id;
}

/// Every request `CountingBackend` observes, summed: reads, heads, stream opens, writes, lists,
/// deletes and publications. The zero-I/O contract is asserted against this total, so a confirm that
/// quietly grew any one of them fails the test rather than the review. `writeTotal` and not `putTotal`:
/// a write that carried a precondition is still a write, and counting only the create-shaped ones left
/// the replace path unwatched.
uint64_t backendRequests(const CountingBackend & b)
{
    return b.headTotal() + b.getTotal() + b.getStreamTotal() + b.writeTotal() + b.listTotal()
        + b.deleteTotal() + b.publishTotal();
}

/// One refusal counter's current value. `confirmExactRef` attributes every `Unknown` to exactly one of
/// these, and a live gate reads them to tell load from a fault from a lost mount -- distinctions the
/// three-value `ConfirmAnswer` cannot carry. A test that checks only the ANSWER passes just as happily
/// when two of them are swapped, so the tests that reach a refusal deterministically assert the
/// attribution as a DELTA around the confirm, never an absolute (the suite shares one process).
uint64_t refusalCount(ProfileEvents::Event event)
{
    return ProfileEvents::global_counters[event].load();
}

/// One-shot throwing probe in the post-durable install region -- the only way to reach `NeedsRecovery`
/// transition now that §A1 made every install region allocation-free. Copied in shape from
/// `gtest_cas_ref_install_safety.cpp`: the exception is built OUTSIDE the region (constructing one
/// inside would allocate and trip `DENY_ALLOCATIONS_IN_SCOPE`), and it is `MEMORY_LIMIT_EXCEEDED`
/// rather than `LOGICAL_ERROR`, which aborts at construction in debug builds.
void armOneShotInstallFailure(const PoolPtr & store)
{
    auto planned = std::make_exception_ptr(DB::Exception(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED,
        "simulated allocation failure inside the post-durable install region"));
    auto fired = std::make_shared<std::atomic<bool>>(false);
    store->setInstallRegionProbeForTest([planned, fired]
    {
        if (fired->exchange(true))
            return;
        ALLOW_ALLOCATIONS_IN_SCOPE;
        std::rethrow_exception(planned);
    });
}

}


/// Rule 5, the affirmative case: a warm, quiescent, `Ready`, fenced table whose committed row for
/// the ref names EXACTLY the asked-about manifest answers `Yes`. Its two negatives share the test
/// because they are the same rule read the other way: a different `ManifestRef` under the right name,
/// and a name that has no committed row at all, are both `No` -- a PROOF of the negative, not an
/// ambiguity.
TEST(CASConfirmExactRef, QuiescentExactMatchIsYes)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_yes"};

    const ManifestId id = publishEmptyPart(store, ns, "x");

    EXPECT_EQ(store->confirmExactRef(ns, "x", id.ref), ConfirmAnswer::Yes);

    /// Same name, a manifest this ref never named.
    ManifestRef other = id.ref;
    ++other.manifest_ordinal;
    EXPECT_EQ(store->confirmExactRef(ns, "x", other), ConfirmAnswer::No);

    /// A name with no committed row at all -- on a warm table that is knowledge, not ambiguity.
    EXPECT_EQ(store->confirmExactRef(ns, "no_such_ref", id.ref), ConfirmAnswer::No);
}


/// Rule 5, the repoint case (spec §testing "repointed live part"): the part is still live and the ref
/// name still resolves, but it now names a DIFFERENT manifest. The old token must be `No` -- this is
/// the case gate 0 cannot see at all, because the part object is `Active` throughout.
TEST(CASConfirmExactRef, RepointedRefIsNo)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_repoint"};

    const ManifestId first = publishEmptyPart(store, ns, "x");
    ASSERT_EQ(store->confirmExactRef(ns, "x", first.ref), ConfirmAnswer::Yes);

    const ManifestId second = publishEmptyPart(store, ns, "x", /*allow_repoint=*/true);
    ASSERT_NE(first.ref, second.ref) << "a repoint must mint a fresh ManifestRef (the ABA barrier)";

    EXPECT_EQ(store->confirmExactRef(ns, "x", first.ref), ConfirmAnswer::No);
    EXPECT_EQ(store->confirmExactRef(ns, "x", second.ref), ConfirmAnswer::Yes);
}


/// Rule 5, the drop-and-recreate case: the ref name is removed and then published again. The name
/// resolves again, so only EXACT `ManifestRef` equality separates the new binding from the old one --
/// mint-tightening (spec §A3) is what guarantees the two can never collide.
TEST(CASConfirmExactRef, DroppedAndRecreatedRefIsNo)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_recreate"};

    const ManifestId first = publishEmptyPart(store, ns, "x");
    ASSERT_EQ(store->confirmExactRef(ns, "x", first.ref), ConfirmAnswer::Yes);

    store->dropRef(ns, "x");
    EXPECT_EQ(store->confirmExactRef(ns, "x", first.ref), ConfirmAnswer::No)
        << "a dropped ref cannot authorize anything";

    const ManifestId second = publishEmptyPart(store, ns, "x");
    ASSERT_NE(first.ref, second.ref);
    EXPECT_EQ(store->confirmExactRef(ns, "x", first.ref), ConfirmAnswer::No);
    EXPECT_EQ(store->confirmExactRef(ns, "x", second.ref), ConfirmAnswer::Yes);
}


/// Rule 2, the cold case: a namespace this mount has never touched has no resident runtime, so the
/// answer is `Unknown` -- and producing it must cost ZERO object-store requests AND must not create a
/// runtime. Materializing one here would let a remote caller populate this writer's table cache with
/// unrecovered entries by asking about namespaces that do not exist.
TEST(CASConfirmExactRef, ColdTableIsUnknownWithZeroBackendRequests)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace warm{"srv1/confirm_cold_warm"};
    const RootNamespace cold{"srv1/confirm_cold_never_touched"};

    const ManifestId id = publishEmptyPart(store, warm, "x");

    const size_t cached_before = store->refTablesCachedCountForTest();
    backend->resetCounts();

    EXPECT_EQ(store->confirmExactRef(cold, "x", id.ref), ConfirmAnswer::Unknown);

    EXPECT_EQ(backendRequests(*backend), 0u)
        << "a cold table must answer Unknown without recovering from storage";
    EXPECT_EQ(store->refTablesCachedCountForTest(), cached_before)
        << "confirmExactRef must find the runtime, never create one";
}


/// Rule 2, the evicted case: a table that WAS warm and was dropped by the whole-table cache budget is
/// indistinguishable from a cold one here -- the runtime is gone, so the committed view is gone with
/// it, and re-reading it would be object-store I/O.
TEST(CASConfirmExactRef, EvictedTableIsUnknownWithZeroBackendRequests)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPoolWithConfig(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test", .ref_table_cache_bytes = 1});
    const RootNamespace ns_a{"srv1/confirm_evict_a"};
    const RootNamespace ns_b{"srv1/confirm_evict_b"};

    const ManifestId id_a = publishEmptyPart(store, ns_a, "x");
    ASSERT_EQ(store->confirmExactRef(ns_a, "x", id_a.ref), ConfirmAnswer::Yes);

    /// A 1-byte budget is below one table's weight, so touching another table evicts the idle one.
    publishEmptyPart(store, ns_b, "y");
    ASSERT_FALSE(store->refTableCachedForTest(ns_a)) << "ns_a must have been evicted";

    backend->resetCounts();
    EXPECT_EQ(store->confirmExactRef(ns_a, "x", id_a.ref), ConfirmAnswer::Unknown);
    EXPECT_EQ(backendRequests(*backend), 0u)
        << "an evicted table must answer Unknown without re-recovering";
    EXPECT_FALSE(store->refTableCachedForTest(ns_a))
        << "the confirm must not have re-recovered the evicted table as a side effect";
}


/// Rule 2, the resident-but-unrecovered case: recovery failed closed, so a runtime EXISTS in the cache
/// with an empty, meaningless `state`. A naive lookup reads that empty state and answers `No`; the
/// correct answer is `Unknown`, because nothing about the durable table is known here.
TEST(CASConfirmExactRef, UnrecoveredResidentTableIsUnknownWithZeroBackendRequests)
{
    auto backend = std::make_shared<RecoveryLatchBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_unrecovered"};
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns);

    const size_t cached_before = store->refTablesCachedCountForTest();
    backend->fail_get_once_key = store->layout().refCkptKey(DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_THROW(store->resolveRef(ns, "x"), DB::Exception);

    ASSERT_EQ(store->refTablesCachedCountForTest(), cached_before + 1u)
        << "the failed recovery must still leave a runtime resident";
    ASSERT_FALSE(store->refTableCachedForTest(ns)) << "that runtime must be UNRECOVERED";

    backend->resetCounts();
    /// An unrecovered table is not a lane fault and not load: it is this mount being unable to speak
    /// for the namespace, and the live gate must be able to tell those apart.
    const uint64_t cannot_speak_before = refusalCount(ProfileEvents::CASRelinkConfirmRefusedMountCannotSpeak);
    const uint64_t broken_before = refusalCount(ProfileEvents::CASRelinkConfirmRefusedLaneBroken);
    EXPECT_EQ(store->confirmExactRef(ns, "x", ManifestRef{1, 1, 1}), ConfirmAnswer::Unknown);
    EXPECT_EQ(refusalCount(ProfileEvents::CASRelinkConfirmRefusedMountCannotSpeak) - cannot_speak_before, 1u)
        << "an unrecovered view must be reported as this mount being unable to speak for the table";
    EXPECT_EQ(refusalCount(ProfileEvents::CASRelinkConfirmRefusedLaneBroken) - broken_before, 0u)
        << "an unrecovered table is not a lane defect; the live gate asserts LaneBroken is zero";
    EXPECT_EQ(backendRequests(*backend), 0u)
        << "an unrecovered table must answer Unknown without driving recovery";
    EXPECT_FALSE(store->refTableCachedForTest(ns))
        << "the confirm must not have recovered the table as a side effect";
}


/// Rule 2, the recovering case: another caller is INSIDE `ensureRefTableRecovered`, parked on its
/// exact `_ckpt` GET. The runtime is resident, `recovery_in_progress` is set, and the state is still
/// empty. Waiting for that recovery would be exactly the "recover from storage to answer" this
/// primitive refuses.
TEST(CASConfirmExactRef, RecoveryInProgressIsUnknownWithZeroBackendRequests)
{
    auto backend = std::make_shared<RecoveryLatchBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_recovering"};
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns);
    backend->armBlockedGet(store->layout().refCkptKey(DB::Cas::tests::fixture::fixtureLife(ns)));
    std::exception_ptr recovery_error;
    std::thread recoverer([&]
    {
        try
        {
            store->resolveRef(ns, "x");
        }
        catch (...)
        {
            recovery_error = std::current_exception();
        }
    });
    backend->awaitBlockedGet();

    backend->resetCounts();
    /// The parked exact GET is outside `state_mutex` (up to the 20s block bound), so this
    /// call must NOT be a blocking acquire of that mutex: waiting would make the confirm pay for
    /// somebody else's recovery while holding pool-wide append admission. The elapsed bound is what
    /// pins that -- it is an order of magnitude below the park, so it cannot pass by luck.
    const auto started = std::chrono::steady_clock::now();
    const ConfirmAnswer answer = store->confirmExactRef(ns, "x", ManifestRef{1, 1, 1});
    const auto elapsed = std::chrono::steady_clock::now() - started;
    const uint64_t requests = backendRequests(*backend);

    backend->releaseBlockedGet();
    recoverer.join();

    if (recovery_error)
    {
        try
        {
            std::rethrow_exception(recovery_error);
        }
        catch (const std::exception & e)
        {
            FAIL() << "the driving recovery unexpectedly failed: " << e.what();
        }
        catch (...)
        {
            FAIL() << "the driving recovery unexpectedly failed with a non-standard exception";
        }
    }

    EXPECT_EQ(answer, ConfirmAnswer::Unknown);
    EXPECT_EQ(requests, 0u)
        << "a recovering table must answer Unknown without issuing (or waiting on) any request";
    EXPECT_LT(elapsed, std::chrono::seconds(5))
        << "the confirm waited for the in-progress recovery instead of answering Unknown";
}


/// Rule 3, the in-flight case: an append is admitted and its leader is parked in the pre-carve window.
/// Nothing is durable yet and the committed row still matches EXACTLY -- which is precisely why a
/// naive implementation answers `Yes` here, and precisely why that is the TOCTOU this design closes.
/// The lane state is still `Ready` and the item is in `pending`, so rule 3 reading the item's scope is
/// what produces the `Unknown`.
TEST(CASConfirmExactRef, InFlightAppendIsUnknown)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_inflight"};

    const ManifestId id = publishEmptyPart(store, ns, "x");
    ASSERT_EQ(store->confirmExactRef(ns, "x", id.ref), ConfirmAnswer::Yes);

    LeaderLatch latch;
    latch.arm(store);
    std::thread dropper([&] { store->dropRef(ns, "x"); });
    latch.awaitEntered();

    /// Sampled while parked, asserted after the join: a failed assertion here must not skip the
    /// release, or the still-joinable `dropper` would terminate the whole suite instead of failing one
    /// test.
    const bool leader_active = store->refLeaderActiveForTest(ns);
    const RefLaneState apply_state = store->laneStateForTest(ns);
    const ConfirmAnswer while_in_flight = store->confirmExactRef(ns, "x", id.ref);

    latch.release();
    dropper.join();
    store->setRefPreCarveHookForTest(nullptr);

    EXPECT_TRUE(leader_active);
    EXPECT_EQ(apply_state, RefLaneState::Ready)
        << "the pre-carve window is before any PUT, so rule 4 must not be what answers here";
    EXPECT_EQ(while_in_flight, ConfirmAnswer::Unknown)
        << "an admitted mutation of THIS ref makes its committed row provisional";

    EXPECT_EQ(store->confirmExactRef(ns, "x", id.ref), ConfirmAnswer::No);
}


/// Rule 3 at a chunk boundary (`CarvePhaseForTest::ChunkReseed`): one leader tenure commits MULTIPLE
/// durable transactions, so between two chunks the table is PARTIALLY durable -- for the refs those
/// chunks mutate. The seed ref is touched by neither, so its row is exactly as authoritative as on an
/// idle lane and it confirms; BOTH carved items' own refs refuse, because their transactions may be
/// durable and not installed -- the second one is what makes a rule that read only the front of the
/// mirror visible. The confirm is issued on the leader's own thread, which is safe because the
/// boundary holds neither lane mutex.
TEST(CASConfirmExactRef, UntouchedRefConfirmsMidTenure)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_mid_tenure"};

    const ManifestId id = publishEmptyPart(store, ns, "seed");
    ASSERT_EQ(store->confirmExactRef(ns, "seed", id.ref), ConfirmAnswer::Yes);

    std::atomic<int> boundaries{0};
    std::atomic<int> yes_for_seed_at_boundary{0};
    std::atomic<int> unknown_for_carved_ref_at_boundary{0};
    std::atomic<int> unknown_for_second_carved_ref_at_boundary{0};
    std::atomic<int> requests_at_boundary{0};
    store->setCarveHookForTest([&](CasRefLedger::CarvePhaseForTest phase)
    {
        if (phase != CasRefLedger::CarvePhaseForTest::ChunkReseed)
            return;
        boundaries.fetch_add(1);
        const uint64_t before = backendRequests(*backend);
        if (store->confirmExactRef(ns, "seed", id.ref) == ConfirmAnswer::Yes)
            yes_for_seed_at_boundary.fetch_add(1);
        /// "aaa_" has no committed row (rule 5 would say `No`), so an `Unknown` here can only come from
        /// rule 3 reading the carved item's scope.
        if (store->confirmExactRef(ns, "aaa_", id.ref) == ConfirmAnswer::Unknown)
            unknown_for_carved_ref_at_boundary.fetch_add(1);
        /// "bbb_" is the mirror's SECOND entry and likewise has no committed row, so this is the same
        /// assertion made about an entry a rule that stopped at the front of `carved` would never
        /// reach.
        if (store->confirmExactRef(ns, "bbb_", id.ref) == ConfirmAnswer::Unknown)
            unknown_for_second_carved_ref_at_boundary.fetch_add(1);
        requests_at_boundary.fetch_add(static_cast<int>(backendRequests(*backend) - before));
    });

    /// Two co-batched items of 3000 ops each (1500 precommit add/remove pairs). One item may not
    /// exceed the 5000-op `ref_txn_max_ops` cap on its own -- that fails the item outright -- so the
    /// chunk boundary has to come from a BATCH: 6000 ops carved into one tenure split into two
    /// transactions, firing exactly one boundary. The pre-carve hook parks the first caller until the
    /// second is queued, which is what makes the co-batching deterministic.
    auto sync = std::make_shared<CaseSync>();
    store->setRefPreCarveHookForTest([sync, store, ns]
    {
        std::unique_lock lk(sync->m);
        if (sync->entered)
            return;
        sync->entered = true;
        sync->cv.notify_all();
        sync->cv.wait_for(lk, std::chrono::seconds(20),
                          [&] { return store->refQueuePendingForTest(ns) >= 2; });
    });

    auto append = [&store, &ns](const String & ref, uint64_t manifest_epoch)
    {
        std::vector<RefOp> item_ops = precommitAddRemovePairs(ref, 1500, manifest_epoch);
        store->appendRefOps(ns, MutationScope::ref(ref),
                            [ops = std::move(item_ops)](const RefTableState &) { return ops; },
                            RootMutationOrigin::Writer, RootMutationKind::Publish);
    };
    std::thread a([&] { append("aaa_", 900000001); });
    {
        std::unique_lock lk(sync->m);
        sync->cv.wait_for(lk, std::chrono::seconds(20), [&] { return sync->entered; });
    }
    std::thread b([&] { append("bbb_", 900000002); });
    /// The parked leader re-evaluates its predicate only when notified, so the queue depth is polled
    /// here and the leader released explicitly once both items are admitted.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (store->refQueuePendingForTest(ns) < 2 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    sync->cv.notify_all();
    a.join();
    b.join();
    store->setRefPreCarveHookForTest(nullptr);
    store->setCarveHookForTest(nullptr);

    ASSERT_GE(boundaries.load(), 1) << "the flush did not chunk -- the mid-tenure window was not exercised";
    EXPECT_EQ(yes_for_seed_at_boundary.load(), boundaries.load())
        << "a ref no carved item names must confirm mid-tenure -- that is the liveness this rule exists for";
    EXPECT_EQ(unknown_for_carved_ref_at_boundary.load(), boundaries.load())
        << "a ref a carved item names must not confirm while its transaction may be durable and not installed";
    EXPECT_EQ(unknown_for_second_carved_ref_at_boundary.load(), boundaries.load())
        << "rule 3 must scan the whole carved mirror: 'bbb_' is its second entry and has no committed "
           "row, so a rule that examined only the front entry would answer No here";
    EXPECT_EQ(requests_at_boundary.load(), 0) << "the mid-tenure confirm must still be I/O-free";

    /// The tenure is over: the seed ref confirms as before.
    EXPECT_EQ(store->confirmExactRef(ns, "seed", id.ref), ConfirmAnswer::Yes);
}


/// Rule 3, the wedge case: the lane holds one conditional `PUT` whose outcome is unknown, so the table
/// may be MISSING a durable transaction -- possibly the very removal being asked about. The committed
/// row still matches exactly, so only the wedge can produce the refusal.
TEST(CASConfirmExactRef, WedgedLaneIsUnknown)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_wedge"};
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns);

    const ManifestId id = publishEmptyPart(store, ns, "x");
    ASSERT_EQ(store->confirmExactRef(ns, "x", id.ref), ConfirmAnswer::Yes);

    store->forceWedgeForTest(ns, /*writer_epoch=*/1, /*ref_sequence=*/9999,
                             store->layout().refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), RefTxnId{1, 9999}), "synthetic");
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));

    backend->resetCounts();
    EXPECT_EQ(store->confirmExactRef(ns, "x", id.ref), ConfirmAnswer::Unknown);
    EXPECT_EQ(backendRequests(*backend), 0u)
        << "a wedged lane must answer Unknown without trying to resolve the wedge";
}


/// Rule 3, a REAL wedge: the removal of x is sent, the response is lost, the single-attempt budget is
/// exhausted, and the lane wedges. `commitRefChunk` completes the chunk's items with an error before
/// the tenure ends, so the transaction that may be durable is recorded nowhere but in the attempt and
/// the lane state -- `pending` and `carved` are both empty. Every ref refuses: x because its removal
/// may be durable, `other` because nothing but the lane state records WHICH ref the wedged transaction
/// touched.
TEST(CASConfirmExactRef, WedgedTransactionRefusesEveryRef)
{
    auto backend = std::make_shared<LatchedChunkFaultBackend>();
    PoolConfig cfg;
    /// The budget bounds the mount lease's own admission arithmetic and nothing else: a write's attempt
    /// count is the `Retry` policy's. What makes the injected fault conclusive is that it stays armed
    /// for the whole call while the injected clock below carries the call to its own deadline.
    CasRequestBudget budget;
    budget.attempt_timeout_ms = 100;
    budget.lease_safety_margin_ms = 100;
    cfg.cas_request_budget = budget;
    /// What the request engine reserves per attempt is the BACKEND's attempt timeout, not the budget
    /// field alone; pair the two so the mount lease's admission arithmetic sees what the budget claims.
    backend->setAttemptTimeoutMs(budget.attempt_timeout_ms);
    auto store = openPoolWithConfig(backend, cfg);
    auto clock = VirtualRetryClock::installOn(store);
    const RootNamespace ns{"srv1/confirm_real_wedge"};
    /// Pins the namespace to the fixture life BEFORE its first real touch, so the fault key computed
    /// from that same life below is the key production actually writes to.
    DB::Cas::tests::casAdmitRecoverableEntry(*backend, store->layout(), ns);

    const ManifestId id_x = publishEmptyPart(store, ns, "x");
    const ManifestId id_other = publishEmptyPart(store, ns, "other");
    ASSERT_EQ(store->confirmExactRef(ns, "x", id_x.ref), ConfirmAnswer::Yes);
    ASSERT_EQ(store->confirmExactRef(ns, "other", id_other.ref), ConfirmAnswer::Yes);

    backend->fault_substr = store->layout().namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    backend->mode = DB::Cas::tests::ChunkFaultBackend::Mode::Unresolved;
    backend->fault_skip = 0;
    backend->fault_count = 1;
    backend->latched = true;
    EXPECT_THROW(store->dropRef(ns, "x"), DB::Exception);
    backend->disarm();
    /// The give-up was the call's OWN retry window: the fault outlasted several reissues and every one
    /// of them paced through the injected sleep rather than a real one.
    EXPECT_GT(clock->pauseCount(), 1u);
    EXPECT_LE(clock->longestPause(), 5000u) << "each pause is the engine's own capped full jitter";
    EXPECT_GE(clock->nowMs(), 60000u);
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));
    ASSERT_EQ(store->refQueuePendingForTest(ns), 0u);
    ASSERT_EQ(store->refCarvedForTest(ns), 0u)
        << "the wedged item was completed and released; only the lane state records its transaction";

    backend->resetCounts();
    /// A wedge and a broken lane are two DIFFERENT things to a live gate -- one is an unresolved append
    /// that the next flush or a remount clears, the other is a lane defect. Both refuse here, and
    /// without these deltas the test would pass just as happily if the wedge branch were deleted and
    /// the broken-lane branch answered for it.
    const uint64_t wedged_before = refusalCount(ProfileEvents::CASRelinkConfirmRefusedLaneWedged);
    const uint64_t broken_before = refusalCount(ProfileEvents::CASRelinkConfirmRefusedLaneBroken);
    EXPECT_EQ(store->confirmExactRef(ns, "x", id_x.ref), ConfirmAnswer::Unknown) << "x's removal may be durable";
    EXPECT_EQ(store->confirmExactRef(ns, "other", id_other.ref), ConfirmAnswer::Unknown)
        << "a wedge refuses table-wide: no per-ref record of the wedged transaction survives the tenure";
    EXPECT_EQ(backendRequests(*backend), 0u) << "a wedged lane must answer without trying to resolve the wedge";
    EXPECT_EQ(refusalCount(ProfileEvents::CASRelinkConfirmRefusedLaneWedged) - wedged_before, 2u)
        << "both refusals must be reported as a wedge";
    EXPECT_EQ(refusalCount(ProfileEvents::CASRelinkConfirmRefusedLaneBroken) - broken_before, 0u)
        << "a wedge is not a lane defect: reporting it as one would send the live gate hunting a bug";
}


/// `carved` bookkeeping: a carved item leaves `pending` at the carve and is completed by its chunk's
/// install (or earlier, by an error), while the mirror is cleared only at the tenure's exit guard, so
/// the confirm reads the item from `rt.carved` from carve to tenure end. Sampled at
/// `PostDurableInstall` -- the transaction is durable, nothing is installed, `pending` is already
/// empty -- and again after the tenure: the mirror must hold exactly the carved item during, and be
/// empty after. The hook runs on the leader's own thread with neither lane mutex held, so the seams
/// (which take `ref_queue_mutex`) are safe to call from it.
TEST(CASConfirmExactRef, CarvedItemIsVisibleFromCarveToTenureEnd)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_carved"};
    publishEmptyPart(store, ns, "x");

    std::atomic<int> samples{0};
    std::atomic<size_t> carved_during{0};
    std::atomic<size_t> pending_during{0};
    store->setCarveHookForTest([&](CasRefLedger::CarvePhaseForTest phase)
    {
        if (phase != CasRefLedger::CarvePhaseForTest::PostDurableInstall)
            return;
        samples.fetch_add(1);
        carved_during.store(store->refCarvedForTest(ns));
        pending_during.store(store->refQueuePendingForTest(ns));
    });
    store->dropRef(ns, "x");
    store->setCarveHookForTest(nullptr);

    ASSERT_EQ(samples.load(), 1) << "the drop must commit exactly one chunk";
    EXPECT_EQ(carved_during.load(), 1u)
        << "the carved removal must be visible while its transaction is durable but not installed";
    EXPECT_EQ(pending_during.load(), 0u)
        << "the carve popped the item out of pending -- carved is the only place it can be seen";
    EXPECT_EQ(store->refCarvedForTest(ns), 0u) << "the exit guard must release the mirror";
}

/// Scope validation: `MutationScope` is what the confirm reads to decide whether an in-flight mutation
/// may change the ref it is asked about, so an item scoped to ref X must fail, alone, before anything
/// is durable, both when its ops mutate ref Y and when they carry a namespace removal, which names no
/// ref and moves every row. It throws `LOGICAL_ERROR`, which aborts the process in debug and
/// sanitizer builds instead of behaving like a catchable exception --
/// `CASConfirmExactRefDeathTest.MisScopedItemAborts` below proves the abort positively in those builds.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASConfirmExactRef, MisScopedItemFailsBeforeAnythingIsDurable)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_misscoped"};
    const ManifestId seed = publishEmptyPart(store, ns, "seed");   /// the namespace is born already

    const uint64_t writes_before = backend->writeTotal();
    RefOp add;
    add.kind = RefOpKind::OwnerTransition;
    add.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "y", ManifestRef{900000003, 1, 1}};
    try
    {
        store->appendRefOps(ns, MutationScope::ref("x"),
                            [add](const RefTableState &) { return std::vector<RefOp>{add}; },
                            RootMutationOrigin::Writer, RootMutationKind::Publish);
        FAIL() << "an item scoped to ref 'x' whose op binds ref 'y' must be refused";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::LOGICAL_ERROR);
    }
    /// A namespace removal names no ref at all, so a scope check that only compared names would let it
    /// through -- and it moves every row, which is the one thing a `Ref` scope promises the confirm
    /// will not happen behind its back.
    RefOp remove_namespace;
    remove_namespace.kind = RefOpKind::RemoveNamespace;
    try
    {
        store->appendRefOps(ns, MutationScope::ref("x"),
                            [remove_namespace](const RefTableState &) { return std::vector<RefOp>{remove_namespace}; },
                            RootMutationOrigin::Writer, RootMutationKind::Publish);
        FAIL() << "an item scoped to ref 'x' carrying a namespace removal must be refused";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::LOGICAL_ERROR)
            << "the scope check must be what rejects it";
    }

    /// The ref-log transaction object -- the only thing that would make this item durable -- is a
    /// create, and this counts every write the backend can observe rather than that one shape, so the
    /// fence still holds if the durable step ever changes shape.
    EXPECT_EQ(backend->writeTotal(), writes_before) << "the refusal must happen before any object is written";
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready) << "a validation failure is not a lane fault";
    EXPECT_EQ(store->confirmExactRef(ns, "seed", seed.ref), ConfirmAnswer::Yes)
        << "the failed item must leave the table exactly as it was";
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASConfirmExactRefDeathTest, MisScopedItemAborts)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_misscoped"};
    publishEmptyPart(store, ns, "seed");

    RefOp add;
    add.kind = RefOpKind::OwnerTransition;
    add.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "y", ManifestRef{900000003, 1, 1}};
    EXPECT_DEATH({
        store->appendRefOps(ns, MutationScope::ref("x"),
                            [add](const RefTableState &) { return std::vector<RefOp>{add}; },
                            RootMutationOrigin::Writer, RootMutationKind::Publish);
    }, "");

    RefOp remove_namespace;
    remove_namespace.kind = RefOpKind::RemoveNamespace;
    EXPECT_DEATH({
        store->appendRefOps(ns, MutationScope::ref("x"),
                            [remove_namespace](const RefTableState &) { return std::vector<RefOp>{remove_namespace}; },
                            RootMutationOrigin::Writer, RootMutationKind::Publish);
    }, "");
}
#endif

/// The mirror must survive an item's COMPLETION, not just its carve: an item is completed by its own
/// chunk's commit, often chunks before the tenure ends. Two items whose op counts force a chunk split
/// (mirrors `UntouchedRefConfirmsMidTenure`'s co-batching) are carved together in one tenure: chunk 1
/// = {aaa_} alone, chunk 2 = {bbb_} alone. Sampled at chunk 2's `PostDurableInstall`, the test PROVES --
/// via `refCarvedItemDoneForTest`, not by inferring from hook order -- that aaa_ is already done, and
/// that it is still counted in `carved` alongside bbb_ until the tenure's exit guard.
TEST(CASConfirmExactRef, CarvedItemSurvivesEarlierChunkCompletion)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_carved_multi_chunk"};
    publishEmptyPart(store, ns, "seed");

    std::atomic<int> boundaries{0};
    std::atomic<bool> aaa_done_at_second_boundary{false};
    std::atomic<size_t> carved_at_second_boundary{0};
    store->setCarveHookForTest([&](CasRefLedger::CarvePhaseForTest phase)
    {
        if (phase != CasRefLedger::CarvePhaseForTest::PostDurableInstall)
            return;
        if (boundaries.fetch_add(1) + 1 != 2)
            return;   /// only chunk 2's durable point is of interest
        aaa_done_at_second_boundary.store(store->refCarvedItemDoneForTest(ns, "aaa_"));
        carved_at_second_boundary.store(store->refCarvedForTest(ns));
    });

    /// Co-batching setup identical to `UntouchedRefConfirmsMidTenure`: the pre-carve hook parks the
    /// first caller until the second is queued, so both items are carved together deterministically.
    auto sync = std::make_shared<CaseSync>();
    store->setRefPreCarveHookForTest([sync, store, ns]
    {
        std::unique_lock lk(sync->m);
        if (sync->entered)
            return;
        sync->entered = true;
        sync->cv.notify_all();
        sync->cv.wait_for(lk, std::chrono::seconds(20),
                          [&] { return store->refQueuePendingForTest(ns) >= 2; });
    });

    auto append = [&store, &ns](const String & ref, uint64_t manifest_epoch)
    {
        std::vector<RefOp> item_ops = precommitAddRemovePairs(ref, 1500, manifest_epoch);
        store->appendRefOps(ns, MutationScope::ref(ref),
                            [ops = std::move(item_ops)](const RefTableState &) { return ops; },
                            RootMutationOrigin::Writer, RootMutationKind::Publish);
    };
    std::thread a([&] { append("aaa_", 900000001); });
    {
        std::unique_lock lk(sync->m);
        sync->cv.wait_for(lk, std::chrono::seconds(20), [&] { return sync->entered; });
    }
    std::thread b([&] { append("bbb_", 900000002); });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (store->refQueuePendingForTest(ns) < 2 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    sync->cv.notify_all();
    a.join();
    b.join();
    store->setRefPreCarveHookForTest(nullptr);
    store->setCarveHookForTest(nullptr);

    ASSERT_EQ(boundaries.load(), 2) << "the flush must chunk into exactly two transactions";
    ASSERT_TRUE(aaa_done_at_second_boundary.load())
        << "chunk 1's item must already be done by the time chunk 2 goes durable";
    EXPECT_EQ(carved_at_second_boundary.load(), 2u)
        << "a completed item must still be counted in the mirror until the tenure's exit guard";
    EXPECT_EQ(store->refCarvedForTest(ns), 0u) << "the exit guard must release the mirror after both chunks";
}


/// `NeedsRecovery` is table-scoped, so confirmation refuses even a row that still looks perfect.
TEST(CASConfirmExactRef, NeedsRecoveryIsUnknown)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_poison"};

    const ManifestId keep = publishEmptyPart(store, ns, "keep");
    ASSERT_EQ(store->confirmExactRef(ns, "keep", keep.ref), ConfirmAnswer::Yes);

    armOneShotInstallFailure(store);
    EXPECT_THROW(publishEmptyPart(store, ns, "other"), DB::Exception);
    store->setInstallRegionProbeForTest(nullptr);

    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);
    ASSERT_FALSE(store->refLaneWedgedForTest(ns));
    ASSERT_FALSE(store->refLeaderActiveForTest(ns));

    /// The positive side of the wedge case's negative: `NeedsRecovery` is the lane defect
    /// `LaneBroken` is FOR, so this is the one refusal that must be reported as one.
    const uint64_t broken_before = refusalCount(ProfileEvents::CASRelinkConfirmRefusedLaneBroken);
    const uint64_t wedged_before = refusalCount(ProfileEvents::CASRelinkConfirmRefusedLaneWedged);
    EXPECT_EQ(store->confirmExactRef(ns, "keep", keep.ref), ConfirmAnswer::Unknown)
        << "a table that may be missing a durable transaction cannot confirm ANY of its rows";
    EXPECT_EQ(refusalCount(ProfileEvents::CASRelinkConfirmRefusedLaneBroken) - broken_before, 1u)
        << "a NeedsRecovery lane is exactly what LaneBroken reports";
    EXPECT_EQ(refusalCount(ProfileEvents::CASRelinkConfirmRefusedLaneWedged) - wedged_before, 0u)
        << "the lane is not wedged here, and the two counters must not be interchangeable";
}


/// Rule 6, checked LAST: the committed row matches exactly, the lane is quiescent and clean -- but this
/// node no longer holds the mount incarnation, so it is no longer the namespace's single writer and
/// cannot speak for the durable table at all. Another writer may already have repointed the ref.
TEST(CASConfirmExactRef, LostMountFenceIsUnknown)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_fence"};

    const ManifestId id = publishEmptyPart(store, ns, "x");
    ASSERT_EQ(store->confirmExactRef(ns, "x", id.ref), ConfirmAnswer::Yes);

    store->tripMountLost();

    ASSERT_TRUE(store->refTableCachedForTest(ns))
        << "the table must still be resident, so it is the FENCE that refuses, not residency";
    backend->resetCounts();
    /// The other arm of the same counter: rule 6's fence check. Losing the mount is the most
    /// safety-relevant refusal this function has, so it must not be the silent one.
    const uint64_t cannot_speak_before = refusalCount(ProfileEvents::CASRelinkConfirmRefusedMountCannotSpeak);
    EXPECT_EQ(store->confirmExactRef(ns, "x", id.ref), ConfirmAnswer::Unknown);
    EXPECT_EQ(refusalCount(ProfileEvents::CASRelinkConfirmRefusedMountCannotSpeak) - cannot_speak_before, 1u)
        << "a refusal for a lost mount fence must be counted, not invisible";
    EXPECT_EQ(backendRequests(*backend), 0u);

    /// The fence is checked LAST, so it gates only the `Yes`: a token that does not match the committed
    /// row is still reported as `No` under a lost fence. That is deliberate and harmless -- `No` and
    /// `Unknown` are the same outcome for the caller (both `SourceProofFailed`) -- and pinning it here
    /// keeps a future reordering of the rules from changing the answer silently.
    ManifestRef other = id.ref;
    ++other.manifest_ordinal;
    EXPECT_EQ(store->confirmExactRef(ns, "x", other), ConfirmAnswer::No);
}


/// The two-mutex snapshot race (spec §testing, "an append admitted concurrently is ordered strictly
/// after the snapshot"). The confirm holds `ref_queue_mutex` across the whole snapshot, and admission
/// (`pending.push_back`) takes that same mutex, so every append is either entirely before the snapshot
/// (and visible as a pending item -> `Unknown`) or entirely after it. What must NOT exist is a window
/// in which the removal is admitted and the confirm still says `Yes`.
///
/// The three phases are driven deterministically rather than hammered: before admission -> `Yes`;
/// from admission until the transaction is durable -> `Unknown`; after -> `No`. `Yes` is never
/// observable once the removal has been admitted.
TEST(CASConfirmExactRef, ConcurrentAppendIsOrderedAfterTheSnapshot)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_race"};

    const ManifestId id = publishEmptyPart(store, ns, "x");
    ASSERT_EQ(store->confirmExactRef(ns, "x", id.ref), ConfirmAnswer::Yes) << "phase 1: before admission";

    LeaderLatch latch;
    latch.arm(store);
    std::thread dropper([&] { store->dropRef(ns, "x"); });
    latch.awaitEntered();

    /// Phase 2: admitted, nothing durable. Sampled repeatedly so a single lucky interleaving cannot
    /// pass for the invariant, and TALLIED rather than asserted -- an assertion here would skip the
    /// release below and terminate the suite on the still-joinable `dropper`.
    int not_unknown = 0;
    int saw_yes = 0;
    for (int i = 0; i < 64; ++i)
    {
        const ConfirmAnswer a = store->confirmExactRef(ns, "x", id.ref);
        if (a != ConfirmAnswer::Unknown)
            ++not_unknown;
        if (a == ConfirmAnswer::Yes)
            ++saw_yes;
    }

    latch.release();
    dropper.join();
    store->setRefPreCarveHookForTest(nullptr);

    EXPECT_EQ(saw_yes, 0) << "phase 2: an admitted removal must never leave a Yes visible";
    EXPECT_EQ(not_unknown, 0) << "phase 2: an admitted append makes the committed view provisional";

    /// Phase 3: durable and applied.
    EXPECT_EQ(store->confirmExactRef(ns, "x", id.ref), ConfirmAnswer::No) << "phase 3: after the removal";
}


/// The livelock shape: a mutation of ANOTHER ref is queued and its leader is parked before the carve,
/// so the lane has a pending item and an active tenure. A confirm about an untouched committed ref must
/// answer `Yes` -- the queued mutation cannot change this ref's binding or the blobs its manifest
/// protects -- while the queued ref itself answers `Unknown`.
///
/// A SECOND queued mutation, of a third ref, sits behind the leader's own item, so the queue holds two
/// and the ref asked about last is not at its front: that is what makes a rule reading only
/// `pending`'s front item visible, which every other test in this file would pass.
TEST(CASConfirmExactRef, UntouchedRefConfirmsWhileAnotherRefIsQueued)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_liveness"};

    const ManifestId id_x = publishEmptyPart(store, ns, "x");
    const ManifestId id_other = publishEmptyPart(store, ns, "other");
    const ManifestId id_third = publishEmptyPart(store, ns, "third");
    ASSERT_EQ(store->confirmExactRef(ns, "x", id_x.ref), ConfirmAnswer::Yes);

    LeaderLatch latch;
    latch.arm(store);
    std::thread dropper([&] { store->dropRef(ns, "other"); });
    latch.awaitEntered();
    /// The leader pushes its own item before it takes the baton, so the queue is [other, third] and
    /// `third` is reachable only by a scan that goes past the front.
    std::thread second_dropper([&] { store->dropRef(ns, "third"); });
    const auto queued_by = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (store->refQueuePendingForTest(ns) < 2 && std::chrono::steady_clock::now() < queued_by)
        std::this_thread::yield();

    /// Sampled while parked, asserted after the join (a failed assertion here would skip the release).
    const bool leader_active = store->refLeaderActiveForTest(ns);
    const size_t pending = store->refQueuePendingForTest(ns);
    /// The refusal counters are the only way a live gate can read WHY a confirm said `Unknown`, so the
    /// attribution is pinned here rather than left to the reader of the .cpp: this pair of confirms is
    /// the one place where a `Yes` and a scope-driven `Unknown` are produced back to back from the same
    /// lane state.
    const uint64_t in_flight_before = refusalCount(ProfileEvents::CASRelinkConfirmRefusedRefMutationInFlight);
    const uint64_t broken_before = refusalCount(ProfileEvents::CASRelinkConfirmRefusedLaneBroken);
    const ConfirmAnswer untouched = store->confirmExactRef(ns, "x", id_x.ref);
    const uint64_t in_flight_after_untouched
        = refusalCount(ProfileEvents::CASRelinkConfirmRefusedRefMutationInFlight);
    const ConfirmAnswer touched = store->confirmExactRef(ns, "other", id_other.ref);
    const uint64_t in_flight_after_touched
        = refusalCount(ProfileEvents::CASRelinkConfirmRefusedRefMutationInFlight);
    const ConfirmAnswer touched_behind_the_front = store->confirmExactRef(ns, "third", id_third.ref);
    const uint64_t in_flight_after_behind_the_front
        = refusalCount(ProfileEvents::CASRelinkConfirmRefusedRefMutationInFlight);
    const uint64_t broken_after = refusalCount(ProfileEvents::CASRelinkConfirmRefusedLaneBroken);

    latch.release();
    dropper.join();
    second_dropper.join();
    store->setRefPreCarveHookForTest(nullptr);

    EXPECT_TRUE(leader_active);
    EXPECT_EQ(pending, 2u) << "the second dropper must be queued behind the parked leader's own item";
    EXPECT_EQ(in_flight_after_untouched - in_flight_before, 0u)
        << "a confirm that answers Yes must not be counted as a refusal";
    EXPECT_EQ(in_flight_after_touched - in_flight_after_untouched, 1u)
        << "the refused confirm must be attributed to the ref-scoped mutation, which is what a live "
           "gate reads to tell load from a lane fault";
    EXPECT_EQ(in_flight_after_behind_the_front - in_flight_after_touched, 1u)
        << "the second queued item's ref must be refused for the same reason as the first";
    EXPECT_EQ(broken_after - broken_before, 0u)
        << "the lane is Ready here: a refusal attributed to a broken lane would misreport a fault";
    EXPECT_EQ(untouched, ConfirmAnswer::Yes)
        << "a queued mutation of another ref must not refuse this one";
    EXPECT_EQ(touched, ConfirmAnswer::Unknown)
        << "the queued ref's own row is provisional";
    EXPECT_EQ(touched_behind_the_front, ConfirmAnswer::Unknown)
        << "rule 3 must scan the whole pending queue, not only its front item";
    EXPECT_EQ(store->confirmExactRef(ns, "x", id_x.ref), ConfirmAnswer::Yes);
    EXPECT_EQ(store->confirmExactRef(ns, "other", id_other.ref), ConfirmAnswer::No);
    EXPECT_EQ(store->confirmExactRef(ns, "third", id_third.ref), ConfirmAnswer::No);
}


/// Rule 3's `WholeShard` arm. A mutation that declares no ref is one that may move EVERY row, so it
/// refuses every ref for as long as it is queued or carved -- unlike a `Ref`-scoped neighbour, which
/// refuses only its own. In production `dropNamespaceImpl` and `sweepStalePrecommitsNow` are the two
/// appenders that declare `wholeShard()`.
///
/// The two confirms of "x" differ in exactly one thing: whether the whole-shard item has been queued.
/// The first is the liveness answer this rule was narrowed to give, the second the refusal the arm
/// exists for, so deleting the arm turns the second into the first.
TEST(CASConfirmExactRef, WholeShardScopedMutationRefusesAnUntouchedRef)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_whole_shard"};

    const ManifestId id_x = publishEmptyPart(store, ns, "x");
    publishEmptyPart(store, ns, "other");
    ASSERT_EQ(store->confirmExactRef(ns, "x", id_x.ref), ConfirmAnswer::Yes);

    LeaderLatch latch;
    latch.arm(store);
    std::thread dropper([&] { store->dropRef(ns, "other"); });
    latch.awaitEntered();

    /// Sampled while parked, asserted after the join (a failed assertion here would skip the release).
    const ConfirmAnswer before_whole_shard = store->confirmExactRef(ns, "x", id_x.ref);

    /// Queued BEHIND the parked leader's own `Ref`-scoped item. The ops are an add/remove precommit
    /// pair on a ref of its own, so the item is ordinary work that happens to declare no ref -- the
    /// scope, not the ops, is what rule 3 reads.
    std::thread whole_shard([&]
    {
        store->appendRefOps(ns, MutationScope::wholeShard(),
                            [](const RefTableState &) { return precommitAddRemovePairs("zzz_", 1, 900000004); },
                            RootMutationOrigin::Writer, RootMutationKind::Publish);
    });
    const auto queued_by = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (store->refQueuePendingForTest(ns) < 2 && std::chrono::steady_clock::now() < queued_by)
        std::this_thread::yield();
    const size_t pending = store->refQueuePendingForTest(ns);

    const uint64_t in_flight_before = refusalCount(ProfileEvents::CASRelinkConfirmRefusedRefMutationInFlight);
    const uint64_t broken_before = refusalCount(ProfileEvents::CASRelinkConfirmRefusedLaneBroken);
    const ConfirmAnswer with_whole_shard = store->confirmExactRef(ns, "x", id_x.ref);
    const uint64_t in_flight_after = refusalCount(ProfileEvents::CASRelinkConfirmRefusedRefMutationInFlight);
    const uint64_t broken_after = refusalCount(ProfileEvents::CASRelinkConfirmRefusedLaneBroken);

    latch.release();
    dropper.join();
    whole_shard.join();
    store->setRefPreCarveHookForTest(nullptr);

    EXPECT_EQ(pending, 2u) << "the whole-shard item must be queued behind the parked leader's own item";
    EXPECT_EQ(before_whole_shard, ConfirmAnswer::Yes)
        << "with only a Ref-scoped mutation of another ref queued, 'x' must still confirm";
    EXPECT_EQ(with_whole_shard, ConfirmAnswer::Unknown)
        << "a queued mutation that declares no ref may move every row, so no ref may confirm";
    EXPECT_EQ(in_flight_after - in_flight_before, 1u)
        << "the refusal must be attributed to an in-flight mutation, not to a lane or mount condition";
    EXPECT_EQ(broken_after - broken_before, 0u)
        << "the lane is Ready here: a refusal attributed to a broken lane would misreport a fault";

    /// The tenure is over and the whole-shard item is applied: 'x' confirms again.
    EXPECT_EQ(store->confirmExactRef(ns, "x", id_x.ref), ConfirmAnswer::Yes);
}


/// The stale-row hazard rule 3 exists for, on the same ref: a repoint of x from m1 to m2 is DURABLE
/// and NOT installed, so the committed row still says m1. A `Yes` here would let a receiver promote a
/// manifest whose blobs the durable repoint may already have retired. The leader is parked at the
/// SECOND `PostDurableInstall` of the repointing publish (the first is its precommit), with no lane
/// mutex held; `pending` is already empty, so only the carved mirror can refuse.
TEST(CASConfirmExactRef, SameRefRepointDurableButNotInstalledIsUnknown)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    const RootNamespace ns{"srv1/confirm_stale_row"};
    const ManifestId m1 = publishEmptyPart(store, ns, "x");
    ASSERT_EQ(store->confirmExactRef(ns, "x", m1.ref), ConfirmAnswer::Yes);

    struct Hold
    {
        std::mutex m;
        std::condition_variable cv;
        int seen = 0;
        bool parked = false;
        bool released = false;
    };
    auto hold = std::make_shared<Hold>();
    store->setCarveHookForTest([hold](CasRefLedger::CarvePhaseForTest phase)
    {
        if (phase != CasRefLedger::CarvePhaseForTest::PostDurableInstall)
            return;
        std::unique_lock lk(hold->m);
        if (++hold->seen != 2)
            return;   /// 1 = the precommit's chunk; 2 = the promote (the repoint) -- park here
        hold->parked = true;
        hold->cv.notify_all();
        hold->cv.wait_for(lk, std::chrono::seconds(20), [&] { return hold->released; });
    });

    ManifestId m2;
    std::thread repointer([&] { m2 = publishEmptyPart(store, ns, "x", /*allow_repoint=*/true); });
    bool parked = false;
    {
        std::unique_lock lk(hold->m);
        parked = hold->cv.wait_for(lk, std::chrono::seconds(20), [&] { return hold->parked; });
    }
    const size_t pending_now = store->refQueuePendingForTest(ns);
    const size_t carved_now = store->refCarvedForTest(ns);
    const RefLaneState lane_now = store->laneStateForTest(ns);
    /// `pending` is empty and the mirror holds one, so this refusal provably comes from the carved
    /// loop -- the only place where its attribution can be pinned.
    const uint64_t in_flight_before = refusalCount(ProfileEvents::CASRelinkConfirmRefusedRefMutationInFlight);
    const uint64_t broken_before = refusalCount(ProfileEvents::CASRelinkConfirmRefusedLaneBroken);
    const ConfirmAnswer stale = store->confirmExactRef(ns, "x", m1.ref);
    const uint64_t in_flight_delta
        = refusalCount(ProfileEvents::CASRelinkConfirmRefusedRefMutationInFlight) - in_flight_before;
    const uint64_t broken_delta = refusalCount(ProfileEvents::CASRelinkConfirmRefusedLaneBroken) - broken_before;
    {
        std::lock_guard lk(hold->m);
        hold->released = true;
    }
    hold->cv.notify_all();
    repointer.join();
    store->setCarveHookForTest(nullptr);

    ASSERT_TRUE(parked) << "the repoint never reached its post-durable window";
    EXPECT_EQ(pending_now, 0u) << "the repoint was carved: pending cannot be what refuses";
    EXPECT_EQ(carved_now, 1u) << "the carved mirror is what the confirm must read";
    EXPECT_EQ(lane_now, RefLaneState::Writing);
    EXPECT_EQ(stale, ConfirmAnswer::Unknown)
        << "x's durable repoint is not installed: its row is stale and must not confirm m1";
    EXPECT_EQ(in_flight_delta, 1u)
        << "a refusal read off the carved mirror is a mutation in flight, not a lane fault";
    EXPECT_EQ(broken_delta, 0u)
        << "the lane is Writing with a carved item -- the healthy shape, not a broken one";
    EXPECT_EQ(store->confirmExactRef(ns, "x", m1.ref), ConfirmAnswer::No) << "installed: m1 is no longer x's binding";
    EXPECT_EQ(store->confirmExactRef(ns, "x", m2.ref), ConfirmAnswer::Yes);
}


/// ===========================================================================================
/// Task 11: the EXCHANGE-level confirm -- `IContentAddressedExchange::ownsNamespace` (routing) and
/// `::confirmExactRef` (the storage forward of gate 1, plus the token text and the disk lifecycle).
///
/// Gate 0 is not exercised here on purpose: it reads a `StorageReplicatedMergeTree` parts set, so
/// `Deleting`, absent, other-disk and the `MOVE ... TO DISK` same-name case are integration-level and
/// belong to the Task 16 pytest battery.
/// ===========================================================================================

namespace
{

/// A storage adapter over its own private local object storage. `startup` is the caller's business:
/// several tests below assert behavior BEFORE it and AFTER `shutdown`.
std::shared_ptr<DB::ContentAddressedMetadataStorage> makeExchangeStorage(const std::string & server_root_id)
{
    auto settings = tests::makeSettingsForTest(
        server_root_id, std::filesystem::temp_directory_path() / "ca_confirm_exchange_scratch");
    return std::make_shared<DB::ContentAddressedMetadataStorage>(
        tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);
}

const std::string kExchangeTableDir = "e11/e11e11e1-0808-4808-8808-080808080808";
const std::string kExchangePartName = "all_1_1_0";
const std::string kExchangePartDir = kExchangeTableDir + "/" + kExchangePartName;

/// Commit one real part through the ordinary transaction path, so the committed binding under test is
/// produced exactly the way an INSERT produces it.
void commitExchangePart(DB::ContentAddressedMetadataStorage & storage)
{
    auto tx = storage.createTransaction();
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
    auto buf = ca_tx.writeFile(kExchangeTableDir + "/tmp_insert_" + kExchangePartName + "/data.bin",
                               65536, DB::WriteMode::Rewrite, {});
    const std::string bytes = "content-of-the-part";
    buf->write(bytes.data(), bytes.size());
    buf->finalize();
    tx->moveDirectory(kExchangeTableDir + "/tmp_insert_" + kExchangePartName, kExchangePartDir);
    tx->commit(DB::NoCommitOptions{});
}

/// The token the sender mints for the committed part, decoded back into its fields. Read through the
/// real offer path rather than reconstructed, so the tests below exercise exactly what goes on the wire.
DB::CasRelinkSourceToken readSourceToken(const DB::ContentAddressedMetadataStorage & storage)
{
    const auto offer = storage.getRelinkOffer(kExchangePartDir);
    EXPECT_TRUE(offer.has_value()) << "the committed part must offer a manifest to relink";
    if (!offer)
        return {};
    const auto token = DB::decodeCasRelinkSourceToken(offer->confirm_token);
    EXPECT_TRUE(token.has_value()) << "the sender minted a token its own decoder rejects";
    return token.value_or(DB::CasRelinkSourceToken{});
}

}


/// The canonical text form of a `ManifestRef` becomes wire input with Task 11: it is what the confirm
/// token carries and what `confirmExactRef` compares. It is therefore parsed as untrusted input --
/// exactly three decimal fields, nothing consumed partially, no sign, no padding -- and the parser is
/// pinned as the exact inverse of the renderer, because a mismatch between the two would silently turn
/// every confirm into an `Unknown` (or, far worse, make two different manifests compare equal).
TEST(CASConfirmExactRef, ManifestRefTextRoundTripsAndRejectsMalformedTokens)
{
    for (const ManifestRef & ref : {ManifestRef{1, 1, 1}, ManifestRef{7, 42, 999999},
                                    ManifestRef{18446744073709551615ULL, 18446744073709551615ULL, 123}})
    {
        const String text = manifestRefDebugString(ref);
        const auto parsed = tryParseManifestRef(text);
        ASSERT_TRUE(parsed.has_value()) << "the renderer produced text its own parser rejects: " << text;
        EXPECT_EQ(*parsed, ref) << text;
    }

    EXPECT_EQ(manifestRefDebugString(ManifestRef{7, 42, 3}), "7:42:3") << "the canonical form is epoch:build:ordinal";

    for (const std::string_view malformed : {
             "", "1", "1:2", "1:2:3:4", ":2:3", "1::3", "1:2:", "1:2:x", "x:2:3", " 1:2:3", "1:2:3 ",
             "1: 2:3", "+1:2:3", "-1:2:3", "1:2:3\n", "0x1:2:3",
             /// `0` is the reserved invalid ordinal and is never emitted; `1000000` is past the six-digit
             /// filename range, so neither can name a real manifest.
             "1:2:0", "1:2:1000000",
             /// One past `uint64` / `uint32` -- `from_chars` reports overflow rather than truncating.
             "18446744073709551616:1:1", "1:18446744073709551616:1", "1:1:4294967296"})
    {
        EXPECT_FALSE(tryParseManifestRef(malformed).has_value())
            << "accepted a malformed manifest reference: '" << malformed << "'";
    }
}


/// Task 13, the confirm token's wire codec (spec §wire-protocol). The token is minted by the sender,
/// stored nowhere, and handed back by an untrusted peer, so the only property that matters is that
/// decode is the exact inverse of encode: the fields the sender meant are the fields that route and
/// compare. A codec that merged two fields, or that let a separator through unescaped, would let a
/// peer aim a confirm at a namespace the sender never named.
TEST(CASConfirmExactRef, SourceTokenRoundTripsThroughItsWireForm)
{
    const auto round_trip = [](const DB::CasRelinkSourceToken & token, const char * what)
    {
        const auto text = DB::encodeCasRelinkSourceToken(token);
        ASSERT_TRUE(text.has_value()) << what;
        /// The wire form is cookie-safe and URL-safe by construction: only the RFC 3986 unreserved set,
        /// the escape character, and the field separator ever appear in it.
        for (const char ch : *text)
            EXPECT_TRUE(std::isalnum(static_cast<unsigned char>(ch))
                        || std::string_view("-._~%|").find(ch) != std::string_view::npos)
                << "the wire form leaked an unsafe character '" << ch << "' from " << what << ": " << *text;

        const auto decoded = DB::decodeCasRelinkSourceToken(*text);
        ASSERT_TRUE(decoded.has_value()) << what << ": " << *text;
        EXPECT_EQ(decoded->pool_uuid, token.pool_uuid) << what;
        EXPECT_EQ(decoded->server_root_id, token.server_root_id) << what;
        EXPECT_EQ(decoded->root_namespace, token.root_namespace) << what;
        EXPECT_EQ(decoded->ref_name, token.ref_name) << what;
        EXPECT_EQ(decoded->part_name, token.part_name) << what;
        EXPECT_EQ(decoded->manifest_ref_text, token.manifest_ref_text) << what;
    };

    round_trip({"abcdef0123456789", "srv1", "srv1/store/abc/abcdef@cas@", "all_1_1_0", "all_1_1_0", "1:1:1"},
               "the ordinary shape");
    /// The characters that make a naive codec wrong: the separator itself, the escape character, the
    /// cookie-forbidden set, and the `/`+`@` a namespace and a detached ref carry as a matter of course.
    round_trip({"p|o%o=l", "srv 1;x,y", "srv 1;x,y/store/abc/abcdef@cas@", "detached/broken_all_1_1_0",
                "all_1_1_0", "18446744073709551615:18446744073709551615:999999"},
               "the hostile shape");
    /// A field at the cap must survive; one past it must not (asserted below).
    round_trip({String(256, 'a'), "srv1", "srv1/store/abc/abcdef@cas@", "all_1_1_0", "all_1_1_0", "1:1:1"},
               "a field at the length cap");
}

/// Everything a peer can hand back that is not a token this sender minted. None of these may decode:
/// a decoded-but-wrong token routes a confirm somewhere, and "somewhere" is exactly what routing exists
/// to prevent. Refusing costs a byte fetch and nothing else.
TEST(CASConfirmExactRef, SourceTokenRejectsMalformedAndOverlongInput)
{
    const DB::CasRelinkSourceToken good{"pool", "srv1", "srv1/store/abc/abcdef@cas@", "all_1_1_0", "all_1_1_0", "1:1:1"};
    const String text = DB::encodeCasRelinkSourceToken(good).value();

    /// Every literal below is a SEVEN-segment token (version + six fields) unless it is testing the
    /// segment count itself, so each case fails for the reason it names and not because it is short.
    for (const std::string_view malformed : {
             /// Empty, no version, the wrong version, and versions that merely start or end right.
             "", "|a|b|c|d|e|f", "car0|a|b|c|d|e|f", "car|a|b|c|d|e|f", "car11|a|b|c|d|e|f",
             /// Too few and too many fields -- a shape that is one field off must not shift the rest.
             "car1|a|b|c|d|e", "car1|a|b|c|d|e|f|g", "car1", "car1|",
             /// An empty field: a token with a hole in it routes somewhere it was not meant to.
             "car1||b|c|d|e|f", "car1|a|b|c|d|e|",
             /// Malformed escapes: truncated, non-hex, and a lone escape character.
             "car1|%|b|c|d|e|f", "car1|%4|b|c|d|e|f", "car1|%zz|b|c|d|e|f", "car1|a%|b|c|d|e|f",
             /// Unescaped bytes outside the unreserved set: the decoder is the encoder's inverse, so
             /// anything the encoder would have escaped is not a token, however readable it looks.
             "car1|a/b|c|d|e|f|g", "car1|a b|c|d|e|f|g", "car1|a@b|c|d|e|f|g", "car1|1:1:1|b|c|d|e|f",
             /// A control character smuggled in as an escape -- the classic forged-log-line vector.
             "car1|a%00b|c|d|e|f|g", "car1|a%0Ab|c|d|e|f|g"})
    {
        EXPECT_FALSE(DB::decodeCasRelinkSourceToken(malformed).has_value())
            << "accepted a malformed source token: '" << malformed << "'";
    }

    /// Over-long: refused in BOTH directions, so an over-long field can neither be minted nor accepted.
    DB::CasRelinkSourceToken too_long = good;
    too_long.ref_name = String(257, 'a');
    EXPECT_FALSE(DB::encodeCasRelinkSourceToken(too_long).has_value());
    EXPECT_FALSE(DB::decodeCasRelinkSourceToken(
        "car1|pool|srv1|ns|" + String(257, 'a') + "|all_1_1_0|1%3A1%3A1").has_value());
    /// A field whose ENCODED form blows the whole-token cap (every byte escapes to three).
    DB::CasRelinkSourceToken all_escaped = good;
    all_escaped.root_namespace = String(200, ' ');
    all_escaped.ref_name = String(200, ' ');
    EXPECT_FALSE(DB::encodeCasRelinkSourceToken(all_escaped).has_value());
    EXPECT_FALSE(DB::decodeCasRelinkSourceToken(String(2000, 'a')).has_value());

    /// An empty field is refused on the way out too, not only on the way in.
    DB::CasRelinkSourceToken empty_field = good;
    empty_field.part_name.clear();
    EXPECT_FALSE(DB::encodeCasRelinkSourceToken(empty_field).has_value());

    /// The control-character refusal is symmetric: the sender cannot mint one either.
    DB::CasRelinkSourceToken control = good;
    control.server_root_id = "srv\n1";
    EXPECT_FALSE(DB::encodeCasRelinkSourceToken(control).has_value());

    /// Sanity: the good token itself decodes, so the rejections above are about the input and not about
    /// a codec that refuses everything.
    EXPECT_TRUE(DB::decodeCasRelinkSourceToken(text).has_value());
}


/// Routing (spec §wire-protocol). A pool UUID is shared by every server root writing into the pool, so
/// it cannot select the mount entitled to answer for a namespace; `ownsNamespace` is what does. It is a
/// pure string question about the mount's own identity: no pool, no I/O, no lifecycle -- asserted here
/// by answering the same before `startup`, while live, and after `shutdown`. A routing predicate that
/// could throw would turn a misrouted question into an error instead of an unproven answer.
TEST(CASConfirmExactRef, OwnsNamespaceSelectsTheMountByServerRootInEveryLifecycleState)
{
    auto storage = makeExchangeStorage("srv1");

    const auto assert_routing = [&](const char * phase)
    {
        /// Live and detached namespaces are `<server_root_id>/<mirrored table dir>` (`liveNamespace`).
        EXPECT_TRUE(storage->ownsNamespace("srv1", "srv1/store/abc/abcdef@cas@")) << phase;
        EXPECT_TRUE(storage->ownsNamespace("srv1", storage->liveNamespace("abcdef").string())) << phase;

        /// A different server root's namespace, and this namespace asked about under a different server
        /// root: the same pool, a different owner. Both must miss, or a confirm could be answered by a
        /// mount that never wrote the ref.
        EXPECT_FALSE(storage->ownsNamespace("srv2", "srv1/store/abc/abcdef@cas@")) << phase;
        EXPECT_FALSE(storage->ownsNamespace("srv1", "srv2/store/abc/abcdef@cas@")) << phase;

        /// The prefix trap: a bare `starts_with(server_root_id)` would let `srv1` claim `srv10`.
        EXPECT_FALSE(storage->ownsNamespace("srv1", "srv10/store/abc/abcdef@cas@")) << phase;
        EXPECT_FALSE(storage->ownsNamespace("srv10", "srv10/store/abc/abcdef@cas@")) << phase;

        /// The server root itself is not a namespace, and an unprefixed shadow path belongs to no mount.
        EXPECT_FALSE(storage->ownsNamespace("srv1", "srv1")) << phase;
        EXPECT_FALSE(storage->ownsNamespace("srv1", "shadow/backup/store/abc/abcdef")) << phase;

        /// A prefixed shadow namespace is ordinary owned content: the freeze belongs to the root
        /// that made it, so relink routing treats it exactly like a live namespace.
        EXPECT_TRUE(storage->ownsNamespace("srv1", "srv1/shadow/backup/store/abc/abcdef")) << phase;
        EXPECT_FALSE(storage->ownsNamespace("srv10", "srv1/shadow/backup/store/abc/abcdef")) << phase;

        /// Empty fields are never a match -- an absent token field must not route anywhere.
        EXPECT_FALSE(storage->ownsNamespace("", "")) << phase;
        EXPECT_FALSE(storage->ownsNamespace("", "srv1/store/abc/abcdef@cas@")) << phase;
        EXPECT_FALSE(storage->ownsNamespace("srv1", "")) << phase;
    };

    assert_routing("before startup");
    storage->startup();
    assert_routing("while live");
    storage->shutdown();
    assert_routing("after shutdown");
}


/// The storage forward of gate 1, driven end to end: a part committed through the ordinary transaction
/// path, and a token read out of the very manifest body the sender puts on the wire. The three
/// non-`Yes` cases pin the two halves this layer adds on top of the ledger -- the token text is decoded
/// here, and a namespace this mount holds no resident runtime for is an ambiguity, not a `No`.
TEST(CASConfirmExactRef, StorageConfirmAnswersForTheCommittedBinding)
{
    auto storage = makeExchangeStorage("test");
    storage->startup();
    commitExchangePart(*storage);

    const DB::CasRelinkSourceToken token = readSourceToken(*storage);
    ASSERT_TRUE(storage->ownsNamespace("test", token.root_namespace))
        << "the sender must route its own committed namespace to itself: " << token.root_namespace;

    EXPECT_EQ(storage->confirmExactRef(token.root_namespace, kExchangePartName, token.manifest_ref_text),
              DB::CasConfirmAnswer::Yes);

    /// A manifest this ref never named, and a ref name that was never committed: both are knowledge on
    /// a warm table, and both are `No` -- which the caller must still treat as "not proven".
    const auto other_ref = tryParseManifestRef(token.manifest_ref_text);
    ASSERT_TRUE(other_ref.has_value());
    EXPECT_EQ(storage->confirmExactRef(token.root_namespace, kExchangePartName,
                                       manifestRefDebugString(ManifestRef{other_ref->writer_epoch,
                                                                          other_ref->build_sequence,
                                                                          other_ref->manifest_ordinal + 1})),
              DB::CasConfirmAnswer::No);
    EXPECT_EQ(storage->confirmExactRef(token.root_namespace, "all_9_9_9", token.manifest_ref_text),
              DB::CasConfirmAnswer::No);

    /// A namespace with no resident runtime: the ledger will not recover one to answer, so this is an
    /// ambiguity. It must not read as "the ref does not exist".
    EXPECT_EQ(storage->confirmExactRef("test/store/zzz/zzzzzz@cas@", kExchangePartName, token.manifest_ref_text),
              DB::CasConfirmAnswer::Unknown);

    /// An unparsable token: the question cannot be understood, so it cannot be answered `No`.
    EXPECT_EQ(storage->confirmExactRef(token.root_namespace, kExchangePartName, "not-a-manifest-ref"),
              DB::CasConfirmAnswer::Unknown);
    EXPECT_EQ(storage->confirmExactRef(token.root_namespace, kExchangePartName, ""),
              DB::CasConfirmAnswer::Unknown);

    storage->shutdown();
}


/// The disk's own lifecycle is an answer, not an exception. The confirm is served on an interserver
/// request, and the caller has a durable precommit waiting on it: a thrown `INVALID_STATE` would have
/// to be classified by the HTTP layer, whereas `Unknown` is already the taxonomy's "not proven". Only
/// `Yes` authorizes, and no lifecycle state can produce one.
TEST(CASConfirmExactRef, StorageConfirmIsUnknownWhenTheDiskCannotSpeakForItsView)
{
    auto storage = makeExchangeStorage("test");

    /// Never started: no pool has ever been published, so there is no committed view at all.
    EXPECT_EQ(storage->confirmExactRef("test/store/abc/abcdef@cas@", kExchangePartName, "1:1:1"),
              DB::CasConfirmAnswer::Unknown);

    storage->startup();
    commitExchangePart(*storage);
    const DB::CasRelinkSourceToken token = readSourceToken(*storage);
    ASSERT_EQ(storage->confirmExactRef(token.root_namespace, kExchangePartName, token.manifest_ref_text),
              DB::CasConfirmAnswer::Yes);

    /// Shut down: the same question, the same token, and the same table -- but this process no longer
    /// speaks for the namespace.
    storage->shutdown();
    EXPECT_EQ(storage->confirmExactRef(token.root_namespace, kExchangePartName, token.manifest_ref_text),
              DB::CasConfirmAnswer::Unknown);
}
