#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

/// Task 10 (rev.7 spec §5): `SYSTEM CAS FORGET` — the operator force-Vanish. FORGET drives a
/// content-addressed pool to `Vanished(forgotten)` with the fence-first protocol: (1) publish terminal
/// intent, (2) trip the local fence, (3+4) stop the GC scheduler, (5) join keeper/remount, drain, retire
/// the keeper WITHOUT an unearned clean farewell, (6) publish `Vanished(forgotten)` with the [D5] message
/// carrying the decommission timestamp. These tests exercise the Pool-level protocol body (`Pool::forgetDisk`)
/// and the end-to-end verb through a real `ContentAddressedMetadataStorage` (the six-class gate wired to the
/// new state). Harness patterns follow gtest_cas_lifecycle_condition.cpp and gtest_cas_operation_gate.cpp.

namespace DB::ErrorCodes
{
extern const int INVALID_STATE;
}

using namespace DB;
using DB::Cas::PoolLifecycle;
using DB::Cas::tests::CountingBackend;

namespace
{

const String kSrid = "test";

/// A test-authored [D5] reason with a RECOGNIZABLE timestamp — the Pool-level tests assert this exact
/// string flows through `enterVanished` into the `throwIfLifecycleTerminal` message (the timestamp
/// threading the metadata storage does in production). It keeps the two [D5] substrings the gate relies on.
const String kForgetReason =
    "decommissioned by SYSTEM CAS FORGET at 2099-01-02 03:04:05 UTC — erasure was NOT "
    "verified; if this was a mistake the data may be intact (restart re-registers the name)";

/// Delete an existing key exactly (its current token comes from the same GET). Mirrors
/// gtest_cas_lifecycle_condition.cpp — used to drive a live pool into `IdentityLost`.
void deleteKeyExact(DB::Cas::Backend & backend, const String & key)
{
    const auto got = backend.get(key);
    ASSERT_TRUE(got.has_value()) << "expected '" << key << "' to exist before deletion";
    if (got)
        backend.deleteExact(key, got->token);
}

/// GC's fence-out applied directly to the mount lease (preserve the body, set `gc_fenced`, bump `seq`) —
/// a subsequent `tryRemountOnce` verdicts `Recover` and reclaims a FRESH incarnation immediately (no
/// lease-expiry wait), reaching `armMountFence`. Mirrors gtest_cas_lifecycle_condition.cpp's helper.
void fenceOutMount(DB::Cas::Backend & backend, const String & mount_key)
{
    const auto got = backend.get(mount_key);
    ASSERT_TRUE(got.has_value());
    DB::Cas::MountLease m = DB::Cas::decodeMountLease(got->bytes);
    m.gc_fenced = true;
    m.seq += 1;
    ASSERT_EQ(backend.putOverwrite(mount_key, DB::Cas::encodeMountLease(m), got->token).outcome,
              DB::Cas::PutOutcome::Done);
}

/// A Backend decorator whose head/get/list throw an untyped transport error while `fail` is armed — so a
/// self-remount attempt verdicts `StayTransient` (fast, no lease-expiry wait) and the remount loop keeps
/// spinning. Starts DISARMED so `Pool::open` succeeds. Mirrors gtest_cas_lifecycle_condition.cpp's decorator.
class ToggleableTransportFaultBackend final : public DB::Cas::InMemoryBackend
{
public:
    using Backend::get;
    using Backend::getStream;
    using Backend::putIfAbsent;
    using Backend::putIfAbsentStream;
    using Backend::putOverwrite;
    using Backend::casPut;

    DB::Cas::HeadResult head(const String & key) override
    {
        if (fail.load())
            throw std::runtime_error("injected fault: transport error");
        return InMemoryBackend::head(key);
    }
    std::optional<DB::Cas::GetResult> get(const String & key, DB::Cas::Range range) override
    {
        if (fail.load())
            throw std::runtime_error("injected fault: transport error");
        return InMemoryBackend::get(key, range);
    }
    DB::Cas::ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        if (fail.load())
            throw std::runtime_error("injected fault: transport error");
        return InMemoryBackend::list(prefix, cursor, limit);
    }

    std::atomic<bool> fail{false};
};

/// The message thrown by `fn`, or a failure if it did not throw a `DB::Exception`.
std::string messageOf(const std::function<void()> & fn)
{
    try
    {
        fn();
    }
    catch (const Exception & e)
    {
        return std::string(e.message());
    }
    ADD_FAILURE() << "expected a DB::Exception";
    return {};
}

/// A live table dir + committed part reused by the end-to-end gate test (the shape
/// gtest_cas_operation_gate.cpp uses).
const std::string kTableDir = "gg0/gg0gg0g0-0808-4808-8808-080808080808";
const std::string kPartDir = kTableDir + "/all_1_1_0";
const std::string kPartFile = kPartDir + "/data.bin";

std::shared_ptr<ContentAddressedMetadataStorage> openForgetStorage()
{
    auto settings = Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "ca_forget_scratch");
    auto storage = std::make_shared<ContentAddressedMetadataStorage>(
        Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);
    storage->startup();
    return storage;
}

void commitOnePart(ContentAddressedMetadataStorage & storage)
{
    auto tx = storage.createTransaction();
    auto & ca_tx = dynamic_cast<ContentAddressedTransaction &>(*tx);
    auto buf = ca_tx.writeFile(kTableDir + "/tmp_insert_all_1_1_0/data.bin", 65536, WriteMode::Rewrite, {});
    const std::string bytes = "content-of-the-part";
    buf->write(bytes.data(), bytes.size());
    buf->finalize();
    tx->moveDirectory(kTableDir + "/tmp_insert_all_1_1_0", kPartDir);
    tx->commit(NoCommitOptions{});
}

/// Deterministically interleave a real FORGET into the admission->lock window of a manual GC verb (the
/// I-1/I-2 admission TOCTOU), with BOUNDED condition-variable waits and never a sleep. The sequence pinned:
///   M (this thread, running `gc_verb`): passes the verb's pre-lock admission gate while `Live`, then the
///     installed seam signals `admitted` and blocks until `forget_done`, then M resumes to acquire
///     `gc_scheduler_mutex` and hit the under-lock re-check.
///   F (the FORGET thread): waits for `admitted`, runs the REAL `forgetDisk` (acquiring lifecycle +
///     gc_scheduler mutexes while M holds NEITHER -- M is parked in the seam BEFORE the lock), settling the
///     pool `Vanished(forgotten)`, then signals `forget_done`.
/// Returns the exception message `gc_verb` threw (via `messageOf`), so the caller asserts the typed [D5]
/// refusal. The 30s bounds trip ONLY on a genuine deadlock regression, never in the happy path.
std::string raceForgetIntoGcVerbWindow(ContentAddressedMetadataStorage & storage,
                                       const std::function<void()> & gc_verb)
{
    std::mutex m;
    std::condition_variable cv;
    bool admitted = false;
    bool forget_done = false;

    storage.setGcVerbAdmitWindowHookForTest([&]
    {
        {
            std::lock_guard lk(m);
            admitted = true;
        }
        cv.notify_all();
        std::unique_lock lk(m);
        EXPECT_TRUE(cv.wait_for(lk, std::chrono::seconds(30), [&] { return forget_done; }))
            << "the concurrent FORGET must complete within the bound (else the interleave deadlocked)";
    });

    std::thread forgetter([&]
    {
        {
            std::unique_lock lk(m);
            EXPECT_TRUE(cv.wait_for(lk, std::chrono::seconds(30), [&] { return admitted; }))
                << "the GC verb must reach the admission->lock window before FORGET runs";
        }
        storage.forgetDisk();
        {
            std::lock_guard lk(m);
            forget_done = true;
        }
        cv.notify_all();
    });

    const std::string msg = messageOf(gc_verb);
    forgetter.join();
    storage.setGcVerbAdmitWindowHookForTest({});   /// clear the seam (references this frame's locals)
    return msg;
}

}

/// (a) FORGET on a LIVE pool: the local fence is tripped, the injected GC-stop step runs, the pool settles
/// `Vanished(forgotten)`, and store-class access fails loud with the timestamped [D5] message.
TEST(CASForget, ForgetOnLivePoolTripsFenceAndVanishes)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::Live);
    ASSERT_TRUE(store->mayMutate());

    bool gc_stopped = false;
    store->forgetDisk([&] { gc_stopped = true; }, kForgetReason);

    /// Step 3/4 ran (the GC-stop callback was invoked from inside the protocol).
    EXPECT_TRUE(gc_stopped);
    /// Terminal truth, fence tripped.
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);
    EXPECT_TRUE(store->isVanished());
    EXPECT_FALSE(store->mayMutate());

    /// The [D5] message carries the operator's FORGET timestamp (threaded through the reason) and still
    /// names the sub-state ("erasure was NOT verified").
    const std::string msg = messageOf([&] { store->throwIfLifecycleTerminal(); });
    EXPECT_NE(msg.find("SYSTEM CAS FORGET at "), std::string::npos) << msg;
    EXPECT_NE(msg.find("2099-01-02 03:04:05 UTC"), std::string::npos) << msg;
    EXPECT_NE(msg.find("erasure was NOT verified"), std::string::npos) << msg;
}

/// (a') FORGET stops AND joins a real `CasGcScheduler`'s worker + heartbeat threads (the injected GC-stop
/// step). A long interval keeps any round from firing during the test window, so this isolates the
/// thread-lifecycle: `start()` spawns the two workers, FORGET's callback `stop()`s + joins them, and the
/// test completing (no hang) plus a clean `isQuiescent()` proves the join.
TEST(CASForget, ForgetStopsAndJoinsRealGcScheduler)
{
    auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);

    Cas::CasGcScheduler sched(store, std::chrono::seconds(3600), "CasForgetTest", "forget-disk");
    sched.start();

    bool gc_joined = false;
    store->forgetDisk([&] { sched.stop(); gc_joined = true; }, kForgetReason);

    EXPECT_TRUE(gc_joined);
    /// What this proves is the JOIN: `stop()` returned, so the worker + heartbeat threads are joined and
    /// the test could not have hung; `isQuiescent()` confirms no round is in flight. NOTE: the callback is
    /// only `sched.stop()`, which does NOT itself clear the in-process `i_am_leader` hint — the
    /// metadata-storage handler clears leadership by DESTROYING the scheduler (see
    /// `ContentAddressedMetadataStorage::forgetDisk`), so asserting `is_leader == false` here would be
    /// vacuous (this 3600s scheduler never led) or, after a real round, wrong.
    EXPECT_TRUE(sched.isQuiescent()) << "no GC round may be in flight after FORGET joined the scheduler";
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);
}

/// (c) Double FORGET is idempotent: the second call is a no-op (the pool is already `Vanished(forgotten)`),
/// so it never re-runs the protocol — the GC-stop callback is NOT invoked again, and the first reason wins.
TEST(CASForget, DoubleForgetIsIdempotent)
{
    auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);

    int gc_stops = 0;
    store->forgetDisk([&] { ++gc_stops; }, kForgetReason);
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);
    ASSERT_EQ(gc_stops, 1);

    /// A second FORGET with a DIFFERENT reason must change nothing (first terminal transition wins) and
    /// must NOT re-enter the teardown (idempotent short-circuit on `isVanished()`).
    store->forgetDisk([&] { ++gc_stops; }, "a different reason that must be ignored");
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);
    EXPECT_EQ(gc_stops, 1) << "the idempotent second FORGET must not re-run the protocol";

    const std::string msg = messageOf([&] { store->throwIfLifecycleTerminal(); });
    EXPECT_NE(msg.find("2099-01-02 03:04:05 UTC"), std::string::npos)
        << "the first FORGET's reason must win: " << msg;
}

/// (d) FORGET on an `IdentityLost` pool → `Vanished(forgotten)` — the escape hatch. `IdentityLost` is
/// non-absorbing and has no benign answer, so FORGET is the operator's way out.
TEST(CASForget, ForgetOnIdentityLostPoolVanishesForgotten)
{
    auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);

    /// Delete both pool sentinels while other objects remain, then drive the identity gate: the pool enters
    /// `IdentityLost` (never `Vanished`) — exactly gtest_cas_lifecycle_condition.cpp scenario (a).
    deleteKeyExact(*backend, store->layout().poolMetaKey());
    deleteKeyExact(*backend, store->layout().ownerKey(kSrid));
    EXPECT_FALSE(store->tryRemountOnce());
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::IdentityLost);
    ASSERT_FALSE(store->isVanished());

    bool gc_stopped = false;
    store->forgetDisk([&] { gc_stopped = true; }, kForgetReason);

    EXPECT_TRUE(gc_stopped);
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);
    EXPECT_TRUE(store->isVanished());
}

/// (a'') The clean-farewell is EARNED, never unconditional: on a drained pool FORGET stamps the mount lease
/// with the terminated sentinel (`min_active == UINT64_MAX`) so a same-server restart reclaims immediately,
/// but with an UNSETTLED (wedged) ref lane it must NOT — the lease is left to expire by observation.
TEST(CASForget, ForgetCleanFarewellGatedOnDrain)
{
    using DB::Cas::decodeMountLease;
    constexpr uint64_t kTerminated = std::numeric_limits<uint64_t>::max();

    /// Drained pool → clean farewell written (lease stamped terminated).
    {
        auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
        auto store = DB::Cas::tests::openPoolForTest(backend);
        const String mount_key = store->layout().mountKey(kSrid);
        ASSERT_NE(decodeMountLease(backend->get(mount_key)->bytes).min_active, kTerminated);   /// baseline

        store->forgetDisk([] {}, kForgetReason);
        ASSERT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);

        const auto got = backend->get(mount_key);
        ASSERT_TRUE(got.has_value());
        EXPECT_EQ(decodeMountLease(got->bytes).min_active, kTerminated)
            << "a drained FORGET earns the clean-release farewell";
    }

    /// Unsettled (wedged) ref lane → NO clean farewell (the drain cannot certify a clean death).
    {
        auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
        auto store = DB::Cas::tests::openPoolForTest(backend);
        const String mount_key = store->layout().mountKey(kSrid);

        const DB::Cas::RootNamespace ns{"test/forget_wedge"};
        store->forceWedgeForTest(ns, /*writer_epoch*/ 1, /*ref_sequence*/ 1, "bogus/_log/key", "bogus-bytes");
        ASSERT_TRUE(store->refLaneWedgedForTest(ns));

        store->forgetDisk([] {}, kForgetReason);
        ASSERT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);

        const auto got = backend->get(mount_key);
        ASSERT_TRUE(got.has_value()) << "the lease object must still be present (expiry by observation)";
        EXPECT_NE(decodeMountLease(got->bytes).min_active, kTerminated)
            << "an unearned clean farewell must NOT be written when the ref lanes did not drain";
    }
}

/// (b1) BOUNDED COMPLETION: FORGET racing an ACTIVE self-remount thread joins it without deadlock. Here the
/// faulting backend keeps every attempt at `StayTransient` (it never reaches `armMountFence`), so this
/// isolates the join/no-deadlock property; the fence re-arm path is covered by (b2) below. Uses a
/// `std::future` timeout wait (never a sleep) — the timeout only fires on a genuine deadlock regression.
TEST(CASForget, ForgetRacingActiveRemountThreadCompletesBounded)
{
    auto backend = std::make_shared<ToggleableTransportFaultBackend>();
    /// `background_watermark = true` so `scheduleRemount` actually spawns a recovery thread (mirrors
    /// gtest_cas_pool.cpp's ShutdownGuardRefusesToArmRemount setup).
    auto store = DB::Cas::Pool::open(backend,
        DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test", .background_watermark = true});

    /// Arm the fault so every remount attempt verdicts `StayTransient` fast (no lease-expiry wait), then
    /// trip the fence and spawn the recovery thread — it now loops `tryRemountOnce` against the fault.
    backend->fail.store(true);
    store->tripMountLost();
    ASSERT_TRUE(store->scheduleRemountForTest()) << "the recovery thread must be armed and running";

    /// FORGET from ANOTHER thread must join the active remount thread and finish in bounded time.
    std::promise<void> done;
    auto fut = done.get_future();
    std::thread forgetter([&]
    {
        store->forgetDisk([] {}, kForgetReason);
        done.set_value();
    });
    EXPECT_EQ(fut.wait_for(std::chrono::seconds(30)), std::future_status::ready)
        << "FORGET must not deadlock against an in-flight self-remount";
    forgetter.join();

    /// Disarm before ~Pool so its residual teardown is not fighting the injected fault.
    backend->fail.store(false);

    EXPECT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);
    EXPECT_FALSE(store->mayMutate()) << "the fence must stay latched even if a raced reclaim re-armed it";
}

/// (b2) FENCE RE-LATCH REGRESSION GUARD (the fix's raison d'être): a self-remount that reaches
/// `armMountFence` re-arms the local fence (`lost=false`) after FORGET has already tripped it. FORGET's
/// SECOND `tripMountLost` — placed AFTER the remount thread is joined — must override it.
///
/// (b1)'s fault keeps every attempt at `StayTransient`, so it can NOT catch removal of that second trip. To
/// make EXACTLY ONE reclaim reach `armMountFence` inside FORGET's window, deterministically and without a
/// sleep, we drive a REAL `tryRemountOnce` from FORGET's own GC-stop step (invoked at spec §5 step 3/4,
/// strictly AFTER the fence trip): the mount is fenced-out so the reclaim succeeds fast and re-arms the
/// fence, and `tryRemountOnce`'s step-0 gate checks `isVanished()` — still false in this window — so it does
/// NOT bail. The re-arm therefore lands after trip#1 and before trip#2, exactly the interval trip#2 guards.
/// Verified to go RED when trip#2 is removed (see task-10-report.md — test_task10b_reddemo.log).
TEST(CASForget, ForgetReLatchesFenceAfterAReclaimReachesArmMountFence)
{
    auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);

    /// Make the current mount claimable so a self-remount SUCCEEDS fast and reaches `armMountFence`.
    fenceOutMount(*backend, store->layout().mountKey(kSrid));

    bool reclaimed = false;
    store->forgetDisk([&] { reclaimed = store->tryRemountOnce(); }, kForgetReason);

    /// Guard against a vacuous pass: if the injected reclaim did not actually succeed (reach
    /// `armMountFence`), there is no re-arm for trip#2 to override and the test proves nothing.
    ASSERT_TRUE(reclaimed) << "the injected reclaim must reach armMountFence, else this guard is vacuous";
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten);
    EXPECT_FALSE(store->mayMutate())
        << "FORGET's post-join fence re-latch (trip#2) must override the fence the reclaim re-armed";
}

/// (b3) PROMOTION-GUARD REGRESSION (spec §9 rev.8 item 7): with the erasure-proof excised, the natural
/// `Vanished(replaced)` verdict is the ONLY remaining mid-FORGET natural-terminal race. A `tryRemountOnce`
/// in flight during FORGET — one that passed step 0's `isVanished()` gate before FORGET published its intent
/// — must NOT settle `Vanished(replaced)` and mislabel the operator-visible reason; FORGET's
/// `Vanished(forgotten)` must win. We drive a REAL `tryRemountOnce` from FORGET's own GC-stop step (spec §5
/// step 3/4, strictly AFTER the step-1 intent publish, BEFORE the step-6 settle), against a FOREIGN
/// `_pool_meta` (the `Replaced` verdict), and assert the guard bailed.
TEST(CASForget, ForgetIntentBlocksNaturalReplacedPromotion)
{
    auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);

    /// Make the identity gate verdict `Replaced`: overwrite `_pool_meta` with a FOREIGN pool_id (present,
    /// mismatched identity) — exactly gtest_cas_lifecycle_condition.cpp scenario (b).
    const String meta_key = store->layout().poolMetaKey();
    const auto got = backend->get(meta_key);
    ASSERT_TRUE(got.has_value());
    DB::Cas::PoolMeta foreign = DB::Cas::decodePoolMeta(got->bytes);
    foreign.pool_id = foreign.pool_id + DB::UInt128(1);
    ASSERT_EQ(backend->putOverwrite(meta_key, DB::Cas::encodePoolMeta(foreign), got->token).outcome,
              DB::Cas::PutOutcome::Done);

    /// The in-flight gate (run from the GC-stop callback) reaches the `Replaced` verdict but must BAIL on the
    /// already-published intent rather than settle `Vanished(replaced)`.
    bool replaced_settled_midforget = false;
    store->forgetDisk(
        [&]
        {
            store->tryRemountOnce();
            replaced_settled_midforget = (store->lifecycle() == PoolLifecycle::VanishedReplaced);
        },
        kForgetReason);

    EXPECT_FALSE(replaced_settled_midforget)
        << "a mid-FORGET Replaced verdict must NOT settle — the intent guard bails before enterVanished(Replaced)";
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::VanishedForgotten)
        << "FORGET's Vanished(forgotten) must win (first terminal STATE transition)";
    const std::string msg = messageOf([&] { store->throwIfLifecycleTerminal(); });
    EXPECT_NE(msg.find("erasure was NOT verified"), std::string::npos) << msg;
    EXPECT_EQ(msg.find("foreign pool"), std::string::npos)
        << "the reason must NOT be the mislabeled Replaced text: " << msg;
}

/// (e) End-to-end through the verb entry `ContentAddressedMetadataStorage::forgetDisk` and the six-class
/// gate: after FORGET, a Probe answers truth-absent, a Remove no-ops, and a content read throws the [D5]
/// message with the REAL decommission timestamp produced by the handler.
TEST(CASForget, ForgetEndToEndGatesTruthWithTimestampedMessage)
{
    auto storage = openForgetStorage();
    commitOnePart(*storage);
    ASSERT_TRUE(storage->existsFile(kPartFile));   /// Live baseline

    storage->forgetDisk();

    /// Probe → truth-absent (no throw): the committed part reads absent on a forgotten disk.
    EXPECT_FALSE(storage->existsFile(kPartFile));
    EXPECT_FALSE(storage->existsDirectory(kPartDir));

    /// Remove → no-op success (this is what lets a forgotten-disk table's DROP complete).
    EXPECT_NO_THROW({
        auto tx = storage->createTransaction();
        tx->removeRecursive(kTableDir, /*should_remove_objects=*/nullptr);
        tx->commit(NoCommitOptions{});
    });

    /// Content read → the typed [D5] message, with the handler's real UTC timestamp.
    const std::string msg = messageOf([&] { storage->getFileSize(kPartFile); });
    EXPECT_NE(msg.find("SYSTEM CAS FORGET at "), std::string::npos) << msg;
    EXPECT_NE(msg.find(" UTC"), std::string::npos) << msg;
    EXPECT_NE(msg.find("erasure was NOT verified"), std::string::npos) << msg;
}

/// (I-1 regression) A manual `SYSTEM CAS GC RUN` admitted while `Live` but that acquires
/// `gc_scheduler_mutex` strictly AFTER a concurrent FORGET completes must NOT resurrect a `CasGcScheduler`
/// on the now-`Vanished` pool: the under-lock admission re-check refuses with the typed [D5] message. The
/// interleave is deterministic (bounded cv waits, no sleep) — the GC-verb seam parks the RUN in the
/// admission→lock window while the FORGET thread drives the real teardown. The lasting-damage observable is
/// `gcHealth()` staying empty: a resurrected scheduler (the pre-fix behavior) would make it non-empty.
/// Verified RED against the pre-fix ordering (see task-17-report.md).
TEST(CASForget, GcRunAdmittedWhileLiveRefusesAfterConcurrentForget)
{
    auto storage = openForgetStorage();
    /// Capture the pool while Live (store() is fail-closed once Vanished) to assert its terminal state after.
    auto pool = storage->store();
    ASSERT_EQ(pool->lifecycle(), PoolLifecycle::Live);
    ASSERT_FALSE(storage->gcHealth().has_value())
        << "no scheduler exists before the first GC round (unit-test null context creates none at startup)";

    const std::string msg = raceForgetIntoGcVerbWindow(
        *storage, [&] { storage->runGarbageCollectionRoundNow(); });

    /// The refusal is the typed FORGET [D5] message (an under-lock admission throw), not a round-internal
    /// error and not a silently-run round.
    EXPECT_NE(msg.find("erasure was NOT verified"), std::string::npos) << msg;
    /// The I-1 lasting-damage observable: NO scheduler was created on the decommissioned pool.
    EXPECT_FALSE(storage->gcHealth().has_value())
        << "a GC RUN refused post-FORGET must NOT resurrect the scheduler on a Vanished pool";
    EXPECT_EQ(pool->lifecycle(), PoolLifecycle::VanishedForgotten);
}

/// (I-2 regression) A `SYSTEM CAS GC REBUILD` holds `gc_scheduler_mutex` for its whole
/// duration, so a concurrent FORGET must SERIALIZE behind it — FORGET cannot report the disk decommissioned
/// while the rebuild is still issuing durable `gc/`-plane writes. Deterministic (bounded cv waits + a bounded
/// negative future poll anchored by a positive control, never a sleep-to-fix-a-race): the in-lock seam parks
/// the rebuild WHILE it holds the mutex; a FORGET launched in that window must NOT complete until the rebuild
/// releases the lock. The pre-fix `runGcRebuildNow` took NO lock, so an in-flight rebuild was invisible to
/// FORGET and FORGET would complete immediately. Verified RED against the pre-fix code (see task-17-report.md).
TEST(CASForget, GcRebuildInFlightSerializesForget)
{
    auto storage = openForgetStorage();
    auto pool = storage->store();   /// captured while Live
    ASSERT_EQ(pool->lifecycle(), PoolLifecycle::Live);

    std::mutex m;
    std::condition_variable cv;
    bool rebuild_holds_lock = false;
    bool may_release = false;

    /// In-lock seam: fires WHILE the rebuild holds `gc_scheduler_mutex`. It parks there (bounded) until the
    /// coordinator has verified FORGET is blocked, then lets the rebuild finish and release the lock.
    storage->setGcVerbAdmitWindowHookForTest([&]
    {
        {
            std::lock_guard lk(m);
            rebuild_holds_lock = true;
        }
        cv.notify_all();
        std::unique_lock lk(m);
        EXPECT_TRUE(cv.wait_for(lk, std::chrono::seconds(30), [&] { return may_release; }))
            << "the coordinator must release the in-flight rebuild within the bound";
    });

    /// The rebuild runs on its own thread; it holds the lock through the seam above.
    std::promise<void> rebuild_done_p;
    auto rebuild_done = rebuild_done_p.get_future();
    std::thread rebuilder([&]
    {
        /// On release the pool may already be Vanished (RED path: FORGET ran unserialized) — `store()` then
        /// throws; swallow it, the assertions below carry the verdict.
        try { storage->runGcRebuildNow(/*force=*/false); } catch (...) {} // NOLINT(bugprone-empty-catch)
        rebuild_done_p.set_value();
    });

    /// Wait until the rebuild is genuinely in flight (holding the lock).
    {
        std::unique_lock lk(m);
        ASSERT_TRUE(cv.wait_for(lk, std::chrono::seconds(30), [&] { return rebuild_holds_lock; }))
            << "the rebuild must reach its in-lock seam";
    }

    /// Launch FORGET while the rebuild holds the lock. With the fix it BLOCKS on `gc_scheduler_mutex`;
    /// without the fix (pre-fix rebuild took no lock) it runs straight through.
    std::promise<void> forget_done_p;
    auto forget_done = forget_done_p.get_future();
    std::thread forgetter([&] { storage->forgetDisk(); forget_done_p.set_value(); });

    /// The discriminator: FORGET must NOT complete while the rebuild holds the lock. This bounded negative
    /// observation is anchored by the positive control below (FORGET DOES complete once the lock releases),
    /// so the window's meaning is real, not a race hidden behind a sleep.
    EXPECT_EQ(forget_done.wait_for(std::chrono::seconds(2)), std::future_status::timeout)
        << "FORGET must serialize behind an in-flight GC rebuild (the rebuild holds gc_scheduler_mutex)";

    /// Release the in-flight rebuild; it finishes and drops the lock, and FORGET can now proceed.
    {
        std::lock_guard lk(m);
        may_release = true;
    }
    cv.notify_all();

    ASSERT_EQ(rebuild_done.wait_for(std::chrono::seconds(30)), std::future_status::ready)
        << "the rebuild must complete after release";
    ASSERT_EQ(forget_done.wait_for(std::chrono::seconds(30)), std::future_status::ready)
        << "once the rebuild releases the lock, the serialized FORGET completes (positive control)";

    rebuilder.join();
    forgetter.join();
    storage->setGcVerbAdmitWindowHookForTest({});

    EXPECT_EQ(pool->lifecycle(), PoolLifecycle::VanishedForgotten)
        << "FORGET settles the pool Vanished(forgotten) once it is no longer serialized behind the rebuild";
}
