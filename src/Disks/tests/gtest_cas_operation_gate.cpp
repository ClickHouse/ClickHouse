#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Common/Exception.h>

#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>

/// Task 8 (rev.7 spec §1): the central six-class operation gate (`checkOpAdmitted`), the `Vanished` truth
/// semantics, and the [D5] per-reason typed messages. These tests build a real
/// `ContentAddressedMetadataStorage` over a Local object storage (the same harness as
/// gtest_ca_transaction.cpp), commit a real part, then force the pool lifecycle condition directly via the
/// Task-5 setter (`Pool::setLifecycleForTest`) to pin each class × state cell of the spec §1 table and
/// assert what every public entry does.
///
/// NOTE the harness idiom: `store()` itself is fail-closed on a terminal pool (it throws), so a test
/// captures the `PoolPtr` ONCE while the pool is still `Live` and drives `setLifecycleForTest` on that
/// captured handle -- the SAME object the metadata storage's `cas_store` points at -- rather than calling
/// `store()` again after forcing a terminal state.

namespace DB::ErrorCodes
{
extern const int INVALID_STATE;
extern const int NETWORK_ERROR;
extern const int FILE_DOESNT_EXIST;
}

using namespace DB;
using DB::Cas::PoolLifecycle;

namespace
{

/// A live table dir + part reused across the tests (the exact shape gtest_ca_transaction.cpp uses).
const std::string kTableDir = "g80/g80g80g8-0808-4808-8808-080808080808";
const std::string kPartDir = kTableDir + "/all_1_1_0";
const std::string kPartFile = kPartDir + "/data.bin";

std::shared_ptr<ContentAddressedMetadataStorage> openGateStorage()
{
    auto settings = Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "ca_op_gate_scratch");
    auto storage = std::make_shared<ContentAddressedMetadataStorage>(
        Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);
    storage->startup();
    return storage;
}

/// Commit one real part (tmp -> final rename -> commit), leaving `kPartFile` durable and `kPartDir`/
/// `kTableDir` non-empty. Every op below runs against this committed state.
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

/// The thrown exception itself, for the tests that assert against an upstream CLASSIFIER rather than
/// against an error code. NEVER returns a null `exception_ptr`: every consumer feeds the result to a
/// classifier that rethrows it, and `std::rethrow_exception(nullptr)` is undefined behaviour that takes
/// the whole binary down instead of failing one test. On the nothing-was-thrown path the failure is
/// recorded and a SENTINEL is returned -- the test has already failed by then, and the sentinel merely
/// keeps the assertion that follows harmless.
std::exception_ptr exceptionOf(const std::function<void()> & fn)
{
    try
    {
        fn();
    }
    catch (...)
    {
        return std::current_exception();
    }
    ADD_FAILURE() << "expected a DB::Exception, nothing was thrown";
    return std::make_exception_ptr(std::runtime_error("exceptionOf sentinel: nothing was thrown"));
}

/// The Pool-level `server_root_id` a test mount uses (mirrors gtest_cas_lifecycle_condition.cpp).
const std::string kSrid = "test";

/// GC's fence-out applied to the mount lease (preserve the body, set `gc_fenced`, bump `seq`) so a
/// subsequent `tryRemountOnce` verdicts `Recover` and reclaims a FRESH incarnation immediately, driving a
/// transient-not-live pool back to `Live` without a lease-expiry wait. Mirrors
/// gtest_cas_lifecycle_condition.cpp's helper.
void fenceOutMount(DB::Cas::Backend & backend, const String & mount_key)
{
    DB::Cas::tests::OperationForTest op(backend);
    const auto got = (*op).read(mount_key, DB::Cas::Retry::standard());
    ASSERT_TRUE(got.has_value());
    DB::Cas::MountLease m = DB::Cas::decodeMountLease(got->bytes);
    m.gc_fenced = true;
    m.seq += 1;
    ASSERT_TRUE(std::holds_alternative<DB::Cas::Committed>(
        (*op).replace(mount_key, DB::Cas::encodeMountLease(m), got->etag, DB::Cas::Retry::standard())));
}

}

/// (a) Probes on a Vanished disk answer the truth: absent/empty, WITHOUT reaching the pool.
TEST(CASOperationGate, ProbesOnVanishedAnswerAbsentEmpty)
{
    auto storage = openGateStorage();
    commitOnePart(*storage);
    auto pool = storage->store();   /// captured while Live

    /// Live baseline: the probes see the committed part.
    ASSERT_TRUE(storage->existsFile(kPartFile));
    ASSERT_TRUE(storage->existsDirectory(kPartDir));
    ASSERT_TRUE(storage->existsFileOrDirectory(kPartFile));
    ASSERT_FALSE(storage->isDirectoryEmpty(kTableDir));
    ASSERT_FALSE(storage->listDirectory(kTableDir).empty());
    ASSERT_TRUE(storage->getStorageObjectsIfExist(kPartFile).has_value());

    pool->setLifecycleForTest(PoolLifecycle::VanishedReplaced);

    EXPECT_FALSE(storage->existsFile(kPartFile));
    EXPECT_FALSE(storage->existsDirectory(kPartDir));
    EXPECT_FALSE(storage->existsFileOrDirectory(kPartFile));
    EXPECT_TRUE(storage->listDirectory(kTableDir).empty());
    EXPECT_FALSE(storage->iterateDirectory(kTableDir)->isValid());
    EXPECT_TRUE(storage->isDirectoryEmpty(kTableDir));
    EXPECT_FALSE(storage->getStorageObjectsIfExist(kPartFile).has_value());
    /// The offender `liveTreeDirHasChildren` hardcoded-true is now truthful too: the disk root reads absent.
    EXPECT_FALSE(storage->liveTreeDirHasChildren(""));
}

/// (b) Removes on a Vanished disk are no-op SUCCESS and never touch the backend: after restoring Live the
/// part is still there. This is what lets a vanished-disk table's DROP complete.
TEST(CASOperationGate, RemovesOnVanishedAreNoOpSuccessBackendUntouched)
{
    auto storage = openGateStorage();
    commitOnePart(*storage);
    auto pool = storage->store();   /// captured while Live
    ASSERT_TRUE(storage->existsDirectory(kPartDir));

    pool->setLifecycleForTest(PoolLifecycle::VanishedReplaced);

    /// A whole-table removeRecursive + commit (the DROP shape): both no-op-succeed.
    {
        auto tx = storage->createTransaction();
        EXPECT_NO_THROW(tx->removeRecursive(kTableDir, /*should_remove_objects=*/nullptr));
        EXPECT_NO_THROW(tx->commit(NoCommitOptions{}));   /// empty parts -> Remove -> no-op success
    }
    /// A single removeDirectory of the part dir + commit: no-op-succeed.
    {
        auto tx = storage->createTransaction();
        EXPECT_NO_THROW(tx->removeDirectory(kPartDir));
        EXPECT_NO_THROW(tx->commit(NoCommitOptions{}));
    }

    /// Truth check: nothing was actually removed. Back on Live the part is intact.
    pool->setLifecycleForTest(PoolLifecycle::Live);
    EXPECT_TRUE(storage->existsDirectory(kPartDir)) << "a remove on a Vanished disk must not touch the backend";
    EXPECT_TRUE(storage->existsFile(kPartFile));
}

/// (c) A content read on a Vanished disk throws the typed per-reason [D5] message -- the exact substring
/// names the ACTUAL sub-state (replaced / forgotten), never a wrong diagnosis.
TEST(CASOperationGate, ContentReadOnVanishedThrowsTypedPerReasonMessage)
{
    auto storage = openGateStorage();
    commitOnePart(*storage);
    auto pool = storage->store();   /// captured while Live

    pool->setLifecycleForTest(PoolLifecycle::VanishedReplaced);
    EXPECT_NE(messageOf([&] { storage->getFileSize(kPartFile); }).find("foreign pool"), std::string::npos);
    EXPECT_NE(messageOf([&] { storage->getStorageObjects(kPartFile); }).find("foreign pool"), std::string::npos);

    pool->setLifecycleForTest(PoolLifecycle::VanishedForgotten);
    EXPECT_NE(messageOf([&] { storage->getFileSize(kPartFile); }).find("erasure was NOT verified"),
              std::string::npos);
}

/// (d1) Every class but Factory refuses on `TransientNotLive` — and the refusal carries the TRANSIENT
/// class (`NETWORK_ERROR`), not the terminal 668. The split from `IdentityLost` (test d2) is the whole
/// point: a lease blip is unavailability, an identity loss is damage, and consumers outside CAS act on
/// the difference. `ReplicatedMergeTreePartCheckThread` declares a part broken and detaches it for any
/// refusal its `isRetryableException` hatch does not recognise, so coding a blip 668 made healthy parts
/// look corrupt (BACKLOG {#lease-blip-part-check-collapse}).
TEST(CASOperationGate, EveryClassThrowsRetryableTransientOnTransientNotLive)
{
    auto storage = openGateStorage();
    commitOnePart(*storage);
    storage->store()->setLifecycleForTest(PoolLifecycle::TransientNotLive);   /// one force from Live

    /// Probe
    Cas::tests::expectThrowsCode(ErrorCodes::NETWORK_ERROR, [&] { storage->existsFile(kPartFile); });
    Cas::tests::expectThrowsCode(ErrorCodes::NETWORK_ERROR, [&] { storage->existsDirectory(kPartDir); });
    Cas::tests::expectThrowsCode(ErrorCodes::NETWORK_ERROR, [&] { storage->listDirectory(kTableDir); });
    Cas::tests::expectThrowsCode(ErrorCodes::NETWORK_ERROR, [&] { storage->isDirectoryEmpty(kTableDir); });
    /// ContentRead
    Cas::tests::expectThrowsCode(ErrorCodes::NETWORK_ERROR, [&] { storage->getFileSize(kPartFile); });
    Cas::tests::expectThrowsCode(ErrorCodes::NETWORK_ERROR, [&] { storage->getStorageObjects(kPartFile); });
    /// Write (via a transaction)
    Cas::tests::expectThrowsCode(ErrorCodes::NETWORK_ERROR, [&] {
        auto tx = storage->createTransaction();
        auto & ca_tx = dynamic_cast<ContentAddressedTransaction &>(*tx);
        ca_tx.writeFile(kTableDir + "/tmp_x/data.bin", 65536, WriteMode::Rewrite, {});
    });
    /// Remove (via a transaction)
    Cas::tests::expectThrowsCode(ErrorCodes::NETWORK_ERROR, [&] {
        auto tx = storage->createTransaction();
        tx->removeRecursive(kTableDir, /*should_remove_objects=*/nullptr);
    });
    /// Admin
    Cas::tests::expectThrowsCode(ErrorCodes::NETWORK_ERROR, [&] { storage->runOneGcRoundForTest(); });

    /// The coarser code buys retryability at the cost of precision, so the MESSAGE carries the whole
    /// truth: which CA condition, and that it is transient rather than an error of record.
    const std::string msg = messageOf([&] { storage->getFileSize(kPartFile); });
    EXPECT_NE(msg.find("mount lease not held"), std::string::npos) << msg;
    EXPECT_NE(msg.find("TRANSIENT"), std::string::npos) << msg;
    EXPECT_NE(msg.find("recovers to Live"), std::string::npos) << msg;
}

/// (d2) `IdentityLost` is TERMINAL — the sentinels are gone, nothing auto-recovers — so it keeps the 668
/// (`INVALID_STATE`) class and its own richer [D5] diagnosis ("identity lost … restart or FORGET").
/// Nothing about the transient re-coding may leak here: a terminal state that read as retryable would
/// make every consumer spin forever on a disk that will never come back.
TEST(CASOperationGate, EveryClassThrows668OnIdentityLost)
{
    auto storage = openGateStorage();
    commitOnePart(*storage);
    storage->store()->setLifecycleForTest(PoolLifecycle::IdentityLost);   /// one force from Live

    /// Probe
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] { storage->existsFile(kPartFile); });
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] { storage->existsDirectory(kPartDir); });
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] { storage->listDirectory(kTableDir); });
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] { storage->isDirectoryEmpty(kTableDir); });
    /// ContentRead
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] { storage->getFileSize(kPartFile); });
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] { storage->getStorageObjects(kPartFile); });
    /// Write (via a transaction)
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] {
        auto tx = storage->createTransaction();
        auto & ca_tx = dynamic_cast<ContentAddressedTransaction &>(*tx);
        ca_tx.writeFile(kTableDir + "/tmp_x/data.bin", 65536, WriteMode::Rewrite, {});
    });
    /// Remove (via a transaction)
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] {
        auto tx = storage->createTransaction();
        tx->removeRecursive(kTableDir, /*should_remove_objects=*/nullptr);
    });
    /// Admin
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] { storage->runOneGcRoundForTest(); });

    EXPECT_NE(messageOf([&] { storage->getFileSize(kPartFile); }).find("identity lost"), std::string::npos);
}

/// (d3) The contract the d1/d2 split exists to satisfy, asserted against upstream's OWN predicate instead
/// of a code number: `isRetryableException` is what `ReplicatedMergeTreePartCheckThread::checkPartImpl`
/// consults before declaring a part broken. A transient CA refusal must satisfy it (the part stays
/// queued); a terminal one must not (the disk is genuinely unusable and must surface). Pinning the
/// predicate rather than `NETWORK_ERROR` keeps this test meaningful if upstream's list ever moves.
TEST(CASOperationGate, TransientRefusalIsUpstreamRetryableTerminalIsNot)
{
    {
        auto storage = openGateStorage();
        commitOnePart(*storage);
        storage->store()->setLifecycleForTest(PoolLifecycle::TransientNotLive);
        EXPECT_TRUE(isRetryableException(exceptionOf([&] { storage->getFileSize(kPartFile); })))
            << "a lease blip must not read as part damage to the part-check thread";
    }
    {
        auto storage = openGateStorage();
        commitOnePart(*storage);
        storage->store()->setLifecycleForTest(PoolLifecycle::IdentityLost);
        /// Pin WHICH error is being classified before classifying it: `EXPECT_FALSE` alone passes for any
        /// non-retryable error, so a future regression that threw something else entirely here -- or threw
        /// from the wrong site -- would slip through as a pass.
        Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] { storage->getFileSize(kPartFile); });
        EXPECT_NE(messageOf([&] { storage->getFileSize(kPartFile); }).find("identity lost"), std::string::npos);
        EXPECT_FALSE(isRetryableException(exceptionOf([&] { storage->getFileSize(kPartFile); })))
            << "a terminal identity loss must NOT be retried forever as if it were transient";
    }
}

/// (e) `createTransaction` (Factory: I/O-free) and the capability/introspection getters construct fine on
/// a Vanished disk -- so a vanished-disk table's DROP can allocate its removal transaction.
TEST(CASOperationGate, FactoryClassWorksOnVanished)
{
    auto storage = openGateStorage();
    storage->store()->setLifecycleForTest(PoolLifecycle::VanishedForgotten);   /// one force from Live

    EXPECT_NO_THROW({ auto tx = storage->createTransaction(); (void)tx; });
    EXPECT_EQ(storage->getType(), MetadataStorageType::CAS);
    EXPECT_NO_THROW((void)storage->getPath());
    EXPECT_NO_THROW((void)storage->isContentAddressed());
}

/// (f) `tryGetInManifestBytes` PROPAGATES the typed refusal — terminal 668 on a `Vanished` disk, the
/// transient class in a lease gap — rather than converting either into a silent-absent `std::nullopt`
/// (the narrowed catch). RED before the narrowing.
TEST(CASOperationGate, TryGetInManifestBytesPropagatesTypedError)
{
    auto storage = openGateStorage();
    commitOnePart(*storage);
    auto pool = storage->store();   /// captured while Live

    pool->setLifecycleForTest(PoolLifecycle::VanishedReplaced);
    /// Never FILE_DOESNT_EXIST, never a swallowed nullopt -- the typed INVALID_STATE escapes.
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] {
        storage->tryGetInManifestBytes(kTableDir + "/format_version.txt");
    });

    pool->setLifecycleForTest(PoolLifecycle::TransientNotLive);
    Cas::tests::expectThrowsCode(ErrorCodes::NETWORK_ERROR, [&] {
        storage->tryGetInManifestBytes(kTableDir + "/format_version.txt");
    });
}

/// (g) (rev.8, Task 15) Null-pool fail-loud: the Dormant/UNMOUNT rollback replaced the transitional
/// not-Mounted branch (which answered `Probe`->benign-absent) with a null-pool fail-loud. A storage whose
/// pool is torn down (`shutdown()`) refuses EVERY class, `Probe` included, with `INVALID_STATE`
/// ("not started") -- there is no benign-absent answer for a not-started disk; only a genuinely `Vanished`
/// POOL answers truth-absent. Replaces the deleted `DormantDiskKeepsOldBenignAbsent_RemoveAtTask15`.
TEST(CASOperationGate, NullPoolFailsLoudForEveryClass)
{
    auto storage = openGateStorage();
    commitOnePart(*storage);
    ASSERT_TRUE(storage->existsDirectory(kPartDir));

    storage->shutdown();   /// null pool -- the ShutDown storage lifecycle

    /// Probes now THROW (not started), NOT the transitional benign-absent answer.
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] { storage->existsFile(kPartFile); });
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] { storage->existsDirectory(kPartDir); });
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] { (void)storage->listDirectory(kTableDir); });
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] { storage->isDirectoryEmpty(kTableDir); });
    /// Store-class ops throw the same INVALID_STATE ("not started"), not the typed Vanished message.
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] { storage->getFileSize(kPartFile); });
}

/// (h) The raw GC round entry points refuse on a not-live pool (Admin class): typed [D5] reason once Vanished.
TEST(CASOperationGate, GcEntryPointsRefuseOnNotLive)
{
    auto storage = openGateStorage();
    auto pool = storage->store();   /// captured while Live

    pool->setLifecycleForTest(PoolLifecycle::VanishedReplaced);
    EXPECT_NE(messageOf([&] { storage->runOneGcRoundForTest(); }).find("foreign pool"), std::string::npos);

    pool->setLifecycleForTest(PoolLifecycle::TransientNotLive);
    Cas::tests::expectThrowsCode(ErrorCodes::NETWORK_ERROR, [&] { storage->runOneGcRoundForTest(); });
}

/// (i) `CasGcScheduler::isQuiescent` reflects the round-in-flight flag: a round in flight => not quiescent.
/// (This is the join-completion signal the FORGET / GC-STOP tests rely on.)
TEST(CASOperationGate, GcSchedulerIsQuiescentReflectsRoundInFlight)
{
    auto backend = std::make_shared<Cas::InMemoryBackend>();
    auto pool = Cas::tests::openPoolForTest(backend);
    auto scheduler = std::make_shared<Cas::CasGcScheduler>(
        pool, std::chrono::seconds(3600), "op-gate-test-gc", "disk", Cas::GcRoundLogger{});
    EXPECT_TRUE(scheduler->isQuiescent());
    scheduler->setRoundInFlightForTest(true);
    EXPECT_FALSE(scheduler->isQuiescent()) << "a round in flight must NOT read as GC-quiescent";
    scheduler->setRoundInFlightForTest(false);
    EXPECT_TRUE(scheduler->isQuiescent());
}

/// (j) (acceptance matrix — transient auto-recovery / DROP-drain round-trip) The full §4 recovery arc on ONE
/// storage: a Remove-class op (the DROP shape) throws the typed transient refusal while the mount lease is
/// lost, then SUCCEEDS and actually drains once the disk self-remounts back to Live — no operator action,
/// no restart. Where test (d) forces `TransientNotLive` via the setter to pin the gap, this drives a REAL
/// transient→Live recovery (`tripMountLost` → fence-out → `tryRemountOnce`) so the throw-then-drain is one
/// continuous arc on the same pool. Closes the "access throws in the gap, auto-recovers, a Remove re-queues
/// and drains" matrix row end-to-end (the per-table DROP re-queue itself is the MergeTree caller's job; the
/// CAS contract is exactly this: refuse in the gap, admit after recovery).
TEST(CASOperationGate, RemoveThrowsDuringTransientAndDrainsAfterRecovery)
{
    auto storage = openGateStorage();
    commitOnePart(*storage);
    auto pool = storage->store();   /// captured while Live (store() is fail-closed once not-live)
    ASSERT_EQ(pool->lifecycle(), PoolLifecycle::Live);
    ASSERT_TRUE(storage->existsDirectory(kPartDir));   /// Live baseline: the part is present.

    /// The mount lease is transiently lost — the pool goes TransientNotLive.
    pool->tripMountLost();
    ASSERT_EQ(pool->lifecycle(), PoolLifecycle::TransientNotLive);

    /// In the gap, EVERY store-class access throws the typed transient refusal — the Remove (DROP shape)
    /// included, and a content read too. Nothing is answered benign, nothing is silently dropped.
    Cas::tests::expectThrowsCode(ErrorCodes::NETWORK_ERROR, [&] {
        auto tx = storage->createTransaction();
        tx->removeRecursive(kTableDir, /*should_remove_objects=*/nullptr);
    });
    Cas::tests::expectThrowsCode(ErrorCodes::NETWORK_ERROR, [&] { storage->getFileSize(kPartFile); });
    const std::string gap_msg = messageOf([&] { storage->getFileSize(kPartFile); });
    EXPECT_NE(gap_msg.find("mount lease not held"), std::string::npos)
        << "the gap message must name the transient (auto-recovering) condition: " << gap_msg;

    /// The lease is restored: the disk self-remounts a fresh incarnation and auto-recovers to Live.
    fenceOutMount(*pool->poolBackendPtr(), pool->layout().mountKey(kSrid));
    ASSERT_TRUE(pool->tryRemountOnce()) << "the self-remount must reclaim a fresh incarnation";
    ASSERT_EQ(pool->lifecycle(), PoolLifecycle::Live) << "the pool must auto-recover to Live";

    /// After recovery the SAME Remove drains: it commits cleanly and actually removes the part.
    {
        auto tx = storage->createTransaction();
        EXPECT_NO_THROW(tx->removeRecursive(kTableDir, /*should_remove_objects=*/nullptr));
        EXPECT_NO_THROW(tx->commit(NoCommitOptions{}));
    }
    EXPECT_FALSE(storage->existsDirectory(kPartDir))
        << "the re-queued removal must drain (really remove the part) once the disk recovers to Live";
    EXPECT_FALSE(storage->existsFile(kPartFile));
}
