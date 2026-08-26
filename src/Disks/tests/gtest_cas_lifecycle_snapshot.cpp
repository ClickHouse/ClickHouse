#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <filesystem>
#include <functional>
#include <memory>
#include <string>

/// Task 12 (rev.7 spec §7, [C5]-visibility): the NON-GATED lifecycle snapshot backing
/// `system.cas_mounts`. A Factory-class read (spec §1): I/O-free, no `store()`/`poolAccess`,
/// truthful in EVERY state — so a not-live / stopped / vanished / never-started disk stays VISIBLE to the
/// operator instead of silently missing from the table. These tests exercise the accessor directly (the
/// SQL-level assertions land in Task 14): `ContentAddressedMetadataStorage::lifecycleSnapshot` at the
/// storage level, and `Pool::lifecycleSnapshot` at the pool level (including the zero-backend-op proof).
/// Harness patterns follow gtest_cas_operation_gate.cpp / gtest_cas_forget.cpp.

namespace DB::ErrorCodes
{
extern const int INVALID_STATE;
}

using namespace DB;
using DB::Cas::PoolLifecycle;
using DB::Cas::tests::CountingBackend;

namespace
{

const std::string kSrid = "test";

/// A live table dir + committed part reused by the storage-level tests (the shape
/// gtest_cas_operation_gate.cpp / gtest_cas_forget.cpp use).
const std::string kTableDir = "sn0/sn0sn0s0-0808-4808-8808-080808080808";
const std::string kPartDir = kTableDir + "/all_1_1_0";
const std::string kPartFile = kPartDir + "/data.bin";

std::shared_ptr<ContentAddressedMetadataStorage> openSnapshotStorage()
{
    auto settings = Cas::tests::makeSettingsForTest(
        kSrid, std::filesystem::temp_directory_path() / "ca_snapshot_scratch");
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

/// Delete an existing key exactly (its current token comes from the same GET) — used to drive a live pool
/// into a NATURAL `IdentityLost`. Mirrors gtest_cas_forget.cpp / gtest_cas_lifecycle_condition.cpp.
void deleteKeyExact(DB::Cas::Backend & backend, const String & key)
{
    const auto got = backend.get(key);
    ASSERT_TRUE(got.has_value()) << "expected '" << key << "' to exist before deletion";
    if (got)
        backend.deleteExact(key, got->token);
}

}

/// (a) Live: the snapshot reads `live` with no reason and no `since`, and always carries the disk's
/// last-known identity (pool_id + server_root_id).
TEST(CASLifecycleSnapshot, LiveIsTruthfulWithIdentity)
{
    auto storage = openSnapshotStorage();
    commitOnePart(*storage);

    const CasLifecycleSnapshot snap = storage->lifecycleSnapshot();
    EXPECT_EQ(snap.lifecycle, "live");
    EXPECT_TRUE(snap.reason.empty()) << snap.reason;
    EXPECT_TRUE(snap.detail.empty()) << snap.detail;
    EXPECT_EQ(snap.since, 0) << "a live pool has no lifecycle `since`";
    EXPECT_EQ(snap.server_root_id, storage->serverRootId());
    EXPECT_FALSE(snap.pool_id.empty()) << "a started disk knows its pool identity";
    EXPECT_EQ(snap.pool_id, storage->getPoolUUID());
}

/// (b) IdentityLost (forced from Live on the captured handle, the gate-test idiom): the snapshot names the
/// non-auto-recovering `identity_lost` state with the [D5] detail present and `since` set. The enum-clean
/// `reason` word is empty here — it carries only the `vanished` sub-state, and `identity_lost` is already
/// fully named by the `lifecycle` column.
TEST(CASLifecycleSnapshot, IdentityLostHasDetailAndSince)
{
    auto storage = openSnapshotStorage();
    commitOnePart(*storage);
    auto pool = storage->store();   /// captured while Live (store() is fail-closed on a terminal pool)

    pool->setLifecycleForTest(PoolLifecycle::IdentityLost);

    const CasLifecycleSnapshot snap = storage->lifecycleSnapshot();
    EXPECT_EQ(snap.lifecycle, "identity_lost");
    EXPECT_TRUE(snap.reason.empty()) << "reason is the vanish sub-state word only: " << snap.reason;
    EXPECT_NE(snap.detail.find("identity lost"), std::string::npos) << snap.detail;
    EXPECT_NE(snap.since, 0) << "a not-live state carries the wall-clock instant it was entered";
    /// Identity survives a terminal state — the disk stays introspectable under it.
    EXPECT_EQ(snap.pool_id, storage->getPoolUUID());
}

/// (c) VanishedForgotten via the REAL verb (`storage->forgetDisk()`): the snapshot reads `vanished` with the
/// enum-clean `reason` word `forgotten` (so Task 14's `lifecycle || '(' || lifecycle_reason || ')'` reads
/// EXACTLY `vanished(forgotten)`), the [D5] `detail` carrying the operator's decommission timestamp, `since`
/// set, and the identity still present.
TEST(CASLifecycleSnapshot, VanishedForgottenIsEnumCleanWithTimestampedDetail)
{
    auto storage = openSnapshotStorage();
    commitOnePart(*storage);
    const String pool_id_before = storage->getPoolUUID();

    storage->forgetDisk();

    const CasLifecycleSnapshot snap = storage->lifecycleSnapshot();
    EXPECT_EQ(snap.lifecycle, "vanished");
    EXPECT_EQ(snap.reason, "forgotten");
    /// Task 14's teardown check depends on this exact concatenation.
    EXPECT_EQ(snap.lifecycle + "(" + snap.reason + ")", "vanished(forgotten)");
    EXPECT_NE(snap.detail.find("SYSTEM CAS FORGET at "), std::string::npos) << snap.detail;
    EXPECT_NE(snap.detail.find("erasure was NOT verified"), std::string::npos) << snap.detail;
    EXPECT_NE(snap.since, 0);
    /// The disk stays registered and introspectable under its identity after FORGET.
    EXPECT_EQ(snap.pool_id, pool_id_before);
    EXPECT_EQ(snap.server_root_id, storage->serverRootId());
}

/// (d) A null pool never crashes the accessor and reports the storage-level lifecycle: `constructing`
/// before the first startup, `shutdown` after teardown. reason/since stay empty/0 (no terminal cause).
TEST(CASLifecycleSnapshot, NullPoolReportsConstructingThenShutdown)
{
    auto settings = Cas::tests::makeSettingsForTest(
        kSrid, std::filesystem::temp_directory_path() / "ca_snapshot_null_scratch");
    auto storage = std::make_shared<ContentAddressedMetadataStorage>(
        Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);

    /// Constructed but never started: no pool published.
    const CasLifecycleSnapshot before = storage->lifecycleSnapshot();
    EXPECT_EQ(before.lifecycle, "constructing");
    EXPECT_TRUE(before.reason.empty());
    EXPECT_TRUE(before.detail.empty());
    EXPECT_EQ(before.since, 0);
    EXPECT_TRUE(before.pool_id.empty()) << "no identity before startup";
    EXPECT_EQ(before.server_root_id, kSrid) << "the identity is known from config even pre-startup";

    storage->startup();
    ASSERT_EQ(storage->lifecycleSnapshot().lifecycle, "live");

    storage->shutdown();
    const CasLifecycleSnapshot after = storage->lifecycleSnapshot();
    EXPECT_EQ(after.lifecycle, "shutdown") << "a torn-down disk is distinguishable from a never-started one";
    EXPECT_FALSE(after.pool_id.empty()) << "the last-known identity survives shutdown";
}

/// (e) The accessor is I/O-free (spec §1 Factory class): NO backend op runs, in any lifecycle state. Proven
/// against a `CountingBackend` — the totals recorded after open do not move across snapshot reads, whether
/// the pool is Live or forced terminal.
TEST(CASLifecycleSnapshot, PerformsZeroBackendOps)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);

    const uint64_t head0 = backend->headTotal();
    const uint64_t get0 = backend->getTotal();
    const uint64_t put0 = backend->putTotal();
    const uint64_t getstream0 = backend->getStreamTotal();
    const uint64_t list0 = backend->listTotal();

    const auto assertNoIo = [&](const char * where)
    {
        EXPECT_EQ(backend->headTotal(), head0) << where;
        EXPECT_EQ(backend->getTotal(), get0) << where;
        EXPECT_EQ(backend->putTotal(), put0) << where;
        EXPECT_EQ(backend->getStreamTotal(), getstream0) << where;
        EXPECT_EQ(backend->listTotal(), list0) << where;
    };

    /// Live snapshot: zero I/O.
    (void)store->lifecycleSnapshot();
    assertNoIo("live snapshot must not touch the backend");

    /// Forced terminal snapshot (the very state the store()-class surface refuses): still zero I/O.
    store->setLifecycleForTest(PoolLifecycle::VanishedReplaced);
    const DB::Cas::Pool::LifecycleSnapshot vanished = store->lifecycleSnapshot();
    assertNoIo("a vanished-pool snapshot must not touch the backend");
    EXPECT_EQ(vanished.lifecycle, PoolLifecycle::VanishedReplaced);
}

/// (f) A NATURAL transition (not the forced setter) captures the detail + `since`, and the snapshot's detail
/// is EXACTLY the [D5] text `throwIfLifecycleTerminal` throws (minus the pool-name prefix) — the spec §1
/// "same reason strings in the snapshot and the error" guarantee, so the two can never drift.
TEST(CASLifecycleSnapshot, NaturalIdentityLostMatchesThrowDetail)
{
    auto backend = std::make_shared<DB::Cas::InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);

    /// Delete both pool sentinels while other objects remain, then drive the identity gate → IdentityLost
    /// (never Vanished), exactly gtest_cas_lifecycle_condition.cpp scenario (a).
    deleteKeyExact(*backend, store->layout().poolMetaKey());
    deleteKeyExact(*backend, store->layout().ownerKey(kSrid));
    EXPECT_FALSE(store->tryRemountOnce());
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::IdentityLost);

    const DB::Cas::Pool::LifecycleSnapshot snap = store->lifecycleSnapshot();
    EXPECT_EQ(snap.lifecycle, PoolLifecycle::IdentityLost);
    EXPECT_NE(snap.since, 0) << "the natural enterIdentityLost transition stamps the wall-clock `since`";
    EXPECT_FALSE(snap.detail.empty());

    /// The snapshot detail is the SAME [D5] text the typed error surfaces: the throw is
    /// "content-addressed pool '<srid>' <detail>", so the error message must contain the snapshot detail.
    std::string thrown;
    try
    {
        store->throwIfLifecycleTerminal();
        ADD_FAILURE() << "IdentityLost must throw from throwIfLifecycleTerminal";
    }
    catch (const Exception & e)
    {
        thrown = std::string(e.message());
    }
    EXPECT_NE(thrown.find(snap.detail), std::string::npos)
        << "snapshot detail and the typed error must not drift\n  detail: " << snap.detail
        << "\n  thrown: " << thrown;
}
