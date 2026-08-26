#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/tests/cas_test_helpers.h>

namespace DB::ErrorCodes
{
extern const int ABORTED;
}

using namespace DB::Cas;
using DB::Cas::tests::MountSlotRaceBackend;
using DB::Cas::tests::expectThrowsCodeWithMessage;

namespace
{

/// One keeper for the mount slot of server-root "r", under (uuid=1, epoch=7) unless overridden.
MountLeaseKeeper makeKeeper(
    const std::shared_ptr<MountSlotRaceBackend> & backend,
    uint64_t & now,
    DB::UInt128 uuid = DB::UInt128(1),
    uint64_t epoch = 7)
{
    return MountLeaseKeeper(
        backend,
        Layout("p"),
        "r",
        uuid,
        epoch,
        std::chrono::milliseconds(100),
        [&now] { return now; },
        [] { return uint64_t{0}; });
}

void markMountGcFenced(MountSlotRaceBackend & backend, const Layout & layout, const String & server_root_id)
{
    const String key = layout.mountKey(server_root_id);
    const auto got = backend.get(key);
    ASSERT_TRUE(got);
    MountLease lease = decodeMountLease(got->bytes);
    lease.gc_fenced = true;
    const PutResult result = backend.putOverwrite(key, encodeMountLease(lease), got->token);
    ASSERT_EQ(result.outcome, PutOutcome::Done);
}

}

TEST(CASMountClaimConflicts, SlotAppearedBetweenHeadAndPutIfAbsent)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    /// Empty at `head`; another process mints it before our `putIfAbsent` lands.
    backend->before_put_if_absent = [&]
    {
        claimMount(*backend, layout, "r", DB::UInt128(2), 1, now, /*ttl_ms=*/100);
    };
    auto keeper = makeKeeper(backend, now);
    expectThrowsCodeWithMessage(
        DB::ErrorCodes::ABORTED,
        "appeared between head and putIfAbsent",
        [&] { keeper.start(); });
}

TEST(CASMountClaimConflicts, SlotVanishedBetweenHeadAndGet)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    ASSERT_EQ(
        claimMount(*backend, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100).kind,
        MountClaimResult::Claimed);
    backend->before_get = [&]
    {
        const auto got = backend->get(layout.mountKey("r"));
        ASSERT_TRUE(got);
        backend->deleteExact(layout.mountKey("r"), got->token);
    };
    auto keeper = makeKeeper(backend, now);
    expectThrowsCodeWithMessage(
        DB::ErrorCodes::ABORTED,
        "vanished between head and get while claiming",
        [&] { keeper.start(); });
}

TEST(CASMountClaimConflicts, SlotHeldByForeignServer)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    ASSERT_EQ(
        claimMount(*backend, layout, "r", DB::UInt128(2), 1, now, /*ttl_ms=*/100).kind,
        MountClaimResult::Claimed);
    auto keeper = makeKeeper(backend, now);
    expectThrowsCodeWithMessage(
        DB::ErrorCodes::ABORTED,
        "held by a foreign server",
        [&] { keeper.start(); });
}

TEST(CASMountClaimConflicts, SlotHeldByDifferentWriterEpoch)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    ASSERT_EQ(
        claimMount(*backend, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100).kind,
        MountClaimResult::Claimed);
    auto keeper = makeKeeper(backend, now, DB::UInt128(1), /*epoch=*/8);
    expectThrowsCodeWithMessage(
        DB::ErrorCodes::ABORTED,
        "held by a different writer_epoch",
        [&] { keeper.start(); });
}

TEST(CASMountClaimConflicts, SlotChangedInsideAdoptionWindow)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    ASSERT_EQ(
        claimMount(*backend, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100).kind,
        MountClaimResult::Claimed);
    /// Rewrite the slot under a NEW token after our `get`, so our adoption `putOverwrite` conflicts.
    backend->before_put_overwrite = [&]
    {
        claimMount(*backend, layout, "r", DB::UInt128(1), 7, now + 1, /*ttl_ms=*/100);
    };
    auto keeper = makeKeeper(backend, now);
    expectThrowsCodeWithMessage(
        DB::ErrorCodes::ABORTED,
        "changed while adopting our own mount slot",
        [&] { keeper.start(); });
}

TEST(CASMountClaimConflicts, SlotVanishedInsideAdoptionWindow)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    ASSERT_EQ(
        claimMount(*backend, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100).kind,
        MountClaimResult::Claimed);
    backend->before_put_overwrite = [&]
    {
        const auto got = backend->get(layout.mountKey("r"));
        ASSERT_TRUE(got);
        backend->deleteExact(layout.mountKey("r"), got->token);
    };
    auto keeper = makeKeeper(backend, now);
    expectThrowsCodeWithMessage(
        DB::ErrorCodes::ABORTED,
        "vanished while adopting our own mount slot",
        [&] { keeper.start(); });
}

/// The two fenced branches keep their own type, and keep PRECEDENCE over the conflicts above: the
/// mount-open loop catches `MountFencedException` by type and recovers with a fresh writer epoch, so
/// a fence reported as a plain conflict would turn a recoverable state into a failed mount.
TEST(CASMountClaimConflicts, FencedBeforeAdoptionRaisesMountFenced)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    ASSERT_EQ(
        claimMount(*backend, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100).kind,
        MountClaimResult::Claimed);
    markMountGcFenced(*backend, layout, "r");
    auto keeper = makeKeeper(backend, now);
    EXPECT_THROW(keeper.start(), MountFencedException);
}

TEST(CASMountClaimConflicts, FencedInsideAdoptionWindowRaisesMountFencedNotAborted)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    ASSERT_EQ(
        claimMount(*backend, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100).kind,
        MountClaimResult::Claimed);
    /// The slot changes inside the adoption window AND the new body is fenced: the fenced branch must
    /// win over the "changed while adopting" one.
    backend->before_put_overwrite = [&] { markMountGcFenced(*backend, layout, "r"); };
    auto keeper = makeKeeper(backend, now);
    EXPECT_THROW(keeper.start(), MountFencedException);
}
