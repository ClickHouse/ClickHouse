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
using DB::Cas::tests::OperationForTest;

namespace
{

/// One renewer for the mount slot of server-root "r", under (uuid=1, epoch=7) unless overridden. Both
/// of its planes are the same open-fence one: what these tests exercise is the mount protocol's own
/// exclusivity, not a fence's, and no test here renews, which is the only caller of the mount plane.
MountLeaseRenewer makeRenewer(
    CasRequests & requests,
    uint64_t & now,
    DB::UInt128 uuid = DB::UInt128(1),
    uint64_t epoch = 7)
{
    return MountLeaseRenewer(
        requests,
        requests,
        Layout("p"),
        "r",
        uuid,
        epoch,
        std::chrono::milliseconds(100),
        [&now] { return now; },
        [] { return uint64_t{0}; });
}

void markMountGcFenced(CasOperation & op, const Layout & layout, const String & server_root_id)
{
    const String key = layout.mountKey(server_root_id);
    const auto got = op.read(key, Retry::standard());
    ASSERT_TRUE(got);
    MountLease lease = decodeMountLease(got->bytes);
    lease.gc_fenced = true;
    ASSERT_TRUE(std::holds_alternative<Committed>(
        op.replace(key, encodeMountLease(lease), got->etag, Retry::standard())));
}

}

TEST(CASMountClaimConflicts, SlotAppearedBetweenTheReadAndTheCreate)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    /// Absent at the read; another process mints it before our create lands.
    backend->before_put_if_absent = [&]
    {
        CasOperation racer = requests.admit();
        claimMount(racer, layout, "r", DB::UInt128(2), 1, now, /*ttl_ms=*/100);
    };
    auto renewer = makeRenewer(requests, now);
    expectThrowsCodeWithMessage(
        DB::ErrorCodes::ABORTED,
        "appeared between the read and the create",
        [&] { renewer.start(); });
}

TEST(CASMountClaimConflicts, SlotHeldByForeignServer)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    ASSERT_EQ(
        claimMount(op, layout, "r", DB::UInt128(2), 1, now, /*ttl_ms=*/100).kind,
        MountClaimResult::Claimed);
    auto renewer = makeRenewer(requests, now);
    expectThrowsCodeWithMessage(
        DB::ErrorCodes::ABORTED,
        "held by a foreign server",
        [&] { renewer.start(); });
}

TEST(CASMountClaimConflicts, SlotHeldByDifferentWriterEpoch)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    ASSERT_EQ(
        claimMount(op, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100).kind,
        MountClaimResult::Claimed);
    auto renewer = makeRenewer(requests, now, DB::UInt128(1), /*epoch=*/8);
    expectThrowsCodeWithMessage(
        DB::ErrorCodes::ABORTED,
        "held by a different writer_epoch",
        [&] { renewer.start(); });
}

TEST(CASMountClaimConflicts, SlotChangedInsideAdoptionWindow)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    ASSERT_EQ(
        claimMount(op, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100).kind,
        MountClaimResult::Claimed);
    /// Rewrite the slot under a NEW incarnation after our read, so our adoption write is refused.
    backend->before_put_overwrite = [&]
    {
        CasOperation racer = requests.admit();
        claimMount(racer, layout, "r", DB::UInt128(1), 7, now + 1, /*ttl_ms=*/100);
    };
    auto renewer = makeRenewer(requests, now);
    expectThrowsCodeWithMessage(
        DB::ErrorCodes::ABORTED,
        "changed while adopting our own mount slot",
        [&] { renewer.start(); });
}

TEST(CASMountClaimConflicts, SlotVanishedInsideAdoptionWindow)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    ASSERT_EQ(
        claimMount(op, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100).kind,
        MountClaimResult::Claimed);
    backend->before_put_overwrite = [&]
    {
        CasOperation racer = requests.admit();
        ASSERT_EQ(racer.removeCurrent(layout.mountKey("r"), Retry::standard()), Removal::Removed);
    };
    auto renewer = makeRenewer(requests, now);
    expectThrowsCodeWithMessage(
        DB::ErrorCodes::ABORTED,
        "vanished while adopting our own mount slot",
        [&] { renewer.start(); });
}

/// The two fenced branches keep their own type, and keep PRECEDENCE over the conflicts above: the
/// mount-open loop catches `MountFencedException` by type and recovers with a fresh writer epoch, so
/// a fence reported as a plain conflict would turn a recoverable state into a failed mount.
TEST(CASMountClaimConflicts, FencedBeforeAdoptionRaisesMountFenced)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    ASSERT_EQ(
        claimMount(op, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100).kind,
        MountClaimResult::Claimed);
    markMountGcFenced(op, layout, "r");
    auto renewer = makeRenewer(requests, now);
    EXPECT_THROW(renewer.start(), MountFencedException);
}

TEST(CASMountClaimConflicts, FencedInsideAdoptionWindowRaisesMountFencedNotAborted)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    ASSERT_EQ(
        claimMount(op, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100).kind,
        MountClaimResult::Claimed);
    /// The slot changes inside the adoption window AND the new body is fenced: the fenced branch must
    /// win over the "changed while adopting" one.
    backend->before_put_overwrite = [&]
    {
        CasOperation racer = requests.admit();
        markMountGcFenced(racer, layout, "r");
    };
    auto renewer = makeRenewer(requests, now);
    EXPECT_THROW(renewer.start(), MountFencedException);
}

/// A raced claim reports a body its caller renders into the fail-closed operator message. Reporting
/// the PROPOSER's own lease there names this very server as the existing mount, which sends an
/// operator hunting a second process that is not the one holding the slot. The write's own resolve
/// read already observed the occupant, so that is what the result must carry.
TEST(CASMountClaimConflicts, ALostCreateReportsTheOccupantNotTheProposer)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    CasRequests requests = openRequestsForTest(backend);
    /// Absent at our read; a foreign server mints the slot before our create lands.
    backend->before_put_if_absent = [&]
    {
        CasOperation racer = requests.admit();
        ASSERT_EQ(claimMount(racer, layout, "r", DB::UInt128(2), 1, now, /*ttl_ms=*/100).kind,
                  MountClaimResult::Claimed);
    };
    CasOperation op = requests.admit();
    const MountClaimResult claim = claimMount(op, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100);

    EXPECT_EQ(claim.kind, MountClaimResult::LiveDoubleStart);
    ASSERT_TRUE(claim.body.has_value());
    EXPECT_EQ(claim.body->server_uuid, DB::UInt128(2)) << "the result named this server's own proposal";
    EXPECT_EQ(claim.body->writer_epoch, 1u);
    ASSERT_TRUE(claim.etag.has_value()) << "the observed occupant's incarnation is what was read";
    EXPECT_NE(mountDoubleStartMessage("r", claim.body).find(u128ToHex(DB::UInt128(2))), String::npos)
        << "the operator message must name the foreign holder";
}

/// The same for the refresh branch: a body that changed under our own adoption is the one the message
/// must name. The reclaim branch reaches the identical helper, so it is not repeated here.
TEST(CASMountClaimConflicts, ALostRefreshReportsTheObservedBodyNotTheProposer)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    CasRequests requests = openRequestsForTest(backend);
    CasOperation seed = requests.admit();
    ASSERT_EQ(claimMount(seed, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100).kind,
              MountClaimResult::Claimed);
    /// A distinguishable body lands under our own refresh, so a result carrying the proposal cannot
    /// pass by accident: our proposal would carry `seq` 2 and this process's pid.
    backend->before_put_overwrite = [&]
    {
        OperationForTest racer(*backend);
        const String key = layout.mountKey("r");
        const auto got = (*racer).read(key, Retry::standard());
        ASSERT_TRUE(got.has_value());
        MountLease raced = decodeMountLease(got->bytes);
        raced.pid = 4242;
        raced.seq = 99;
        ASSERT_TRUE(std::holds_alternative<Committed>(
            (*racer).replace(key, encodeMountLease(raced), got->etag, Retry::standard())));
    };
    CasOperation op = requests.admit();
    const MountClaimResult claim = claimMount(op, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100);

    EXPECT_EQ(claim.kind, MountClaimResult::LiveDoubleStart);
    ASSERT_TRUE(claim.body.has_value());
    EXPECT_EQ(claim.body->pid, 4242);
    EXPECT_EQ(claim.body->seq, 99u);
    ASSERT_TRUE(claim.etag.has_value());
}

/// The residue of the same rule: a raced write whose conflict settles to no observation saw nobody, so
/// there is no holder to name. Reporting the lease this server merely PROPOSED would put this very
/// process in the "Existing mount" line of an operator message -- the same defect as naming it after a
/// conflict that did observe someone. `ProvenAbsent` is the reachable half; `NotObserved` (the resolve
/// read itself failed) leaves through the same branch.
TEST(CASMountClaimConflicts, ARacedRefreshThatObservedNothingNamesNoHolder)
{
    auto backend = std::make_shared<MountSlotRaceBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    CasRequests requests = openRequestsForTest(backend);
    CasOperation seed = requests.admit();
    ASSERT_EQ(claimMount(seed, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100).kind,
              MountClaimResult::Claimed);
    /// The slot is removed under our own refresh, so the refused precondition resolves to a proven
    /// absence rather than to an occupant.
    backend->before_put_overwrite = [&]
    {
        OperationForTest racer(*backend);
        const String key = layout.mountKey("r");
        const auto got = (*racer).read(key, Retry::standard());
        ASSERT_TRUE(got.has_value());
        ASSERT_EQ((*racer).remove(key, got->etag, Retry::standard()), Removal::Removed);
    };
    CasOperation op = requests.admit();
    const MountClaimResult claim = claimMount(op, layout, "r", DB::UInt128(1), 7, now, /*ttl_ms=*/100);

    EXPECT_EQ(claim.kind, MountClaimResult::LiveDoubleStart);
    EXPECT_FALSE(claim.etag.has_value());
    EXPECT_FALSE(claim.body.has_value()) << "a result that observed nobody reported a lease anyway";
    const String message = mountDoubleStartMessage("r", claim.body);
    EXPECT_NE(message.find("could not be observed"), String::npos)
        << "the message named a holder nobody saw: " << message;
}
