#include <gtest/gtest.h>
#include "cas_test_helpers.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Common/ProfileEvents.h>

#include <chrono>
#include <limits>
#include <map>
#include <string>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
}

namespace ProfileEvents
{
    extern const Event CASMountLeaseLost;
    extern const Event CASMountExclusivityViolation;
}

using namespace DB::Cas;

namespace
{

const ObserveRefCatalog & emptyCatalogObservation()
{
    static const ObserveRefCatalog observe = [] { return RefCatalog{}; };
    return observe;
}

RefCatalog catalogOwning(const String & ns, NsState state)
{
    CatalogEntry entry{.ns = RootNamespace{ns}, .state = state, .incarnation = UInt128{42}};
    if (state == NsState::Creating)
        entry.creator = CreatorFence{.server_root_id = "root/x", .writer_epoch = 1, .fence_generation = 1};
    return RefCatalog{.entries = {std::move(entry)}};
}

class OwnerConflictRevealsManifestBackend : public InMemoryBackend
{
public:
    using InMemoryBackend::putIfAbsent;

    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        if (!fired && key == "p/gc/server-roots/root/x/owner")
        {
            fired = true;
            InMemoryBackend::putIfAbsent("p/cas/manifests/root/x/table/debris", "x");
            return {PutOutcome::PreconditionFailed, {}};
        }
        return InMemoryBackend::putIfAbsent(key, bytes, meta);
    }

    bool fired = false;
};

class EpochConflictRevealsManifestBackend : public InMemoryBackend
{
public:
    using InMemoryBackend::casPut;

    CasResult casPut(
        const String & key, const String & bytes, const std::optional<Token> & expected,
        const ObjectMeta & meta) override
    {
        if (!fired && key == "p/gc/server-roots/root/x/epoch")
        {
            fired = true;
            /// Install the competing allocator's winning epoch before revealing owned work. The
            /// retry must not accept that now-present epoch without rechecking the entire emptiness
            /// bundle that authorized the original absent-epoch attempt.
            const CasResult winner = InMemoryBackend::casPut(
                key, encodeServerEpoch(ServerEpoch{.next_writer_epoch = 2}), expected, meta);
            winner_installed = winner.outcome == CasOutcome::Committed;
            InMemoryBackend::putIfAbsent("p/cas/manifests/root/x/table/debris", "x");
            return {CasOutcome::Conflict, {}};
        }
        return InMemoryBackend::casPut(key, bytes, expected, meta);
    }

    bool fired = false;
    bool winner_installed = false;
};

}

TEST(CASServerRootId, ValidationAcceptsCleanPathsRejectsBad)
{
    EXPECT_NO_THROW(validateServerRootId("replica-a"));
    EXPECT_NO_THROW(validateServerRootId("shard-01/replica-a"));
    EXPECT_THROW(validateServerRootId(""), DB::Exception);
    EXPECT_THROW(validateServerRootId("/replica"), DB::Exception);
    EXPECT_THROW(validateServerRootId("replica/"), DB::Exception);
    EXPECT_THROW(validateServerRootId("a//b"), DB::Exception);
    EXPECT_THROW(validateServerRootId("a/../b"), DB::Exception);
    EXPECT_THROW(validateServerRootId("a/_files/b"), DB::Exception);
}

TEST(CASServerRoot, KeysAndCodecsRoundTrip)
{
    Layout layout("p");

    /// Layout keys under gc/server-roots/<srid>/.
    EXPECT_EQ(layout.serverRootPrefix("replica-a"), "p/gc/server-roots/replica-a/");
    EXPECT_EQ(layout.ownerKey("replica-a"), "p/gc/server-roots/replica-a/owner");
    EXPECT_EQ(layout.epochKey("replica-a"), "p/gc/server-roots/replica-a/epoch");
    EXPECT_EQ(layout.mountKey("replica-a"), "p/gc/server-roots/replica-a/mount");

    /// Owner round-trip.
    {
        OwnerObject o;
        o.server_uuid = (UInt128(0x0123456789abcdefULL) << 64) | UInt128(0xfedcba9876543210ULL);
        const OwnerObject back = decodeOwner(encodeOwner(o));
        EXPECT_EQ(back.server_uuid, o.server_uuid);
    }

    /// ServerEpoch round-trip.
    {
        ServerEpoch e;
        e.next_writer_epoch = 4242;
        const ServerEpoch back = decodeServerEpoch(encodeServerEpoch(e));
        EXPECT_EQ(back.next_writer_epoch, e.next_writer_epoch);
    }

    /// MountLease round-trip.
    {
        MountLease m;
        m.server_uuid = (UInt128(0xdeadbeefcafef00dULL) << 64) | UInt128(0x0011223344556677ULL);
        m.writer_epoch = 7;
        m.hostname = "host-1.example.com";
        m.pid = 12345;
        m.started_at_ms = 1700000000000ULL;
        m.seq = 99;
        m.expires_at_ms = 1700000030000ULL;
        const MountLease back = decodeMountLease(encodeMountLease(m));
        EXPECT_EQ(back.server_uuid, m.server_uuid);
        EXPECT_EQ(back.writer_epoch, m.writer_epoch);
        EXPECT_EQ(back.hostname, m.hostname);
        EXPECT_EQ(back.pid, m.pid);
        EXPECT_EQ(back.started_at_ms, m.started_at_ms);
        EXPECT_EQ(back.seq, m.seq);
        EXPECT_EQ(back.expires_at_ms, m.expires_at_ms);
    }

    /// Fail-closed decode on garbage bytes.
    EXPECT_THROW(decodeOwner("not-a-proto-with-magic"), DB::Exception);
    EXPECT_THROW(decodeServerEpoch(""), DB::Exception);
    EXPECT_THROW(decodeMountLease(""), DB::Exception);
}

TEST(CASServerRootClaim, OwnerStickyAndForeignFailsClosed)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    EXPECT_NO_THROW(claimOwnerOrThrow(*b, l, "r", UInt128(1), emptyCatalogObservation()));     // fresh empty root → claim
    EXPECT_NO_THROW(claimOwnerOrThrow(*b, l, "r", UInt128(1), emptyCatalogObservation()));     // same uuid → ok
    EXPECT_THROW(claimOwnerOrThrow(*b, l, "r", UInt128(2), emptyCatalogObservation()), DB::Exception);  // foreign → fail closed
}

TEST(CASServerRootClaim, TombstonedSameOwnerFailsClosed)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    b->putIfAbsent(l.ownerKey("r"), encodeOwner(OwnerObject{
        .server_uuid = UInt128(1),
        .retired_at_ms = 1752537600000ULL,
    }));

    try
    {
        claimOwnerOrThrow(*b, l, "r", UInt128(1), emptyCatalogObservation());
        FAIL() << "expected a tombstoned owner claim to fail closed";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
        EXPECT_NE(e.message().find("decommissioned"), String::npos) << e.message();
        EXPECT_EQ(e.message().find("owned by a different server"), String::npos) << e.message();
    }
}

TEST(CASServerRootEpoch, AllocatorIsMonotoneAndSurvivesMountConcept)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("r");
    claimOwnerOrThrow(*b, l, "r", UInt128(1), emptyCatalogObservation());
    const uint64_t e1 = allocateWriterEpoch(*b, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation());
    const uint64_t e2 = allocateWriterEpoch(*b, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation());
    EXPECT_GE(e1, 1u);                                             // 0 is a reserved sentinel
    EXPECT_GT(e2, e1);                                             // strictly increasing

    /// Deleting the (separate) mount object must NOT reset the epoch. No mount has been written in
    /// Task 4, so deleteExact of a non-existent mount is a NotFound no-op that touches nothing.
    const auto del = b->deleteExact(l.mountKey("r"), b->head(l.mountKey("r")).token);
    EXPECT_EQ(del.kind, DeleteOutcome::Kind::NotFound);
    EXPECT_GT(allocateWriterEpoch(*b, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), e2);
}

/// Phase C (spec rev.4): an ABSENT epoch object over a PRESENT mount object means durable epoch
/// state was lost while a mount is live/recent — re-minting epoch 1 there is how a same-(uuid,
/// epoch) twin is born. Refuse.
TEST(CASMount, EpochRemintOverExistingMountRefuses)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    claimOwnerOrThrow(*b, l, "r", UInt128(1), emptyCatalogObservation());
    ASSERT_EQ(claimMount(*b, l, "r", UInt128(1), /*our_epoch=*/1, /*now_ms=*/1000, /*ttl_ms=*/30000).kind,
              MountClaimResult::Claimed);
    /// The epoch object is ABSENT (never created in this sequence) while the mount exists:
    EXPECT_THROW(allocateWriterEpoch(*b, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), DB::Exception);   /// CORRUPTED_DATA
}

TEST(CASMount, EpochRemintAuthoritativeAbsenceMints)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    claimOwnerOrThrow(*b, l, "r", UInt128(1), emptyCatalogObservation());
    EXPECT_EQ(allocateWriterEpoch(*b, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), 1u);   /// fresh root: both control objects absent
    EXPECT_EQ(allocateWriterEpoch(*b, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), 2u);   /// epoch present now: normal CAS bump, no probe
}

/// The probe outcome gates the mint: anything short of authoritative KeyAbsent fails closed.
TEST(CASMount, EpochRemintIndeterminateProbeFailsClosed)
{
    class IndeterminateProbeBackend final : public InMemoryBackend
    {
    public:
        SentinelProbeResult probeSentinelRaw(const String &) override
        {
            return {.outcome = ProbeOutcome::Indeterminate, .body = std::nullopt};
        }
    };
    auto b = std::make_shared<IndeterminateProbeBackend>();
    Layout l("p");
    claimOwnerOrThrow(*b, l, "r", UInt128(1), emptyCatalogObservation());
    EXPECT_THROW(allocateWriterEpoch(*b, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), DB::Exception);
}

/// Decommission over a TERMINAL (expired/fenced) mount with a lost epoch object proceeds and mints
/// an epoch DISTINCT from the surviving mount's — the same-pair state is unrepresentable.
TEST(CASMount, DecommissionRemintOverTerminalMountMintsDistinctEpoch)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    claimOwnerOrThrow(*b, l, "r", UInt128(1), emptyCatalogObservation());
    ASSERT_EQ(claimMount(*b, l, "r", UInt128(1), /*our_epoch=*/3, /*now_ms=*/1000, /*ttl_ms=*/100).kind,
              MountClaimResult::Claimed);
    /// now_ms=5000: the ttl_ms=100 lease above is long expired -> terminal.
    EXPECT_EQ(allocateWriterEpoch(*b, l, "r", EpochMintPolicy::DecommissionRecovery, /*now_ms=*/5000, emptyCatalogObservation()), 4u);
}

/// Decommission over a LIVE mount with a lost epoch refuses — the blind bypass would recreate the
/// forbidden pair (codex round-3 finding 1) and defeat CASDecommission.RefusesLiveMember.
TEST(CASMount, DecommissionRemintOverLiveMountRefuses)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    claimOwnerOrThrow(*b, l, "r", UInt128(1), emptyCatalogObservation());
    ASSERT_EQ(claimMount(*b, l, "r", UInt128(1), /*our_epoch=*/1, /*now_ms=*/1000, /*ttl_ms=*/30000).kind,
              MountClaimResult::Claimed);
    EXPECT_THROW(allocateWriterEpoch(*b, l, "r", EpochMintPolicy::DecommissionRecovery, /*now_ms=*/2000, emptyCatalogObservation()),
                 DB::Exception);   /// ABORTED: live member
}

/// The steady-state path (epoch object PRESENT) must never pay the probe — pins the zero
/// normal-path cost the spec claims.
TEST(CASMount, EpochBumpWithPresentEpochIssuesNoProbe)
{
    class ProbeCountingBackend final : public InMemoryBackend
    {
    public:
        int probes = 0;
        SentinelProbeResult probeSentinelRaw(const String & k) override
        {
            ++probes;
            return InMemoryBackend::probeSentinelRaw(k);
        }
    };
    auto b = std::make_shared<ProbeCountingBackend>();
    Layout l("p");
    claimOwnerOrThrow(*b, l, "r", UInt128(1), emptyCatalogObservation());
    EXPECT_EQ(allocateWriterEpoch(*b, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), 1u);   /// bootstrap: ONE probe (absent-epoch branch)
    const int probes_after_bootstrap = b->probes;
    EXPECT_EQ(allocateWriterEpoch(*b, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), 2u);   /// epoch present: normal CAS bump...
    EXPECT_EQ(b->probes, probes_after_bootstrap) << "...must not probe the mount key";
}

TEST(CASServerRootClaim, MissingOwnerOverNonEmptyRootIsCorrupted)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    /// Simulate existing data without an owner (identity lost): plant a key under roots/<srid>/.
    b->putIfAbsent(l.serverRootDataPrefix("r") + "some-data", "x");
    EXPECT_THROW(claimOwnerOrThrow(*b, l, "r", UInt128(1), emptyCatalogObservation()), DB::Exception);
}

TEST(CASServerRootSafety, EveryCatalogLifecycleStateBlocksOwnerAndEpochRecreation)
{
    const Layout layout("p");
    for (const NsState state : {NsState::Creating, NsState::Live, NsState::Removing})
    {
        RefCatalog catalog = catalogOwning("root/x/table", state);
        const ObserveRefCatalog observe = [catalog] { return catalog; };

        InMemoryBackend owner_backend;
        EXPECT_THROW(claimOwnerOrThrow(owner_backend, layout, "root/x", UInt128{1}, observe), DB::Exception);
        EXPECT_FALSE(owner_backend.head(layout.ownerKey("root/x")).exists);

        InMemoryBackend epoch_backend;
        EXPECT_THROW(allocateWriterEpoch(
            epoch_backend, layout, "root/x", EpochMintPolicy::NormalMount, 0, observe), DB::Exception);
        EXPECT_FALSE(epoch_backend.head(layout.epochKey("root/x")).exists);
    }
}

TEST(CASServerRootSafety, OwnershipUsesAPathComponentBoundary)
{
    InMemoryBackend backend;
    const Layout layout("p");
    EXPECT_TRUE(serverRootSubtreeEmpty(
        backend, layout, "root/x", catalogOwning("root/xy/table", NsState::Live)));
    EXPECT_FALSE(serverRootSubtreeEmpty(
        backend, layout, "root/x", catalogOwning("root/x/table", NsState::Live)));
}

TEST(CASServerRootSafety, OpaqueStreamAndStateDebrisAloneDoesNotBlockRecreation)
{
    InMemoryBackend backend;
    const Layout layout("p");
    const NamespaceLifeId dead = NamespaceLifeId::fromCatalogEntry(RootNamespace{"unowned"}, UInt128{99});
    ASSERT_EQ(backend.putIfAbsent(layout.refLogKey(dead, RefTxnId{1, 1}), "debris").outcome, PutOutcome::Done);
    ASSERT_EQ(backend.putIfAbsent(layout.refCkptKey(dead), "debris").outcome, PutOutcome::Done);
    ASSERT_EQ(backend.putIfAbsent(layout.namespaceFileKey(dead, "f"), "debris").outcome, PutOutcome::Done);

    EXPECT_NO_THROW(claimOwnerOrThrow(backend, layout, "root/x", UInt128{1}, emptyCatalogObservation()));
    EXPECT_EQ(allocateWriterEpoch(
        backend, layout, "root/x", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), 1u);
}

TEST(CASServerRootSafety, ManifestAndLooseRootDebrisStillBlockRecreation)
{
    const Layout layout("p");
    for (const String & key : {
             layout.casManifestsServerPrefix("root/x") + "table/debris",
             layout.serverRootDataPrefix("root/x") + "loose"})
    {
        InMemoryBackend backend;
        ASSERT_EQ(backend.putIfAbsent(key, "x").outcome, PutOutcome::Done);
        EXPECT_THROW(claimOwnerOrThrow(
            backend, layout, "root/x", UInt128{1}, emptyCatalogObservation()), DB::Exception);
        EXPECT_THROW(allocateWriterEpoch(
            backend, layout, "root/x", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), DB::Exception);
    }
}

TEST(CASServerRootSafety, UnreadableCatalogNeverFallsBackToPhysicalGuesses)
{
    InMemoryBackend backend;
    const Layout layout("p");
    const ObserveRefCatalog unreadable = []() -> RefCatalog
    {
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "injected unreadable catalog");
    };
    EXPECT_THROW(claimOwnerOrThrow(backend, layout, "root/x", UInt128{1}, unreadable), DB::Exception);
    EXPECT_THROW(allocateWriterEpoch(
        backend, layout, "root/x", EpochMintPolicy::NormalMount, 0, unreadable), DB::Exception);
    EXPECT_FALSE(backend.head(layout.ownerKey("root/x")).exists);
    EXPECT_FALSE(backend.head(layout.epochKey("root/x")).exists);
}

TEST(CASServerRootSafety, OwnerConflictRecomputesTheWholeEmptinessBundle)
{
    OwnerConflictRevealsManifestBackend backend;
    const Layout layout("p");
    EXPECT_THROW(claimOwnerOrThrow(
        backend, layout, "root/x", UInt128{1}, emptyCatalogObservation()), DB::Exception);
    EXPECT_TRUE(backend.fired);
    EXPECT_FALSE(backend.head(layout.ownerKey("root/x")).exists);
}

TEST(CASServerRootSafety, EpochConflictRecomputesTheWholeEmptinessBundle)
{
    EpochConflictRevealsManifestBackend backend;
    const Layout layout("p");
    EXPECT_THROW(allocateWriterEpoch(
        backend, layout, "root/x", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), DB::Exception);
    EXPECT_TRUE(backend.fired);
    ASSERT_TRUE(backend.winner_installed);
    const auto epoch = backend.get(layout.epochKey("root/x"));
    ASSERT_TRUE(epoch.has_value());
    EXPECT_EQ(decodeServerEpoch(epoch->bytes).next_writer_epoch, 2u)
        << "the rejected allocator must not consume an epoch from the conflict winner";
}

TEST(CASMountLease, AbsentClaimThenRenewBumpsSeq)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    uint64_t now = 1000;
    auto r = claimMount(*b, l, "r", UInt128(1), /*epoch*/ 7, now, /*ttl*/ 100);
    EXPECT_EQ(r.kind, MountClaimResult::Claimed);
    MountLeaseKeeper k(b, l, "r", UInt128(1), 7, std::chrono::milliseconds(100), [&] { return now; },
                       [] { return uint64_t{0}; });
    k.start();
    EXPECT_EQ(decodeMountLease(b->get(l.mountKey("r"))->bytes).seq, 1u);
    k.renewOnce();
    EXPECT_EQ(decodeMountLease(b->get(l.mountKey("r"))->bytes).seq, 2u);
}

/// STID 3982-3b48: `rm -rf` of the pool dir under a live mount deletes the mount slot object out from
/// under a running keeper. The next background renewal must fail closed (stop renewing, latch the
/// write fence to lost) WITHOUT constructing a `LOGICAL_ERROR` -- that aborts debug/ASan builds at
/// exception construction, and there is no foreign writer here to fail closed against, only an
/// environmental condition.
TEST(CASMountLease, VanishedBackingStoreStopsRenewalWithoutLogicalError)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    uint64_t now = 1000;
    ASSERT_EQ(claimMount(*b, l, "r", UInt128(1), /*epoch*/ 7, now, /*ttl*/ 100).kind, MountClaimResult::Claimed);
    MountLeaseKeeper k(b, l, "r", UInt128(1), 7, std::chrono::milliseconds(100), [&] { return now; },
                       [] { return uint64_t{0}; });
    k.start();

    const String mount_key = l.mountKey("r");
    const auto lost_before = ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load();  /// NOLINT(clang-analyzer-deadcode.DeadStores)

    /// Simulate `rm -rf` of the backing store: the mount slot object is gone, but the keeper still
    /// holds a (now stale) token for it.
    ASSERT_EQ(b->deleteExact(mount_key, b->head(mount_key).token).kind, DeleteOutcome::Kind::Deleted);

    try
    {
        k.renewOnce();
        FAIL() << "renew against a vanished mount object must throw";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::FILE_DOESNT_EXIST) << e.message();
        EXPECT_NE(e.code(), DB::ErrorCodes::LOGICAL_ERROR);
    }
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load(), lost_before + 1);
}

/// STID 3982-3b48 (part 1b): the terminal/clean-release counterpart to the renewal fix above. When
/// the backing store vanishes (`rm -rf` of the pool dir), the renewal side already stops non-fatally
/// (see the previous test); teardown then runs the terminal release (`stop()` -> `terminate()`),
/// which used to unconditionally throw `LOGICAL_ERROR` once the token-guarded farewell PUT observed
/// an absent object. The desired end state of a release ("no live lease object") is already true, so
/// this must be a no-op, never a `LOGICAL_ERROR` (which aborts debug/ASan builds).
///
/// Driven WITHOUT a prior failed renew, so the count is deterministic: this is the only place along
/// this path that increments `CASMountLeaseLost`, so we expect exactly +1 (not +2, since renewal was
/// never invoked here).
TEST(CASMountLease, TerminateAfterVanishedBackingStoreIsNoOpRelease)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    uint64_t now = 1000;
    ASSERT_EQ(claimMount(*b, l, "r", UInt128(1), /*epoch*/ 7, now, /*ttl*/ 100).kind, MountClaimResult::Claimed);
    MountLeaseKeeper k(b, l, "r", UInt128(1), 7, std::chrono::milliseconds(100), [&] { return now; },
                       [] { return uint64_t{0}; });
    k.start();

    const String mount_key = l.mountKey("r");
    const auto lost_before = ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load();

    /// Simulate `rm -rf` of the backing store: the mount slot object is gone before we ever attempt
    /// a renewal, so `terminate()`'s token-guarded farewell PUT is the first thing to observe it.
    ASSERT_EQ(b->deleteExact(mount_key, b->head(mount_key).token).kind, DeleteOutcome::Kind::Deleted);

    EXPECT_NO_THROW(k.stop())
        << "clean release against a vanished store must be a no-op, not a LOGICAL_ERROR abort";
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load(), lost_before + 1);
}

/// rev.6: a bare `claimMount` (no `proven_dead_token`) NEVER reclaims a same-uuid, different-epoch
/// lease off a wall-clock-looking-expired stamp — only `claimMountAwaitingExpiry`'s observation loop
/// can turn that into a reclaim. Renamed from `...ExpiredReclaims` to describe the corrected behavior.
TEST(CASMountLease, SameUuidLiveFailsForeignFailsExpiredStillLiveDoubleStart)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    claimMount(*b, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 100);    // A live until 1100
    // same uuid, lease still live → double-start guard:
    EXPECT_EQ(claimMount(*b, l, "r", UInt128(1), 8, 1050, 100).kind, MountClaimResult::LiveDoubleStart);
    // foreign uuid, even after expiry → fail closed:
    EXPECT_EQ(claimMount(*b, l, "r", UInt128(2), 1, 1200, 100).kind, MountClaimResult::ForeignOwner);
    // same uuid, even after the stamp LOOKS expired on our wall clock → still LiveDoubleStart: no
    // proven_dead_token was supplied, so there is no certificate of death to reclaim on.
    EXPECT_EQ(claimMount(*b, l, "r", UInt128(1), 9, 1200, 100).kind, MountClaimResult::LiveDoubleStart);
}

TEST(CASMountMessage, DoubleStartTextHasIdentityAndRemediation)
{
    MountLease m;
    m.server_uuid = (UInt128(0xdeadbeefcafef00dULL) << 64) | UInt128(0x0011223344556677ULL);
    m.writer_epoch = 7;
    m.hostname = "host-9.example.com";
    m.pid = 4242;
    m.seq = 13;
    m.expires_at_ms = 1700000030000ULL;

    const std::string msg = mountDoubleStartMessage("replica-a", m);

    /// Identity / existing-holder fields.
    EXPECT_NE(msg.find("server_root_id"), std::string::npos);
    EXPECT_NE(msg.find("'replica-a'"), std::string::npos);
    EXPECT_NE(msg.find("hostname=host-9.example.com"), std::string::npos);
    EXPECT_NE(msg.find("pid=4242"), std::string::npos);
    EXPECT_NE(msg.find("last_seq=13"), std::string::npos);
    EXPECT_NE(msg.find("expires_at_ms=1700000030000"), std::string::npos);
    /// New wait-aware remediation (this server already waited; the lease kept being renewed).
    EXPECT_NE(msg.find("waited"), std::string::npos);
    EXPECT_NE(msg.find("unique"), std::string::npos);
    EXPECT_NE(msg.find("reclaim the mount on restart"), std::string::npos);
    EXPECT_NE(msg.find("uuid file"), std::string::npos);
    /// Clock-skew caveat + manual mount-object delete escape hatch.
    EXPECT_NE(msg.find("CLOCK SKEW"), std::string::npos);
    EXPECT_NE(msg.find("NTP"), std::string::npos);
    EXPECT_NE(msg.find("manually delete the mount"), std::string::npos);
    EXPECT_NE(msg.find("gc/server-roots/replica-a/mount"), std::string::npos);
}

/// rev.6: a stamped `expires_at_ms` that already looks past-due on our wall clock must NOT shortcut
/// the observation wait — the old "instant, zero-sleep" reclaim this test name described was exactly
/// the cross-node wall-clock trust rev.6 removes. Renamed to describe the CORRECTED behavior: the
/// wall-clock-looking-expired stamp buys nothing, the full threshold is still observed.
TEST(CASMountAwaitExpiry, PastExpiryStillPaysTheFullObservationThreshold)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    /// A prior incarnation (uuid=1, epoch=7) claimed a lease live until 1100.
    ASSERT_EQ(claimMount(*b, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 100).kind, MountClaimResult::Claimed);

    uint64_t wall = 1200;                // already past 1100 on wall clock — irrelevant to the decision
    uint64_t mono = 0;
    int sleeps = 0;
    auto now_fn = [&] { return wall; };
    auto mono_fn = [&] { return mono; };
    auto sleep_fn = [&](uint64_t ms) { wall += ms; mono += ms; ++sleeps; };

    const auto r = claimMountAwaitingExpiry(
        *b, l, "r", UInt128(1), /*our_epoch*/ 8, now_fn, mono_fn, /*ttl*/ 100, /*poll*/ 25, sleep_fn);
    EXPECT_EQ(r.kind, MountClaimResult::Claimed);
    EXPECT_GT(sleeps, 0);                                    // NOT instant — no wall-clock trust
    EXPECT_GE(mono, 100 + 100 / 20 + 25);                     // full observation threshold paid
    EXPECT_EQ(decodeMountLease(b->get(l.mountKey("r"))->bytes).writer_epoch, 8u);   // reclaimed as us
}

TEST(CASMountAwaitExpiry, FutureExpiryReclaimsAfterClockAdvances)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    ASSERT_EQ(claimMount(*b, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 100).kind, MountClaimResult::Claimed);

    uint64_t wall = 1000;                // lease looks live until 1100, holder does NOT renew
    uint64_t mono = 0;
    auto now_fn = [&] { return wall; };
    auto mono_fn = [&] { return mono; };
    auto sleep_fn = [&](uint64_t ms) { wall += ms; mono += ms; };

    const auto r = claimMountAwaitingExpiry(
        *b, l, "r", UInt128(1), /*our_epoch*/ 8, now_fn, mono_fn, /*ttl*/ 100, /*poll*/ 50, sleep_fn);
    EXPECT_EQ(r.kind, MountClaimResult::Claimed);
    const auto body = decodeMountLease(b->get(l.mountKey("r"))->bytes);
    EXPECT_EQ(body.writer_epoch, 8u);
    EXPECT_EQ(body.seq, 2u);                                         // reclaim continues seq (prev 1 + 1)
}

/// rev.6: a genuinely live twin now times out via BOUNDED OBSERVATION RESTARTS (its every renewal
/// bumps the write-token, forcing a restart each poll), never via a wall-clock deadline.
TEST(CASMountAwaitExpiry, LiveRenewingTwinTimesOutAsDoubleStart)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    ASSERT_EQ(claimMount(*b, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 100).kind, MountClaimResult::Claimed);

    uint64_t wall = 1000;
    uint64_t mono = 0;
    auto now_fn = [&] { return wall; };
    auto mono_fn = [&] { return mono; };
    /// Each poll: both clocks advance AND the live holder (uuid=1, epoch=7) renews its own lease —
    /// the observed write-token changes on EVERY poll, forcing a restart every time.
    auto sleep_fn = [&](uint64_t ms)
    {
        wall += ms;
        mono += ms;
        ASSERT_EQ(claimMount(*b, l, "r", UInt128(1), 7, wall, 100).kind, MountClaimResult::Claimed);
    };

    const auto r = claimMountAwaitingExpiry(
        *b, l, "r", UInt128(1), /*our_epoch*/ 8, now_fn, mono_fn, /*ttl*/ 100, /*poll*/ 20, sleep_fn);
    EXPECT_EQ(r.kind, MountClaimResult::LiveDoubleStart);
    EXPECT_EQ(decodeMountLease(b->get(l.mountKey("r"))->bytes).writer_epoch, 7u);   // still the holder's
}

namespace
{
/// fix-round F5 harness: makes the mount key vanish to EVERY `get()`, unconditionally, while the real
/// underlying object stays put -- forcing `claimMount`'s own internal GET to take the absent-slot race
/// branch every call (its `putIfAbsent` then fails against the real, still-present object, returning
/// `LiveDoubleStart` with no token -- fix-round F8 leaves `.token` unset on exactly this branch, since
/// no re-read was done). That in turn forces `claimMountAwaitingExpiry`'s F8 fallback re-GET, which
/// ALSO sees the slot as vanished -- deterministically reproducing "the slot vanished between
/// claimMount's own GET and ours" on EVERY loop iteration, not just a lucky one-shot race.
class AlwaysVanishesBackend final : public DB::Cas::Backend
{
public:
    explicit AlwaysVanishesBackend(std::shared_ptr<DB::Cas::Backend> inner_) : inner(std::move(inner_)) {}
    String watched_key;

    std::optional<DB::Cas::GetResult> get(const String & k, DB::Cas::Range r) override
    {
        if (k == watched_key)
            return std::nullopt;
        return inner->get(k, r);
    }
    std::optional<DB::Cas::GetStreamResult> getStream(const String & k, DB::Cas::Range r) override { return inner->getStream(k, r); }
    DB::Cas::HeadResult head(const String & k) override { return inner->head(k); }
    DB::Cas::ListPage list(const String & p, const String & c, size_t l) override { return inner->list(p, c, l); }
    DB::Cas::PutResult putIfAbsent(const String & k, const String & b, const DB::Cas::ObjectMeta & m) override { return inner->putIfAbsent(k, b, m); }
    DB::Cas::WriteSinkPtr putIfAbsentStream(const String & k, const DB::Cas::ObjectMeta & m) override { return inner->putIfAbsentStream(k, m); }
    DB::Cas::PutResult putOverwrite(const String & k, const String & b, const DB::Cas::Token & e, const DB::Cas::ObjectMeta & m) override { return inner->putOverwrite(k, b, e, m); }
    DB::Cas::CasResult casPut(const String & k, const String & b, const std::optional<DB::Cas::Token> & e, const DB::Cas::ObjectMeta & m) override { return inner->casPut(k, b, e, m); }
    DB::Cas::DeleteOutcome deleteExact(const String & k, const DB::Cas::Token & t) override { return inner->deleteExact(k, t); }
    bool supportsListTokens() const override { return inner->supportsListTokens(); }

private:
    std::shared_ptr<DB::Cas::Backend> inner;
};
}

/// fix-round F5 (author-review: `!got -> continue` in the observation loop, with no sleep and outside
/// the restart limit, spins `get`/`claimMount`/`put` at backend RTT under persistent slot churn). A
/// backend that makes the mount slot look vanished to every GET must still terminate (bounded restarts,
/// not an infinite loop) AND must pace itself (the injected `sleep_fn` must actually fire) rather than
/// busy-spin.
TEST(CASMountAwaitExpiry, PersistentSlotVanishPacesAndBoundsRestartsInsteadOfSpinning)
{
    auto inner = std::make_shared<InMemoryBackend>();
    Layout l("p");
    /// A real slot exists underneath (uuid 1, epoch 7) so `claimMount`'s absent-slot `putIfAbsent`
    /// genuinely fails every time (never accidentally re-mints).
    ASSERT_EQ(claimMount(*inner, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 100).kind, MountClaimResult::Claimed);

    auto vanishing = std::make_shared<AlwaysVanishesBackend>(inner);
    vanishing->watched_key = l.mountKey("r");

    uint64_t wall = 1000;
    uint64_t mono = 0;
    int sleeps = 0;
    auto now_fn = [&] { return wall; };
    auto mono_fn = [&] { return mono; };
    auto sleep_fn = [&](uint64_t ms) { wall += ms; mono += ms; ++sleeps; };

    const auto r = claimMountAwaitingExpiry(
        *vanishing, l, "r", UInt128(1), /*our_epoch*/ 8, now_fn, mono_fn, /*ttl*/ 100, /*poll*/ 20, sleep_fn);
    EXPECT_EQ(r.kind, MountClaimResult::LiveDoubleStart) << "must terminate (bounded), not loop forever";
    EXPECT_GT(sleeps, 0) << "a persistently vanishing slot must still pace via sleep_fn, not busy-spin";
    /// The real epoch-7 lease is untouched -- every `putIfAbsent` attempt against it genuinely fails
    /// (the object is still there), so it is never accidentally re-minted over.
    EXPECT_EQ(decodeMountLease(inner->get(l.mountKey("r"))->bytes).writer_epoch, 7u);
}

TEST(CASMountAwaitExpiry, ForeignUuidFailsClosedImmediately)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    /// A foreign server (uuid=2) holds the mount.
    ASSERT_EQ(claimMount(*b, l, "r", UInt128(2), 1, /*now*/ 1000, /*ttl*/ 100).kind, MountClaimResult::Claimed);

    uint64_t now = 1000;
    int sleeps = 0;
    auto now_fn = [&] { return now; };
    auto mono_fn = [&] { return uint64_t{0}; };
    auto sleep_fn = [&](uint64_t ms) { now += ms; ++sleeps; };

    const auto r = claimMountAwaitingExpiry(
        *b, l, "r", UInt128(1), /*our_epoch*/ 8, now_fn, mono_fn, /*ttl*/ 100, /*poll*/ 25, sleep_fn);
    EXPECT_EQ(r.kind, MountClaimResult::ForeignOwner);
    EXPECT_EQ(sleeps, 0);                                            // never waits across UUIDs
}

/// rev.6: the predecessor's own stamped `expires_at_ms` (however skewed) is NEVER consulted for the
/// reclaim decision any more — the wait is bounded purely by OUR OWN `ttl_ms`-derived threshold. A
/// prior incarnation minted with an absurdly large `ttl` (so its own stamp claims aliveness for
/// ~100000ms) still reclaims within the SAME small threshold as any other case, because that stamp is
/// never read for timing.
TEST(CASMountAwaitExpiry, SkewedFarFutureExpiryHasNoEffectOnObservationThreshold)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    ASSERT_EQ(claimMount(*b, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 100000).kind, MountClaimResult::Claimed);

    uint64_t wall = 1000;
    uint64_t mono = 0;
    auto now_fn = [&] { return wall; };
    auto mono_fn = [&] { return mono; };
    auto sleep_fn = [&](uint64_t ms) { wall += ms; mono += ms; };

    const auto r = claimMountAwaitingExpiry(
        *b, l, "r", UInt128(1), /*our_epoch*/ 8, now_fn, mono_fn, /*ttl*/ 100, /*poll*/ 20, sleep_fn);
    EXPECT_EQ(r.kind, MountClaimResult::Claimed);
    EXPECT_LE(mono, 100u + 100u / 20 + 20u + 20u);      // bounded by OUR threshold, not the predecessor's stamp
    EXPECT_EQ(decodeMountLease(b->get(l.mountKey("r"))->bytes).writer_epoch, 8u);   // reclaimed
}

TEST(CASMountLease, KeeperStartAdoptsOurOwnClaimNotDoubleStart)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    uint64_t now = 1000;
    // The normal flow: claimMount writes the live mount under (uuid=1, epoch=7), THEN keeper.start().
    ASSERT_EQ(claimMount(*b, l, "r", UInt128(1), /*epoch*/ 7, now, /*ttl*/ 100).kind, MountClaimResult::Claimed);
    MountLeaseKeeper k(b, l, "r", UInt128(1), /*epoch*/ 7, std::chrono::milliseconds(100), [&] { return now; },
                       [] { return uint64_t{0}; });
    EXPECT_NO_THROW(k.start());     // adopts our own live (uuid=1,epoch=7) mount — NOT a double-start
    EXPECT_EQ(decodeMountLease(b->get(l.mountKey("r"))->bytes).writer_epoch, 7u);

    // A keeper for the SAME uuid but a DIFFERENT live epoch must fail closed (superseded/double-start):
    MountLeaseKeeper k2(b, l, "r", UInt128(1), /*epoch*/ 8, std::chrono::milliseconds(100), [&] { return now; },
                        [] { return uint64_t{0}; });
    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            k2.start();
        },
        "held by a different writer_epoch");
}

TEST(CASMountFence, SupersededWriterRefusedNoS3Read)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "r"});

    /// Permissive default: a Pool that has NOT armed the fence allows mutations.
    EXPECT_TRUE(store->mayMutate());

    /// Latching loss: once the renewer trips the fence it stays lost (purely local — no S3 read).
    store->tripMountLost();
    EXPECT_FALSE(store->mayMutate());

    /// A real mutate entrypoint that funnels through mutateShard now fails closed at the gate, BEFORE
    /// the mutate lambda runs (so this is the ABORTED gate throw, not a FILE_DOESNT_EXIST from inside).
    const RootNamespace ns{"srv1/tbl"};
    EXPECT_THROW(store->dropRef(ns, "any_ref"), DB::Exception);
}

TEST(CASMountStartup, SecondServerSameRootFailsClosed)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s1 = Pool::open(b, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r"});
    /// A second server (different uuid) on the SAME server_root_id + same backend → fail closed
    /// (the owner gate rejects the foreign uuid before any mount/epoch mutation).
    EXPECT_THROW(
        Pool::open(b, PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(2), .server_root_id = "r"}),
        DB::Exception);
}

TEST(CASMountStartup, WriterEpochStrictlyIncreasesAcrossReopen)
{
    auto b = std::make_shared<InMemoryBackend>();
    auto s1 = Pool::open(b, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r"});
    const uint64_t e1 = s1->writerEpoch();

    /// Simulate shutdown: the Pool dtor stops the keeper, whose terminate() retires the lease
    /// (stamps it already-expired). The owner + the durable epoch object stay sticky.
    s1.reset();

    /// Same server reopen → reclaims the (now-expired, different-epoch) mount and allocates a strictly
    /// higher durable writer_epoch.
    auto s2 = Pool::open(b, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r"});
    const uint64_t e2 = s2->writerEpoch();
    EXPECT_GT(e2, e1);
}

TEST(CASMountStartup, FreshWritablePoolBootstrapsAnExplicitEmptyCatalog)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");

    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r",
        .skip_access_check = true});

    const auto catalog = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(catalog.has_value());
    EXPECT_TRUE(decodeRefCatalog(catalog->bytes).entries.empty());
}

TEST(CASMountStartup, ExistingPoolWithoutCatalogFailsBeforeSlotMutation)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");
    {
        auto store = Pool::open(backend, PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r",
            .skip_access_check = true});
    }

    /// Old raw fixtures did not persist an empty catalog. Make this an explicit existing-pool
    /// fixture before removing the mandatory object whose loss the mount must reject.
    if (!backend->head(layout.refCatalogKey()).exists)
        ASSERT_EQ(backend->putIfAbsent(layout.refCatalogKey(), encodeRefCatalog(RefCatalog{})).outcome,
                  PutOutcome::Done);
    const HeadResult catalog_head = backend->head(layout.refCatalogKey());
    ASSERT_TRUE(catalog_head.exists);
    ASSERT_EQ(backend->deleteExact(layout.refCatalogKey(), catalog_head.token).kind,
              DeleteOutcome::Kind::Deleted);

    const auto owner_before = backend->get(layout.ownerKey("r"));
    const auto epoch_before = backend->get(layout.epochKey("r"));
    const auto mount_before = backend->get(layout.mountKey("r"));
    ASSERT_TRUE(owner_before.has_value());
    ASSERT_TRUE(epoch_before.has_value());
    ASSERT_TRUE(mount_before.has_value());

    EXPECT_THROW(Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r",
        .skip_access_check = true}), DB::Exception);

    const auto owner_after = backend->get(layout.ownerKey("r"));
    const auto epoch_after = backend->get(layout.epochKey("r"));
    const auto mount_after = backend->get(layout.mountKey("r"));
    ASSERT_TRUE(owner_after.has_value());
    ASSERT_TRUE(epoch_after.has_value());
    ASSERT_TRUE(mount_after.has_value());
    EXPECT_EQ(owner_after->bytes, owner_before->bytes);
    EXPECT_EQ(owner_after->token, owner_before->token);
    EXPECT_EQ(epoch_after->bytes, epoch_before->bytes);
    EXPECT_EQ(epoch_after->token, epoch_before->token);
    EXPECT_EQ(mount_after->bytes, mount_before->bytes);
    EXPECT_EQ(mount_after->token, mount_before->token);
}

TEST(CASMountReadOnly, ForeignOwnedPoolOpensWithoutMutation)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");

    /// Server A claims the pool (writable): owner = uuid(1), a durable epoch + a live mount lease.
    auto a = Pool::open(b, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r"});

    /// Capture the control objects BEFORE the read-only open so we can prove it mutated nothing.
    const auto owner_before = b->get(l.ownerKey("r"));
    const auto mount_before = b->get(l.mountKey("r"));
    const auto epoch_before = b->get(l.epochKey("r"));
    ASSERT_TRUE(owner_before.has_value());
    ASSERT_TRUE(mount_before.has_value());
    ASSERT_TRUE(epoch_before.has_value());

    /// A READ-ONLY observer with a DIFFERENT server_id on the SAME backend/server_root_id must NOT
    /// throw — a read-only mount never participates in the owner/epoch/mount protocol, so a pool
    /// owned by another server_uuid is freely observable.
    PoolPtr ro;
    EXPECT_NO_THROW(
        ro = Pool::open(b, PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(2), .server_root_id = "r",
            .read_only = true}));
    EXPECT_NE(ro, nullptr);

    /// And it mutated nothing: owner still decodes to A's uuid, the mount body is still A's, and the
    /// raw bytes of owner/epoch/mount are byte-for-byte unchanged (no second owner, no re-claim).
    const auto owner_after = b->get(l.ownerKey("r"));
    const auto mount_after = b->get(l.mountKey("r"));
    const auto epoch_after = b->get(l.epochKey("r"));
    ASSERT_TRUE(owner_after.has_value());
    ASSERT_TRUE(mount_after.has_value());
    ASSERT_TRUE(epoch_after.has_value());

    EXPECT_EQ(decodeOwner(owner_after->bytes).server_uuid, UInt128(1));
    EXPECT_EQ(decodeMountLease(mount_after->bytes).server_uuid, UInt128(1));

    EXPECT_EQ(owner_after->bytes, owner_before->bytes);
    EXPECT_EQ(mount_after->bytes, mount_before->bytes);
    EXPECT_EQ(epoch_after->bytes, epoch_before->bytes);
}

/// Pool::open must call validateCasRequestBudget itself (not just the free function in isolation —
/// see gtest_cas_request_control.cpp for that): an inconsistent cas_request_budget must refuse a
/// writable mount end-to-end (RFC cas-s3-timeout-retry-control §required-timeout-model), never mount
/// silently with a budget that could let a controlled attempt outlive the lease it is fenced under.
TEST(CASMountStartup, RefusesWritableOpenWithInconsistentCasRequestBudget)
{
    auto b = std::make_shared<InMemoryBackend>();

    /// attempt_timeout_ms + lease_safety_margin_ms == mount_lease_ttl_ms below (30000): not STRICTLY
    /// less, so this must be rejected.
    const CasRequestBudget bad_budget{
        .attempt_timeout_ms = 25000, .operation_deadline_ms = 30000, .max_attempts = 3, .lease_safety_margin_ms = 5000};
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&]
    {
        Pool::open(b, PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r",
            .mount_lease_ttl_ms = std::chrono::milliseconds(30000),
            .cas_request_budget = bad_budget});
    });
}

TEST(CASMountStartup, StaleSelfMountReclaimedAfterWait)
{
    auto b = std::make_shared<InMemoryBackend>();

    /// Server A opens writable with a SHORT lease TTL and no background renewer (`background_watermark`
    /// defaults false). The test captures its live mount body, destroys the real Pool cleanly, then
    /// replays that body to simulate a crashed process whose lease survives but is never renewed.
    /// This test's short lease TTL is far below the CasRequestBudget defaults (RFC
    /// cas-s3-timeout-retry-control §required-timeout-model requires attempt_timeout + safety_margin <
    /// lease TTL), so it also scales down cas_request_budget to fit — the budget itself is not
    /// exercised here, only Pool::open's validateCasRequestBudget startup gate.
    const CasRequestBudget tiny_budget{
        .attempt_timeout_ms = 50, .operation_deadline_ms = 500, .max_attempts = 1, .lease_safety_margin_ms = 50};
    auto a = Pool::open(b, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r",
        .mount_lease_ttl_ms = std::chrono::milliseconds(300),
        .mount_renew_period = std::chrono::milliseconds(100),
        .cas_request_budget = tiny_budget});
    ASSERT_NE(a, nullptr);
    const uint64_t e1 = a->writerEpoch();
    const String mount_key = a->layout().mountKey("r");
    const auto stale_mount = b->get(mount_key);
    ASSERT_TRUE(stale_mount.has_value());

    /// Preserve A's live lease as if its process disappeared without running C++ teardown. Destroying
    /// the real Pool first keeps the parent process valid; replaying the saved body recreates the exact
    /// durable stale-lease state that a crashed process would leave behind.
    a.reset();
    const auto farewell = b->get(mount_key);
    ASSERT_TRUE(farewell.has_value());
    ASSERT_EQ(b->putOverwrite(mount_key, stale_mount->bytes, farewell->token).outcome, PutOutcome::Done);

    /// A restart of the SAME server (same uuid) must NOT abort: it waits out the stale lease (<= ~300ms)
    /// and reclaims the mount, coming up with a strictly higher durable writer_epoch. The replayed live
    /// body hides A's clean farewell, so the reclaim is `MountPriorState::UncleanObserved`. Inject a
    /// fake `boot_ms_fn` + `wait_sleep_fn` (mirroring
    /// `CASMountOpenWaits.UncleanOpenPaysOnlyTheObservationWindow`) so the observation window resolves
    /// instantly instead of blocking this test on real time.
    uint64_t a2_fake_boot = 0;
    PoolPtr a2;
    EXPECT_NO_THROW(
        a2 = Pool::open(b, PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r",
            .mount_lease_ttl_ms = std::chrono::milliseconds(300),
            .mount_renew_period = std::chrono::milliseconds(100),
            .cas_request_budget = tiny_budget,
            .boot_ms_fn = [&a2_fake_boot] { return a2_fake_boot; },
            .wait_sleep_fn = [&a2_fake_boot](uint64_t ms) { a2_fake_boot += ms; }}));
    ASSERT_NE(a2, nullptr);
    EXPECT_GT(a2->writerEpoch(), e1);

    /// The original live-object overlap: a first Pool is still alive when a replacement reclaims its
    /// slot, so the first one's release meets a stranger. This was an `EXPECT_DEATH` pinning a
    /// `LOGICAL_ERROR` abort — which fires from `~Pool`, defeating `finishTeardown`'s own catch by
    /// aborting at exception construction. The first Pool never observed a deposition (nothing failed
    /// its renewal; the slot was reclaimed underneath it), so this is the exclusivity-violation arm:
    /// refuse, leave the reclaimer's slot untouched, latch the fence, and SURVIVE.
    auto overlap_backend = std::make_shared<InMemoryBackend>();
    auto first = Pool::open(overlap_backend, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r",
        .mount_lease_ttl_ms = std::chrono::milliseconds(300),
        .mount_renew_period = std::chrono::milliseconds(100),
        .cas_request_budget = tiny_budget});
    const String overlap_mount_key = first->layout().mountKey("r");

    uint64_t overlap_fake_boot = 0;
    auto replacement = Pool::open(overlap_backend, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r",
        .mount_lease_ttl_ms = std::chrono::milliseconds(300),
        .mount_renew_period = std::chrono::milliseconds(100),
        .cas_request_budget = tiny_budget,
        .boot_ms_fn = [&overlap_fake_boot] { return overlap_fake_boot; },
        .wait_sleep_fn = [&overlap_fake_boot](uint64_t ms) { overlap_fake_boot += ms; }});
    ASSERT_NE(replacement, nullptr);

    const auto reclaimer_slot_before = overlap_backend->get(overlap_mount_key);
    ASSERT_TRUE(reclaimer_slot_before.has_value());
    const uint64_t overlap_violations_before
        = ProfileEvents::global_counters[ProfileEvents::CASMountExclusivityViolation].load();

    first.reset();   /// must not abort, must not terminate

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountExclusivityViolation].load(),
              overlap_violations_before + 1);
    const auto reclaimer_slot_after = overlap_backend->get(overlap_mount_key);
    ASSERT_TRUE(reclaimer_slot_after.has_value());
    EXPECT_EQ(reclaimer_slot_after->bytes, reclaimer_slot_before->bytes)
        << "the deposed Pool's release must not retire the reclaimer's lease";
    EXPECT_TRUE(replacement->mayMutate()) << "and must not disturb the live reclaimer";
}

TEST(CASMountLease, BodyCarriesFloorAndFence)
{
    MountLease m;
    m.server_uuid = UInt128(0xAB);
    m.writer_epoch = 7;
    m.hostname = "h";
    m.pid = 42;
    m.started_at_ms = 1000;
    m.seq = 3;
    m.expires_at_ms = 2000;
    m.min_active = 5;
    m.gc_fenced = true;
    const MountLease d = decodeMountLease(encodeMountLease(m));
    EXPECT_EQ(d.min_active, 5u);
    EXPECT_TRUE(d.gc_fenced);
    EXPECT_EQ(d.writer_epoch, 7u);
}

TEST(CASMountLease, RetiredSentinelRoundTrips)
{
    MountLease m;
    m.min_active = std::numeric_limits<uint64_t>::max();
    EXPECT_EQ(decodeMountLease(encodeMountLease(m)).min_active,
              std::numeric_limits<uint64_t>::max());
}

/// ---- Task 7 / Task 9: GC heartbeat classification with token-guarded, observation-based fence-out ----

namespace
{
/// A fixed, fake "now" — no real clocks in these tests. Lease timestamps are chosen relative to it.
/// Rev.6 §token-stability observation removed the wall clock from the fence DECISION; `kNowMs` below
/// is threaded through only as `computeHeartbeatFloor`'s audit-only `now_ms`.
constexpr uint64_t kNowMs = 1'000'000;
/// The fence-out threshold measured on the LEADER's OWN monotonic clock (`mono_now_ms`), independent
/// of any lease's stamped `expires_at_ms`.
constexpr uint64_t kStableThresholdMs = 10'000;

/// Seed one mount body under mountKey(srid) via the on-storage codec (`encodeMountLease` +
/// `putIfAbsent`) — the same interface the keeper writes through.
MountLease seedMount(
    Backend & b, const Layout & l, const String & srid,
    uint64_t expires_at_ms, bool gc_fenced, uint64_t min_active, uint64_t seq = 1)
{
    MountLease m;
    m.server_uuid = UInt128(srid.back());   // distinct per srid; content is irrelevant to the gate
    m.writer_epoch = 1;
    m.hostname = "h-" + srid;
    m.pid = 100;
    m.started_at_ms = kNowMs;
    m.seq = seq;
    m.expires_at_ms = expires_at_ms;
    m.min_active = min_active;
    m.gc_fenced = gc_fenced;
    b.putIfAbsent(l.mountKey(srid), encodeMountLease(m));
    return m;
}

/// Simulate a keeper's real renewal between two `computeHeartbeatFloor` calls: a token-guarded
/// overwrite that bumps `seq` (and so mints a fresh backend token), leaving everything else as-is.
/// Models the one thing the observation-based fence cares about: the write token changed, so any
/// in-progress observation of the OLD token must restart.
void renewMount(Backend & b, const Layout & l, const String & srid)
{
    const auto got = b.get(l.mountKey(srid));
    ASSERT_TRUE(got.has_value());
    MountLease m = decodeMountLease(got->bytes);
    m.seq += 1;
    const PutResult res = b.putOverwrite(l.mountKey(srid), encodeMountLease(m), got->token);
    ASSERT_EQ(res.outcome, PutOutcome::Done);
}
}

TEST(CASHeartbeatFloor, FirstSightNeverFencesEvenIfStampLooksExpired)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");

    /// A stamp that would have read as long-expired under the old skew-margin comparison — under
    /// rev.6 observation the stamp is never even consulted for the fence decision.
    seedMount(*b, l, "s1", /*expires*/ 10, /*fenced*/ false, /*min_active*/ 0);

    MountObservationMap obs;
    const HeartbeatFloor floor = computeHeartbeatFloor(*b, l, /*now_ms*/ kNowMs, /*mono_now_ms*/ 0,
                                                         kStableThresholdMs, obs);

    EXPECT_EQ(floor.fenced_now, 0u);
    EXPECT_EQ(floor.live, 1u);
    ASSERT_TRUE(obs.contains("s1"));
    EXPECT_EQ(obs.at("s1").first_seen_mono_ms, 0u);
}

TEST(CASHeartbeatFloor, StableTokenPastThresholdIsFenced)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    seedMount(*b, l, "s1", /*expires*/ 10, /*fenced*/ false, /*min_active*/ 0);

    MountObservationMap obs;
    const HeartbeatFloor floor_before = computeHeartbeatFloor(*b, l, kNowMs, /*mono*/ 0, kStableThresholdMs, obs);
    EXPECT_EQ(floor_before.fenced_now, 0u);

    const MountLease before = decodeMountLease(b->get(l.mountKey("s1"))->bytes);

    /// No renewal in between: the SAME token, observed since mono 0, is now stable for the full
    /// threshold on the leader's own clock.
    const HeartbeatFloor floor2 = computeHeartbeatFloor(*b, l, kNowMs, /*mono*/ kStableThresholdMs,
                                                          kStableThresholdMs, obs);

    EXPECT_EQ(floor2.fenced_now, 1u);
    EXPECT_EQ(floor2.fenced_srids, std::vector<String>{"s1"});
    const MountLease fenced = decodeMountLease(b->get(l.mountKey("s1"))->bytes);
    EXPECT_TRUE(fenced.gc_fenced);
    EXPECT_EQ(fenced.seq, before.seq + 1);
}

TEST(CASHeartbeatFloor, RenewalBetweenRoundsRestartsObservation)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    seedMount(*b, l, "s1", /*expires*/ 10, /*fenced*/ false, /*min_active*/ 0);

    MountObservationMap obs;
    computeHeartbeatFloor(*b, l, kNowMs, /*mono*/ 0, kStableThresholdMs, obs);
    ASSERT_TRUE(obs.contains("s1"));
    const Token first_token = obs.at("s1").token;

    renewMount(*b, l, "s1");
    const Token renewed_token = b->get(l.mountKey("s1"))->token;
    EXPECT_NE(renewed_token, first_token);

    const HeartbeatFloor floor2 = computeHeartbeatFloor(*b, l, kNowMs, /*mono*/ kStableThresholdMs,
                                                          kStableThresholdMs, obs);

    EXPECT_EQ(floor2.fenced_now, 0u);
    ASSERT_TRUE(obs.contains("s1"));
    EXPECT_EQ(obs.at("s1").token, renewed_token);
    EXPECT_EQ(obs.at("s1").first_seen_mono_ms, kStableThresholdMs);
}

/// fix-round F7 (author-review: `Gc::mount_obs` not pruned for srids gone from LIST -> slow unbounded
/// growth on a long-lived leader, worsened by pool-member decommission). A srid whose `/mount` key is
/// removed ENTIRELY (not merely fenced/terminated -- those already `obs.erase` themselves mid-loop) is
/// never visited by a later LIST pass again, so its observation entry must be pruned at end-of-round,
/// not linger in `obs` forever.
TEST(CASHeartbeatFloor, UnseenSridPrunedFromObservationMap)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    seedMount(*b, l, "s1", /*expires*/ 10, /*fenced*/ false, /*min_active*/ 0);
    seedMount(*b, l, "s2", /*expires*/ 10, /*fenced*/ false, /*min_active*/ 0);

    MountObservationMap obs;
    computeHeartbeatFloor(*b, l, kNowMs, /*mono*/ 0, kStableThresholdMs, obs);
    ASSERT_TRUE(obs.contains("s1"));
    ASSERT_TRUE(obs.contains("s2"));

    /// s2's `/mount` key is removed entirely -- e.g. `SYSTEM CAS DROP POOL MEMBER` -- so
    /// no future LIST pass will ever visit it again. s1 renews (a live keeper would), so its OWN
    /// observation restarts and it stays `live` -- isolating this test to the pruning behavior alone,
    /// not confounding it with s1 also becoming fence-eligible (which would erase its `obs` entry too,
    /// for an unrelated reason).
    renewMount(*b, l, "s1");
    const auto s2_key = l.mountKey("s2");
    const auto got = b->get(s2_key);
    ASSERT_TRUE(got.has_value());
    ASSERT_EQ(b->deleteExact(s2_key, got->token).kind, DeleteOutcome::Kind::Deleted);

    computeHeartbeatFloor(*b, l, kNowMs, /*mono*/ kStableThresholdMs, kStableThresholdMs, obs);
    EXPECT_TRUE(obs.contains("s1"));
    EXPECT_FALSE(obs.contains("s2"))
        << "a srid removed from the LIST entirely must be pruned from obs, not linger forever";
}

TEST(CASHeartbeatFloor, ClassifiesAndFencesOut)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");

    /// two live mounts — genuinely renewing between the two rounds below, so their observation never
    /// stabilizes.
    seedMount(*b, l, "s1", /*expires*/ kNowMs + 60'000, /*fenced*/ false, /*min_active*/ 0);
    seedMount(*b, l, "s2", /*expires*/ kNowMs + 60'000, /*fenced*/ false, /*min_active*/ 0);
    /// dead — no renewal between the two rounds below — must be fenced-out by the second call.
    seedMount(*b, l, "s3", /*expires*/ kNowMs - 60'000, /*fenced*/ false, /*min_active*/ 0);
    /// already-fenced — excluded, body byte-identical after both calls (no PUT).
    seedMount(*b, l, "s4", /*expires*/ kNowMs - 60'000, /*fenced*/ true, /*min_active*/ 0);
    /// terminated (min_active == UINT64_MAX) with expired-looking timestamps — excluded, not fenced.
    seedMount(*b, l, "s5", /*expires*/ kNowMs - 60'000, /*fenced*/ false,
              /*min_active*/ std::numeric_limits<uint64_t>::max());

    MountObservationMap obs;

    /// Round 1 (mono 0): first sight of every non-terminal mount — nothing is fence-eligible yet.
    const HeartbeatFloor floor_before = computeHeartbeatFloor(*b, l, kNowMs, /*mono*/ 0, kStableThresholdMs, obs);
    EXPECT_EQ(floor_before.live, 3u);            // s1, s2, s3: observation just started
    EXPECT_EQ(floor_before.terminated, 1u);      // s5
    EXPECT_EQ(floor_before.fenced_now, 0u);
    EXPECT_EQ(floor_before.already_fenced, 1u);  // s4

    /// s1 and s2 renew between rounds (as a live keeper would); s3 does not (it crashed).
    renewMount(*b, l, "s1");
    renewMount(*b, l, "s2");

    const auto s3_before = b->get(l.mountKey("s3"));
    const auto s4_before = b->get(l.mountKey("s4"));
    ASSERT_TRUE(s3_before.has_value());
    ASSERT_TRUE(s4_before.has_value());

    /// Round 2 (mono == threshold): s1/s2's renewed tokens restart their observation (still live);
    /// s3's original token has now held stable for the full threshold -> fenced.
    const HeartbeatFloor floor2 = computeHeartbeatFloor(*b, l, kNowMs, /*mono*/ kStableThresholdMs,
                                                          kStableThresholdMs, obs);

    EXPECT_EQ(floor2.live, 2u);            // s1, s2: renewed, observation restarted
    EXPECT_EQ(floor2.terminated, 1u);      // s5
    EXPECT_EQ(floor2.fenced_now, 1u);      // s3
    EXPECT_EQ(floor2.already_fenced, 1u);  // s4

    /// The dead body was fenced: gc_fenced set, seq bumped, the rest of the body preserved.
    const auto s3_after = b->get(l.mountKey("s3"));
    ASSERT_TRUE(s3_after.has_value());
    const MountLease s3_prev = decodeMountLease(s3_before->bytes);
    const MountLease s3_now = decodeMountLease(s3_after->bytes);
    EXPECT_TRUE(s3_now.gc_fenced);
    EXPECT_EQ(s3_now.seq, s3_prev.seq + 1);
    EXPECT_EQ(s3_now.server_uuid, s3_prev.server_uuid);
    EXPECT_EQ(s3_now.writer_epoch, s3_prev.writer_epoch);
    EXPECT_EQ(s3_now.hostname, s3_prev.hostname);
    EXPECT_EQ(s3_now.expires_at_ms, s3_prev.expires_at_ms);

    /// The already-fenced body was not touched (no PUT) across either call.
    const auto s4_after = b->get(l.mountKey("s4"));
    ASSERT_TRUE(s4_after.has_value());
    EXPECT_EQ(s4_after->bytes, s4_before->bytes);
}

namespace
{
/// A delegating backend whose `putOverwrite` of the target mount key first performs an inner renewal
/// (a real, token-correct overwrite that pushes expiry far into the future) and THEN delegates — so
/// the caller's fence-out overwrite lands on a stale token and returns PreconditionFailed. The inner
/// renewal runs exactly once (`renewed`), modelling a holder that renews concurrently in the window
/// between the function's GET and its fence-out PUT.
class RenewOnFenceBackend : public InMemoryBackend
{
public:
    RenewOnFenceBackend(String target_key_, uint64_t renewed_expires_ms_)
        : target_key(std::move(target_key_)), renewed_expires_ms(renewed_expires_ms_)
    {
    }

    PutResult putOverwrite(const String & key, const String & bytes, const Token & expected,
                           const ObjectMeta & meta) override
    {
        if (key == target_key && !renewed)
        {
            renewed = true;
            /// The holder renews under the real current token: fresh far-future expiry.
            const auto got = InMemoryBackend::get(key, {});
            MountLease m = decodeMountLease(got->bytes);
            m.seq += 1;
            m.expires_at_ms = renewed_expires_ms;
            const PutResult renew = InMemoryBackend::putOverwrite(key, encodeMountLease(m), got->token);
            EXPECT_EQ(renew.outcome, PutOutcome::Done);
        }
        return InMemoryBackend::putOverwrite(key, bytes, expected, meta);
    }

private:
    String target_key;
    uint64_t renewed_expires_ms;
    bool renewed = false;
};
}

TEST(CASHeartbeatFloor, FenceOutLosesTokenRaceReclassifiesLive)
{
    Layout l("p");
    auto b = std::make_shared<RenewOnFenceBackend>(
        l.mountKey("s1"), /*renewed_expires*/ kNowMs + 120'000);

    seedMount(*b, l, "s1", /*expires*/ kNowMs - 60'000, /*fenced*/ false, /*min_active*/ 0);

    MountObservationMap obs;
    /// Round 1: first sight, observation starts — never reaches the fence-out path (the race
    /// decorator stays armed for round 2).
    const HeartbeatFloor floor_before = computeHeartbeatFloor(*b, l, kNowMs, /*mono*/ 0, kStableThresholdMs, obs);
    EXPECT_EQ(floor_before.fenced_now, 0u);

    /// Round 2: the token has been stable past threshold, so the function attempts the fence-out.
    /// The decorator renews concurrently under the real token, the PUT hits PreconditionFailed, the
    /// function re-GETs and reclassifies it as live (observation restarted on the new token) — never
    /// fenced.
    const HeartbeatFloor floor2 = computeHeartbeatFloor(*b, l, kNowMs, /*mono*/ kStableThresholdMs,
                                                          kStableThresholdMs, obs);

    EXPECT_EQ(floor2.fenced_now, 0u);
    EXPECT_EQ(floor2.live, 1u);

    const auto after = b->get(l.mountKey("s1"));
    ASSERT_TRUE(after.has_value());
    EXPECT_FALSE(decodeMountLease(after->bytes).gc_fenced);
}

TEST(CASHeartbeatFloor, EmptyPrefixYieldsNoLiveMounts)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");

    MountObservationMap obs;
    const HeartbeatFloor floor = computeHeartbeatFloor(*b, l, kNowMs, /*mono*/ 0, kStableThresholdMs, obs);

    EXPECT_EQ(floor.live, 0u);
    EXPECT_EQ(floor.terminated, 0u);
    EXPECT_EQ(floor.fenced_now, 0u);
    EXPECT_EQ(floor.already_fenced, 0u);
}

/// ---- Task 1 (Phase 2): `listMounts` — read-only mount-slot enumeration for introspection ----

TEST(CASListMounts, ClassifiesEveryStateReadOnly)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const uint64_t now_ms = 1'000'000;
    const uint64_t ttl_ms = 10'000;

    /// live: fresh claim for srid "a"
    ASSERT_EQ(claimMount(*backend, layout, "a", UInt128{1}, /*our_epoch=*/1, now_ms, ttl_ms).kind,
              MountClaimResult::Claimed);
    /// expired: claim for "b" whose lease ran out long before now_ms
    ASSERT_EQ(claimMount(*backend, layout, "b", UInt128{2}, 1, now_ms - 100'000, ttl_ms).kind,
              MountClaimResult::Claimed);
    /// corrupt: garbage bytes in "c"'s mount slot
    backend->putIfAbsent(layout.mountKey("c"), "garbage-not-a-proto", {});

    auto mounts = listMounts(*backend, layout, now_ms, /*skew_margin_ms=*/ttl_ms / 2);
    ASSERT_EQ(mounts.size(), 3u);
    std::map<String, String> by_srid;
    for (const auto & m : mounts)
        by_srid[m.srid] = m.state;
    EXPECT_EQ(by_srid["a"], "live");
    EXPECT_EQ(by_srid["b"], "expired");
    EXPECT_EQ(by_srid["c"], "corrupt");

    /// READ-ONLY guarantee: "b" is expired but must NOT be fenced by listMounts
    /// (computeHeartbeatFloor would stamp gc_fenced=true; the introspection view must not).
    auto again = listMounts(*backend, layout, now_ms, ttl_ms / 2);
    for (const auto & m : again)
        if (m.srid == "b")
        {
            EXPECT_FALSE(m.lease.gc_fenced);
            EXPECT_EQ(m.state, "expired");
        }
}

/// A `srid` may itself contain `/` (e.g. `shard-01/replica-a` — legal per
/// `CASServerRootId.ValidationAcceptsCleanPathsRejectsBad`). Slicing the key by the last `/` before
/// the `/mount` suffix (as opposed to by `serverRootsPrefix()` length) truncates it to `replica-a`.
TEST(CASListMounts, NestedSridIsNotTruncated)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    const uint64_t now_ms = 1'000'000;
    const uint64_t ttl_ms = 10'000;

    ASSERT_EQ(claimMount(*backend, layout, "shard-01/replica-a", UInt128{1}, /*our_epoch=*/1, now_ms, ttl_ms).kind,
              MountClaimResult::Claimed);

    auto mounts = listMounts(*backend, layout, now_ms, /*skew_margin_ms=*/ttl_ms / 2);
    ASSERT_EQ(mounts.size(), 1u);
    EXPECT_EQ(mounts[0].srid, "shard-01/replica-a");
    EXPECT_EQ(mounts[0].state, "live");
}

/// "A fence costs an epoch": a same-(uuid, epoch) re-claim must NOT refresh a `gc_fenced` body in
/// place — that would resurrect a fenced incarnation. It is terminal for THIS epoch; only a
/// DIFFERENT (fresh) epoch may reclaim the slot.
TEST(CASClaimMount, SameEpochFencedIsNotRefreshable)
{
    using namespace DB::Cas;
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    /// mint for (uuid 1, epoch 1), then fence it in place (what computeHeartbeatFloor does):
    ASSERT_EQ(claimMount(*backend, layout, "a", DB::UInt128{1}, 1, 1000, 10'000).kind,
              MountClaimResult::Claimed);
    {
        auto got = backend->get(layout.mountKey("a"));
        MountLease fenced = decodeMountLease(got->bytes);
        fenced.gc_fenced = true;
        fenced.seq += 1;
        ASSERT_EQ(backend->putOverwrite(layout.mountKey("a"), encodeMountLease(fenced), got->token).outcome,
                  PutOutcome::Done);
    }
    /// Same (uuid, epoch) re-claim must NOT refresh a fenced body — a fence costs an epoch:
    const auto r = claimMount(*backend, layout, "a", DB::UInt128{1}, 1, 2000, 10'000);
    EXPECT_EQ(r.kind, MountClaimResult::FencedSelf);
    /// The body on the backend is still the fenced one (no write happened):
    EXPECT_TRUE(decodeMountLease(backend->get(layout.mountKey("a"))->bytes).gc_fenced);
    /// A DIFFERENT epoch reclaims immediately (existing branch, unchanged):
    EXPECT_EQ(claimMount(*backend, layout, "a", DB::UInt128{1}, 2, 2000, 10'000).kind,
              MountClaimResult::Claimed);
}

/// ---- rev.6 Task 4: observation-based lease reclaim (no cross-node wall-clock trust) ----

/// A same-uuid, different-epoch lease whose STAMPED `expires_at_ms` looks long expired on OUR wall
/// clock must NOT be reclaimed by that comparison alone — a clock-skewed or simply late-observing
/// caller must never trust a bare wall-clock read across incarnations. `claimMount` (without a
/// `proven_dead_token`) always reports `LiveDoubleStart` for this branch now; only the observation
/// loop (`claimMountAwaitingExpiry`) may turn it into a reclaim, and only after proving death on ITS
/// OWN clock.
TEST(CASMountObservation, ExpiredLookingLeaseIsNotReclaimedByWallClock)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l{"p"};
    /// Predecessor epoch 7 stamped expires_at_ms = 1000; our wall clock says 999999 (long past).
    auto first = claimMount(*b, l, "r", UInt128(1), 7, /*now_ms=*/500, /*ttl_ms=*/500);
    ASSERT_EQ(first.kind, MountClaimResult::Claimed);
    auto r = claimMount(*b, l, "r", UInt128(1), /*our_epoch=*/8, /*now_ms=*/999999, 500);
    EXPECT_EQ(r.kind, MountClaimResult::LiveDoubleStart);  /// no wall-clock trust
}

/// The observation loop reclaims once the write-token has held stable for the FULL rate-bound
/// threshold (`ttl_ms + ttl_ms/20 + poll_interval_ms`) on its OWN (injected, fake) clock — never
/// short-circuiting on the wall clock, which this test drives to an irrelevant, already-expired value.
TEST(CASMountObservation, TokenStableForThresholdThenReclaimed)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l{"p"};
    ASSERT_EQ(claimMount(*b, l, "r", UInt128(1), 7, 500, 500).kind, MountClaimResult::Claimed);
    uint64_t mono = 0;
    std::vector<uint64_t> sleeps;
    auto r = claimMountAwaitingExpiry(*b, l, "r", UInt128(1), 8,
        []{ return uint64_t{999999}; },                 /// wall clock: irrelevant
        [&]{ return mono; },                             /// observation clock
        /*ttl_ms=*/500, /*poll_interval_ms=*/50,
        [&](uint64_t ms){ sleeps.push_back(ms); mono += ms; });
    EXPECT_EQ(r.kind, MountClaimResult::Claimed);
    EXPECT_EQ(r.prior, MountPriorState::UncleanObserved);
    EXPECT_GE(mono, 500 + 500 / 20 + 50);               /// full threshold actually waited
}

/// A renewal DURING the observation window (the real holder is still alive) bumps the write-token —
/// the loop must detect the mismatch and RESTART the observation from the new token, never reclaiming
/// off a window that started watching a now-superseded token.
TEST(CASMountObservation, RenewalDuringObservationRestartsIt)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l{"p"};
    ASSERT_EQ(claimMount(*b, l, "r", UInt128(1), 7, 500, 500).kind, MountClaimResult::Claimed);

    /// The real (still-alive) holder's keeper for epoch 7: `start()` adopts the slot `claimMount` just
    /// wrote (no seq bump, per the ADOPT RULE), then `renewOnce()` bumps the token mid-observation.
    uint64_t keeper_wall = 500;
    MountLeaseKeeper keeper(b, l, "r", UInt128(1), 7, std::chrono::milliseconds(500),
                             [&] { return keeper_wall; }, [] { return uint64_t{0}; });
    keeper.start();

    const uint64_t threshold_ms = 500 + 500 / 20 + 50;   /// = 575
    uint64_t mono = 0;
    bool renewed = false;
    int wait_starts = 0;
    auto r = claimMountAwaitingExpiry(*b, l, "r", UInt128(1), 8,
        []{ return uint64_t{999999}; },                 /// wall clock: irrelevant
        [&]{ return mono; },                             /// observation clock
        /*ttl_ms=*/500, /*poll_interval_ms=*/50,
        [&](uint64_t ms)
        {
            mono += ms;
            /// Renew once, close to (but before) the first window's threshold would complete —
            /// almost the whole first window is wasted, forcing a near-full second window.
            if (!renewed && mono >= threshold_ms - 50)
            {
                renewed = true;
                keeper.renewOnce();
            }
        },
        /*on_wait_start=*/[&](const MountLease &, uint64_t) { ++wait_starts; });

    EXPECT_EQ(r.kind, MountClaimResult::Claimed);
    EXPECT_EQ(r.prior, MountPriorState::UncleanObserved);
    EXPECT_EQ(wait_starts, 2);                           /// the renewal forced exactly one restart
    /// The restart's own window did not begin until at least (threshold - poll) had already elapsed,
    /// so total elapsed time is well over a single threshold window.
    EXPECT_GE(mono, (threshold_ms - 50) + threshold_ms);
}

/// A GC-fenced lease is a terminal, already-threshold-gated certificate of death (the fence-out
/// itself cost the predecessor an epoch) — the observation loop must reclaim it on the FIRST attempt,
/// with zero polling/sleeping.
TEST(CASMountObservation, GcFencedIsReclaimedInstantlyWithPriorFenced)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l{"p"};
    ASSERT_EQ(claimMount(*b, l, "r", UInt128(1), 7, 1000, 500).kind, MountClaimResult::Claimed);

    /// Fence it manually (what `computeHeartbeatFloor`'s fence-out does): gc_fenced=true, seq+1,
    /// token-guarded.
    {
        auto got = b->get(l.mountKey("r"));
        ASSERT_TRUE(got.has_value());
        MountLease fenced = decodeMountLease(got->bytes);
        fenced.gc_fenced = true;
        fenced.seq += 1;
        ASSERT_EQ(b->putOverwrite(l.mountKey("r"), encodeMountLease(fenced), got->token).outcome,
                  PutOutcome::Done);
    }

    int sleeps = 0;
    auto r = claimMountAwaitingExpiry(*b, l, "r", UInt128(1), /*our_epoch=*/8,
        []{ return uint64_t{999999}; },
        []{ return uint64_t{0}; },
        /*ttl_ms=*/500, /*poll_interval_ms=*/50,
        [&](uint64_t) { ++sleeps; });

    EXPECT_EQ(r.kind, MountClaimResult::Claimed);
    EXPECT_EQ(r.prior, MountPriorState::Fenced);
    EXPECT_EQ(sleeps, 0);
}

/// ---- Stage B Task 3: `isCreatorFenceTerminal` -- the cross-process terminality predicate
/// `CasRefCatalog::reconcileStaleCreator` gates on. Built from `writer_epoch` plus the SAME two
/// clock-free certificates `probeNonTerminalMountSlots`/`computeHeartbeatFloor` already use, PLUS a
/// third certificate available only here: a currently-live DIFFERENT `writer_epoch` at the slot. ----

TEST(CASFenceTerminal, AbsentMountSlotIsNotTerminal)
{
    InMemoryBackend b;
    Layout l{"p"};
    EXPECT_FALSE(isCreatorFenceTerminal(b, l, "never-mounted", 1))
        << "absence proves nothing about liveness -- never waved through";
}

TEST(CASFenceTerminal, UndecodableMountBodyIsNotTerminal)
{
    InMemoryBackend b;
    Layout l{"p"};
    b.putIfAbsent(l.mountKey("r"), "garbage-not-a-lease", {});
    EXPECT_FALSE(isCreatorFenceTerminal(b, l, "r", 1))
        << "an unreadable lease of some other format generation must block, never wave through";
}

TEST(CASFenceTerminal, GcFencedIsTerminal)
{
    InMemoryBackend b;
    Layout l{"p"};
    ASSERT_EQ(claimMount(b, l, "r", UInt128(1), /*our_epoch=*/7, 1000, 500).kind, MountClaimResult::Claimed);
    auto got = b.get(l.mountKey("r"));
    ASSERT_TRUE(got.has_value());
    MountLease fenced = decodeMountLease(got->bytes);
    fenced.gc_fenced = true;
    ASSERT_EQ(b.putOverwrite(l.mountKey("r"), encodeMountLease(fenced), got->token).outcome, PutOutcome::Done);

    EXPECT_TRUE(isCreatorFenceTerminal(b, l, "r", 7));
}

TEST(CASFenceTerminal, CleanFarewellIsTerminal)
{
    InMemoryBackend b;
    Layout l{"p"};
    ASSERT_EQ(claimMount(b, l, "r", UInt128(1), /*our_epoch=*/7, 1000, 500).kind, MountClaimResult::Claimed);
    auto got = b.get(l.mountKey("r"));
    ASSERT_TRUE(got.has_value());
    MountLease retired = decodeMountLease(got->bytes);
    retired.min_active = std::numeric_limits<uint64_t>::max();
    ASSERT_EQ(b.putOverwrite(l.mountKey("r"), encodeMountLease(retired), got->token).outcome, PutOutcome::Done);

    EXPECT_TRUE(isCreatorFenceTerminal(b, l, "r", 7));
}

TEST(CASFenceTerminal, ADifferentLiveWriterEpochIsTerminalForTheOldOne)
{
    InMemoryBackend b;
    Layout l{"p"};
    /// Slot now held at epoch 8 -- epoch 7's incarnation is superseded regardless of ITS OWN
    /// certificate (neither fenced nor farewelled).
    ASSERT_EQ(claimMount(b, l, "r", UInt128(1), /*our_epoch=*/8, 1000, 500).kind, MountClaimResult::Claimed);

    EXPECT_TRUE(isCreatorFenceTerminal(b, l, "r", 7))
        << "a different epoch is currently live at this slot -- epoch 7 can never reclaim it";
    EXPECT_FALSE(isCreatorFenceTerminal(b, l, "r", 8))
        << "epoch 8 IS the current live epoch -- not terminal";
}

/// A merely EXPIRED lease (wall-clock past `expires_at_ms`, same epoch, no certificate) must NOT be
/// treated as terminal -- mirrors `claimMount`'s own refusal to trust a bare timestamp comparison.
TEST(CASFenceTerminal, ExpiredButSameEpochAndUncertifiedIsNotTerminal)
{
    InMemoryBackend b;
    Layout l{"p"};
    /// A lease whose stamped expiry is already far in the past, same epoch throughout.
    ASSERT_EQ(claimMount(b, l, "r", UInt128(1), /*our_epoch=*/7, /*now_ms=*/0, /*ttl_ms=*/1).kind,
              MountClaimResult::Claimed);

    EXPECT_FALSE(isCreatorFenceTerminal(b, l, "r", 7))
        << "expiry alone is never a certificate of death, exactly like claimMount's own discipline";
}
