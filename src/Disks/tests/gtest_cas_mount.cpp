#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "cas_test_helpers.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Poco/Exception.h>
#include <Poco/Message.h>
#include <Poco/StreamChannel.h>

#include <atomic>
#include <chrono>
#include <limits>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

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

void renewOrThrow(MountLeaseRenewer & renewer)
{
    const MountRenewResult result = renewer.renew(MountRenewOperationEnvironment{});
    if (result.outcome == MountRenewOutcome::Terminal)
        std::rethrow_exception(result.failure);
    ASSERT_EQ(result.outcome, MountRenewOutcome::Committed);
}

/// The two request planes a renewer in this file runs on, plus one operation for the protocol calls
/// driven directly. Both planes are open-fence: these fixtures hold no mount lease, so nothing here
/// should be refused by a fence it does not have. The clock and the sleep are ALWAYS injected -- a
/// fixture that drives a lease deadline passes its own so a slow machine cannot run the bound out
/// mid-test, and one that does not still must not sleep for real when a fault sends the engine round
/// again. `tests::OperationForTest` covers the one-operation case but neither the two planes nor the
/// clock, which is why this stays local.
class Ops
{
public:
    explicit Ops(std::shared_ptr<Backend> backend) : Ops(std::move(backend), nullptr) {}

    Ops(std::shared_ptr<Backend> backend, uint64_t * boot_ms)
        : mount(openRequestsForTest(backend))
        , farewell(openRequestsForTest(std::move(backend)))
        , op(mount.admit())
    {
        uint64_t * clock = boot_ms ? boot_ms : &own_clock;
        for (CasRequests * requests : {&mount, &farewell})
        {
            requests->setNowFnForTest([clock] { return *clock; });
            requests->setSleepFnForTest([clock](uint64_t ms) { *clock += ms; });
        }
    }

    Ops(const Ops &) = delete;
    Ops & operator=(const Ops &) = delete;

    CasRequests mount;
    CasRequests farewell;
    CasOperation op;

private:
    uint64_t own_clock = 0;
};

/// The incarnation currently at `key`, for a fixture that has to name it as a precondition.
Etag currentEtag(CasOperation & op, const String & key)
{
    const auto got = op.read(key, Retry::standard());
    if (!got)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "test fixture read of '{}' found nothing", key);
    return got->etag;
}

/// A fixture write that must land, so a mis-seeded fixture fails where it is written rather than in
/// the assertion it silently invalidated.
void mustCommit(WriteResult && result, const String & what)
{
    if (!std::holds_alternative<Committed>(result))
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "test fixture write '{}' did not commit", what);
}

class OwnerConflictRevealsManifestBackend : public InMemoryBackend
{
public:
    std::expected<String, RawConflict> write(
        const String & key, const String & bytes,
        const std::optional<String> & expected_value, TransportAccess & access) override
    {
        if (!fired && !expected_value && key == "p/gc/server-roots/root/x/owner")
        {
            fired = true;
            InMemoryBackend::write("p/cas/manifests/root/x/table/debris", "x", std::nullopt, access);
            return std::unexpected(RawConflict{});
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }

    bool fired = false;
};

/// Loses the owner key to a racing claimer between the read and the create: installs `winner`'s owner
/// object, then refuses this write. The subtree stays empty, so the emptiness recompute passes and the
/// claim has to decide the race from what its own write observed.
class OwnerRaceBackend : public InMemoryBackend
{
public:
    explicit OwnerRaceBackend(UInt128 winner_) : winner(winner_) {}

    /// Counts only reads of the owner key, so an extra re-read the conflict decision no longer needs
    /// is visible even though the resolve read on the same key already counts once.
    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (key == "p/gc/server-roots/r/owner")
            ++owner_reads;
        return InMemoryBackend::read(key, access);
    }

    std::expected<String, RawConflict> write(
        const String & key, const String & bytes,
        const std::optional<String> & expected_value, TransportAccess & access) override
    {
        if (!fired && !expected_value && key == "p/gc/server-roots/r/owner")
        {
            fired = true;
            InMemoryBackend::write(
                key, encodeOwner(OwnerObject{.server_uuid = winner, .retired_at_ms = std::nullopt}),
                std::nullopt, access);
            return std::unexpected(RawConflict{});
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }

    bool fired = false;
    size_t owner_reads = 0;

private:
    UInt128 winner;
};

/// Refuses the FIRST write of the epoch key after installing a competing allocator's own epoch, so the
/// absent-epoch decision has to be made a second time. `reveal_owned_work` decides whether owned work
/// becomes visible at that same instant -- the fact the second decision must re-establish.
class EpochConflictBackend : public InMemoryBackend
{
public:
    explicit EpochConflictBackend(bool reveal_owned_work_ = true) : reveal_owned_work(reveal_owned_work_) {}

    std::expected<String, RawConflict> write(
        const String & key, const String & bytes,
        const std::optional<String> & expected_value, TransportAccess & access) override
    {
        if (!fired && key == "p/gc/server-roots/root/x/epoch")
        {
            fired = true;
            const auto winner = InMemoryBackend::write(
                key, encodeServerEpoch(ServerEpoch{.next_writer_epoch = 2}), expected_value, access);
            winner_installed = winner.has_value();
            if (reveal_owned_work)
                InMemoryBackend::write("p/cas/manifests/root/x/table/debris", "x", std::nullopt, access);
            return std::unexpected(RawConflict{});
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }

    bool fired = false;
    bool winner_installed = false;

private:
    bool reveal_owned_work;
};

/// Counts every request that reaches the store, per primitive, so a test can pin how many a protocol
/// step costs rather than only what it produced.
class RequestCountingBackend final : public InMemoryBackend
{
public:
    size_t reads = 0;
    size_t heads = 0;
    size_t writes = 0;

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        ++reads;
        return InMemoryBackend::read(key, access);
    }

    std::optional<RawMeta> head(const String & key, TransportAccess & access) override
    {
        ++heads;
        return InMemoryBackend::head(key, access);
    }

    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value,
                                             TransportAccess & access) override
    {
        ++writes;
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }
};

class RenewalLogBackend final : public InMemoryBackend
{
public:
    bool throw_before_next_overwrite = false;

    /// The fault lives on the primitive every write reaches the store through, keyed to the mount
    /// slot so the pool's other conditional writes pass untouched.
    std::expected<String, RawConflict> write(
        const String & key,
        const String & bytes,
        const std::optional<String> & expected_value,
        TransportAccess & access) override
    {
        if (expected_value && key.ends_with("/mount") && std::exchange(throw_before_next_overwrite, false))
            throw Poco::TimeoutException("injected renewal timeout before commit");
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }
};

class ScopedRenewalLogCapture
{
public:
    explicit ScopedRenewalLogCapture(const String & level)
        : logger(getLogger("CasMountLeaseRenewer"))
        , channel(new Poco::StreamChannel(stream))
        , old_channel(logger->getChannel(), /*shared=*/true)
        , old_level(logger->getLevel())
    {
        logger->setChannel(channel.get());
        logger->setLevel(level);
    }

    ~ScopedRenewalLogCapture()
    {
        logger->setChannel(old_channel);
        logger->setLevel(old_level);
    }

    String captured() const { return stream.str(); }

private:
    LoggerPtr logger;
    std::ostringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::AutoPtr<Poco::StreamChannel> channel;
    /// A real reference (shared=true), so the parked previous channel cannot die while ours is installed.
    Poco::AutoPtr<Poco::Channel> old_channel;
    int old_level;
};

size_t countRenewalLogText(const String & haystack, std::string_view needle)
{
    size_t count = 0;
    for (size_t pos = 0; (pos = haystack.find(needle, pos)) != String::npos; pos += needle.size())
        ++count;
    return count;
}

CasRequestBudget renewalLogBudget()
{
    return CasRequestBudget{
        .attempt_timeout_ms = 10,
        .lease_safety_margin_ms = 20,
        .connect_timeout_cap_ms = std::nullopt,
    };
}

}

/// `CASMountAudit.PhysicalRetryCannotBeDelayedByDebugLogging` was retired when mount renewal moved onto
/// `CasRequests`/`CasOperation` (the old hand-written renewal controller had a per-attempt progress
/// callback the test used to interleave a blocking debug log with the retry loop's own pacing; nothing
/// still exposes such a callback). Verified still true against the current engine, not just the
/// migration's own commit message: `grep -n "LOG_\|getLogger" .../Backend/CasRequests.cpp` finds exactly
/// one log call in the whole write-retry engine, `logCasWriteRetryLater`, reached only from the
/// `[[noreturn]]` `throwCasWriteRetryLater` -- the terminal give-up, called once, never between
/// attempts. No replacement test is needed: there is no per-attempt log call left to race.
TEST(CASMountAudit, RenewalDefaultLogsAreBounded)
{
    /// `boot_ms` is a shared, heap-owned atomic, not a plain reference parameter: the last block below
    /// mutates it after the Pool exists, and the Pool can outlive this lambda's own call (a background
    /// publish holds `shared_from_this()`), so a by-reference capture of a caller-local would dangle.
    const auto open_store = [](const std::shared_ptr<RenewalLogBackend> & backend,
        const std::shared_ptr<std::atomic<uint64_t>> & boot_ms, const String & prefix)
    {
        /// What the request engine reserves per attempt is the BACKEND's attempt timeout, not the
        /// budget field alone; pair the two so the fence math below matches what admits.
        backend->setAttemptTimeoutMs(renewalLogBudget().attempt_timeout_ms);
        return Pool::open(backend, PoolConfig{
            .pool_prefix = prefix,
            .server_root_id = "test",
            .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
            .cas_request_budget = renewalLogBudget(),
            .boot_ms_fn = [boot_ms]
            {
                return boot_ms->load();
            },
        });
    };

    {
        auto backend = std::make_shared<RenewalLogBackend>();
        auto boot_ms = std::make_shared<std::atomic<uint64_t>>(100);
        auto store = open_store(backend, boot_ms, "renewal-log-silent");
        ScopedRenewalLogCapture capture("information");
        EXPECT_NO_THROW(store->renewWatermarkOnce());
        EXPECT_EQ(countRenewalLogText(capture.captured(), "CAS mount renewal"), 0u);
    }

    {
        auto backend = std::make_shared<RenewalLogBackend>();
        auto boot_ms = std::make_shared<std::atomic<uint64_t>>(100);
        auto store = open_store(backend, boot_ms, "renewal-log-recovered");
        ScopedRenewalLogCapture capture("information");
        backend->throw_before_next_overwrite = true;
        EXPECT_NO_THROW(store->renewWatermarkOnce());
        const String output = capture.captured();
        EXPECT_EQ(countRenewalLogText(output, "CAS mount renewal"), 1u) << output;
        EXPECT_EQ(countRenewalLogText(output, "recovered"), 1u) << output;
        EXPECT_EQ(countRenewalLogText(output, "physical retry attempt"), 0u) << output;
    }

    {
        auto backend = std::make_shared<RenewalLogBackend>();
        auto boot_ms = std::make_shared<std::atomic<uint64_t>>(100);
        auto store = open_store(backend, boot_ms, "renewal-log-debug");
        ScopedRenewalLogCapture capture("debug");
        backend->throw_before_next_overwrite = true;
        EXPECT_NO_THROW(store->renewWatermarkOnce());
        EXPECT_EQ(countRenewalLogText(capture.captured(), "physical retry attempt 2"), 1u);
    }

    {
        auto backend = std::make_shared<RenewalLogBackend>();
        auto boot_ms = std::make_shared<std::atomic<uint64_t>>(100);
        auto store = open_store(backend, boot_ms, "renewal-log-fenced");
        ScopedRenewalLogCapture capture("information");
        /// The lease was claimed at boot 100 with the 1000 ms TTL above, so it expires at 1100. The
        /// fence admits only while the remaining time strictly clears the safety margin plus whatever
        /// the attempt reserves, so exactly `margin` remaining (with the reservation on top) refuses.
        boot_ms->store(1100 - renewalLogBudget().lease_safety_margin_ms);
        EXPECT_THROW(store->renewWatermarkOnce(), DB::Exception);
        const String output = capture.captured();
        EXPECT_EQ(countRenewalLogText(output, "CAS mount renewal"), 1u) << output;
        EXPECT_EQ(countRenewalLogText(output, "fenced"), 1u) << output;
    }
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
        m.write_attempt_id = UInt128{1};
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
    Ops ops(b);
    EXPECT_NO_THROW(claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation()));     // fresh empty root → claim
    EXPECT_NO_THROW(claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation()));     // same uuid → ok
    try
    {
        claimOwnerOrThrow(ops.op, l, "r", UInt128(2), emptyCatalogObservation());
        FAIL() << "expected a foreign owner to fail closed";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
        EXPECT_NE(e.message().find("<cas_server_root_id>"), String::npos) << e.message();
    }
}

TEST(CASServerRootClaim, TombstonedSameOwnerFailsClosed)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    mustCommit(ops.op.create(l.ownerKey("r"), encodeOwner(OwnerObject{
        .server_uuid = UInt128(1),
        .retired_at_ms = 1752537600000ULL,
    }), Retry::standard()), "tombstoned owner");

    try
    {
        claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation());
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
    Ops ops(b);
    claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation());
    const uint64_t e1 = allocateWriterEpoch(ops.op, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation());
    const uint64_t e2 = allocateWriterEpoch(ops.op, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation());
    EXPECT_GE(e1, 1u);                                             // 0 is a reserved sentinel
    EXPECT_GT(e2, e1);                                             // strictly increasing

    /// Deleting the (separate) mount object must NOT reset the epoch. No mount has been written yet,
    /// so the removal is a no-op that touches nothing.
    ASSERT_FALSE(ops.op.head(l.mountKey("r"), Retry::standard()).has_value());
    EXPECT_EQ(ops.op.removeCurrent(l.mountKey("r"), Retry::standard()), Removal::Gone);
    EXPECT_GT(allocateWriterEpoch(ops.op, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), e2);
}

/// Phase C (spec rev.4): an ABSENT epoch object over a PRESENT mount object means durable epoch
/// state was lost while a mount is live/recent — re-minting epoch 1 there is how a same-(uuid,
/// epoch) twin is born. Refuse.
TEST(CASMount, EpochRemintOverExistingMountRefuses)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation());
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), /*our_epoch=*/1, /*now_ms=*/1000, /*ttl_ms=*/30000).kind,
              MountClaimResult::Claimed);
    /// The epoch object is ABSENT (never created in this sequence) while the mount exists:
    EXPECT_THROW(allocateWriterEpoch(ops.op, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), DB::Exception);   /// CORRUPTED_DATA
}

TEST(CASMount, EpochRemintAuthoritativeAbsenceMints)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation());
    EXPECT_EQ(allocateWriterEpoch(ops.op, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), 1u);   /// fresh root: both control objects absent
    EXPECT_EQ(allocateWriterEpoch(ops.op, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), 2u);   /// epoch present now: normal conditional bump, no probe
}

/// The probe outcome gates the mint: anything short of authoritative KeyAbsent fails closed.
TEST(CASMount, EpochRemintIndeterminateProbeFailsClosed)
{
    class IndeterminateProbeBackend final : public InMemoryBackend
    {
    public:
        SentinelProbeResult probeSentinelRaw(const String &, TransportAccess &) override
        {
            return {.outcome = ProbeOutcome::Indeterminate, .body = std::nullopt};
        }
    };
    auto b = std::make_shared<IndeterminateProbeBackend>();
    Layout l("p");
    Ops ops(b);
    claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation());
    EXPECT_THROW(allocateWriterEpoch(ops.op, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), DB::Exception);
}

/// Decommission over a TERMINAL (expired/fenced) mount with a lost epoch object proceeds and mints
/// an epoch DISTINCT from the surviving mount's — the same-pair state is unrepresentable.
TEST(CASMount, DecommissionRemintOverTerminalMountMintsDistinctEpoch)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation());
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), /*our_epoch=*/3, /*now_ms=*/1000, /*ttl_ms=*/100).kind,
              MountClaimResult::Claimed);
    /// now_ms=5000: the ttl_ms=100 lease above is long expired -> terminal.
    EXPECT_EQ(allocateWriterEpoch(ops.op, l, "r", EpochMintPolicy::DecommissionRecovery, /*now_ms=*/5000, emptyCatalogObservation()), 4u);
}

/// Decommission over a LIVE mount with a lost epoch refuses — the blind bypass would recreate the
/// forbidden pair (codex round-3 finding 1) and defeat CASDecommission.RefusesLiveMember.
TEST(CASMount, DecommissionRemintOverLiveMountRefuses)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation());
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), /*our_epoch=*/1, /*now_ms=*/1000, /*ttl_ms=*/30000).kind,
              MountClaimResult::Claimed);
    EXPECT_THROW(allocateWriterEpoch(ops.op, l, "r", EpochMintPolicy::DecommissionRecovery, /*now_ms=*/2000, emptyCatalogObservation()),
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
        SentinelProbeResult probeSentinelRaw(const String & k, TransportAccess & access) override
        {
            ++probes;
            return InMemoryBackend::probeSentinelRaw(k, access);
        }
    };
    auto b = std::make_shared<ProbeCountingBackend>();
    Layout l("p");
    Ops ops(b);
    claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation());
    EXPECT_EQ(allocateWriterEpoch(ops.op, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), 1u);   /// bootstrap: ONE probe (absent-epoch branch)
    const int probes_after_bootstrap = b->probes;
    EXPECT_EQ(allocateWriterEpoch(ops.op, l, "r", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), 2u);   /// epoch present: normal conditional bump...
    EXPECT_EQ(b->probes, probes_after_bootstrap) << "...must not probe the mount key";
}

TEST(CASServerRootClaim, MissingOwnerOverNonEmptyRootIsCorrupted)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    /// Simulate existing data without an owner (identity lost): plant a key under roots/<srid>/.
    mustCommit(ops.op.create(l.serverRootDataPrefix("r") + "some-data", "x", Retry::standard()), "root debris");
    EXPECT_THROW(claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation()), DB::Exception);
}

TEST(CASServerRootSafety, EveryCatalogLifecycleStateBlocksOwnerAndEpochRecreation)
{
    const Layout layout("p");
    for (const NsState state : {NsState::Creating, NsState::Live, NsState::Removing})
    {
        RefCatalog catalog = catalogOwning("root/x/table", state);
        const ObserveRefCatalog observe = [catalog] { return catalog; };

        Ops owner_ops(std::make_shared<InMemoryBackend>());
        EXPECT_THROW(claimOwnerOrThrow(owner_ops.op, layout, "root/x", UInt128{1}, observe), DB::Exception);
        EXPECT_FALSE(owner_ops.op.head(layout.ownerKey("root/x"), Retry::standard()).has_value());

        Ops epoch_ops(std::make_shared<InMemoryBackend>());
        EXPECT_THROW(allocateWriterEpoch(
            epoch_ops.op, layout, "root/x", EpochMintPolicy::NormalMount, 0, observe), DB::Exception);
        EXPECT_FALSE(epoch_ops.op.head(layout.epochKey("root/x"), Retry::standard()).has_value());
    }
}

TEST(CASServerRootSafety, OwnershipUsesAPathComponentBoundary)
{
    Ops ops(std::make_shared<InMemoryBackend>());
    const Layout layout("p");
    EXPECT_TRUE(serverRootSubtreeEmpty(
        ops.op, layout, "root/x", catalogOwning("root/xy/table", NsState::Live)));
    EXPECT_FALSE(serverRootSubtreeEmpty(
        ops.op, layout, "root/x", catalogOwning("root/x/table", NsState::Live)));
}

TEST(CASServerRootSafety, OpaqueStreamAndStateDebrisAloneDoesNotBlockRecreation)
{
    Ops ops(std::make_shared<InMemoryBackend>());
    const Layout layout("p");
    const NamespaceLifeId dead = NamespaceLifeId::fromCatalogEntry(RootNamespace{"unowned"}, UInt128{99});
    mustCommit(ops.op.create(layout.refLogKey(dead, RefTxnId{1, 1}), "debris", Retry::standard()), "ref-log debris");
    mustCommit(ops.op.create(layout.refCkptKey(dead), "debris", Retry::standard()), "ckpt debris");
    mustCommit(ops.op.create(layout.namespaceFileKey(dead, "f"), "debris", Retry::standard()), "ns-file debris");

    EXPECT_NO_THROW(claimOwnerOrThrow(ops.op, layout, "root/x", UInt128{1}, emptyCatalogObservation()));
    EXPECT_EQ(allocateWriterEpoch(
        ops.op, layout, "root/x", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), 1u);
}

TEST(CASServerRootSafety, ManifestAndLooseRootDebrisStillBlockRecreation)
{
    const Layout layout("p");
    for (const String & key : {
             layout.casManifestsServerPrefix("root/x") + "table/debris",
             layout.serverRootDataPrefix("root/x") + "loose"})
    {
        Ops ops(std::make_shared<InMemoryBackend>());
        mustCommit(ops.op.create(key, "x", Retry::standard()), "blocking debris");
        EXPECT_THROW(claimOwnerOrThrow(
            ops.op, layout, "root/x", UInt128{1}, emptyCatalogObservation()), DB::Exception);
        EXPECT_THROW(allocateWriterEpoch(
            ops.op, layout, "root/x", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), DB::Exception);
    }
}

TEST(CASServerRootSafety, UnreadableCatalogNeverFallsBackToPhysicalGuesses)
{
    Ops ops(std::make_shared<InMemoryBackend>());
    const Layout layout("p");
    const ObserveRefCatalog unreadable = []() -> RefCatalog
    {
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "injected unreadable catalog");
    };
    EXPECT_THROW(claimOwnerOrThrow(ops.op, layout, "root/x", UInt128{1}, unreadable), DB::Exception);
    EXPECT_THROW(allocateWriterEpoch(
        ops.op, layout, "root/x", EpochMintPolicy::NormalMount, 0, unreadable), DB::Exception);
    EXPECT_FALSE(ops.op.head(layout.ownerKey("root/x"), Retry::standard()).has_value());
    EXPECT_FALSE(ops.op.head(layout.epochKey("root/x"), Retry::standard()).has_value());
}

TEST(CASServerRootSafety, OwnerConflictRecomputesTheWholeEmptinessBundle)
{
    auto backend = std::make_shared<OwnerConflictRevealsManifestBackend>();
    Ops ops(backend);
    const Layout layout("p");
    /// The message, not just the code: without the post-conflict recompute the claim still throws
    /// `CORRUPTED_DATA`, from the vanished-anchor arm below it, so a bare code assertion would hold
    /// with the behaviour this test is named for deleted.
    DB::Cas::tests::expectThrowsCodeWithMessage(
        DB::ErrorCodes::CORRUPTED_DATA,
        "newly visible owned work blocks recreation",
        [&] { claimOwnerOrThrow(ops.op, layout, "root/x", UInt128{1}, emptyCatalogObservation()); });
    EXPECT_TRUE(backend->fired);
    EXPECT_FALSE(ops.op.head(layout.ownerKey("root/x"), Retry::standard()).has_value());
}

TEST(CASServerRootSafety, EpochConflictRecomputesTheWholeEmptinessBundle)
{
    auto backend = std::make_shared<EpochConflictBackend>();
    Ops ops(backend);
    const Layout layout("p");
    EXPECT_THROW(allocateWriterEpoch(
        ops.op, layout, "root/x", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), DB::Exception);
    EXPECT_TRUE(backend->fired);
    ASSERT_TRUE(backend->winner_installed);
    const auto epoch = ops.op.read(layout.epochKey("root/x"), Retry::standard());
    ASSERT_TRUE(epoch.has_value());
    EXPECT_EQ(decodeServerEpoch(epoch->bytes).next_writer_epoch, 2u)
        << "the rejected allocator must not consume an epoch from the conflict winner";
}

TEST(CASMountLease, AbsentClaimThenRenewBumpsSeq)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    uint64_t now = 1000;
    uint64_t boot = 0;
    Ops ops(b, &boot);
    auto r = claimMount(ops.op, l, "r", UInt128(1), /*epoch*/ 7, now, /*ttl*/ 100);
    EXPECT_EQ(r.kind, MountClaimResult::Claimed);
    MountLeaseRenewer k(ops.mount, ops.farewell, l, "r", UInt128(1), 7, std::chrono::milliseconds(100),
                       [&] { return now; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(0),
                       [&] { return boot; });
    k.start();
    EXPECT_EQ(decodeMountLease(ops.op.read(l.mountKey("r"), Retry::standard())->bytes).seq, 1u);
    renewOrThrow(k);
    EXPECT_EQ(decodeMountLease(ops.op.read(l.mountKey("r"), Retry::standard())->bytes).seq, 2u);
}

TEST(CASMountLease, HolderBodiesMintFreshAttemptIdsAndFenceCopiesIt)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("p");
    uint64_t now = 1000;
    uint64_t boot = 0;
    Ops ops(backend, &boot);
    ASSERT_EQ(claimMount(ops.op, layout, "r", UInt128{1}, 7, now, 100).kind, MountClaimResult::Claimed);
    const String key = layout.mountKey("r");
    const MountLease claimed = decodeMountLease(ops.op.read(key, Retry::standard())->bytes);

    MountLeaseRenewer renewer(ops.mount, ops.farewell, layout, "r", UInt128{1}, 7, std::chrono::milliseconds(100),
                            [&] { return now; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(0),
                            [&] { return boot; });
    renewer.start();
    renewOrThrow(renewer);
    const MountLease renewed = decodeMountLease(ops.op.read(key, Retry::standard())->bytes);
    EXPECT_NE(claimed.write_attempt_id, UInt128{});
    EXPECT_NE(renewed.write_attempt_id, UInt128{});
    EXPECT_NE(claimed.write_attempt_id, renewed.write_attempt_id);

    auto observed = ops.op.read(key, Retry::standard());
    ASSERT_TRUE(observed.has_value());
    MountLease fenced = decodeMountLease(observed->bytes);
    fenced.gc_fenced = true;
    ++fenced.seq;
    mustCommit(ops.op.replace(key, encodeMountLease(fenced), observed->etag, Retry::standard()), "fence-out");
    EXPECT_EQ(decodeMountLease(ops.op.read(key, Retry::standard())->bytes).write_attempt_id, renewed.write_attempt_id);
}

TEST(CASMountLease, ReclaimAndSuccessorBodiesMintNewAttemptIds)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("p");
    Ops ops(backend);
    const String key = layout.mountKey("r");
    ASSERT_EQ(claimMount(ops.op, layout, "r", UInt128{1}, 7, 1000, 100).kind, MountClaimResult::Claimed);
    const MountLease first = decodeMountLease(ops.op.read(key, Retry::standard())->bytes);

    auto observed = ops.op.read(key, Retry::standard());
    ASSERT_TRUE(observed.has_value());
    MountLease fenced = decodeMountLease(observed->bytes);
    fenced.gc_fenced = true;
    ++fenced.seq;
    mustCommit(ops.op.replace(key, encodeMountLease(fenced), observed->etag, Retry::standard()), "fence-out");
    const MountLease fence = decodeMountLease(ops.op.read(key, Retry::standard())->bytes);
    EXPECT_EQ(fence.write_attempt_id, first.write_attempt_id);

    ASSERT_EQ(claimMount(ops.op, layout, "r", UInt128{1}, 8, 2000, 100).kind, MountClaimResult::Claimed);
    const MountLease successor = decodeMountLease(ops.op.read(key, Retry::standard())->bytes);
    EXPECT_NE(successor.write_attempt_id, first.write_attempt_id);
    EXPECT_NE(successor.write_attempt_id, UInt128{});
}

/// STID 3982-3b48: `rm -rf` of the pool dir under a live mount deletes the mount slot object out from
/// under a running renewer. The next synchronous renewal must return terminal WITHOUT constructing a
/// `LOGICAL_ERROR` -- that aborts debug/ASan builds at
/// exception construction, and there is no foreign writer here to fail closed against, only an
/// environmental condition.
TEST(CASMountLease, VanishedBackingStoreStopsRenewalWithoutLogicalError)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    uint64_t now = 1000;
    uint64_t boot = 0;
    Ops ops(b, &boot);
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), /*epoch*/ 7, now, /*ttl*/ 100).kind, MountClaimResult::Claimed);
    MountLeaseRenewer k(ops.mount, ops.farewell, l, "r", UInt128(1), 7, std::chrono::milliseconds(100),
                       [&] { return now; }, [] { return uint64_t{0}; }, {}, std::chrono::milliseconds(0),
                       [&] { return boot; });
    k.start();

    const String mount_key = l.mountKey("r");
    const auto lost_before = ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load();  /// NOLINT(clang-analyzer-deadcode.DeadStores)

    /// Simulate `rm -rf` of the backing store: the mount slot object is gone, but the renewer still
    /// names a (now stale) incarnation as its precondition.
    ASSERT_EQ(ops.op.removeCurrent(mount_key, Retry::standard()), Removal::Removed);

    try
    {
        renewOrThrow(k);
        FAIL() << "renew against a vanished mount object must throw";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::FILE_DOESNT_EXIST) << e.message();
        EXPECT_NE(e.code(), DB::ErrorCodes::LOGICAL_ERROR);
    }
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load(), lost_before)
        << "renewer classification is metric-free; the runtime records operational loss";
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
    Ops ops(b);
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), /*epoch*/ 7, now, /*ttl*/ 100).kind, MountClaimResult::Claimed);
    MountLeaseRenewer k(ops.mount, ops.farewell, l, "r", UInt128(1), 7, std::chrono::milliseconds(100),
                       [&] { return now; }, [] { return uint64_t{0}; });
    k.start();

    const String mount_key = l.mountKey("r");
    const auto lost_before = ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load();

    /// Simulate `rm -rf` of the backing store: the mount slot object is gone before we ever attempt
    /// a renewal, so the farewell's guarded write is the first thing to observe it.
    ASSERT_EQ(ops.op.removeCurrent(mount_key, Retry::standard()), Removal::Removed);

    EXPECT_NO_THROW(k.release())
        << "clean release against a vanished store must be a no-op, not a LOGICAL_ERROR abort";
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountLeaseLost].load(), lost_before);
}

/// rev.6: a bare `claimMount` (no `proven_dead_incarnation`) NEVER reclaims a same-uuid, different-epoch
/// lease off a wall-clock-looking-expired stamp — only `claimMountAwaitingExpiry`'s observation loop
/// can turn that into a reclaim. Renamed from `...ExpiredReclaims` to describe the corrected behavior.
TEST(CASMountLease, SameUuidLiveFailsForeignFailsExpiredStillLiveDoubleStart)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    claimMount(ops.op, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 100);    // A live until 1100
    // same uuid, lease still live → double-start guard:
    EXPECT_EQ(claimMount(ops.op, l, "r", UInt128(1), 8, 1050, 100).kind, MountClaimResult::LiveDoubleStart);
    // foreign uuid, even after expiry → fail closed:
    EXPECT_EQ(claimMount(ops.op, l, "r", UInt128(2), 1, 1200, 100).kind, MountClaimResult::ForeignOwner);
    // same uuid, even after the stamp LOOKS expired on our wall clock → still LiveDoubleStart: no
    // proven_dead_incarnation was supplied, so there is no certificate of death to reclaim on.
    EXPECT_EQ(claimMount(ops.op, l, "r", UInt128(1), 9, 1200, 100).kind, MountClaimResult::LiveDoubleStart);
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
    EXPECT_NE(msg.find("<cas_server_root_id>"), std::string::npos);
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
    /// Token-stability liveness statement (replaces the old wall-clock CLOCK SKEW caveat) + manual
    /// mount-object delete escape hatch + the unsafe-knob escape hatch.
    EXPECT_NE(msg.find("own clock"), std::string::npos);
    EXPECT_NE(msg.find("diagnostic"), std::string::npos);
    EXPECT_NE(msg.find("manually delete the mount"), std::string::npos);
    EXPECT_NE(msg.find("gc/server-roots/replica-a/mount"), std::string::npos);
    EXPECT_NE(msg.find("cas_unsafe_remount_no_delay"), std::string::npos);
}

/// rev.6: a stamped `expires_at_ms` that already looks past-due on our wall clock must NOT shortcut
/// the observation wait — the old "instant, zero-sleep" reclaim this test name described was exactly
/// the cross-node wall-clock trust rev.6 removes. Renamed to describe the CORRECTED behavior: the
/// wall-clock-looking-expired stamp buys nothing, the full threshold is still observed.
TEST(CASMountAwaitExpiry, PastExpiryStillPaysTheFullObservationThreshold)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    /// A prior incarnation (uuid=1, epoch=7) claimed a lease live until 1100.
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 100).kind, MountClaimResult::Claimed);

    uint64_t wall = 1200;                // already past 1100 on wall clock — irrelevant to the decision
    uint64_t mono = 0;
    int sleeps = 0;
    auto now_fn = [&] { return wall; };
    auto mono_fn = [&] { return mono; };
    auto sleep_fn = [&](uint64_t ms) { wall += ms; mono += ms; ++sleeps; };

    const auto r = claimMountAwaitingExpiry(
        ops.op, l, "r", UInt128(1), /*our_epoch*/ 8, now_fn, mono_fn, /*ttl*/ 100, /*poll*/ 25, sleep_fn);
    EXPECT_EQ(r.kind, MountClaimResult::Claimed);
    EXPECT_GT(sleeps, 0);                                    // NOT instant — no wall-clock trust
    EXPECT_GE(mono, 100 + 100 / 20 + 25);                     // full observation threshold paid
    EXPECT_EQ(decodeMountLease(ops.op.read(l.mountKey("r"), Retry::standard())->bytes).writer_epoch, 8u);   // reclaimed as us
}

TEST(CASMountAwaitExpiry, FutureExpiryReclaimsAfterClockAdvances)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 100).kind, MountClaimResult::Claimed);

    uint64_t wall = 1000;                // lease looks live until 1100, holder does NOT renew
    uint64_t mono = 0;
    auto now_fn = [&] { return wall; };
    auto mono_fn = [&] { return mono; };
    auto sleep_fn = [&](uint64_t ms) { wall += ms; mono += ms; };

    const auto r = claimMountAwaitingExpiry(
        ops.op, l, "r", UInt128(1), /*our_epoch*/ 8, now_fn, mono_fn, /*ttl*/ 100, /*poll*/ 50, sleep_fn);
    EXPECT_EQ(r.kind, MountClaimResult::Claimed);
    const auto body = decodeMountLease(ops.op.read(l.mountKey("r"), Retry::standard())->bytes);
    EXPECT_EQ(body.writer_epoch, 8u);
    EXPECT_EQ(body.seq, 2u);                                         // reclaim continues seq (prev 1 + 1)
}

/// rev.6: a genuinely live twin now times out via BOUNDED OBSERVATION RESTARTS (its every renewal
/// bumps the write-token, forcing a restart each poll), never via a wall-clock deadline.
TEST(CASMountAwaitExpiry, LiveRenewingTwinTimesOutAsDoubleStart)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 100).kind, MountClaimResult::Claimed);

    uint64_t wall = 1000;
    uint64_t mono = 0;
    auto now_fn = [&] { return wall; };
    auto mono_fn = [&] { return mono; };
    /// Each poll: both clocks advance AND the live holder (uuid=1, epoch=7) renews its own lease —
    /// the observed incarnation changes on EVERY poll, forcing a restart every time.
    auto sleep_fn = [&](uint64_t ms)
    {
        wall += ms;
        mono += ms;
        ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), 7, wall, 100).kind, MountClaimResult::Claimed);
    };

    const auto r = claimMountAwaitingExpiry(
        ops.op, l, "r", UInt128(1), /*our_epoch*/ 8, now_fn, mono_fn, /*ttl*/ 100, /*poll*/ 20, sleep_fn);
    EXPECT_EQ(r.kind, MountClaimResult::LiveDoubleStart);
    EXPECT_EQ(decodeMountLease(ops.op.read(l.mountKey("r"), Retry::standard())->bytes).writer_epoch, 7u);   // still the holder's
}

namespace
{
/// fix-round F5 harness: makes the mount key vanish to EVERY read, unconditionally, while the real
/// underlying object stays put -- forcing `claimMount`'s own read to take the absent-slot race
/// branch every call (its create then conflicts against the real, still-present object, returning
/// `LiveDoubleStart` with no incarnation -- that branch deliberately leaves `.etag` unset,
/// since no re-read was done). That in turn forces `claimMountAwaitingExpiry`'s fallback re-read,
/// which ALSO sees the slot as vanished -- deterministically reproducing "the slot vanished between
/// claimMount's own read and ours" on EVERY loop iteration, not just a lucky one-shot race.
class AlwaysVanishesBackend final : public DB::Cas::Backend
{
public:
    explicit AlwaysVanishesBackend(std::shared_ptr<DB::Cas::Backend> inner_) : inner(std::move(inner_)) {}
    String watched_key;

    bool supportsListTokens() const override { return inner->supportsListTokens(); }

    /// The fault is on the read primitive, which is the only way anything now reaches the store.
    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (key == watched_key)
            return std::nullopt;
        return inner->read(key, access);
    }
    std::optional<RawMeta> head(const String & key, TransportAccess & access) override { return inner->head(key, access); }
    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override { return inner->list(prefix, cursor, limit, access); }
    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override { return inner->remove(key, expected_value, access); }
    void removeManyWriteOnce(const std::vector<WriteOnceKey> & keys, TransportAccess & access) override { inner->removeManyWriteOnce(keys, access); }
    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value, TransportAccess & access) override
    {
        return inner->write(key, bytes, expected_value, access);
    }
    std::unique_ptr<DB::ReadBuffer> stream(const String & key, TransportAccess & access) override { return inner->stream(key, access); }
    void publish(const BlobPublishRequest & request, TransportAccess & access) override { inner->publish(request, access); }
    Dialect dialect() const override { return inner->dialect(); }

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
    Ops inner_ops(inner);
    /// A real slot exists underneath (uuid 1, epoch 7) so `claimMount`'s absent-slot create
    /// genuinely conflicts every time (never accidentally re-mints).
    ASSERT_EQ(claimMount(inner_ops.op, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 100).kind, MountClaimResult::Claimed);

    auto vanishing = std::make_shared<AlwaysVanishesBackend>(inner);
    vanishing->watched_key = l.mountKey("r");
    Ops ops(vanishing);

    uint64_t wall = 1000;
    uint64_t mono = 0;
    int sleeps = 0;
    auto now_fn = [&] { return wall; };
    auto mono_fn = [&] { return mono; };
    auto sleep_fn = [&](uint64_t ms) { wall += ms; mono += ms; ++sleeps; };

    const auto r = claimMountAwaitingExpiry(
        ops.op, l, "r", UInt128(1), /*our_epoch*/ 8, now_fn, mono_fn, /*ttl*/ 100, /*poll*/ 20, sleep_fn);
    EXPECT_EQ(r.kind, MountClaimResult::LiveDoubleStart) << "must terminate (bounded), not loop forever";
    EXPECT_GT(sleeps, 0) << "a persistently vanishing slot must still pace via sleep_fn, not busy-spin";
    /// The real epoch-7 lease is untouched -- every create attempt against it genuinely conflicts
    /// (the object is still there), so it is never accidentally re-minted over.
    EXPECT_EQ(decodeMountLease(inner_ops.op.read(l.mountKey("r"), Retry::standard())->bytes).writer_epoch, 7u);
}

TEST(CASMountAwaitExpiry, ForeignUuidFailsClosedImmediately)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    /// A foreign server (uuid=2) holds the mount.
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(2), 1, /*now*/ 1000, /*ttl*/ 100).kind, MountClaimResult::Claimed);

    uint64_t now = 1000;
    int sleeps = 0;
    auto now_fn = [&] { return now; };
    auto mono_fn = [&] { return uint64_t{0}; };
    auto sleep_fn = [&](uint64_t ms) { now += ms; ++sleeps; };

    const auto r = claimMountAwaitingExpiry(
        ops.op, l, "r", UInt128(1), /*our_epoch*/ 8, now_fn, mono_fn, /*ttl*/ 100, /*poll*/ 25, sleep_fn);
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
    Ops ops(b);
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 100000).kind, MountClaimResult::Claimed);

    uint64_t wall = 1000;
    uint64_t mono = 0;
    auto now_fn = [&] { return wall; };
    auto mono_fn = [&] { return mono; };
    auto sleep_fn = [&](uint64_t ms) { wall += ms; mono += ms; };

    const auto r = claimMountAwaitingExpiry(
        ops.op, l, "r", UInt128(1), /*our_epoch*/ 8, now_fn, mono_fn, /*ttl*/ 100, /*poll*/ 20, sleep_fn);
    EXPECT_EQ(r.kind, MountClaimResult::Claimed);
    EXPECT_LE(mono, 100u + 100u / 20 + 20u + 20u);      // bounded by OUR threshold, not the predecessor's stamp
    EXPECT_EQ(decodeMountLease(ops.op.read(l.mountKey("r"), Retry::standard())->bytes).writer_epoch, 8u);   // reclaimed
}

TEST(CASMountClaim, UnsafeAuthorizationIsTokenExact)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 30000).kind, MountClaimResult::Claimed);
    const Etag stale = ops.op.read(l.mountKey("r"), Retry::standard())->etag;

    /// `Etag` equality compares (key, value), and it is minted only through the request planes -- there
    /// is no cross-key comparison to exercise here. Build the stale token by refreshing the SAME slot a
    /// second time (same uuid, same epoch): `stale`, read before this refresh, is then a genuinely stale
    /// token for the slot's CURRENT value, without touching an unrelated key.
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), 7, /*now*/ 1000, /*ttl*/ 30000).kind, MountClaimResult::Claimed);
    const Etag current = ops.op.read(l.mountKey("r"), Retry::standard())->etag;

    /// A stale token is refused: nothing authorizes a reclaim over a slot that moved.
    const MountClaimResult refused = claimMount(ops.op, l, "r", UInt128(1), 8, 1000, 30000, /*proven_dead=*/{},
                                                /*sink=*/{}, /*unsafe_reclaim_authorization=*/stale);
    EXPECT_EQ(refused.kind, MountClaimResult::LiveDoubleStart);

    /// A foreign uuid is refused before the authorization is consulted.
    const MountClaimResult foreign = claimMount(ops.op, l, "r", UInt128(2), 8, 1000, 30000, {}, {}, current);
    EXPECT_EQ(foreign.kind, MountClaimResult::ForeignOwner);

    /// The exact token reclaims, with the prior state and the audit reason naming the setting.
    std::vector<CasEvent> events;
    const MountClaimResult reclaimed = claimMount(ops.op, l, "r", UInt128(1), 8, 1000, 30000, {},
                                                  [&](CasEvent e) { events.push_back(std::move(e)); }, current);
    ASSERT_EQ(reclaimed.kind, MountClaimResult::Claimed);
    EXPECT_EQ(reclaimed.prior, MountPriorState::UncleanUnsafe);
    ASSERT_FALSE(events.empty());
    EXPECT_THAT(events.back().reason, testing::HasSubstr("cas_unsafe_remount_no_delay"));
    /// Pin the fields `system.cas_log` consumers actually key on, not just the free-form reason: the
    /// unsafe reclaim shares the same event type and outcome as every other reclaim (`emitMountEvent`'s
    /// "reclaim" branch argument), so nothing about this path is a separate, unaudited channel.
    EXPECT_EQ(events.back().type, CasEventType::MountClaim);
    EXPECT_EQ(events.back().outcome, "reclaim");
    EXPECT_EQ(decodeMountLease(ops.op.read(l.mountKey("r"), Retry::standard())->bytes).writer_epoch, 8u);
}

TEST(CASMountLease, RenewerStartAdoptsOurOwnClaimNotDoubleStart)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    uint64_t now = 1000;
    Ops ops(b);
    // The normal flow: claimMount writes the live mount under (uuid=1, epoch=7), THEN renewer.start().
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), /*epoch*/ 7, now, /*ttl*/ 100).kind, MountClaimResult::Claimed);
    MountLeaseRenewer k(ops.mount, ops.farewell, l, "r", UInt128(1), /*epoch*/ 7, std::chrono::milliseconds(100),
                       [&] { return now; }, [] { return uint64_t{0}; });
    EXPECT_NO_THROW(k.start());     // adopts our own live (uuid=1,epoch=7) mount — NOT a double-start
    EXPECT_EQ(decodeMountLease(ops.op.read(l.mountKey("r"), Retry::standard())->bytes).writer_epoch, 7u);
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

    /// Simulate shutdown: the Pool dtor stops the renewer, whose terminate() retires the lease
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

    Ops ops(backend);
    const auto catalog = ops.op.read(layout.refCatalogKey(), Retry::standard());
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

    Ops ops(backend);
    /// Old raw fixtures did not persist an empty catalog. Make this an explicit existing-pool
    /// fixture before removing the mandatory object whose loss the mount must reject.
    if (!ops.op.head(layout.refCatalogKey(), Retry::standard()))
        mustCommit(ops.op.create(layout.refCatalogKey(), encodeRefCatalog(RefCatalog{}), Retry::standard()),
                   "empty catalog");
    ASSERT_EQ(ops.op.removeCurrent(layout.refCatalogKey(), Retry::standard()), Removal::Removed);

    const auto owner_before = ops.op.read(layout.ownerKey("r"), Retry::standard());
    const auto epoch_before = ops.op.read(layout.epochKey("r"), Retry::standard());
    const auto mount_before = ops.op.read(layout.mountKey("r"), Retry::standard());
    ASSERT_TRUE(owner_before.has_value());
    ASSERT_TRUE(epoch_before.has_value());
    ASSERT_TRUE(mount_before.has_value());

    EXPECT_THROW(Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r",
        .skip_access_check = true}), DB::Exception);

    const auto owner_after = ops.op.read(layout.ownerKey("r"), Retry::standard());
    const auto epoch_after = ops.op.read(layout.epochKey("r"), Retry::standard());
    const auto mount_after = ops.op.read(layout.mountKey("r"), Retry::standard());
    ASSERT_TRUE(owner_after.has_value());
    ASSERT_TRUE(epoch_after.has_value());
    ASSERT_TRUE(mount_after.has_value());
    EXPECT_EQ(owner_after->bytes, owner_before->bytes);
    EXPECT_EQ(owner_after->etag, owner_before->etag);
    EXPECT_EQ(epoch_after->bytes, epoch_before->bytes);
    EXPECT_EQ(epoch_after->etag, epoch_before->etag);
    EXPECT_EQ(mount_after->bytes, mount_before->bytes);
    EXPECT_EQ(mount_after->etag, mount_before->etag);
}

TEST(CASMountReadOnly, ForeignOwnedPoolOpensWithoutMutation)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");

    /// Server A claims the pool (writable): owner = uuid(1), a durable epoch + a live mount lease.
    auto a = Pool::open(b, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r"});

    Ops ops(b);
    /// Capture the control objects BEFORE the read-only open so we can prove it mutated nothing.
    const auto owner_before = ops.op.read(l.ownerKey("r"), Retry::standard());
    const auto mount_before = ops.op.read(l.mountKey("r"), Retry::standard());
    const auto epoch_before = ops.op.read(l.epochKey("r"), Retry::standard());
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
    const auto owner_after = ops.op.read(l.ownerKey("r"), Retry::standard());
    const auto mount_after = ops.op.read(l.mountKey("r"), Retry::standard());
    const auto epoch_after = ops.op.read(l.epochKey("r"), Retry::standard());
    ASSERT_TRUE(owner_after.has_value());
    ASSERT_TRUE(mount_after.has_value());
    ASSERT_TRUE(epoch_after.has_value());

    EXPECT_EQ(decodeOwner(owner_after->bytes).server_uuid, UInt128(1));
    EXPECT_EQ(decodeMountLease(mount_after->bytes).server_uuid, UInt128(1));

    EXPECT_EQ(owner_after->bytes, owner_before->bytes);
    EXPECT_EQ(mount_after->bytes, mount_before->bytes);
    EXPECT_EQ(epoch_after->bytes, epoch_before->bytes);
}

/// `validateCasRequestBudget` itself, isolated from `Pool::open`: a consistent default budget is
/// accepted silently, and the overflow-safe comparison (subtraction against the TTL rather than
/// computing `attempt_timeout_ms + lease_safety_margin_ms` directly) really does reject an absurd
/// near-`UINT64_MAX` config rather than letting the sum wrap to a spuriously small value that would
/// pass the inequality when it should fail closed.
TEST(CASRequestBudget, ValidateAcceptsDefaultsAndRejectsAnOverflowingSumWithoutWrapping)
{
    EXPECT_NO_THROW(validateCasRequestBudget(
        CasRequestBudget{}, /*mount_lease_ttl_ms=*/30000, /*mount_renew_period_ms=*/10000, /*background_renewal=*/false));

    const CasRequestBudget overflowing{
        .attempt_timeout_ms = std::numeric_limits<uint64_t>::max() - 100,
        .lease_safety_margin_ms = std::numeric_limits<uint64_t>::max() - 100};
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&]
    {
        validateCasRequestBudget(overflowing, /*mount_lease_ttl_ms=*/30000, /*mount_renew_period_ms=*/10000, /*background_renewal=*/false);
    });
}

/// Pool::open must call validateCasRequestBudget itself (not just the free function in isolation,
/// pinned directly above): an inconsistent cas_request_budget must refuse a writable mount end-to-end
/// (RFC cas-s3-timeout-retry-control §required-timeout-model), never mount silently with a budget that
/// could let a controlled attempt outlive the lease it is fenced under.
TEST(CASMountStartup, RefusesWritableOpenWithInconsistentCasRequestBudget)
{
    auto b = std::make_shared<InMemoryBackend>();

    /// attempt_timeout_ms + lease_safety_margin_ms == mount_lease_ttl_ms below (30000): not STRICTLY
    /// less, so this must be rejected.
    const CasRequestBudget bad_budget{
        .attempt_timeout_ms = 25000, .lease_safety_margin_ms = 5000};
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
        .attempt_timeout_ms = 50, .lease_safety_margin_ms = 50, .connect_timeout_cap_ms = std::nullopt};
    auto a = Pool::open(b, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r",
        .mount_lease_ttl_ms = std::chrono::milliseconds(300),
        .mount_renew_period = std::chrono::milliseconds(100),
        .cas_request_budget = tiny_budget});
    ASSERT_NE(a, nullptr);
    const uint64_t e1 = a->writerEpoch();
    const String mount_key = a->layout().mountKey("r");
    Ops ops(b);
    const auto stale_mount = ops.op.read(mount_key, Retry::standard());
    ASSERT_TRUE(stale_mount.has_value());

    /// Preserve A's live lease as if its process disappeared without running C++ teardown. Destroying
    /// the real Pool first keeps the parent process valid; replaying the saved body recreates the exact
    /// durable stale-lease state that a crashed process would leave behind.
    a.reset();
    const auto farewell = ops.op.read(mount_key, Retry::standard());
    ASSERT_TRUE(farewell.has_value());
    mustCommit(ops.op.replace(mount_key, stale_mount->bytes, farewell->etag, Retry::standard()),
               "replayed stale lease");

    /// A restart of the SAME server (same uuid) must NOT abort: it waits out the stale lease (<= ~300ms)
    /// and reclaims the mount, coming up with a strictly higher durable writer_epoch. The replayed live
    /// body hides A's clean farewell, so the reclaim is `MountPriorState::UncleanObserved`. Inject a
    /// fake `boot_ms_fn` + `wait_sleep_fn` (mirroring
    /// `CASMountOpenWaits.UncleanOpenPaysOnlyTheObservationWindow`) so the observation window resolves
    /// instantly instead of blocking this test on real time.
    /// Held in a shared atomic, not a plain local: `wait_sleep_fn` below mutates it, and the Pool can
    /// outlive this stack frame (a background publish holds `shared_from_this()`), so a by-reference
    /// capture of a local would dangle.
    auto a2_fake_boot = std::make_shared<std::atomic<uint64_t>>(0);
    PoolPtr a2;
    EXPECT_NO_THROW(
        a2 = Pool::open(b, PoolConfig{
            .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r",
            .mount_lease_ttl_ms = std::chrono::milliseconds(300),
            .mount_renew_period = std::chrono::milliseconds(100),
            .cas_request_budget = tiny_budget,
            .boot_ms_fn = [a2_fake_boot]
            {
                return a2_fake_boot->load();
            },
            .wait_sleep_fn = [a2_fake_boot](uint64_t ms)
            {
                *a2_fake_boot += ms;
            }}));
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

    /// Held in a shared atomic, not a plain local: `wait_sleep_fn` below mutates it, and the Pool can
    /// outlive this stack frame (a background publish holds `shared_from_this()`), so a by-reference
    /// capture of a local would dangle.
    auto overlap_fake_boot = std::make_shared<std::atomic<uint64_t>>(0);
    auto replacement = Pool::open(overlap_backend, PoolConfig{
        .pool_prefix = "p", .server_id = UInt128(1), .server_root_id = "r",
        .mount_lease_ttl_ms = std::chrono::milliseconds(300),
        .mount_renew_period = std::chrono::milliseconds(100),
        .cas_request_budget = tiny_budget,
        .boot_ms_fn = [overlap_fake_boot]
        {
            return overlap_fake_boot->load();
        },
        .wait_sleep_fn = [overlap_fake_boot](uint64_t ms)
        {
            *overlap_fake_boot += ms;
        }});
    ASSERT_NE(replacement, nullptr);

    Ops overlap_ops(overlap_backend);
    const auto reclaimer_slot_before = overlap_ops.op.read(overlap_mount_key, Retry::standard());
    ASSERT_TRUE(reclaimer_slot_before.has_value());
    const uint64_t overlap_violations_before
        = ProfileEvents::global_counters[ProfileEvents::CASMountExclusivityViolation].load();

    first.reset();   /// must not abort, must not terminate

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASMountExclusivityViolation].load(),
              overlap_violations_before + 1);
    const auto reclaimer_slot_after = overlap_ops.op.read(overlap_mount_key, Retry::standard());
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
    m.min_active_build_sequence = 5;
    m.gc_fenced = true;
    m.write_attempt_id = UInt128{1};
    const MountLease d = decodeMountLease(encodeMountLease(m));
    EXPECT_EQ(d.min_active_build_sequence, 5u);
    EXPECT_TRUE(d.gc_fenced);
    EXPECT_EQ(d.writer_epoch, 7u);
}

TEST(CASMountLease, RetiredSentinelRoundTrips)
{
    MountLease m;
    m.min_active_build_sequence = std::numeric_limits<uint64_t>::max();
    m.write_attempt_id = UInt128{1};
    EXPECT_EQ(decodeMountLease(encodeMountLease(m)).min_active_build_sequence,
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

/// Seed one mount body under mountKey(srid) via the on-storage codec — the same interface the renewer
/// writes through.
MountLease seedMount(
    CasOperation & op, const Layout & l, const String & srid,
    uint64_t expires_at_ms, bool gc_fenced, uint64_t min_active_build_sequence, uint64_t seq = 1)
{
    MountLease m;
    m.server_uuid = UInt128(srid.back());   // distinct per srid; content is irrelevant to the gate
    m.writer_epoch = 1;
    m.hostname = "h-" + srid;
    m.pid = 100;
    m.started_at_ms = kNowMs;
    m.seq = seq;
    m.expires_at_ms = expires_at_ms;
    m.min_active_build_sequence = min_active_build_sequence;
    m.gc_fenced = gc_fenced;
    m.write_attempt_id = UInt128{1};
    mustCommit(op.create(l.mountKey(srid), encodeMountLease(m), Retry::standard()), "seeded mount " + srid);
    return m;
}

/// Simulate a renewer's real renewal between two `computeHeartbeatFloor` calls: a guarded write that
/// bumps `seq` (and so mints a fresh incarnation), leaving everything else as-is. Models the one
/// thing the observation-based fence cares about: the incarnation changed, so any in-progress
/// observation of the OLD one must restart.
void renewMount(CasOperation & op, const Layout & l, const String & srid)
{
    const auto got = op.read(l.mountKey(srid), Retry::standard());
    ASSERT_TRUE(got.has_value());
    MountLease m = decodeMountLease(got->bytes);
    m.seq += 1;
    mustCommit(op.replace(l.mountKey(srid), encodeMountLease(m), got->etag, Retry::standard()),
               "renewed mount " + srid);
}
}

TEST(CASHeartbeatFloor, FirstSightNeverFencesEvenIfStampLooksExpired)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");

    Ops ops(b);
    /// A stamp that would have read as long-expired under the old skew-margin comparison — under
    /// rev.6 observation the stamp is never even consulted for the fence decision.
    seedMount(ops.op, l, "s1", /*expires*/ 10, /*fenced*/ false, /*min_active_build_sequence*/ 0);

    MountObservationMap obs;
    const HeartbeatFloor floor = computeHeartbeatFloor(ops.op, l, /*now_ms*/ kNowMs, /*mono_now_ms*/ 0,
                                                         kStableThresholdMs, obs);

    EXPECT_EQ(floor.fenced_now, 0u);
    EXPECT_EQ(floor.live, 1u);
    ASSERT_TRUE(obs.contains("s1"));
    EXPECT_EQ(obs.at("s1").first_seen_mono_ms, 0u);
}

TEST(CASHeartbeatFloor, StableIncarnationPastThresholdIsFenced)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    seedMount(ops.op, l, "s1", /*expires*/ 10, /*fenced*/ false, /*min_active_build_sequence*/ 0);

    MountObservationMap obs;
    const HeartbeatFloor floor_before = computeHeartbeatFloor(ops.op, l, kNowMs, /*mono*/ 0, kStableThresholdMs, obs);
    EXPECT_EQ(floor_before.fenced_now, 0u);

    const MountLease before = decodeMountLease(ops.op.read(l.mountKey("s1"), Retry::standard())->bytes);

    /// No renewal in between: the SAME incarnation, observed since mono 0, is now stable for the full
    /// threshold on the leader's own clock.
    const HeartbeatFloor floor2 = computeHeartbeatFloor(ops.op, l, kNowMs, /*mono*/ kStableThresholdMs,
                                                          kStableThresholdMs, obs);

    EXPECT_EQ(floor2.fenced_now, 1u);
    EXPECT_EQ(floor2.fenced_srids, std::vector<String>{"s1"});
    const MountLease fenced = decodeMountLease(ops.op.read(l.mountKey("s1"), Retry::standard())->bytes);
    EXPECT_TRUE(fenced.gc_fenced);
    EXPECT_EQ(fenced.seq, before.seq + 1);
}

TEST(CASHeartbeatFloor, RenewalBetweenRoundsRestartsObservation)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");
    Ops ops(b);
    seedMount(ops.op, l, "s1", /*expires*/ 10, /*fenced*/ false, /*min_active_build_sequence*/ 0);

    MountObservationMap obs;
    computeHeartbeatFloor(ops.op, l, kNowMs, /*mono*/ 0, kStableThresholdMs, obs);
    ASSERT_TRUE(obs.contains("s1"));
    const Etag first_etag = obs.at("s1").etag;

    renewMount(ops.op, l, "s1");
    const Etag renewed_etag = currentEtag(ops.op, l.mountKey("s1"));
    EXPECT_NE(renewed_etag, first_etag);

    const HeartbeatFloor floor2 = computeHeartbeatFloor(ops.op, l, kNowMs, /*mono*/ kStableThresholdMs,
                                                          kStableThresholdMs, obs);

    EXPECT_EQ(floor2.fenced_now, 0u);
    ASSERT_TRUE(obs.contains("s1"));
    EXPECT_EQ(obs.at("s1").etag, renewed_etag);
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
    Ops ops(b);
    seedMount(ops.op, l, "s1", /*expires*/ 10, /*fenced*/ false, /*min_active_build_sequence*/ 0);
    seedMount(ops.op, l, "s2", /*expires*/ 10, /*fenced*/ false, /*min_active_build_sequence*/ 0);

    MountObservationMap obs;
    computeHeartbeatFloor(ops.op, l, kNowMs, /*mono*/ 0, kStableThresholdMs, obs);
    ASSERT_TRUE(obs.contains("s1"));
    ASSERT_TRUE(obs.contains("s2"));

    /// s2's `/mount` key is removed entirely -- e.g. `SYSTEM CAS DROP POOL MEMBER` -- so
    /// no future LIST pass will ever visit it again. s1 renews (a live renewer would), so its OWN
    /// observation restarts and it stays `live` -- isolating this test to the pruning behavior alone,
    /// not confounding it with s1 also becoming fence-eligible (which would erase its `obs` entry too,
    /// for an unrelated reason).
    renewMount(ops.op, l, "s1");
    ASSERT_EQ(ops.op.removeCurrent(l.mountKey("s2"), Retry::standard()), Removal::Removed);

    computeHeartbeatFloor(ops.op, l, kNowMs, /*mono*/ kStableThresholdMs, kStableThresholdMs, obs);
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
    Ops ops(b);
    seedMount(ops.op, l, "s1", /*expires*/ kNowMs + 60'000, /*fenced*/ false, /*min_active_build_sequence*/ 0);
    seedMount(ops.op, l, "s2", /*expires*/ kNowMs + 60'000, /*fenced*/ false, /*min_active_build_sequence*/ 0);
    /// dead — no renewal between the two rounds below — must be fenced-out by the second call.
    seedMount(ops.op, l, "s3", /*expires*/ kNowMs - 60'000, /*fenced*/ false, /*min_active_build_sequence*/ 0);
    /// already-fenced — excluded, body byte-identical after both calls (no write).
    seedMount(ops.op, l, "s4", /*expires*/ kNowMs - 60'000, /*fenced*/ true, /*min_active_build_sequence*/ 0);
    /// terminated (min_active_build_sequence == UINT64_MAX) with expired-looking timestamps — excluded, not fenced.
    seedMount(ops.op, l, "s5", /*expires*/ kNowMs - 60'000, /*fenced*/ false,
              /*min_active_build_sequence*/ std::numeric_limits<uint64_t>::max());

    MountObservationMap obs;

    /// Round 1 (mono 0): first sight of every non-terminal mount — nothing is fence-eligible yet.
    const HeartbeatFloor floor_before = computeHeartbeatFloor(ops.op, l, kNowMs, /*mono*/ 0, kStableThresholdMs, obs);
    EXPECT_EQ(floor_before.live, 3u);            // s1, s2, s3: observation just started
    EXPECT_EQ(floor_before.terminated, 1u);      // s5
    EXPECT_EQ(floor_before.fenced_now, 0u);
    EXPECT_EQ(floor_before.already_fenced, 1u);  // s4

    /// s1 and s2 renew between rounds (as a live renewer would); s3 does not (it crashed).
    renewMount(ops.op, l, "s1");
    renewMount(ops.op, l, "s2");

    const auto s3_before = ops.op.read(l.mountKey("s3"), Retry::standard());
    const auto s4_before = ops.op.read(l.mountKey("s4"), Retry::standard());
    ASSERT_TRUE(s3_before.has_value());
    ASSERT_TRUE(s4_before.has_value());

    /// Round 2 (mono == threshold): s1/s2's renewed incarnations restart their observation (still
    /// live); s3's original incarnation has now held stable for the full threshold -> fenced.
    const HeartbeatFloor floor2 = computeHeartbeatFloor(ops.op, l, kNowMs, /*mono*/ kStableThresholdMs,
                                                          kStableThresholdMs, obs);

    EXPECT_EQ(floor2.live, 2u);            // s1, s2: renewed, observation restarted
    EXPECT_EQ(floor2.terminated, 1u);      // s5
    EXPECT_EQ(floor2.fenced_now, 1u);      // s3
    EXPECT_EQ(floor2.already_fenced, 1u);  // s4

    /// The dead body was fenced: gc_fenced set, seq bumped, the rest of the body preserved.
    const auto s3_after = ops.op.read(l.mountKey("s3"), Retry::standard());
    ASSERT_TRUE(s3_after.has_value());
    const MountLease s3_prev = decodeMountLease(s3_before->bytes);
    const MountLease s3_now = decodeMountLease(s3_after->bytes);
    EXPECT_TRUE(s3_now.gc_fenced);
    EXPECT_EQ(s3_now.seq, s3_prev.seq + 1);
    EXPECT_EQ(s3_now.server_uuid, s3_prev.server_uuid);
    EXPECT_EQ(s3_now.writer_epoch, s3_prev.writer_epoch);
    EXPECT_EQ(s3_now.hostname, s3_prev.hostname);
    EXPECT_EQ(s3_now.expires_at_ms, s3_prev.expires_at_ms);

    /// The already-fenced body was not touched (no write) across either call.
    const auto s4_after = ops.op.read(l.mountKey("s4"), Retry::standard());
    ASSERT_TRUE(s4_after.has_value());
    EXPECT_EQ(s4_after->bytes, s4_before->bytes);
}

namespace
{
/// A delegating backend whose guarded write of the target mount key first performs an inner renewal
/// (a real, correctly-guarded write that pushes expiry far into the future) and THEN delegates — so
/// the caller's fence-out write lands on a stale precondition and is refused. The inner renewal runs
/// exactly once (`renewed`), modelling a holder that renews concurrently in the window between the
/// function's read and its fence-out write.
class RenewOnFenceBackend : public InMemoryBackend
{
public:
    RenewOnFenceBackend(String target_key_, uint64_t renewed_expires_ms_)
        : target_key(std::move(target_key_)), renewed_expires_ms(renewed_expires_ms_)
    {
    }

    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value,
                                             TransportAccess & access) override
    {
        if (expected_value && key == target_key && !renewed)
        {
            renewed = true;
            /// The holder renews under the real current incarnation: fresh far-future expiry.
            const auto got = InMemoryBackend::read(key, access);
            MountLease m = decodeMountLease(got->bytes);
            m.seq += 1;
            m.expires_at_ms = renewed_expires_ms;
            const auto renew = InMemoryBackend::write(key, encodeMountLease(m), got->value, access);
            EXPECT_TRUE(renew.has_value());
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }

private:
    String target_key;
    uint64_t renewed_expires_ms;
    bool renewed = false;
};
}

TEST(CASHeartbeatFloor, FenceOutLosesTheIncarnationRaceAndReclassifiesLive)
{
    Layout l("p");
    auto b = std::make_shared<RenewOnFenceBackend>(
        l.mountKey("s1"), /*renewed_expires*/ kNowMs + 120'000);
    Ops ops(b);

    seedMount(ops.op, l, "s1", /*expires*/ kNowMs - 60'000, /*fenced*/ false, /*min_active_build_sequence*/ 0);

    MountObservationMap obs;
    /// Round 1: first sight, observation starts — never reaches the fence-out path (the race
    /// decorator stays armed for round 2).
    const HeartbeatFloor floor_before = computeHeartbeatFloor(ops.op, l, kNowMs, /*mono*/ 0, kStableThresholdMs, obs);
    EXPECT_EQ(floor_before.fenced_now, 0u);

    /// Round 2: the incarnation has been stable past threshold, so the function attempts the
    /// fence-out. The decorator renews concurrently under the real incarnation, the write is refused,
    /// and the re-decision reclassifies the slot as live (observation restarted on the new
    /// incarnation) — never fenced.
    const HeartbeatFloor floor2 = computeHeartbeatFloor(ops.op, l, kNowMs, /*mono*/ kStableThresholdMs,
                                                          kStableThresholdMs, obs);

    EXPECT_EQ(floor2.fenced_now, 0u);
    EXPECT_EQ(floor2.live, 1u);

    const auto after = ops.op.read(l.mountKey("s1"), Retry::standard());
    ASSERT_TRUE(after.has_value());
    EXPECT_FALSE(decodeMountLease(after->bytes).gc_fenced);
}

TEST(CASHeartbeatFloor, EmptyPrefixYieldsNoLiveMounts)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l("p");

    Ops ops(b);
    MountObservationMap obs;
    const HeartbeatFloor floor = computeHeartbeatFloor(ops.op, l, kNowMs, /*mono*/ 0, kStableThresholdMs, obs);

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

    Ops ops(backend);
    /// live: fresh claim for srid "a"
    ASSERT_EQ(claimMount(ops.op, layout, "a", UInt128{1}, /*our_epoch=*/1, now_ms, ttl_ms).kind,
              MountClaimResult::Claimed);
    /// expired: claim for "b" whose lease ran out long before now_ms
    ASSERT_EQ(claimMount(ops.op, layout, "b", UInt128{2}, 1, now_ms - 100'000, ttl_ms).kind,
              MountClaimResult::Claimed);
    /// corrupt: garbage bytes in "c"'s mount slot
    mustCommit(ops.op.create(layout.mountKey("c"), "garbage-not-a-proto", Retry::standard()), "corrupt slot");

    auto mounts = listMounts(ops.op, layout, now_ms, /*skew_margin_ms=*/ttl_ms / 2);
    ASSERT_EQ(mounts.size(), 3u);
    std::map<String, String> by_srid;
    for (const auto & m : mounts)
        by_srid[m.srid] = m.state;
    EXPECT_EQ(by_srid["a"], "live");
    EXPECT_EQ(by_srid["b"], "expired");
    EXPECT_EQ(by_srid["c"], "corrupt");

    /// READ-ONLY guarantee: "b" is expired but must NOT be fenced by listMounts
    /// (computeHeartbeatFloor would stamp gc_fenced=true; the introspection view must not).
    auto again = listMounts(ops.op, layout, now_ms, ttl_ms / 2);
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

    Ops ops(backend);
    ASSERT_EQ(claimMount(ops.op, layout, "shard-01/replica-a", UInt128{1}, /*our_epoch=*/1, now_ms, ttl_ms).kind,
              MountClaimResult::Claimed);

    auto mounts = listMounts(ops.op, layout, now_ms, /*skew_margin_ms=*/ttl_ms / 2);
    ASSERT_EQ(mounts.size(), 1u);
    EXPECT_EQ(mounts[0].srid, "shard-01/replica-a");
    EXPECT_EQ(mounts[0].state, "live");
}

/// "A fence costs an epoch": a same-(uuid, epoch) re-claim must NOT refresh a `gc_fenced` body in
/// place — that would reactivate a fenced incarnation. It is terminal for THIS epoch; only a
/// DIFFERENT (fresh) epoch may reclaim the slot.
TEST(CASClaimMount, SameEpochFencedIsNotRefreshable)
{
    using namespace DB::Cas;
    auto backend = std::make_shared<InMemoryBackend>();
    Layout layout("pool");
    Ops ops(backend);
    /// mint for (uuid 1, epoch 1), then fence it in place (what computeHeartbeatFloor does):
    ASSERT_EQ(claimMount(ops.op, layout, "a", DB::UInt128{1}, 1, 1000, 10'000).kind,
              MountClaimResult::Claimed);
    {
        auto got = ops.op.read(layout.mountKey("a"), Retry::standard());
        MountLease fenced = decodeMountLease(got->bytes);
        fenced.gc_fenced = true;
        fenced.seq += 1;
        mustCommit(ops.op.replace(layout.mountKey("a"), encodeMountLease(fenced), got->etag, Retry::standard()),
                   "fence-out");
    }
    /// Same (uuid, epoch) re-claim must NOT refresh a fenced body — a fence costs an epoch:
    const auto r = claimMount(ops.op, layout, "a", DB::UInt128{1}, 1, 2000, 10'000);
    EXPECT_EQ(r.kind, MountClaimResult::FencedSelf);
    /// The body on the backend is still the fenced one (no write happened):
    EXPECT_TRUE(decodeMountLease(ops.op.read(layout.mountKey("a"), Retry::standard())->bytes).gc_fenced);
    /// A DIFFERENT epoch reclaims immediately (existing branch, unchanged):
    EXPECT_EQ(claimMount(ops.op, layout, "a", DB::UInt128{1}, 2, 2000, 10'000).kind,
              MountClaimResult::Claimed);
}

/// ---- rev.6 Task 4: observation-based lease reclaim (no cross-node wall-clock trust) ----

/// A same-uuid, different-epoch lease whose STAMPED `expires_at_ms` looks long expired on OUR wall
/// clock must NOT be reclaimed by that comparison alone — a clock-skewed or simply late-observing
/// caller must never trust a bare wall-clock read across incarnations. `claimMount` (without a
/// `proven_dead_incarnation`) always reports `LiveDoubleStart` for this branch now; only the observation
/// loop (`claimMountAwaitingExpiry`) may turn it into a reclaim, and only after proving death on ITS
/// OWN clock.
TEST(CASMountObservation, ExpiredLookingLeaseIsNotReclaimedByWallClock)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l{"p"};
    Ops ops(b);
    /// Predecessor epoch 7 stamped expires_at_ms = 1000; our wall clock says 999999 (long past).
    auto first = claimMount(ops.op, l, "r", UInt128(1), 7, /*now_ms=*/500, /*ttl_ms=*/500);
    ASSERT_EQ(first.kind, MountClaimResult::Claimed);
    auto r = claimMount(ops.op, l, "r", UInt128(1), /*our_epoch=*/8, /*now_ms=*/999999, 500);
    EXPECT_EQ(r.kind, MountClaimResult::LiveDoubleStart);  /// no wall-clock trust
}

/// The observation loop reclaims once the observed incarnation has held stable for the FULL
/// rate-bound threshold (`ttl_ms + ttl_ms/20 + poll_interval_ms`) on its OWN (injected, fake) clock —
/// never short-circuiting on the wall clock, which this test drives to an irrelevant, already-expired
/// value.
TEST(CASMountObservation, IncarnationStableForThresholdThenReclaimed)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l{"p"};
    Ops ops(b);
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), 7, 500, 500).kind, MountClaimResult::Claimed);
    uint64_t mono = 0;
    std::vector<uint64_t> sleeps;
    auto r = claimMountAwaitingExpiry(ops.op, l, "r", UInt128(1), 8,
        []{ return uint64_t{999999}; },                 /// wall clock: irrelevant
        [&]{ return mono; },                             /// observation clock
        /*ttl_ms=*/500, /*poll_interval_ms=*/50,
        [&](uint64_t ms){ sleeps.push_back(ms); mono += ms; });
    EXPECT_EQ(r.kind, MountClaimResult::Claimed);
    EXPECT_EQ(r.prior, MountPriorState::UncleanObserved);
    EXPECT_GE(mono, 500 + 500 / 20 + 50);               /// full threshold actually waited
}

/// A renewal DURING the observation window (the real holder is still alive) mints a new incarnation —
/// the loop must detect the mismatch and RESTART the observation from it, never reclaiming off a
/// window that started watching a now-superseded incarnation.
TEST(CASMountObservation, RenewalDuringObservationRestartsIt)
{
    auto b = std::make_shared<InMemoryBackend>();
    Layout l{"p"};
    uint64_t renewer_boot = 0;
    Ops ops(b, &renewer_boot);
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), 7, 500, 500).kind, MountClaimResult::Claimed);

    /// The real (still-alive) holder's renewer for epoch 7: `start()` adopts the slot `claimMount` just
    /// wrote (no seq bump, per the ADOPT RULE), then a synchronous renewal mints a new incarnation
    /// mid-observation.
    uint64_t renewer_wall = 500;
    MountLeaseRenewer renewer(ops.mount, ops.farewell, l, "r", UInt128(1), 7, std::chrono::milliseconds(500),
                             [&] { return renewer_wall; }, [] { return uint64_t{0}; }, {},
                             std::chrono::milliseconds(0), [&] { return renewer_boot; });
    renewer.start();

    const uint64_t threshold_ms = 500 + 500 / 20 + 50;   /// = 575
    uint64_t mono = 0;
    bool renewed = false;
    int wait_starts = 0;
    auto r = claimMountAwaitingExpiry(ops.op, l, "r", UInt128(1), 8,
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
                renewOrThrow(renewer);
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
    Ops ops(b);
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), 7, 1000, 500).kind, MountClaimResult::Claimed);

    /// Fence it manually (what `computeHeartbeatFloor`'s fence-out does): gc_fenced=true, seq+1,
    /// guarded by the observed incarnation.
    {
        auto got = ops.op.read(l.mountKey("r"), Retry::standard());
        ASSERT_TRUE(got.has_value());
        MountLease fenced = decodeMountLease(got->bytes);
        fenced.gc_fenced = true;
        fenced.seq += 1;
        mustCommit(ops.op.replace(l.mountKey("r"), encodeMountLease(fenced), got->etag, Retry::standard()),
                   "fence-out");
    }

    int sleeps = 0;
    auto r = claimMountAwaitingExpiry(ops.op, l, "r", UInt128(1), /*our_epoch=*/8,
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
    Ops ops(std::make_shared<InMemoryBackend>());
    Layout l{"p"};
    EXPECT_FALSE(isCreatorFenceTerminal(ops.op, l, "never-mounted", 1))
        << "absence proves nothing about liveness -- never waved through";
}

TEST(CASFenceTerminal, UndecodableMountBodyIsNotTerminal)
{
    Ops ops(std::make_shared<InMemoryBackend>());
    Layout l{"p"};
    mustCommit(ops.op.create(l.mountKey("r"), "garbage-not-a-lease", Retry::standard()), "undecodable lease");
    EXPECT_FALSE(isCreatorFenceTerminal(ops.op, l, "r", 1))
        << "an unreadable lease of some other format generation must block, never wave through";
}

TEST(CASFenceTerminal, GcFencedIsTerminal)
{
    Ops ops(std::make_shared<InMemoryBackend>());
    Layout l{"p"};
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), /*our_epoch=*/7, 1000, 500).kind, MountClaimResult::Claimed);
    auto got = ops.op.read(l.mountKey("r"), Retry::standard());
    ASSERT_TRUE(got.has_value());
    MountLease fenced = decodeMountLease(got->bytes);
    fenced.gc_fenced = true;
    mustCommit(ops.op.replace(l.mountKey("r"), encodeMountLease(fenced), got->etag, Retry::standard()),
               "fence-out");

    EXPECT_TRUE(isCreatorFenceTerminal(ops.op, l, "r", 7));
}

TEST(CASFenceTerminal, CleanFarewellIsTerminal)
{
    Ops ops(std::make_shared<InMemoryBackend>());
    Layout l{"p"};
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), /*our_epoch=*/7, 1000, 500).kind, MountClaimResult::Claimed);
    auto got = ops.op.read(l.mountKey("r"), Retry::standard());
    ASSERT_TRUE(got.has_value());
    MountLease retired = decodeMountLease(got->bytes);
    retired.min_active_build_sequence = std::numeric_limits<uint64_t>::max();
    mustCommit(ops.op.replace(l.mountKey("r"), encodeMountLease(retired), got->etag, Retry::standard()),
               "farewell");

    EXPECT_TRUE(isCreatorFenceTerminal(ops.op, l, "r", 7));
}

TEST(CASFenceTerminal, ADifferentLiveWriterEpochIsTerminalForTheOldOne)
{
    Ops ops(std::make_shared<InMemoryBackend>());
    Layout l{"p"};
    /// Slot now held at epoch 8 -- epoch 7's incarnation is superseded regardless of ITS OWN
    /// certificate (neither fenced nor farewelled).
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), /*our_epoch=*/8, 1000, 500).kind, MountClaimResult::Claimed);

    EXPECT_TRUE(isCreatorFenceTerminal(ops.op, l, "r", 7))
        << "a different epoch is currently live at this slot -- epoch 7 can never reclaim it";
    EXPECT_FALSE(isCreatorFenceTerminal(ops.op, l, "r", 8))
        << "epoch 8 IS the current live epoch -- not terminal";
}

/// A merely EXPIRED lease (wall-clock past `expires_at_ms`, same epoch, no certificate) must NOT be
/// treated as terminal -- mirrors `claimMount`'s own refusal to trust a bare timestamp comparison.
TEST(CASFenceTerminal, ExpiredButSameEpochAndUncertifiedIsNotTerminal)
{
    Ops ops(std::make_shared<InMemoryBackend>());
    Layout l{"p"};
    /// A lease whose stamped expiry is already far in the past, same epoch throughout.
    ASSERT_EQ(claimMount(ops.op, l, "r", UInt128(1), /*our_epoch=*/7, /*now_ms=*/0, /*ttl_ms=*/1).kind,
              MountClaimResult::Claimed);

    EXPECT_FALSE(isCreatorFenceTerminal(ops.op, l, "r", 7))
        << "expiry alone is never a certificate of death, exactly like claimMount's own discipline";
}

/// The absent-epoch path's post-conflict recheck is a CHECK, not a blanket refusal, and both halves
/// have to hold: work that became visible across the conflict must block the allocation, and an
/// unchanged, still-empty subtree must let it proceed from the winner's own epoch state. Dropping the
/// recheck breaks the first arm; turning it into an unconditional refusal breaks the second.
TEST(CASServerRoot, AllocateWriterEpochKeepsThePostConflictCorruptionCheck)
{
    const Layout layout("p");
    {
        auto backend = std::make_shared<EpochConflictBackend>(/*reveal_owned_work=*/true);
        Ops ops(backend);
        EXPECT_THROW(allocateWriterEpoch(
            ops.op, layout, "root/x", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), DB::Exception);
        EXPECT_TRUE(backend->fired);
        ASSERT_TRUE(backend->winner_installed);
    }
    {
        auto backend = std::make_shared<EpochConflictBackend>(/*reveal_owned_work=*/false);
        Ops ops(backend);
        EXPECT_EQ(allocateWriterEpoch(
            ops.op, layout, "root/x", EpochMintPolicy::NormalMount, 0, emptyCatalogObservation()), 2u)
            << "the second decision must allocate from the conflict winner's epoch, not refuse outright";
        EXPECT_TRUE(backend->fired);
        ASSERT_TRUE(backend->winner_installed);
        const auto epoch = ops.op.read(layout.epochKey("root/x"), Retry::standard());
        ASSERT_TRUE(epoch.has_value());
        EXPECT_EQ(decodeServerEpoch(epoch->bytes).next_writer_epoch, 3u);
    }
}

/// Adoption costs exactly two requests: one read that both decides the branch and supplies the
/// precondition, and one write. A presence probe ahead of the read, or a second read to recover a
/// precondition the first one already carried, shows up here as a third request.
TEST(CASMountLease, ClaimAdoptIsTwoRequests)
{
    auto backend = std::make_shared<RequestCountingBackend>();
    Layout l("p");
    uint64_t now = 1000;
    Ops ops(backend);

    /// The absent-slot mint.
    backend->reads = backend->heads = backend->writes = 0;
    MountLeaseRenewer minting(ops.mount, ops.farewell, l, "fresh", UInt128(1), 7,
                             std::chrono::milliseconds(100), [&] { return now; }, [] { return uint64_t{0}; });
    minting.start();
    EXPECT_EQ(backend->reads, 1u);
    EXPECT_EQ(backend->writes, 1u);
    EXPECT_EQ(backend->heads, 0u);

    /// The adoption of a slot `claimMount` already wrote.
    ASSERT_EQ(claimMount(ops.op, l, "adopted", UInt128(1), /*epoch*/ 7, now, /*ttl*/ 100).kind,
              MountClaimResult::Claimed);
    backend->reads = backend->heads = backend->writes = 0;
    MountLeaseRenewer adopting(ops.mount, ops.farewell, l, "adopted", UInt128(1), 7,
                              std::chrono::milliseconds(100), [&] { return now; }, [] { return uint64_t{0}; });
    adopting.start();
    EXPECT_EQ(backend->reads, 1u);
    EXPECT_EQ(backend->writes, 1u);
    EXPECT_EQ(backend->heads, 0u);
}

/// A mount whose fence has dropped must still hand its slot back: the renewal is refused (it would be
/// writing under authority this node no longer holds), while the farewell runs on the open plane and
/// lands. Deliberately two renewers: `release` is admitted only from `Active`, so a renewer whose
/// renewal already went terminal never reaches its own farewell -- the ordering the two halves below
/// pin separately.
TEST(CASMountLease, FarewellRunsOnAnOpenFenceAfterTheMountFenceIsLost)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout l("p");
    uint64_t now = 1000;
    uint64_t boot = 0;
    bool fence_lost = false;

    CasRequests mount_requests(backend, Fence{
        [] { return uint64_t{0}; },
        [&fence_lost](uint64_t, uint64_t) { return fence_lost ? Fence::Admit::LostOrRearmed : Fence::Admit::Ok; },
        [&fence_lost](uint64_t)
        {
            if (fence_lost)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "mount fence lost");
        }});
    mount_requests.setNowFnForTest([&boot] { return boot; });
    mount_requests.setSleepFnForTest([&boot](uint64_t ms) { boot += ms; });
    CasRequests open_requests = openRequestsForTest(backend);
    open_requests.setNowFnForTest([&boot] { return boot; });
    open_requests.setSleepFnForTest([&boot](uint64_t ms) { boot += ms; });
    CasOperation seed = open_requests.admit();

    ASSERT_EQ(claimMount(seed, l, "renewing", UInt128(1), 7, now, /*ttl*/ 1000).kind, MountClaimResult::Claimed);
    ASSERT_EQ(claimMount(seed, l, "departing", UInt128(1), 7, now, /*ttl*/ 1000).kind, MountClaimResult::Claimed);

    MountLeaseRenewer renewing(mount_requests, open_requests, l, "renewing", UInt128(1), 7,
                              std::chrono::milliseconds(1000), [&] { return now; }, [] { return uint64_t{0}; },
                              {}, std::chrono::milliseconds(0), [&] { return boot; });
    MountLeaseRenewer departing(mount_requests, open_requests, l, "departing", UInt128(1), 7,
                               std::chrono::milliseconds(1000), [&] { return now; }, [] { return uint64_t{0}; },
                               {}, std::chrono::milliseconds(0), [&] { return boot; });
    renewing.start();
    departing.start();

    fence_lost = true;

    const MountRenewResult refused = renewing.renew(MountRenewOperationEnvironment{});
    EXPECT_EQ(refused.outcome, MountRenewOutcome::Terminal);
    EXPECT_FALSE(refused.sent_any);
    EXPECT_FALSE(renewing.canRelease()) << "a terminal renewal leaves no farewell to run";

    EXPECT_NO_THROW(departing.release());
    const MountLease farewell = decodeMountLease(seed.read(l.mountKey("departing"), Retry::standard())->bytes);
    EXPECT_EQ(farewell.min_active_build_sequence, std::numeric_limits<uint64_t>::max());
}

/// The claim is admitted off the mount fence, and it has to be: a self-remount runs with the fence
/// already latched lost, so a claim gated on it could never reclaim the slot. What keeps the claim
/// safe is the conditional write it makes, not the fence.
TEST(CASMountLease, ClaimIsNotAdmittedUnderTheMountFence)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout l("p");
    uint64_t now = 1000;
    uint64_t boot = 0;

    CasRequests mount_requests(backend, Fence{
        [] { return uint64_t{0}; },
        [](uint64_t, uint64_t) { return Fence::Admit::LostOrRearmed; },
        [](uint64_t) { throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "mount fence lost"); }});
    mount_requests.setNowFnForTest([&boot] { return boot; });
    mount_requests.setSleepFnForTest([&boot](uint64_t ms) { boot += ms; });
    CasRequests open_requests = openRequestsForTest(backend);
    open_requests.setNowFnForTest([&boot] { return boot; });
    open_requests.setSleepFnForTest([&boot](uint64_t ms) { boot += ms; });

    MountLeaseRenewer renewer(mount_requests, open_requests, l, "r", UInt128(1), 7,
                            std::chrono::milliseconds(1000), [&] { return now; }, [] { return uint64_t{0}; },
                            {}, std::chrono::milliseconds(0), [&] { return boot; });
    EXPECT_NO_THROW(renewer.start());

    CasOperation reader = open_requests.admit();
    const MountLease claimed = decodeMountLease(reader.read(l.mountKey("r"), Retry::standard())->bytes);
    EXPECT_EQ(claimed.writer_epoch, 7u);
    EXPECT_EQ(claimed.seq, 1u);
}

/// A lost owner-claim race is decided from the conflict's OWN resolve observation, so the two outcomes
/// have to be told apart from that alone: a racer that installed our uuid leaves nothing to do, a
/// foreign one fails closed. Reading the key again would answer a later question than the conflict
/// asked, and would cost a request per race.
TEST(CASServerRootClaim, OwnerLostToARacerIsDecidedFromTheConflictObservation)
{
    Layout l("p");
    {
        auto backend = std::make_shared<OwnerRaceBackend>(UInt128(1));
        Ops ops(backend);
        EXPECT_NO_THROW(claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation()));
        EXPECT_TRUE(backend->fired);
        /// The pre-claim read plus the create's own conflict-resolve read, and no third: a re-read
        /// added back for the decision itself would raise this to 3.
        EXPECT_EQ(backend->owner_reads, 2u);
    }
    {
        auto backend = std::make_shared<OwnerRaceBackend>(UInt128(2));
        Ops ops(backend);
        DB::Cas::tests::expectThrowsCodeWithMessage(
            DB::ErrorCodes::CORRUPTED_DATA,
            "claimed by a different server during our claim",
            [&] { claimOwnerOrThrow(ops.op, l, "r", UInt128(1), emptyCatalogObservation()); });
        EXPECT_TRUE(backend->fired);
        EXPECT_EQ(backend->owner_reads, 2u);
    }
}

/// A remount re-anchors its lease BEFORE it arms the fence for the new incarnation, so the fence is
/// still latched lost at that moment. The steady-state renewal is refused there — the sibling test
/// above pins that — and the remount's own renewal has to be admitted off the fence, or the pool could
/// never re-anchor and the remount attempt would fail on exactly the throttled store that caused it.
TEST(CASMountLease, RemountRenewalIsAdmittedOffTheMountFence)
{
    auto backend = std::make_shared<InMemoryBackend>();
    Layout l("p");
    uint64_t now = 1000;
    uint64_t boot = 0;

    CasRequests mount_requests(backend, Fence{
        [] { return uint64_t{0}; },
        [](uint64_t, uint64_t) { return Fence::Admit::LostOrRearmed; },
        [](uint64_t) { throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "mount fence lost"); }});
    mount_requests.setNowFnForTest([&boot] { return boot; });
    mount_requests.setSleepFnForTest([&boot](uint64_t ms) { boot += ms; });
    CasRequests open_requests = openRequestsForTest(backend);
    open_requests.setNowFnForTest([&boot] { return boot; });
    open_requests.setSleepFnForTest([&boot](uint64_t ms) { boot += ms; });

    MountLeaseRenewer renewer(mount_requests, open_requests, l, "r", UInt128(1), 7,
                            std::chrono::milliseconds(1000), [&] { return now; }, [] { return uint64_t{0}; },
                            {}, std::chrono::milliseconds(0), [&] { return boot; });
    renewer.start();

    const MountRenewResult redo = renewer.renewForRemount();
    EXPECT_EQ(redo.outcome, MountRenewOutcome::Committed);

    CasOperation reader = open_requests.admit();
    EXPECT_EQ(decodeMountLease(reader.read(l.mountKey("r"), Retry::standard())->bytes).seq, 2u);
}
