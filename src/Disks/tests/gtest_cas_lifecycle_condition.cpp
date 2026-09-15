#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <stdexcept>
#include <string>

/// Task 5 (spec §§1-3): the pool lifecycle condition + the identity gate at step 0 of `tryRemountOnce`.
/// These tests open a real writable `Pool` over the in-memory ("Emulated"-style) backend, manipulate the
/// pool sentinels behind the pool's back, then drive the gate through the synchronous `tryRemountOnce`
/// seam and assert the resulting lifecycle condition + the store()-class refusal. They follow
/// gtest_cas_sentinel_probe.cpp's harness patterns; the op counter is `tests::CountingBackend`.

namespace DB::ErrorCodes
{
extern const int INVALID_STATE;
}

using namespace DB::Cas;
using DB::Cas::tests::CountingBackend;

namespace
{

const String kSrid = "test";

/// Delete an existing key exactly (its current token comes from the same GET). Returns the deleted body
/// so a test can restore it verbatim later (scenario d).
String deleteKeyReturningBody(Backend & backend, const String & key)
{
    DB::Cas::tests::OperationForTest op(backend);
    const auto got = (*op).read(key, Retry::once());
    EXPECT_TRUE(got.has_value()) << "expected '" << key << "' to exist before deletion";
    if (!got)
        return {};
    (*op).remove(key, got->etag, Retry::once());
    return got->bytes;
}

/// GC's fence-out applied directly to the mount lease: preserve the body, set `gc_fenced`, bump `seq`
/// (token-guarded). A subsequent `tryRemountOnce` whose identity gate verdicts `Recover` then reclaims a
/// fresh incarnation and returns true. Mirrors gtest_cas_pool.cpp's `fenceOutMount`.
void fenceOutMount(Backend & backend, const String & mount_key)
{
    DB::Cas::tests::OperationForTest op(backend);
    const auto got = (*op).read(mount_key, Retry::once());
    ASSERT_TRUE(got.has_value());
    MountLease m = decodeMountLease(got->bytes);
    m.gc_fenced = true;
    m.seq += 1;
    const auto put = (*op).replace(mount_key, encodeMountLease(m), got->etag, Retry::once());
    ASSERT_TRUE(std::holds_alternative<Committed>(put));
}

/// A Backend decorator whose reads, heads and lists throw a transport-classified error while `fail` is
/// armed, counting every attempt so a test can prove the probe path was actually reached (and stopped
/// where it should) rather than some other short-circuit. Starts DISARMED so `Pool::open` succeeds; a
/// test arms it only to make the identity probe inconclusive. Mirrors
/// gtest_cas_sentinel_probe.cpp's `TransportFaultBackend`, but toggleable AFTER open.
///
/// The fault is `Poco::TimeoutException`: `Backend::probeSentinelRaw`'s default implementation (the one
/// `InMemoryBackend` uses) calls `head`/`read` directly and folds ANY exception from either into
/// `Indeterminate` with its own `catch (...)` -- so the exception never reaches `CasOperation`'s
/// transport-vs-local classification at all here. A `Poco::TimeoutException` is still the right class to
/// inject: it is what a real backend's probe would actually throw, and the point of the counters below
/// is to prove `head` was reached and actually failed, not skipped by some other short-circuit.
/// `tryRemountOnce` retries its own whole chain internally (well past the single probe attempt), so
/// the exact count per call is not pinned here -- only that a call growing it proves the fault path
/// stayed live across it, rather than a stale verdict being served from a cache.
class ToggleableTransportFaultBackend final : public InMemoryBackend
{
public:
    /// Unhide the LEGACY convenience overloads that the primitive overrides below would otherwise hide.
    using Backend::head;
    using Backend::list;

    std::optional<RawMeta> head(const String & key, TransportAccess & access) override
    {
        ++head_attempts;
        if (fail.load())
            throw Poco::TimeoutException("injected fault: transport error");
        return InMemoryBackend::head(key, access);
    }

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        ++read_attempts;
        if (fail.load())
            throw Poco::TimeoutException("injected fault: transport error");
        return InMemoryBackend::read(key, access);
    }

    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
    {
        ++list_attempts;
        if (fail.load())
            throw Poco::TimeoutException("injected fault: transport error");
        return InMemoryBackend::list(prefix, cursor, limit, access);
    }

    std::atomic<bool> fail{false};
    std::atomic<uint64_t> head_attempts{0};
    std::atomic<uint64_t> read_attempts{0};
    std::atomic<uint64_t> list_attempts{0};
};

}

/// (a) `_pool_meta` + the owner anchor authoritatively absent → the gate enters `IdentityLost` (never
/// `Vanished`) and store()-class access fails loud. rev.8: `IdentityLost` is a fail-loud TERMINAL state —
/// `isVanished()` still reads false (it is a distinct terminal), but a direct gate re-probe refuses without
/// ever claiming/allocating/writing (the thread-exit behavior of the background observer is covered by
/// `RemountThreadSelfExitsOnceIdentityLost` below).
TEST(CASLifecycleCondition, SentinelsDeletedEntersIdentityLostTerminal)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::Live);

    const String meta_key = store->layout().poolMetaKey();
    const String owner_key = store->layout().ownerKey(kSrid);

    /// Both sentinels gone (other objects may or may not remain — rev.8 does not distinguish).
    deleteKeyReturningBody(*backend, meta_key);
    deleteKeyReturningBody(*backend, owner_key);

    /// Even from `Live` (no fence trip), a direct remount attempt transitions through `TransientNotLive`
    /// and enters `IdentityLost` at step 0 — WITHOUT reaching `claimOwnerOrThrow`.
    EXPECT_FALSE(store->tryRemountOnce());
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::IdentityLost);
    EXPECT_FALSE(store->isVanished()) << "IdentityLost is a distinct terminal, not a Vanished state";

    /// store()-class access now fails loud with the typed lifecycle error.
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::INVALID_STATE, [&] { store->throwIfLifecycleTerminal(); });

    /// A direct gate re-probe still refuses without mutating: it probes the sentinels authoritatively and
    /// performs ZERO writes (never claims/allocates/mounts on a terminal pool).
    backend->resetCounts();
    EXPECT_FALSE(store->tryRemountOnce());
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::IdentityLost);
    EXPECT_EQ(backend->writeTotal(), 0u) << "a terminal-IdentityLost gate probe must never claim, allocate, or write";
    EXPECT_GE(backend->headCount(meta_key), 1u) << "the gate still probes _pool_meta authoritatively";
}

/// (a2) rev.8 worker-exit: `IdentityLost` is terminal, so the persistent self-remount worker must self-exit
/// — mirroring how a `Vanished` pool refuses to latch work. With `background_watermark = true`, `scheduleRemount`
/// must REFUSE to latch a recovery generation once the pool is `IdentityLost` (`remountTerminal` covers it),
/// exactly as it refuses on a published `Vanished` intent.
TEST(CASLifecycleCondition, RemountThreadSelfExitsOnceIdentityLost)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// `background_watermark = true` so the persistent recovery worker exists in production mode
    /// (mirrors gtest_cas_pool.cpp's ShutdownGuardRefusesToArmRemount setup).
    auto store = DB::Cas::Pool::open(backend,
        DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test", .background_watermark = true});

    /// Drive the pool terminal (`IdentityLost`) synchronously before latching any recovery work.
    deleteKeyReturningBody(*backend, store->layout().poolMetaKey());
    deleteKeyReturningBody(*backend, store->layout().ownerKey(kSrid));
    EXPECT_FALSE(store->tryRemountOnce());
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::IdentityLost);

    /// The runtime terminal consumer (or any direct `scheduleRemount`) must now refuse: no worker runs on a
    /// terminal pool.
    EXPECT_FALSE(store->scheduleRemountForTest())
        << "an IdentityLost pool is terminal (rev.8) — scheduleRemount must not latch recovery work";
}

/// (b) `_pool_meta` present but its `pool_id` is foreign → `Vanished(replaced)` immediately.
TEST(CASLifecycleCondition, PoolMetaForeignPoolIdEntersVanishedReplacedImmediately)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::Live);

    /// Overwrite `_pool_meta` with a FOREIGN pool_id (identity replaced); the object stays present.
    const String meta_key = store->layout().poolMetaKey();
    DB::Cas::tests::OperationForTest op(*backend);
    const auto got = (*op).read(meta_key, Retry::once());
    ASSERT_TRUE(got.has_value());
    PoolMeta foreign = decodePoolMeta(got->bytes);
    foreign.pool_id = foreign.pool_id + DB::UInt128(1);
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).replace(meta_key, encodePoolMeta(foreign), got->etag, Retry::once())));

    EXPECT_FALSE(store->tryRemountOnce());
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::VanishedReplaced);
    EXPECT_TRUE(store->isVanished());
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::INVALID_STATE, [&] { store->throwIfLifecycleTerminal(); });
}

/// (c) [B6] trap: `_pool_meta` present, pool_id + blob_header_len match, but `algos_used` differs → NOT a
/// replacement (`algos_used` is legally mutable); the existing recovery proceeds and the pool returns to
/// `Live`.
TEST(CASLifecycleCondition, PoolMetaAlgosUsedDifferIsNotReplacementRecoveryProceeds)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);

    const String meta_key = store->layout().poolMetaKey();
    DB::Cas::tests::OperationForTest op(*backend);
    const auto got = (*op).read(meta_key, Retry::once());
    ASSERT_TRUE(got.has_value());
    PoolMeta mutated = decodePoolMeta(got->bytes);
    /// pool_id + blob_header_len UNCHANGED; only `algos_used` gains a member (a mutable field, [B6]).
    const auto extra = static_cast<uint8_t>(BlobHashAlgo::XXH3_128);
    ASSERT_FALSE(std::binary_search(mutated.algos_used.begin(), mutated.algos_used.end(), extra));
    mutated.algos_used.push_back(extra);
    std::sort(mutated.algos_used.begin(), mutated.algos_used.end());
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).replace(meta_key, encodePoolMeta(mutated), got->etag, Retry::once())));

    /// Fence out the mount so the (correctly non-replacement) recovery cleanly reclaims a fresh incarnation.
    fenceOutMount(*backend, store->layout().mountKey(kSrid));

    /// A differing `algos_used` must NOT read as a foreign pool: the gate verdicts `Recover`, recovery
    /// completes, and the pool is `Live` — never `Vanished`.
    EXPECT_TRUE(store->tryRemountOnce());
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::Live);
    EXPECT_FALSE(store->isVanished());
}

/// (d) [D3] no auto-revival: from `IdentityLost`, restoring both sentinels with matching identity does NOT
/// bring the disk back — the observer stays fail-loud; only a restart recovers.
TEST(CASLifecycleCondition, IdentityLostDoesNotAutoReviveWhenSentinelsRestored)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);

    const String meta_key = store->layout().poolMetaKey();
    const String owner_key = store->layout().ownerKey(kSrid);

    const String meta_body = deleteKeyReturningBody(*backend, meta_key);
    const String owner_body = deleteKeyReturningBody(*backend, owner_key);

    EXPECT_FALSE(store->tryRemountOnce());
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::IdentityLost);

    /// Restore both sentinels verbatim (a backup restore with matching identity).
    DB::Cas::tests::OperationForTest op(*backend);
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).create(meta_key, meta_body, Retry::once())));
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).create(owner_key, owner_body, Retry::once())));

    /// The gate now sees Present+match, but the state is `IdentityLost`, so it stays fail-loud.
    EXPECT_FALSE(store->tryRemountOnce());
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::IdentityLost);
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::INVALID_STATE, [&] { store->throwIfLifecycleTerminal(); });
}

/// (e) Transport error from the probe → the pool stays `TransientNotLive` (recoverable); absence is never
/// proven, so no terminal transition fires and store()-class access does NOT throw the terminal lifecycle
/// error (the transient class stays fence-gated until Task 8).
TEST(CASLifecycleCondition, ProbeTransportErrorStaysTransientAndRetries)
{
    auto backend = std::make_shared<ToggleableTransportFaultBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::Live);

    /// Arm the transport fault: every request the identity probe issues now throws → Indeterminate.
    backend->fail.store(true);

    EXPECT_FALSE(store->tryRemountOnce());
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::TransientNotLive);
    EXPECT_FALSE(store->isVanished());
    EXPECT_NO_THROW(store->throwIfLifecycleTerminal());
    /// The `_pool_meta` probe was actually reached and actually failed at `head` -- proving the
    /// TransientNotLive verdict above came from the probe's own `Indeterminate` classification, not
    /// from some other short-circuit that never touched the fault at all.
    const uint64_t first_head_attempts = backend->head_attempts.load();
    EXPECT_GT(first_head_attempts, 0u);

    /// A second attempt with the fault still armed remains transient (retries continue) and probes
    /// again -- proving each `tryRemountOnce` re-probes rather than caching the first call's
    /// inconclusive verdict.
    EXPECT_FALSE(store->tryRemountOnce());
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::TransientNotLive);
    EXPECT_GT(backend->head_attempts.load(), first_head_attempts);

    /// Disarm before teardown so `~Pool()`'s clean-farewell write is not fighting the injected fault.
    backend->fail.store(false);
}
