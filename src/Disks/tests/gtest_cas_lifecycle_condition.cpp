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
    const auto got = backend.get(key);
    EXPECT_TRUE(got.has_value()) << "expected '" << key << "' to exist before deletion";
    if (!got)
        return {};
    backend.deleteExact(key, got->token);
    return got->bytes;
}

/// GC's fence-out applied directly to the mount lease: preserve the body, set `gc_fenced`, bump `seq`
/// (token-guarded). A subsequent `tryRemountOnce` whose identity gate verdicts `Recover` then reclaims a
/// fresh incarnation and returns true. Mirrors gtest_cas_pool.cpp's `fenceOutMount`.
void fenceOutMount(Backend & backend, const String & mount_key)
{
    const auto got = backend.get(mount_key);
    ASSERT_TRUE(got.has_value());
    MountLease m = decodeMountLease(got->bytes);
    m.gc_fenced = true;
    m.seq += 1;
    ASSERT_EQ(backend.putOverwrite(mount_key, encodeMountLease(m), got->token).outcome, PutOutcome::Done);
}

/// A Backend decorator whose head/get/list throw an untyped transport error while `fail` is armed. Starts
/// DISARMED so `Pool::open` succeeds; a test arms it only to make the identity probe inconclusive. Mirrors
/// gtest_cas_sentinel_probe.cpp's `TransportFaultBackend`, but toggleable AFTER open.
class ToggleableTransportFaultBackend final : public InMemoryBackend
{
public:
    /// Unhide the base convenience overloads, matching every other Backend subclass in this suite.
    using Backend::get;
    using Backend::getStream;
    using Backend::putIfAbsent;
    using Backend::putIfAbsentStream;
    using Backend::putOverwrite;
    using Backend::casPut;

    HeadResult head(const String & key) override
    {
        if (fail.load())
            throw std::runtime_error("injected fault: transport error");
        return InMemoryBackend::head(key);
    }

    std::optional<GetResult> get(const String & key, Range range) override
    {
        if (fail.load())
            throw std::runtime_error("injected fault: transport error");
        return InMemoryBackend::get(key, range);
    }

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        if (fail.load())
            throw std::runtime_error("injected fault: transport error");
        return InMemoryBackend::list(prefix, cursor, limit);
    }

    std::atomic<bool> fail{false};
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
    EXPECT_EQ(backend->putTotal(), 0u) << "a terminal-IdentityLost gate probe must never claim, allocate, or write";
    EXPECT_GE(backend->headCount(meta_key), 1u) << "the gate still probes _pool_meta authoritatively";
}

/// (a2) rev.8 thread-exit: `IdentityLost` is terminal, so the background self-remount thread must self-exit
/// — mirroring how a `Vanished` pool refuses to arm one. With `background_watermark = true`, `scheduleRemount`
/// must REFUSE to arm a recovery thread once the pool is `IdentityLost` (`remountTerminal()` covers it),
/// exactly as it refuses on a published `Vanished` intent.
TEST(CASLifecycleCondition, RemountThreadSelfExitsOnceIdentityLost)
{
    auto backend = std::make_shared<InMemoryBackend>();
    /// `background_watermark = true` so `scheduleRemount` actually arms a recovery thread in production mode
    /// (mirrors gtest_cas_pool.cpp's ShutdownGuardRefusesToArmRemount setup).
    auto store = DB::Cas::Pool::open(backend,
        DB::Cas::PoolConfig{.pool_prefix = "p", .server_root_id = "test", .background_watermark = true});

    /// Drive the pool terminal (IdentityLost) synchronously first — a direct gate call, no thread spawned.
    deleteKeyReturningBody(*backend, store->layout().poolMetaKey());
    deleteKeyReturningBody(*backend, store->layout().ownerKey(kSrid));
    EXPECT_FALSE(store->tryRemountOnce());
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::IdentityLost);

    /// The keeper's on-lost callback (or any `scheduleRemount`) must now refuse: no observer runs on a
    /// terminal pool.
    EXPECT_FALSE(store->scheduleRemountForTest())
        << "an IdentityLost pool is terminal (rev.8) — scheduleRemount must not arm a recovery thread";
}

/// (b) `_pool_meta` present but its `pool_id` is foreign → `Vanished(replaced)` immediately.
TEST(CASLifecycleCondition, PoolMetaForeignPoolIdEntersVanishedReplacedImmediately)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = DB::Cas::tests::openPoolForTest(backend);
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::Live);

    /// Overwrite `_pool_meta` with a FOREIGN pool_id (identity replaced); the object stays present.
    const String meta_key = store->layout().poolMetaKey();
    const auto got = backend->get(meta_key);
    ASSERT_TRUE(got.has_value());
    PoolMeta foreign = decodePoolMeta(got->bytes);
    foreign.pool_id = foreign.pool_id + DB::UInt128(1);
    ASSERT_EQ(backend->putOverwrite(meta_key, encodePoolMeta(foreign), got->token).outcome, PutOutcome::Done);

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
    const auto got = backend->get(meta_key);
    ASSERT_TRUE(got.has_value());
    PoolMeta mutated = decodePoolMeta(got->bytes);
    /// pool_id + blob_header_len UNCHANGED; only `algos_used` gains a member (a mutable field, [B6]).
    const auto extra = static_cast<uint8_t>(BlobHashAlgo::XXH3_128);
    ASSERT_FALSE(std::binary_search(mutated.algos_used.begin(), mutated.algos_used.end(), extra));
    mutated.algos_used.push_back(extra);
    std::sort(mutated.algos_used.begin(), mutated.algos_used.end());
    ASSERT_EQ(backend->putOverwrite(meta_key, encodePoolMeta(mutated), got->token).outcome, PutOutcome::Done);

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
    ASSERT_EQ(backend->putIfAbsent(meta_key, meta_body).outcome, PutOutcome::Done);
    ASSERT_EQ(backend->putIfAbsent(owner_key, owner_body).outcome, PutOutcome::Done);

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

    /// Arm the transport fault: the identity probe's head/get/list now throw → Indeterminate.
    backend->fail.store(true);

    EXPECT_FALSE(store->tryRemountOnce());
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::TransientNotLive);
    EXPECT_FALSE(store->isVanished());
    EXPECT_NO_THROW(store->throwIfLifecycleTerminal());

    /// A second attempt with the fault still armed remains transient (retries continue).
    EXPECT_FALSE(store->tryRemountOnce());
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::TransientNotLive);

    /// Disarm before teardown so `~Pool()`'s clean-farewell write is not fighting the injected fault.
    backend->fail.store(false);
}
