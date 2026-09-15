#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include "cas_test_helpers.h"
#include <Common/Exception.h>
#include <Poco/Exception.h>

#include <atomic>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

/// Task 7 (spec §2 "Startup [C4], ordered vs the capability probe [D2]"): the writable `Pool::open`
/// bootstrap sequence is (0) a ZERO-WRITE residual check FIRST — before any probe write — that ignores
/// structurally-valid `_probe/` debris; (1) only then the mutating `_probe/` capability battery; (2) then
/// `PoolMeta::createOrValidate`, which may mint a missing `_pool_meta` only over a genuinely empty prefix.
/// A missing `_pool_meta` over residual (non-`_probe`) data fails startup loud with ZERO writes — closing
/// the "restart poisons a partially-erased pool" hole. These are black-box tests over `Pool::open`,
/// asserting behavior AND ordering via an op-recording backend (they fail on the pre-Task-7 open, which
/// bootstraps a fresh identity unconditionally and performs no residual LIST before the battery).

namespace DB::ErrorCodes
{
extern const int INVALID_STATE;
extern const int NOT_IMPLEMENTED;
}

using namespace DB::Cas;

namespace
{

const String kPrefix = "p";
const String kSrid = "test";
const String kPoolMetaKey = "p/_pool_meta";
/// A well-formed per-mount probe uid: exactly 32 lowercase hex chars (`u128ToHex`'s shape).
const String kProbeUid = "0123456789abcdef0123456789abcdef";
const String kProbeUid2 = "fedcba9876543210fedcba9876543210";

/// Records the ORDER of backend operations so a test can assert that the residual LIST precedes the first
/// write, and that a fail path performs zero writes. Delegates every operation to `InMemoryBackend`
/// unchanged; `Pool::open` wraps this in its `InstrumentedBackend`, which forwards every op here.
///
/// The `write` primitive covers create, replace and conditional-put alike, so the log distinguishes
/// only writes from removals -- which is all the ordering assertions ask.
class RecordingBackend : public InMemoryBackend
{
public:
    /// Unhide the legacy `list` overloads the primitive override below would otherwise hide: the tests
    /// seed and inspect this store through them.
    using Backend::list;

    enum class Op : uint8_t { List, Write, Remove };
    struct Entry
    {
        Op op;
        String key;   /// the LIST prefix, or the written key
    };

    /// Recorded at the PRIMITIVE, which every legacy forwarder reaches too, so an op is logged
    /// whichever surface issued it.
    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
    {
        record(Op::List, prefix);
        return InMemoryBackend::list(prefix, cursor, limit, access);
    }
    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value,
                                             TransportAccess & access) override
    {
        record(Op::Write, key);
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }
    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override
    {
        record(Op::Remove, key);
        return InMemoryBackend::remove(key, expected_value, access);
    }
    /// `publish` is the one mutating primitive left unrecorded: it writes a blob, and the bootstrap
    /// path (battery + `createOrValidate` + mount protocol) publishes none.

    static bool isWrite(Op op)
    {
        return op == Op::Write || op == Op::Remove;
    }

    void clearLog()
    {
        std::lock_guard l(mutex_);
        log_.clear();
    }
    std::vector<Entry> snapshot() const
    {
        std::lock_guard l(mutex_);
        return log_;
    }
    size_t writeCount() const
    {
        std::lock_guard l(mutex_);
        size_t n = 0;
        for (const auto & e : log_)
            if (isWrite(e.op))
                ++n;
        return n;
    }

private:
    void record(Op op, const String & key)
    {
        std::lock_guard l(mutex_);
        log_.push_back({op, key});
    }
    mutable std::mutex mutex_;
    std::vector<Entry> log_;
};

/// Models a stale LIST result for `cas/ref_catalog`: the object was listed, then disappeared before
/// the exact validation GET. The bootstrap must treat this as residual, never as a new-pool proof.
class CatalogMissingAfterListBackend final : public InMemoryBackend
{
public:
    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (key == Layout{kPrefix}.refCatalogKey())
            return std::nullopt;
        return InMemoryBackend::read(key, access);
    }
};

PoolConfig makeConfig()
{
    PoolConfig cfg;
    cfg.pool_prefix = kPrefix;
    cfg.server_root_id = kSrid;
    cfg.wait_sleep_fn = [](uint64_t) {};   /// never block a synchronous test on an open/teardown wait
    return cfg;
}

/// A one-shot `create` for seeding fixture bytes before `Pool::open` runs, asserting it committed.
void seedObject(Backend & backend, const String & key, const String & bytes)
{
    DB::Cas::tests::OperationForTest op(backend);
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).create(key, bytes, Retry::once())));
}

/// Whether `key` has a value, through an exact read (mirrors the retired `backend->get(key).has_value()`).
bool readPresent(Backend & backend, const String & key)
{
    DB::Cas::tests::OperationForTest op(backend);
    return (*op).read(key, Retry::standard()).has_value();
}

/// Whether `key` has a value, through a HEAD (mirrors the retired `backend->head(key).exists`).
bool headPresent(Backend & backend, const String & key)
{
    DB::Cas::tests::OperationForTest op(backend);
    return (*op).head(key, Retry::standard()).has_value();
}

template <typename F>
void expectThrowsCodeContaining(int expected_code, const String & needle, F && fn);

void expectCatalogResidueRefusesWithoutPoolMeta(const String & bytes, const String & extra_key = {})
{
    auto backend = std::make_shared<RecordingBackend>();
    const Layout layout{kPrefix};
    seedObject(*backend, layout.refCatalogKey(), bytes);
    if (!extra_key.empty())
        seedObject(*backend, extra_key, "residual");
    backend->clearLog();

    try
    {
        Pool::open(backend, makeConfig());
        FAIL() << "expected residual catalog bootstrap refusal";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::INVALID_STATE);
    }
    EXPECT_EQ(backend->writeCount(), 0u);
    EXPECT_FALSE(headPresent(*backend, layout.poolMetaKey()));
}

/// Index of the first op matching `pred`, if any.
template <typename Pred>
std::optional<size_t> firstIndex(const std::vector<RecordingBackend::Entry> & log, Pred && pred)
{
    for (size_t i = 0; i < log.size(); ++i)
        if (pred(log[i]))
            return i;
    return std::nullopt;
}

/// Assert `fn` throws a DB::Exception with `expected_code` AND a message containing `needle`.
template <typename F>
void expectThrowsCodeContaining(int expected_code, const String & needle, F && fn)
{
    try
    {
        fn();
        FAIL() << "expected a DB::Exception, none thrown";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), expected_code);
        EXPECT_NE(e.message().find(needle), String::npos)
            << "message did not contain '" << needle << "': " << e.message();
    }
}

}

/// (a) Empty prefix → open succeeds, `_pool_meta` is created, AND the op-log proves the residual LIST of
/// the pool prefix happened BEFORE any write (the ordering [D2] mandates: no probe write may precede the
/// emptiness proof).
TEST(CASBootstrapOrdering, EmptyPrefixOpensAndListsBeforeAnyWrite)
{
    auto backend = std::make_shared<RecordingBackend>();
    backend->clearLog();

    PoolPtr store = Pool::open(backend, makeConfig());
    ASSERT_EQ(store->lifecycle(), PoolLifecycle::Live);
    EXPECT_TRUE(readPresent(*backend, kPoolMetaKey)) << "_pool_meta must be created on a fresh empty prefix";

    const auto log = backend->snapshot();
    const auto residual_list = firstIndex(log, [](const RecordingBackend::Entry & e)
        { return e.op == RecordingBackend::Op::List && e.key == kPrefix + "/"; });
    const auto first_write = firstIndex(log, [](const RecordingBackend::Entry & e)
        { return RecordingBackend::isWrite(e.op); });

    ASSERT_TRUE(residual_list.has_value()) << "the zero-write residual LIST of '" << kPrefix << "/' must run";
    ASSERT_TRUE(first_write.has_value()) << "a fresh open must eventually write (battery/meta/mount)";
    EXPECT_LT(*residual_list, *first_write) << "the residual LIST must precede every write";
}

/// The residue an incomplete erase would have left behind: a real ref-log object key, built through
/// `Layout` so it carries the life segment every ref key has. The residual check is LIST-based and
/// never parses it, but seeding a shape this build cannot write would make the comment below a lie.
namespace
{
String residualRefLogKey()
{
    return Layout{"p"}.refLogKey(DB::Cas::tests::fixture::fixtureLife(RootNamespace{"test%2Fabcd"}), RefTxnId{1, 1});
}
}

/// (b) A prefix holding `cas/ns/stream/…` residue but NO `_pool_meta` → open fails typed (INVALID_STATE),
/// and ZERO writes hit the backend (the mutating battery must NOT have run — the residual check throws
/// first).
TEST(CASBootstrapOrdering, ResidualWithoutMetaFailsTypedWithZeroWrites)
{
    auto backend = std::make_shared<RecordingBackend>();
    /// Seed residue an incomplete erase would have left behind (a ref-log object), with no `_pool_meta`.
    seedObject(*backend, residualRefLogKey(), "x");
    backend->clearLog();

    expectThrowsCodeContaining(DB::ErrorCodes::INVALID_STATE, "refusing to bootstrap over residual data",
                               [&] { Pool::open(backend, makeConfig()); });

    EXPECT_EQ(backend->writeCount(), 0u) << "the fail path must perform zero writes (battery never ran)";
    EXPECT_FALSE(readPresent(*backend, kPoolMetaKey)) << "a fresh _pool_meta must NOT have been minted";
}

/// The engine's attempt number reaches the transport even through the bootstrap's own residual LIST.
/// A backend that fails only the FIRST attempt of every LIST
/// (as the adaptive-timeout fuse would) must still let the residual check succeed on attempt 2 -- if
/// propagation were broken every attempt would look like attempt 1 and the LIST would never succeed,
/// which the bootstrap reports as `BootstrapResidual::Indeterminate` ("could not authoritatively list"),
/// a DIFFERENT message from the one asserted below. Reuses `ResidualWithoutMetaFailsTypedWithZeroWrites`'s
/// exact seeding helper and expected error code so the assertion distinguishes "refused because listed"
/// from "refused because the LIST failed".
TEST(CASBootstrapOrdering, ResidualListSucceedsOnTheSecondAttempt)
{
    /// Every LIST whose attempt number is 1 fails as the first-attempt fuse would; attempt 2 answers.
    struct FuseOnFirstList : RecordingBackend
    {
        RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
        {
            if (access.attemptNo() == 1)
                throw Poco::TimeoutException("Timeout");
            return RecordingBackend::list(prefix, cursor, limit, access);
        }
    };
    auto backend = std::make_shared<FuseOnFirstList>();
    /// A healthy pool without `_pool_meta` is the shape that needs the LIST: seed one residual key.
    seedObject(*backend, residualRefLogKey(), "x");
    backend->clearLog();

    expectThrowsCodeContaining(DB::ErrorCodes::INVALID_STATE, "refusing to bootstrap over residual data",
                               [&] { Pool::open(backend, makeConfig()); });

    bool listed_on_second = false;
    for (const auto & e : backend->snapshot())
        listed_on_second |= (e.op == RecordingBackend::Op::List);
    EXPECT_TRUE(listed_on_second) << "the residual LIST must have been answered (on attempt 2), not merely failed forever";
}

/// (b') The residual verdict is decided by the first residual key, not by an enumeration of the whole
/// prefix: forty residue keys and a 32-key page must cost exactly ONE list request. Enumerating a
/// large prefix is the one request a slow store cannot answer within an attempt, and a refusal
/// needs none of it.
TEST(CASBootstrapOrdering, ResidualWithoutMetaIsDecidedByTheFirstPage)
{
    auto backend = std::make_shared<RecordingBackend>();
    for (uint64_t i = 1; i <= 40; ++i)
        seedObject(*backend, Layout{"p"}.refLogKey(DB::Cas::tests::fixture::fixtureLife(RootNamespace{"test%2Fabcd"}), RefTxnId{1, i}), "x");
    backend->clearLog();

    expectThrowsCodeContaining(DB::ErrorCodes::INVALID_STATE, "refusing to bootstrap over residual data",
                               [&] { Pool::open(backend, makeConfig()); });

    size_t root_lists = 0;
    for (const auto & e : backend->snapshot())
        if (e.op == RecordingBackend::Op::List && e.key == kPrefix + "/")
            ++root_lists;
    EXPECT_EQ(root_lists, 1u) << "the first residual key settles the verdict; nothing past it may be enumerated";
    EXPECT_EQ(backend->writeCount(), 0u);
}

/// (c) A prefix containing ONLY stale, structurally-valid `_probe/<hex>/…` debris (a crash-mid-battery
/// leftover) → treated as empty → open succeeds and bootstraps a fresh pool. The debris-skip is what makes
/// a normal restart-after-crash recover instead of wedging.
TEST(CASBootstrapOrdering, StaleProbeDebrisOnlyIsTreatedAsEmpty)
{
    auto backend = std::make_shared<RecordingBackend>();
    seedObject(*backend, "p/_probe/" + kProbeUid + "/token", "probe-v1");
    seedObject(*backend, "p/_probe/" + kProbeUid + "/cas", "cas-s1");
    backend->clearLog();

    PoolPtr store;
    ASSERT_NO_THROW(store = Pool::open(backend, makeConfig()));
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::Live);
    EXPECT_TRUE(readPresent(*backend, kPoolMetaKey)) << "_pool_meta must be created over a probe-only prefix";
}

TEST(CASBootstrapOrdering, CanonicalEmptyCatalogOnlyIsTheSoleRetryablePreMetaResidue)
{
    auto backend = std::make_shared<RecordingBackend>();
    const Layout layout{kPrefix};
    seedObject(*backend, layout.refCatalogKey(), encodeRefCatalog(RefCatalog{}));
    seedObject(*backend, kPrefix + "/_probe/" + kProbeUid + "/token", "probe-v1");
    backend->clearLog();

    PoolPtr store;
    ASSERT_NO_THROW(store = Pool::open(backend, makeConfig()));
    EXPECT_TRUE(headPresent(*backend, layout.poolMetaKey()));
}

TEST(CASBootstrapOrdering, MalformedCatalogOnlyResidueRefusesWithoutPoolMeta)
{
    expectCatalogResidueRefusesWithoutPoolMeta("not a catalog");
}

TEST(CASBootstrapOrdering, NoncanonicalCatalogOnlyResidueRefusesWithoutPoolMeta)
{
    String noncanonical = encodeRefCatalog(RefCatalog{});
    noncanonical.insert(noncanonical.find('\n') - 1, ",\"noncanonical\":0");
    ASSERT_TRUE(decodeRefCatalog(noncanonical).entries.empty()) << "fixture must be decodable but noncanonical";
    expectCatalogResidueRefusesWithoutPoolMeta(noncanonical);
}

TEST(CASBootstrapOrdering, NonemptyCatalogOnlyResidueRefusesWithoutPoolMeta)
{
    const RefCatalog nonempty{.entries = {CatalogEntry{
        .ns = RootNamespace{"test/nonempty"}, .state = NsState::Live, .incarnation = UInt128{1}, .creator = std::nullopt}}};
    expectCatalogResidueRefusesWithoutPoolMeta(encodeRefCatalog(nonempty));
}

TEST(CASBootstrapOrdering, CatalogWithAnyOtherCasResidueRefusesWithoutPoolMeta)
{
    const String canonical_empty = encodeRefCatalog(RefCatalog{});
    const Layout layout{kPrefix};
    const std::vector<String> residuals{
        layout.ownerKey("test"), layout.epochKey("test"), layout.mountKey("test"),
        layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(RootNamespace{"test/ns"}), RefTxnId{1, 1}),
        layout.manifestKey(ManifestId{RootNamespace{"test/ns"}, ManifestRef{1, 1, 1}}),
        layout.serverRootDataPrefix("test") + "residual", kPrefix + "/unknown"};
    for (const String & residual : residuals)
        expectCatalogResidueRefusesWithoutPoolMeta(canonical_empty, residual);
}

TEST(CASBootstrapOrdering, ListedCatalogMissingAtExactGetRefusesWithoutPoolMeta)
{
    auto backend = std::make_shared<CatalogMissingAfterListBackend>();
    const Layout layout{kPrefix};
    seedObject(*backend, layout.refCatalogKey(), encodeRefCatalog(RefCatalog{}));

    expectThrowsCodeContaining(DB::ErrorCodes::INVALID_STATE, "refusing to bootstrap over residual data",
                               [&] { Pool::open(backend, makeConfig()); });
    EXPECT_FALSE(headPresent(*backend, layout.poolMetaKey()));
}

/// (d) An existing healthy pool (meta present + data) → reopen is unchanged: the pool identity is
/// PRESERVED (the residual check sees `_pool_meta` present → the normal validate path; `_pool_meta` is
/// never re-minted).
TEST(CASBootstrapOrdering, HealthyPoolReopenPreservesIdentity)
{
    auto backend = std::make_shared<RecordingBackend>();

    UInt128 pool_id_first;
    {
        PoolPtr store = Pool::open(backend, makeConfig());
        pool_id_first = store->poolMeta().pool_id;
    }   /// clean teardown: drained farewell, so the reopen reclaims immediately

    PoolPtr store2 = Pool::open(backend, makeConfig());
    EXPECT_EQ(store2->lifecycle(), PoolLifecycle::Live);
    EXPECT_EQ(store2->poolMeta().pool_id, pool_id_first)
        << "a healthy reopen must NOT re-mint _pool_meta — the pool identity must be preserved";
}

/// (d') An existing pool whose prefix the store cannot LIST at the moment (a large prefix on a store
/// that enumerates slowly, a LIST budget that expires) still reopens: `_pool_meta` present is proven by
/// ONE exact read, and the residual LIST is only the absent-key path. Before this, a pool that could be
/// read perfectly well refused to start because the enumeration that would have found the same key
/// did not return in time.
TEST(CASBootstrapOrdering, HealthyPoolReopensWhenThePrefixCannotBeListed)
{
    /// Refuses every LIST of the pool root once armed; everything else is the ordinary store.
    class UnlistableRootBackend final : public RecordingBackend
    {
    public:
        using RecordingBackend::list;
        RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
        {
            if (refuse_root_list && prefix == kPrefix + "/")
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                                    "UnlistableRootBackend: the pool root cannot be enumerated right now");
            return RecordingBackend::list(prefix, cursor, limit, access);
        }
        std::atomic<bool> refuse_root_list{false};
    };

    auto backend = std::make_shared<UnlistableRootBackend>();

    UInt128 pool_id_first;
    {
        PoolPtr store = Pool::open(backend, makeConfig());
        pool_id_first = store->poolMeta().pool_id;
    }   /// clean teardown: drained farewell, so the reopen reclaims immediately

    backend->refuse_root_list = true;
    backend->clearLog();
    PoolPtr store2;
    ASSERT_NO_THROW(store2 = Pool::open(backend, makeConfig()))
        << "an existing pool must reopen on the exact read of _pool_meta alone";
    EXPECT_EQ(store2->lifecycle(), PoolLifecycle::Live);
    EXPECT_EQ(store2->poolMeta().pool_id, pool_id_first);
    const auto log = backend->snapshot();
    EXPECT_FALSE(firstIndex(log, [](const RecordingBackend::Entry & e)
        { return e.op == RecordingBackend::Op::List && e.key == kPrefix + "/"; }).has_value())
        << "a pool whose _pool_meta was read must not be enumerated to prove it exists";
}

/// (e) [D2] concurrent-opener case: debris from a SECOND concurrent fresh opener's in-flight battery (a
/// distinct probe uid) is skipped by the SAME structural rule as (c). Two openers racing over one shared
/// pool prefix must not make each other's zero-write residual check fail.
TEST(CASBootstrapOrdering, ConcurrentOpenerProbeDebrisIsAlsoSkipped)
{
    auto backend = std::make_shared<RecordingBackend>();
    /// This mount's own crashed battery AND a concurrent opener's in-flight battery.
    seedObject(*backend, "p/_probe/" + kProbeUid + "/token", "probe-v1");
    seedObject(*backend, "p/_probe/" + kProbeUid2 + "/token", "probe-v1");
    seedObject(*backend, "p/_probe/" + kProbeUid2 + "/cas", "cas-s1");
    backend->clearLog();

    PoolPtr store;
    ASSERT_NO_THROW(store = Pool::open(backend, makeConfig()));
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::Live);
}

/// (f) The reserved subtree boundary: only objects strictly under `<prefix>/_probe/` are ignorable
/// debris. A SIBLING look-alike that merely starts with `_probe` but is NOT under the `_probe/` subtree
/// (here `_probelike/…`) is genuine residual — the trailing `/` in the reserved prefix keeps it out — so
/// bootstrap fails closed over it. (Any object literally under `_probe/`, whatever its leaf shape, is
/// ephemeral capability-probe scratch a content-addressed pool never uses for durable state.)
TEST(CASBootstrapOrdering, ProbeSiblingLookalikeIsResidualNotDebris)
{
    auto backend = std::make_shared<RecordingBackend>();
    seedObject(*backend, "p/_probelike/token", "x");
    backend->clearLog();

    expectThrowsCodeContaining(DB::ErrorCodes::INVALID_STATE, "refusing to bootstrap over residual data",
                               [&] { Pool::open(backend, makeConfig()); });
    EXPECT_EQ(backend->writeCount(), 0u);
    EXPECT_FALSE(readPresent(*backend, kPoolMetaKey));
}

/// (g) An OBSERVE / read-only open over a partially-erased pool (residual data, `_pool_meta` deleted)
/// must NOT mint a fresh `_pool_meta` — there is no truly-read-only backend, so a mint here is a real
/// write that would poison the next writable mount's residual check. It fails closed (typed INVALID_STATE)
/// with ZERO writes. The read-only path skips the residual check, so the fail-closed gate lives in
/// `createOrValidate` (`allow_mint=false`).
TEST(CASBootstrapOrdering, ReadOnlyOverResidualWithoutMetaFailsClosedNoMint)
{
    auto backend = std::make_shared<RecordingBackend>();
    seedObject(*backend, residualRefLogKey(), "x");
    backend->clearLog();

    PoolConfig cfg = makeConfig();
    cfg.read_only = true;
    expectThrowsCodeContaining(DB::ErrorCodes::INVALID_STATE, "refusing to mint outside the verified bootstrap path",
                               [&] { Pool::open(backend, cfg); });

    EXPECT_EQ(backend->writeCount(), 0u) << "an observe open must never write (least of all mint _pool_meta)";
    EXPECT_FALSE(readPresent(*backend, kPoolMetaKey));
}

/// (h) An observe / read-only open over a HEALTHY pool (meta present) is unchanged: it validates the
/// existing `_pool_meta` and succeeds, preserving the pool identity. `allow_mint=false` is never consulted
/// on the validate path.
TEST(CASBootstrapOrdering, ReadOnlyOverHealthyPoolSucceedsUnchanged)
{
    auto backend = std::make_shared<RecordingBackend>();
    UInt128 pool_id_first;
    {
        PoolPtr store = Pool::open(backend, makeConfig());   /// writable: creates _pool_meta
        pool_id_first = store->poolMeta().pool_id;
    }

    PoolConfig cfg = makeConfig();
    cfg.read_only = true;
    PoolPtr ro;
    ASSERT_NO_THROW(ro = Pool::open(backend, cfg));
    ASSERT_TRUE(ro);
    EXPECT_EQ(ro->poolMeta().pool_id, pool_id_first) << "an observe open over a healthy pool must not re-mint";
}

/// (i) `openForDecommission` over a pool whose `_pool_meta` is absent but whose owner anchor survives (a
/// partial erase) must NOT bootstrap a fresh identity — it fails closed (typed INVALID_STATE) with no
/// mint. Decommission operates on an existing member; a missing meta is a broken state, not a bootstrap.
TEST(CASBootstrapOrdering, DecommissionWithAbsentMetaFailsClosedNoMint)
{
    auto backend = std::make_shared<RecordingBackend>();
    {
        PoolPtr store = Pool::open(backend, makeConfig());   /// establishes owner anchor + _pool_meta
    }
    /// Delete only `_pool_meta`, leaving the owner anchor (and other control objects) behind.
    {
        DB::Cas::tests::OperationForTest op(*backend);
        const auto h = (*op).head(kPoolMetaKey, Retry::standard());
        ASSERT_TRUE(h.has_value());
        ASSERT_EQ((*op).remove(kPoolMetaKey, h->etag, Retry::once()), Removal::Removed);
    }
    backend->clearLog();

    expectThrowsCodeContaining(DB::ErrorCodes::INVALID_STATE, "refusing to mint outside the verified bootstrap path",
                               [&] { Pool::openForDecommission(backend, makeConfig(), kSrid); });

    EXPECT_EQ(backend->writeCount(), 0u) << "decommission must not mint a fresh _pool_meta";
    EXPECT_FALSE(readPresent(*backend, kPoolMetaKey));
}
