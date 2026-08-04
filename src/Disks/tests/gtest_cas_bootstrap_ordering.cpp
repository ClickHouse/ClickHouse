#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include "cas_test_helpers.h"
#include <Common/Exception.h>

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
class RecordingBackend final : public InMemoryBackend
{
public:
    using Backend::get;
    using Backend::getStream;
    using Backend::putIfAbsent;
    using Backend::putIfAbsentStream;
    using Backend::putOverwrite;
    using Backend::casPut;

    enum class Op : uint8_t { List, PutIfAbsent, PutOverwrite, CasPut, Delete };
    struct Entry
    {
        Op op;
        String key;   /// the LIST prefix, or the written key
    };

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        record(Op::List, prefix);
        return InMemoryBackend::list(prefix, cursor, limit);
    }
    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override
    {
        record(Op::PutIfAbsent, key);
        return InMemoryBackend::putIfAbsent(key, bytes, meta);
    }
    PutResult putOverwrite(const String & key, const String & bytes, const Token & expected, const ObjectMeta & meta) override
    {
        record(Op::PutOverwrite, key);
        return InMemoryBackend::putOverwrite(key, bytes, expected, meta);
    }
    CasResult casPut(const String & key, const String & bytes, const std::optional<Token> & expected, const ObjectMeta & meta) override
    {
        record(Op::CasPut, key);
        return InMemoryBackend::casPut(key, bytes, expected, meta);
    }
    DeleteOutcome deleteExact(const String & key, const Token & token) override
    {
        record(Op::Delete, key);
        return InMemoryBackend::deleteExact(key, token);
    }
    /// The bootstrap path (battery + createOrValidate + mount protocol) issues only whole-String writes,
    /// never a streaming create, so recording the four write ops above captures every write `open` can do.

    static bool isWrite(Op op)
    {
        return op == Op::PutIfAbsent || op == Op::PutOverwrite || op == Op::CasPut || op == Op::Delete;
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
    using Backend::get;

    std::optional<GetResult> get(const String & key, Range range) override
    {
        if (key == Layout{kPrefix}.refCatalogKey())
            return std::nullopt;
        return InMemoryBackend::get(key, range);
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

template <typename F>
void expectThrowsCodeContaining(int expected_code, const String & needle, F && fn);

void expectCatalogResidueRefusesWithoutPoolMeta(const String & bytes, const String & extra_key = {})
{
    auto backend = std::make_shared<RecordingBackend>();
    const Layout layout{kPrefix};
    ASSERT_EQ(backend->putIfAbsent(layout.refCatalogKey(), bytes).outcome, PutOutcome::Done);
    if (!extra_key.empty())
        ASSERT_EQ(backend->putIfAbsent(extra_key, "residual").outcome, PutOutcome::Done);
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
    EXPECT_FALSE(backend->head(layout.poolMetaKey()).exists);
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
    EXPECT_TRUE(backend->get(kPoolMetaKey).has_value()) << "_pool_meta must be created on a fresh empty prefix";

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
    ASSERT_EQ(backend->putIfAbsent(residualRefLogKey(), "x").outcome,
              PutOutcome::Done);
    backend->clearLog();

    expectThrowsCodeContaining(DB::ErrorCodes::INVALID_STATE, "refusing to bootstrap over residual data",
                               [&] { Pool::open(backend, makeConfig()); });

    EXPECT_EQ(backend->writeCount(), 0u) << "the fail path must perform zero writes (battery never ran)";
    EXPECT_FALSE(backend->get(kPoolMetaKey).has_value()) << "a fresh _pool_meta must NOT have been minted";
}

/// (c) A prefix containing ONLY stale, structurally-valid `_probe/<hex>/…` debris (a crash-mid-battery
/// leftover) → treated as empty → open succeeds and bootstraps a fresh pool. The debris-skip is what makes
/// a normal restart-after-crash recover instead of wedging.
TEST(CASBootstrapOrdering, StaleProbeDebrisOnlyIsTreatedAsEmpty)
{
    auto backend = std::make_shared<RecordingBackend>();
    ASSERT_EQ(backend->putIfAbsent("p/_probe/" + kProbeUid + "/token", "probe-v1").outcome, PutOutcome::Done);
    ASSERT_EQ(backend->putIfAbsent("p/_probe/" + kProbeUid + "/cas", "cas-s1").outcome, PutOutcome::Done);
    backend->clearLog();

    PoolPtr store;
    ASSERT_NO_THROW(store = Pool::open(backend, makeConfig()));
    EXPECT_EQ(store->lifecycle(), PoolLifecycle::Live);
    EXPECT_TRUE(backend->get(kPoolMetaKey).has_value()) << "_pool_meta must be created over a probe-only prefix";
}

TEST(CASBootstrapOrdering, CanonicalEmptyCatalogOnlyIsTheSoleRetryablePreMetaResidue)
{
    auto backend = std::make_shared<RecordingBackend>();
    const Layout layout{kPrefix};
    ASSERT_EQ(backend->putIfAbsent(layout.refCatalogKey(), encodeRefCatalog(RefCatalog{})).outcome, PutOutcome::Done);
    ASSERT_EQ(backend->putIfAbsent(kPrefix + "/_probe/" + kProbeUid + "/token", "probe-v1").outcome, PutOutcome::Done);
    backend->clearLog();

    PoolPtr store;
    ASSERT_NO_THROW(store = Pool::open(backend, makeConfig()));
    EXPECT_TRUE(backend->head(layout.poolMetaKey()).exists);
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
    ASSERT_EQ(backend->putIfAbsent(layout.refCatalogKey(), encodeRefCatalog(RefCatalog{})).outcome, PutOutcome::Done);

    expectThrowsCodeContaining(DB::ErrorCodes::INVALID_STATE, "refusing to bootstrap over residual data",
                               [&] { Pool::open(backend, makeConfig()); });
    EXPECT_FALSE(backend->head(layout.poolMetaKey()).exists);
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

/// (e) [D2] concurrent-opener case: debris from a SECOND concurrent fresh opener's in-flight battery (a
/// distinct probe uid) is skipped by the SAME structural rule as (c). Two openers racing over one shared
/// pool prefix must not make each other's zero-write residual check fail.
TEST(CASBootstrapOrdering, ConcurrentOpenerProbeDebrisIsAlsoSkipped)
{
    auto backend = std::make_shared<RecordingBackend>();
    /// This mount's own crashed battery AND a concurrent opener's in-flight battery.
    ASSERT_EQ(backend->putIfAbsent("p/_probe/" + kProbeUid + "/token", "probe-v1").outcome, PutOutcome::Done);
    ASSERT_EQ(backend->putIfAbsent("p/_probe/" + kProbeUid2 + "/token", "probe-v1").outcome, PutOutcome::Done);
    ASSERT_EQ(backend->putIfAbsent("p/_probe/" + kProbeUid2 + "/cas", "cas-s1").outcome, PutOutcome::Done);
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
    ASSERT_EQ(backend->putIfAbsent("p/_probelike/token", "x").outcome, PutOutcome::Done);
    backend->clearLog();

    expectThrowsCodeContaining(DB::ErrorCodes::INVALID_STATE, "refusing to bootstrap over residual data",
                               [&] { Pool::open(backend, makeConfig()); });
    EXPECT_EQ(backend->writeCount(), 0u);
    EXPECT_FALSE(backend->get(kPoolMetaKey).has_value());
}

/// (g) An OBSERVE / read-only open over a partially-erased pool (residual data, `_pool_meta` deleted)
/// must NOT mint a fresh `_pool_meta` — there is no truly-read-only backend, so a mint here is a real
/// write that would poison the next writable mount's residual check. It fails closed (typed INVALID_STATE)
/// with ZERO writes. The read-only path skips the residual check, so the fail-closed gate lives in
/// `createOrValidate` (`allow_mint=false`).
TEST(CASBootstrapOrdering, ReadOnlyOverResidualWithoutMetaFailsClosedNoMint)
{
    auto backend = std::make_shared<RecordingBackend>();
    ASSERT_EQ(backend->putIfAbsent(residualRefLogKey(), "x").outcome,
              PutOutcome::Done);
    backend->clearLog();

    PoolConfig cfg = makeConfig();
    cfg.read_only = true;
    expectThrowsCodeContaining(DB::ErrorCodes::INVALID_STATE, "refusing to mint outside the verified bootstrap path",
                               [&] { Pool::open(backend, cfg); });

    EXPECT_EQ(backend->writeCount(), 0u) << "an observe open must never write (least of all mint _pool_meta)";
    EXPECT_FALSE(backend->get(kPoolMetaKey).has_value());
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
        const auto h = backend->head(kPoolMetaKey);
        ASSERT_TRUE(h.exists);
        ASSERT_EQ(backend->deleteExact(kPoolMetaKey, h.token).kind, DeleteOutcome::Kind::Deleted);
    }
    backend->clearLog();

    expectThrowsCodeContaining(DB::ErrorCodes::INVALID_STATE, "refusing to mint outside the verified bootstrap path",
                               [&] { Pool::openForDecommission(backend, makeConfig(), kSrid); });

    EXPECT_EQ(backend->writeCount(), 0u) << "decommission must not mint a fresh _pool_meta";
    EXPECT_FALSE(backend->get(kPoolMetaKey).has_value());
}
