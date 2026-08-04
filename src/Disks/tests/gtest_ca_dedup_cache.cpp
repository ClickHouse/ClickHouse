#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
extern const Event CASDeduplicationCacheHits;
extern const Event CASDeduplicationCacheMisses;
}

using namespace DB::Cas;
using DB::Cas::tests::idOf;
using DB::Cas::tests::u128Of;

namespace
{

/// A transparent delegating Backend that counts the two ops P1/P2 trade off against each other:
/// `head` (the cheap probe HEAD-before-PUT issues) and `putIfAbsentStream` (the body upload a present
/// HEAD avoids). Everything else is forwarded verbatim so the wrapped backend behaves exactly as a bare
/// InMemoryBackend would.
class CountingBackend final : public Backend
{
public:
    explicit CountingBackend(BackendPtr inner_) : inner(std::move(inner_)) {}

    size_t heads = 0;
    size_t stream_puts = 0;

    HeadResult head(const String & k) override { ++heads; return inner->head(k); }
    WriteSinkPtr putIfAbsentStream(const String & k, const ObjectMeta & meta) override
    {
        ++stream_puts;
        return inner->putIfAbsentStream(k, meta);
    }

    std::optional<GetResult> get(const String & k, Range r) override { return inner->get(k, r); }
    std::optional<GetStreamResult> getStream(const String & k, Range r) override { return inner->getStream(k, r); }
    ListPage list(const String & p, const String & c, size_t l) override { return inner->list(p, c, l); }
    PutResult putIfAbsent(const String & k, const String & b, const ObjectMeta & m) override { return inner->putIfAbsent(k, b, m); }
    PutResult putOverwrite(const String & k, const String & b, const Token & e, const ObjectMeta & m) override { return inner->putOverwrite(k, b, e, m); }
    CasResult casPut(const String & k, const String & b, const std::optional<Token> & e, const ObjectMeta & m) override { return inner->casPut(k, b, e, m); }
    DeleteOutcome deleteExact(const String & k, const Token & t) override { return inner->deleteExact(k, t); }
    bool supportsListTokens() const override { return inner->supportsListTokens(); }

private:
    BackendPtr inner;
};

PoolConfig cfg(uint64_t cache_bytes, uint64_t head_first_min_bytes)
{
    PoolConfig c{.pool_prefix = "p", .server_root_id = "test"};
    c.deduplication_cache_bytes = cache_bytes;
    c.deduplication_head_first_min_bytes = head_first_min_bytes;
    return c;
}

}

/// Task 2: the cache itself — add then contains.
TEST(CASDeduplicationCache, AddThenContains)
{
    auto s = Pool::open(std::make_shared<InMemoryBackend>(), cfg(64ULL << 20, 1ULL << 20));
    const DB::UInt128 h = u128Of("x");
    EXPECT_FALSE(s->dedupCacheContains(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(h)}));
    s->dedupCacheAdd(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(h)});
    EXPECT_TRUE(s->dedupCacheContains(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(h)}));
}

/// Task 2: deduplication_cache_bytes == 0 disables the cache — add is a no-op, contains is always false.
TEST(CASDeduplicationCache, DisabledNeverContains)
{
    auto s = Pool::open(std::make_shared<InMemoryBackend>(), cfg(/*cache_bytes*/ 0, 1ULL << 20));
    const DB::UInt128 h = u128Of("x");
    s->dedupCacheAdd(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(h)});
    EXPECT_FALSE(s->dedupCacheContains(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(h)}));
}

/// Task 2: the cache is bounded by bytes — at 64 B/entry a 256 B ceiling holds ~4 entries, so the
/// earliest-added hash is evicted while a recently-added one survives.
TEST(CASDeduplicationCache, BoundedByBytes)
{
    auto s = Pool::open(std::make_shared<InMemoryBackend>(), cfg(/*cache_bytes*/ 256, 1ULL << 20));
    const DB::UInt128 first = u128Of("k0");
    s->dedupCacheAdd(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(first)});
    for (int i = 1; i < 100; ++i)
        s->dedupCacheAdd(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of("k" + std::to_string(i)))});
    EXPECT_FALSE(s->dedupCacheContains(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(first)}));            /// evicted long ago
    EXPECT_TRUE(s->dedupCacheContains(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of("k99"))}));     /// most recent survives
}

/// Task 5 (P1): a cache hit takes the HEAD-first path and skips the body PUT entirely.
/// (Counters are reset right before each measured putBlob — Pool::open's probe/watermark and
/// beginPartWrite's heartbeat issue their own backend ops that are irrelevant to the trade-off under test.)
TEST(CASDeduplicationCache, HitTakesHeadFirstNoBodyPut)
{
    auto counting = std::make_shared<CountingBackend>(std::make_shared<InMemoryBackend>());
    auto s = Pool::open(counting, cfg(64ULL << 20, 1ULL << 20));

    /// First writer: small body, cold cache, below the P2 size threshold ⇒ a normal body PUT.
    auto b1 = s->beginPartWrite({});
    counting->stream_puts = 0;
    b1->putBlob(idOf("dup"), BlobSource::fromString("dup"));
    EXPECT_EQ(counting->stream_puts, 1u);

    /// Second writer of the same content: the cache now says present ⇒ HEAD-first, no second body PUT.
    /// The head-first hit ADOPTS an existing incarnation, so it must run under a durable precommit edge
    /// (EDGE-BEFORE-OBSERVE: stageManifest -> precommitAdd before putBlob). Counters are reset AFTER that
    /// ceremony so only the measured putBlob is counted.
    PartWriteInfo info2;
    info2.intended_ref = "srv/tbl/ref2";
    auto b2 = s->beginPartWrite(info2);
    ManifestEntry e2;
    e2.path = "data.bin";
    e2.placement = EntryPlacement::Blob;
    e2.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of("dup"))};

    e2.blob_size = 3;
    const ManifestId id2 = b2->stageManifest({e2});
    b2->precommitAdd(RootNamespace{"srv/tbl"}, "ref2", id2);
    counting->stream_puts = 0;
    b2->putBlob(idOf("dup"), BlobSource::fromString("dup"));
    EXPECT_EQ(counting->stream_puts, 0u);                  /// body PUT avoided
}

/// Task 3 (Round-B §0.3 introspection): the raw dedup_cache presence-lookup counters increment
/// independently of what putBlob does with the answer. First lookup of a fresh hash misses (nothing
/// cached yet); the identical second writer's lookup hits (the first writer's dedupCacheAdd populated
/// the entry). putBlob's HEAD-first branch re-checks dedupCacheContains a second time purely to
/// attribute CASBlobBodyPutAvoided to the cache (CasPartWriteTxn.cpp), so a genuine hit can bump
/// CASDeduplicationCacheHits twice for one putBlob call -- hence GE, not EQ, on the hit delta below.
TEST(CASDeduplicationCache, HitMissCountersIncrement)
{
    using ProfileEvents::global_counters;
    auto counting = std::make_shared<CountingBackend>(std::make_shared<InMemoryBackend>());
    auto s = Pool::open(counting, cfg(64ULL << 20, 1ULL << 20));

    const auto miss_before = global_counters[ProfileEvents::CASDeduplicationCacheMisses].load();
    const auto hits_before = global_counters[ProfileEvents::CASDeduplicationCacheHits].load();

    /// First writer: cold cache, small body below the P2 size threshold -> the lookup misses.
    auto b1 = s->beginPartWrite({});
    b1->putBlob(idOf("dup"), BlobSource::fromString("dup"));
    EXPECT_EQ(global_counters[ProfileEvents::CASDeduplicationCacheMisses].load() - miss_before, 1);

    /// Second writer, identical content: the cache now holds the hash -> the lookup hits. The
    /// head-first hit ADOPTS an existing incarnation, so it must run under a durable precommit edge
    /// (EDGE-BEFORE-OBSERVE: stageManifest -> precommitAdd before putBlob), mirroring
    /// HitTakesHeadFirstNoBodyPut above.
    PartWriteInfo info2;
    info2.intended_ref = "srv/tbl/ref2";
    auto b2 = s->beginPartWrite(info2);
    ManifestEntry e2;
    e2.path = "data.bin";
    e2.placement = EntryPlacement::Blob;
    e2.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of("dup"))};
    e2.blob_size = 3;
    const ManifestId id2 = b2->stageManifest({e2});
    b2->precommitAdd(RootNamespace{"srv/tbl"}, "ref2", id2);
    b2->putBlob(idOf("dup"), BlobSource::fromString("dup"));

    EXPECT_EQ(global_counters[ProfileEvents::CASDeduplicationCacheMisses].load() - miss_before, 1);
    EXPECT_GE(global_counters[ProfileEvents::CASDeduplicationCacheHits].load() - hits_before, 1);
}

/// Task 5 (P1 safety): a STALE cache hit (hash marked present but absent in the store) must not cause a
/// dangle — the mandatory HEAD sees 404 and the writer falls through to a real body PUT.
TEST(CASDeduplicationCache, StaleHitFallsThroughToPut)
{
    auto counting = std::make_shared<CountingBackend>(std::make_shared<InMemoryBackend>());
    auto s = Pool::open(counting, cfg(64ULL << 20, 1ULL << 20));

    /// Poison the cache: claim "stale" is present though nothing was ever uploaded.
    s->dedupCacheAdd(DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of("stale"))});

    auto b = s->beginPartWrite({});
    counting->heads = 0;
    counting->stream_puts = 0;
    auto ref = b->putBlob(idOf("stale"), BlobSource::fromString("stale"));
    EXPECT_EQ(ref.size, 5u);
    EXPECT_GE(counting->heads, 1u);                        /// the safety HEAD ran
    EXPECT_EQ(counting->stream_puts, 1u);                  /// and the body was actually uploaded
    EXPECT_TRUE(counting->head(s->layout().blobKey(ref.ref)).exists);
}

/// Task 5 (P2): on a cold cache, a body at/above deduplication_head_first_min_bytes still probes HEAD-first
/// (here the size trigger fires for a tiny body because the threshold is set to 1). The miss falls
/// through to a real PUT.
TEST(CASDeduplicationCache, LargeBlobMissTakesHeadFirst)
{
    auto counting = std::make_shared<CountingBackend>(std::make_shared<InMemoryBackend>());
    auto s = Pool::open(counting, cfg(64ULL << 20, /*head_first_min_bytes*/ 1));

    auto b = s->beginPartWrite({});
    counting->heads = 0;
    counting->stream_puts = 0;
    b->putBlob(idOf("big"), BlobSource::fromString("big"));
    EXPECT_EQ(counting->heads, 1u);                        /// P2 probed before the PUT
    EXPECT_EQ(counting->stream_puts, 1u);                  /// cold miss ⇒ body uploaded
}
