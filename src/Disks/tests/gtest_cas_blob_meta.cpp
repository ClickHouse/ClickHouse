#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobMeta.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasInspect.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include "cas_test_helpers.h"

#include <Poco/Exception.h>

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
}

using namespace DB::Cas;
using namespace DB::Cas::tests;

/// Codec tests (round-trip both states, fail-closed decode) moved to gtest_cas_blob_meta_format.cpp
/// with the v3 text cutover; the lifecycle + inspect tests below stay — they exercise the Core ops
/// and CasInspect against the stable encode/decode signatures and must pass unchanged.

TEST(CASBlobMeta, PutIfAbsentThenCasTransitions)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    CasOperation op = store->mountRequests().admit();
    const BlobRef ref{BlobHashAlgo::CityHash128, BlobDigest::fromU128(u128Of("hash-a"))};
    const BlobMeta clean{.state = MetaState::Clean, .size = 10};

    EXPECT_TRUE(std::holds_alternative<Committed>(putMetaIfAbsent(op, store->layout(), ref, clean)));

    /// A create that nothing of its own left unresolved never adopts what is already at the key, even
    /// byte-identical: the marker was somebody else's write, and the conflict carries it.
    EXPECT_TRUE(std::holds_alternative<Conflict>(putMetaIfAbsent(op, store->layout(), ref, clean)));

    const auto lm = loadMeta(op, store->layout(), ref);
    ASSERT_TRUE(lm.has_value());
    EXPECT_EQ(lm->meta.state, MetaState::Clean);

    EXPECT_TRUE(std::holds_alternative<Committed>(casMeta(op, store->layout(), ref, lm->etag,
        BlobMeta{.state = MetaState::Condemned, .condemn_round = 5, .size = 10})));

    /// the stale incarnation loses
    EXPECT_TRUE(std::holds_alternative<Conflict>(casMeta(op, store->layout(), ref, lm->etag,
        BlobMeta{.state = MetaState::Clean})));
}

TEST(CASBlobMeta, DeleteMetaExactMatchesTheObservedIncarnation)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    CasOperation op = store->mountRequests().admit();
    const BlobRef ref{BlobHashAlgo::CityHash128, BlobDigest::fromU128(u128Of("hash-b"))};
    putMetaIfAbsent(op, store->layout(), ref, BlobMeta{.state = MetaState::Condemned});
    const auto lm = loadMeta(op, store->layout(), ref);
    ASSERT_TRUE(lm.has_value());
    EXPECT_EQ(deleteMetaExact(op, store->layout(), ref, lm->etag), Removal::Removed);
    EXPECT_FALSE(loadMeta(op, store->layout(), ref).has_value());
}

/// Phase 3 T3 (mixed-algo pools, was CAS pluggable-blob-hash Phase 2 Task 5 crux Test 2): the `.meta`
/// API round-trips a 32-byte (`sha256`-width) `BlobRef` key — the meta object lands under a 64-hex
/// key, exercising the SAME `putMetaIfAbsent`/`loadMeta`/`casMeta`/`deleteMetaExact` surface PartWriteTxn/Gc
/// use, just at a wider algo.
TEST(CASBlobMeta, PutLoadCasDeleteRoundTripAtWidth32)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    CasOperation op = store->mountRequests().admit();
    const Layout & layout = store->layout();

    /// A distinguishable 32-byte digest (not merely a 16-byte value zero-tailed): every byte set.
    BlobDigest h;
    for (size_t i = 0; i < h.bytes.size(); ++i)
        h.bytes[i] = static_cast<uint8_t>(i + 1);
    const BlobRef ref{BlobHashAlgo::Sha256, h};
    const String hex = codecFor(BlobHashAlgo::Sha256).toHex(h);
    EXPECT_EQ(hex.size(), 64u) << "a 32-byte digest renders 64 hex chars";

    ASSERT_TRUE(std::holds_alternative<Committed>(
        putMetaIfAbsent(op, store->layout(), ref, BlobMeta{.state = MetaState::Clean, .size = 555})));
    EXPECT_TRUE(op.head(layout.blobMetaKey(ref), Retry::standard()).has_value())
        << "the meta object must land under the 64-hex key, not a truncated 32-hex one";

    const auto lm = loadMeta(op, layout, ref);
    ASSERT_TRUE(lm.has_value());
    EXPECT_EQ(lm->meta.state, MetaState::Clean);
    EXPECT_EQ(lm->meta.size, 555u);

    ASSERT_TRUE(std::holds_alternative<Committed>(casMeta(op, layout, ref, lm->etag,
        BlobMeta{.state = MetaState::Condemned, .condemn_round = 7, .size = 555})));
    const auto lm2 = loadMeta(op, layout, ref);
    ASSERT_TRUE(lm2.has_value());
    EXPECT_EQ(lm2->meta.state, MetaState::Condemned);

    EXPECT_EQ(deleteMetaExact(op, layout, ref, lm2->etag), Removal::Removed);
    EXPECT_FALSE(loadMeta(op, layout, ref).has_value());
}

namespace
{

/// The fault sits on the transport primitive, which is what every marker write reaches the store
/// through: a create is a `write` with no precondition, a compare-swap a `write` with one.
class ControlledMetaWriteFaultBackend : public InMemoryBackend
{
public:
    bool throw_next_create = false;
    bool throw_next_overwrite = false;
    uint64_t create_attempts = 0;
    uint64_t overwrite_attempts = 0;

    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value,
                                             TransportAccess & access) override
    {
        if (expected_value)
        {
            ++overwrite_attempts;
            if (throw_next_overwrite)
            {
                throw_next_overwrite = false;
                throw Poco::TimeoutException("scripted meta overwrite ambiguity");
            }
        }
        else
        {
            ++create_attempts;
            if (throw_next_create)
            {
                throw_next_create = false;
                throw Poco::TimeoutException("scripted meta create ambiguity");
            }
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }
};

}

/// An ambiguous marker write is settled by the engine's exact read and reissued, rather than escaping
/// as a raw transport error: the create's resolve proves the key still absent, the compare-swap's
/// proves the expected incarnation still current, and both are repeatable.
TEST(CASBlobMeta, AnAmbiguousMarkerWriteIsResolvedAndReissued)
{
    auto backend = std::make_shared<ControlledMetaWriteFaultBackend>();
    auto store = openPoolForTest(backend);
    store->setCasRetrySleepForTest([](uint64_t) {});
    CasOperation op = store->mountRequests().admit();
    backend->create_attempts = 0;
    backend->overwrite_attempts = 0;

    const BlobRef ref{BlobHashAlgo::CityHash128, BlobDigest::fromU128(u128Of("hash-controlled"))};
    backend->throw_next_create = true;
    EXPECT_TRUE(std::holds_alternative<Committed>(
        putMetaIfAbsent(op, store->layout(), ref, BlobMeta{.state = MetaState::Clean, .size = 10})));
    EXPECT_EQ(backend->create_attempts, 2u);

    const auto clean = loadMeta(op, store->layout(), ref);
    ASSERT_TRUE(clean.has_value());
    backend->throw_next_overwrite = true;
    EXPECT_TRUE(std::holds_alternative<Committed>(casMeta(op, store->layout(), ref, clean->etag,
        BlobMeta{.state = MetaState::Condemned, .condemn_round = 1, .size = 10})));
    EXPECT_EQ(backend->overwrite_attempts, 2u);
}

/// `cas-inspect` dispatch (CasInspect.cpp): a `.meta` key must decode as a BlobMeta, NOT fall through
/// to the `blobs/` envelope branch (the `.meta` key shares the `blobsPrefix()` prefix with a body key).
TEST(CASBlobMeta, InspectRendersCondemnedMeta)
{
    const Layout layout("p");
    const BlobRef ref{BlobHashAlgo::CityHash128, BlobDigest::fromU128(u128Of("hash-inspect"))};
    const String key = layout.blobMetaKey(ref);
    const BlobMeta m{.version = 1, .state = MetaState::Condemned, .condemn_round = 9, .size = 123};

    const String json = caInspectToJson(layout, key, encodeBlobMeta(m));
    EXPECT_NE(json.find("\"object\":\"blob_meta\""), String::npos);
    EXPECT_NE(json.find("\"condemned\""), String::npos);
    EXPECT_NE(json.find("\"condemn_round\":9"), String::npos);
    EXPECT_NE(json.find("\"size\":123"), String::npos);
}

TEST(CASBlobMeta, InspectRendersCleanMeta)
{
    const Layout layout("p");
    const BlobRef ref{BlobHashAlgo::CityHash128, BlobDigest::fromU128(u128Of("hash-inspect-clean"))};
    const String key = layout.blobMetaKey(ref);
    const BlobMeta m{.version = 1, .state = MetaState::Clean, .condemn_round = 0, .size = 7};

    const String json = caInspectToJson(layout, key, encodeBlobMeta(m));
    EXPECT_NE(json.find("\"clean\""), String::npos);
}
