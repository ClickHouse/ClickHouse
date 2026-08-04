#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <unordered_set>

using namespace DB::Cas;

TEST(CASBlobRef, SameDigestDifferentAlgoAreDistinct)
{
    const BlobDigest d = BlobDigest::fromU128(UInt128(0xDEADBEEF));
    const BlobRef a{BlobHashAlgo::CityHash128, d};
    const BlobRef b{BlobHashAlgo::XXH3_128, d};
    EXPECT_NE(a, b);
    EXPECT_LT(a, b);                                    /// algo=1 < algo=2
    std::unordered_set<BlobRef, BlobRefHash> s{a, b};
    EXPECT_EQ(s.size(), 2u);
}

TEST(CASBlobRef, OrderIsAlgoThenDigest)
{
    const BlobRef small_algo_big_digest{BlobHashAlgo::CityHash128, BlobDigest::fromU128(UInt128(0) - 1)};
    const BlobRef big_algo_small_digest{BlobHashAlgo::Sha256, BlobDigest::fromU128(UInt128(1))};
    EXPECT_LT(small_algo_big_digest, big_algo_small_digest);   /// algo decides first
}

TEST(CASBlobRef, HexAndIdRenderAtAlgoWidth)
{
    BlobRef r16{BlobHashAlgo::XXH3_128, BlobDigest::fromU128(UInt128(0xAB))};
    EXPECT_EQ(blobHexOf(r16).size(), 32u);
    EXPECT_EQ(blobIdOf(r16).substr(0, 5), "xxh3:");
    BlobRef r32{BlobHashAlgo::Sha256, {}};
    for (size_t i = 0; i < 32; ++i) r32.digest.bytes[i] = static_cast<uint8_t>(i);
    EXPECT_EQ(blobHexOf(r32).size(), 64u);
    EXPECT_EQ(blobIdOf(r32).substr(0, 7), "sha256:");
}
