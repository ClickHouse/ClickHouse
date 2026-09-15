#include <gtest/gtest.h>

/// `BlobDigest` is the pool-scoped variable-length content digest. It is additive: existing
/// `UInt128 blob_hash` fields retain their representation. The `PoolMeta`-scoped
/// `DigestCodec` all digest<->hex/bytes conversion must route through.
///
/// THE KEY GATE (`ShardOfBitIdenticalToOldHighBitsOver200RandomValues` below): `DigestCodec`'s
/// `shardOf` (an explicit big-endian read of the first 8 digest bytes) must be bit-identical to
/// today's `static_cast<uint64_t>(blob_hash >> 64)` (`CasGcShardPlan.h`'s `blobShard`) for every
/// 128-bit digest -- otherwise an existing cityHash128/xxh3-128 pool would silently reshard on
/// upgrade. This is load-bearing: it is what makes Phase 2 safe to land under running pools.

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasCodecUtil.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcShardPlan.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include "cas_test_helpers.h"

#include <base/defines.h>

#include <cstdint>
#include <random>
#include <unordered_map>
#include <vector>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

UInt128 randomU128(std::mt19937_64 & rng)
{
    const UInt128 hi = rng();
    const UInt128 lo = rng();
    return (hi << 64) | lo;
}

}

/// ---- THE KEY GATE ----

TEST(CASBlobDigest, ShardOfBitIdenticalToOldHighBitsOver200RandomValues)
{
    std::mt19937_64 rng(0xC0FFEE); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed is required for reproducible property coverage.
    const DigestCodec codec16(/*blob_hash_len*/ 16);

    for (int i = 0; i < 200; ++i)
    {
        const UInt128 v = randomU128(rng);
        const uint64_t old_high64 = static_cast<uint64_t>(v >> 64);
        const uint64_t got = codec16.shardOf(BlobDigest::fromU128(v));
        EXPECT_EQ(got, old_high64) << "mismatch for random UInt128 iteration " << i;
    }

    /// Edge cases: all-zero and all-one high halves.
    EXPECT_EQ(codec16.shardOf(BlobDigest::fromU128(UInt128(0))), 0u);
    const UInt128 all_ones = ~UInt128(0);
    EXPECT_EQ(codec16.shardOf(BlobDigest::fromU128(all_ones)), static_cast<uint64_t>(all_ones >> 64));
}

/// The same gate, but via `Cas::codecFor` (`CasBlobRef.h`), the ONE way production code obtains a
/// codec (Phase 3 T4 deleted the pool-scoped `DigestCodec(PoolMeta)` constructor -- a mixed-algo
/// pool has no single width; the codec is selected per-algo, never per-pool).
TEST(CASBlobDigest, ShardOfViaPoolMetaConstructedCodecMatchesOldBlobShard)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");
    OperationForTest meta_op(*backend);
    const PoolMeta pm = PoolMeta::createOrValidate(*meta_op, layout, /*blob_header_len*/ 256, BlobHashAlgo::CityHash128, /*allow_new*/ false, /*allow_mint*/ true);
    ASSERT_EQ(pm.algos_used, (std::vector<uint8_t>{static_cast<uint8_t>(BlobHashAlgo::CityHash128)}));
    const DigestCodec codec = codecFor(BlobHashAlgo::CityHash128);

    std::mt19937_64 rng(12345); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed is required for reproducible property coverage.
    for (int i = 0; i < 200; ++i)
    {
        const UInt128 v = randomU128(rng);
        EXPECT_EQ(codec.shardOf(BlobDigest::fromU128(v)), static_cast<uint64_t>(v >> 64));
        /// `blobShard` (`CasGcShardPlan.h`) additionally takes `% gc_shards`; at `gc_shards == 1`
        /// every hash routes to shard 0, so this only pins the trivial single-shard case -- the
        /// bit-identical pre-mod value is already pinned by the assertion above.
        EXPECT_EQ(blobShard(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(v)}, /*gc_shards*/ 1), 0u);
    }
}

/// ---- round-trip ----

TEST(CASBlobDigest, HexRoundTripLen16)
{
    const DigestCodec codec(16);
    std::mt19937_64 rng(1); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed is required for reproducible property coverage.
    for (int i = 0; i < 50; ++i)
    {
        const BlobDigest d = BlobDigest::fromU128(randomU128(rng));
        const String hex = codec.toHex(d);
        EXPECT_EQ(hex.size(), 32u);
        EXPECT_EQ(codec.fromHex(hex), d);
    }
}

TEST(CASBlobDigest, HexRoundTripLen32)
{
    const DigestCodec codec(32);
    BlobDigest d;
    for (size_t i = 0; i < d.bytes.size(); ++i)
        d.bytes[i] = static_cast<uint8_t>(i * 7 + 1);

    const String hex = codec.toHex(d);
    EXPECT_EQ(hex.size(), 64u);
    EXPECT_EQ(codec.fromHex(hex), d);
}

TEST(CASBlobDigest, BytesBERoundTripLen16)
{
    const DigestCodec codec(16);
    std::mt19937_64 rng(2); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed is required for reproducible property coverage.
    for (int i = 0; i < 50; ++i)
    {
        const BlobDigest d = BlobDigest::fromU128(randomU128(rng));
        const String bytes = codec.toBytesBE(d);
        EXPECT_EQ(bytes.size(), 16u);
        EXPECT_EQ(codec.fromBytesBE(bytes), d);
    }
}

TEST(CASBlobDigest, BytesBERoundTripLen32)
{
    const DigestCodec codec(32);
    BlobDigest d;
    for (size_t i = 0; i < d.bytes.size(); ++i)
        d.bytes[i] = static_cast<uint8_t>(255 - i);

    const String bytes = codec.toBytesBE(d);
    EXPECT_EQ(bytes.size(), 32u);
    EXPECT_EQ(codec.fromBytesBE(bytes), d);
}

/// `toBytesBE` at len16 must produce exactly the `u128ToBytesBE` bytes for the 16-byte prefix --
/// same byte order, so a 128-bit pool's on-wire bytes stay unchanged when a later task migrates a
/// field from `UInt128` to `BlobDigest`.
TEST(CASBlobDigest, BytesBEAgreesWithU128ToBytesBEAtLen16)
{
    const DigestCodec codec(16);
    std::mt19937_64 rng(3); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed is required for reproducible property coverage.
    for (int i = 0; i < 20; ++i)
    {
        const UInt128 v = randomU128(rng);
        EXPECT_EQ(codec.toBytesBE(BlobDigest::fromU128(v)), u128ToBytesBE(v));
    }
}

/// ---- width rejection ----

TEST(CASBlobDigest, FromHexRejectsWrongWidth)
{
    const DigestCodec codec16(16);
    const DigestCodec codec32(32);

    /// A 16-byte codec must reject a 64-hex (32-byte) string.
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&] { codec16.fromHex(std::string(64, 'a')); });
    /// A 32-byte codec must reject a 32-hex (16-byte) string.
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&] { codec32.fromHex(std::string(32, 'a')); });
    /// Any non-hex character is rejected too.
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&] { codec16.fromHex(std::string(31, 'a') + "z"); });
}

TEST(CASBlobDigest, FromBytesBERejectsWrongWidth)
{
    const DigestCodec codec16(16);
    const DigestCodec codec32(32);

    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&] { codec16.fromBytesBE(std::string(32, '\0')); });
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&] { codec32.fromBytesBE(std::string(16, '\0')); });
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&] { codec16.fromBytesBE(std::string(15, '\0')); });
}

/// ---- UInt128 conversion ----

TEST(CASBlobDigest, U128RoundTrip)
{
    std::mt19937_64 rng(4); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed is required for reproducible property coverage.
    for (int i = 0; i < 200; ++i)
    {
        const UInt128 v = randomU128(rng);
        EXPECT_EQ(BlobDigest::fromU128(v).toU128(), v);
    }
    EXPECT_EQ(BlobDigest::fromU128(UInt128(0)).toU128(), UInt128(0));
}

TEST(CASBlobDigest, FromU128LeavesTailZero)
{
    std::mt19937_64 rng(5); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed is required for reproducible property coverage.
    const UInt128 v = randomU128(rng);
    const BlobDigest d = BlobDigest::fromU128(v);
    for (size_t i = 16; i < d.bytes.size(); ++i)
        EXPECT_EQ(d.bytes[i], 0u) << "tail byte " << i << " must be zero for a 128-bit-pool digest";
}

/// ---- hasher / container use ----

TEST(CASBlobDigest, UsableAsUnorderedMapKey)
{
    std::mt19937_64 rng(6); // NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed is required for reproducible property coverage.
    std::unordered_map<BlobDigest, int, BlobDigestHash> m;
    std::vector<BlobDigest> digests;
    for (int i = 0; i < 20; ++i)
    {
        const BlobDigest d = BlobDigest::fromU128(randomU128(rng));
        digests.push_back(d);
        m[d] = i;
    }
    for (int i = 0; i < 20; ++i)
        EXPECT_EQ(m.at(digests[static_cast<size_t>(i)]), i);
}

/// ---- PoolMeta::algos_used records the creating algo (Phase 3 T4 -- the width itself is no longer
/// pool state at all: `blobHashLenFor(algo)`/`codecFor(algo)` derive it per-algo, never per-pool) ----

TEST(CASBlobDigest, PoolMetaRecordsCreatingAlgoAndWidthDerivesFromIt)
{
    {
        auto backend = std::make_shared<InMemoryBackend>();
        const Layout layout("p1");
        OperationForTest meta_op(*backend);
        const PoolMeta pm = PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::CityHash128, /*allow_new*/ false, /*allow_mint*/ true);
        EXPECT_EQ(pm.algos_used, (std::vector<uint8_t>{static_cast<uint8_t>(BlobHashAlgo::CityHash128)}));
        EXPECT_EQ(blobHashLenFor(BlobHashAlgo::CityHash128), 16u);
    }
    {
        auto backend = std::make_shared<InMemoryBackend>();
        const Layout layout("p2");
        OperationForTest meta_op(*backend);
        const PoolMeta pm = PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::XXH3_128, /*allow_new*/ false, /*allow_mint*/ true);
        EXPECT_EQ(pm.algos_used, (std::vector<uint8_t>{static_cast<uint8_t>(BlobHashAlgo::XXH3_128)}));
        EXPECT_EQ(blobHashLenFor(BlobHashAlgo::XXH3_128), 16u);
    }
    {
        auto backend = std::make_shared<InMemoryBackend>();
        const Layout layout("p3");
        OperationForTest meta_op(*backend);
        const PoolMeta pm = PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::Sha256, /*allow_new*/ false, /*allow_mint*/ true);
        EXPECT_EQ(pm.algos_used, (std::vector<uint8_t>{static_cast<uint8_t>(BlobHashAlgo::Sha256)}));
        EXPECT_EQ(blobHashLenFor(BlobHashAlgo::Sha256), 32u);

        /// Reopen (decode path) must re-derive the same recorded algo.
        const PoolMeta reopened = PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::Sha256);
        EXPECT_EQ(reopened.algos_used, (std::vector<uint8_t>{static_cast<uint8_t>(BlobHashAlgo::Sha256)}));
    }
}

/// ---- zero-tail len-drift guard (debug/sanitizer builds only: chassert aborts the process) ----

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASBlobDigestDeathTest, ZeroTailChassertFiresOnNonZeroTailAtLen16)
{
    const DigestCodec codec16(16);
    BlobDigest d = BlobDigest::fromU128(UInt128(1));
    d.bytes[16] = 0x42; /// corrupt a tail byte beyond the pool's 16-byte width

    EXPECT_DEATH({ (void)codec16.toHex(d); }, "");
    EXPECT_DEATH({ (void)codec16.toBytesBE(d); }, "");
}
#endif
