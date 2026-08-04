#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasCodecUtil.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Common/Exception.h>

using namespace DB::Cas;

TEST(CASSourceEdge, IdIsDeterministicAndPathSensitive)
{
    const ManifestId id{RootNamespace{"00/aa@cas@"}, ManifestRef{.writer_epoch = 1, .build_sequence = 15, .manifest_ordinal = 1}};
    EXPECT_EQ(sourceEdgeId(id, "a.bin"), sourceEdgeId(id, "a.bin"));           // deterministic
    EXPECT_NE(sourceEdgeId(id, "a.bin"), sourceEdgeId(id, "b.bin"));           // path-sensitive
    const ManifestId id2{id.root_namespace, ManifestRef{.writer_epoch = 1, .build_sequence = 31, .manifest_ordinal = 1}};
    EXPECT_NE(sourceEdgeId(id, "a.bin"), sourceEdgeId(id2, "a.bin"));          // ref-sensitive
}

TEST(CASSourceEdge, RunKeyRoundTripsAndOrdersByBlobThenSource)
{
    const BlobRef b1{BlobHashAlgo::CityHash128, BlobDigest::fromU128(UInt128(1))};
    const BlobRef b2{BlobHashAlgo::CityHash128, BlobDigest::fromU128(UInt128(2))};
    const UInt128 s1(10);
    const UInt128 s2(20);

    BlobRef gb;
    UInt128 gs;
    SourceEdgeKeyCodec::parse(SourceEdgeKeyCodec::key(b1, s1), gb, gs);
    EXPECT_EQ(gb, b1);
    EXPECT_EQ(gs, s1);
    EXPECT_LT(SourceEdgeKeyCodec::key(b1, s2), SourceEdgeKeyCodec::key(b2, s1));   // ref is the primary sort
    EXPECT_LT(SourceEdgeKeyCodec::key(b1, s1), SourceEdgeKeyCodec::key(b1, s2));   // source_id is the secondary sort
}

TEST(CASSourceEdge, KeyCodecSha256RoundTripAndRejectsBadSizes)
{
    /// sha256 (32-byte digest) round trip: key is 1 + 32 + 16 = 49 bytes, parse recovers the full ref.
    BlobDigest d32{};
    for (size_t i = 0; i < d32.bytes.size(); ++i)
        d32.bytes[i] = static_cast<uint8_t>(i + 1);
    const BlobRef sha_ref{BlobHashAlgo::Sha256, d32};
    const UInt128 sid(0xABCDu);
    const String key32 = SourceEdgeKeyCodec::key(sha_ref, sid);
    ASSERT_EQ(key32.size(), 49u);
    BlobRef gb;
    UInt128 gs;
    SourceEdgeKeyCodec::parse(key32, gb, gs);
    EXPECT_EQ(gb, sha_ref);
    EXPECT_EQ(gs, sid);

    /// ch128 (16-byte digest): key is 1 + 16 + 16 = 33 bytes.
    const BlobRef ch_ref{BlobHashAlgo::CityHash128, BlobDigest::fromU128(UInt128(0x0102030405060708ULL))};
    const String key16 = SourceEdgeKeyCodec::key(ch_ref, sid);
    ASSERT_EQ(key16.size(), 33u);
    EXPECT_EQ(key16.substr(1), String(reinterpret_cast<const char *>(ch_ref.digest.bytes.data()), 16) + u128ToBytesBE(sid));

    /// Fail-close: a wrong-size key throws CORRUPTED_DATA, never a silent false. `key16` truncated by
    /// one byte still declares algo=ch128 (33-byte width expected) but is only 32 bytes.
    EXPECT_THROW(SourceEdgeKeyCodec::parse(key16.substr(0, key16.size() - 1), gb, gs), DB::Exception);
    EXPECT_THROW(SourceEdgeKeyCodec::parse(String(20, '\0'), gb, gs), DB::Exception);

    /// Unknown algo byte -> NOT_IMPLEMENTED (fail closed).
    String bad_key = key32;
    bad_key[0] = static_cast<char>(99);
    EXPECT_THROW(SourceEdgeKeyCodec::parse(bad_key, gb, gs), DB::Exception);
}

TEST(CASSourceEdge, KeyOrderSentinelFirstAtLen32)
{
    /// At sha256 width, the sentinel (source_id 0) sorts before any nonzero source_id for the same
    /// digest, and digest magnitude order is preserved (big-endian raw-byte lexicographic order ==
    /// numeric magnitude order for a width-homogeneous run — the consult's load-bearing fact).
    BlobDigest d{};
    d.bytes[0] = 0x10;
    const BlobRef ref{BlobHashAlgo::Sha256, d};
    EXPECT_LT(SourceEdgeKeyCodec::key(ref, UInt128(0)), SourceEdgeKeyCodec::key(ref, UInt128(1)));

    BlobDigest d_small{};
    d_small.bytes[0] = 0x01;
    BlobDigest d_large{};
    d_large.bytes[0] = 0x02;
    const BlobRef ref_small{BlobHashAlgo::Sha256, d_small};
    const BlobRef ref_large{BlobHashAlgo::Sha256, d_large};
    EXPECT_LT(SourceEdgeKeyCodec::key(ref_small, UInt128(5)), SourceEdgeKeyCodec::key(ref_large, UInt128(5)));
}
