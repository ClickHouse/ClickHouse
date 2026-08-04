#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <unordered_set>

using namespace DB::Cas;

TEST(CASIds, StrongTypingAndContainers)
{
    /// Test the strong-typed-string class `RootNamespace`.
    /// (`BlobId` was deleted in the mixed-algo-pools refactor; `TreeId` was part of the
    /// standalone-tree layer excised in the rev. 15 `PartManifest` redesign.)
    RootNamespace ns1{"srv1"};
    RootNamespace ns2{"srv1"};
    RootNamespace ns3{"srv2"};
    EXPECT_EQ(ns1, ns2);
    EXPECT_NE(ns1, ns3);
    std::unordered_set<RootNamespace> s{ns1, ns3};
    EXPECT_EQ(s.size(), 2u);
}

TEST(CASIds, HexU128RoundTrip)
{
    // UInt128 is a global typedef (wide::integer<128,unsigned>), not in DB:: namespace.
    const UInt128 v = (UInt128(0x0123456789abcdefULL) << 64) | 0xfedcba9876543210ULL;
    const auto hex = u128ToHex(v);
    EXPECT_EQ(hex.size(), 32u);
    EXPECT_EQ(hexToU128(hex), v);
    EXPECT_THROW(hexToU128("zz"), DB::Exception);          // not hex
    EXPECT_THROW(hexToU128("0123"), DB::Exception);        // wrong length
}

TEST(CASToken, Basics)
{
    Token a{"etag-1", TokenType::ETag};
    Token b{"etag-1", TokenType::ETag};
    Token c{"etag-2", TokenType::ETag};
    EXPECT_EQ(a, b);
    EXPECT_NE(a, c);
    EXPECT_TRUE(Token{}.empty());
    EXPECT_FALSE(a.empty());
}
