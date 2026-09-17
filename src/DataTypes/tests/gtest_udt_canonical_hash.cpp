#include <gtest/gtest.h>

#include <DataTypes/UDT/CanonicalHash.h>

#include <array>
#include <stdexcept>
#include <string>

namespace DB::UDT
{
namespace
{

std::string toHex(const Digest & digest)
{
    constexpr char digits[] = "0123456789abcdef";
    std::string result(digest.size() * 2, '0');
    for (size_t index = 0; index < digest.size(); ++index)
    {
        result[2 * index] = digits[digest[index] >> 4];
        result[2 * index + 1] = digits[digest[index] & 0x0f];
    }
    return result;
}

std::span<const CanonicalByte> asCanonicalBytes(std::string_view value)
{
    return {reinterpret_cast<const CanonicalByte *>(value.data()), value.size()};
}

UUID uuidFromWords(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

}

TEST(UDTCanonicalHash, NISTSHA256Vectors)
{
    EXPECT_EQ(toHex(sha256(std::string_view{})), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    EXPECT_EQ(toHex(sha256("abc")), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

TEST(UDTCanonicalHash, StreamingIsExactAndBinarySafe)
{
    CanonicalHasher streaming("ClickHouse UDT test V1");
    streaming.update("a");
    const std::array<CanonicalByte, 2> remainder{'b', 'c'};
    streaming.update(remainder);

    EXPECT_EQ(toHex(streaming.finalize()), "bb7450f875e5f879ec5801b7e1fa9d6a3b3534df11223cd2e638bab2697f0805");
    EXPECT_THROW(streaming.update(std::string_view{}), std::logic_error);
    EXPECT_THROW(streaming.finalize(), std::logic_error);

    EXPECT_EQ(hashDomainSeparated("ClickHouse UDT test V1", "abc"), hashDomainSeparated("ClickHouse UDT test V1", asCanonicalBytes("abc")));

    const std::string_view binary_payload{"a\0b", 3};
    EXPECT_EQ(
        hashDomainSeparated("ClickHouse UDT test V1", binary_payload),
        hashDomainSeparated("ClickHouse UDT test V1", asCanonicalBytes(binary_payload)));
    EXPECT_NE(hashDomainSeparated("ClickHouse UDT test V1", binary_payload), hashDomainSeparated("ClickHouse UDT test V1", "a"));
}

TEST(UDTCanonicalHash, DomainsAreValidatedAndSeparated)
{
    EXPECT_THROW(CanonicalHasher(""), std::invalid_argument);
    EXPECT_THROW(CanonicalHasher(std::string_view{"bad\0domain", 10}), std::invalid_argument);

    EXPECT_NE(hashDomainSeparated("ClickHouse UDT A V1", "payload"), hashDomainSeparated("ClickHouse UDT B V1", "payload"));
    EXPECT_NE(hashDomainSeparated("ClickHouse UDT A V1", "payload"), hashDomainSeparated("ClickHouse UDT A V1", "payload2"));
}

TEST(UDTCanonicalHash, FramedPayloadCommitsItsMinimalLength)
{
    const auto framed = hashFramedDomainSeparated("ClickHouse UDT framed test V1", "abc");
    EXPECT_EQ(toHex(framed), "17c98aa7e749c8d3415a5653832a221c535398256caa9e3225496bbdd84b08b5");

    CanonicalHasher independent("ClickHouse UDT framed test V1");
    const std::array<CanonicalByte, 4> expected_payload{3, 'a', 'b', 'c'};
    independent.update(expected_payload);
    EXPECT_EQ(framed, independent.finalize());
    EXPECT_NE(framed, hashDomainSeparated("ClickHouse UDT framed test V1", "abc"));
    EXPECT_EQ(
        hashFramedDomainSeparated("ClickHouse UDT framed test V1", "abc"),
        hashFramedDomainSeparated("ClickHouse UDT framed test V1", asCanonicalBytes("abc")));
}

TEST(UDTCanonicalHash, UUIDUsesRFCTextualByteOrder)
{
    /// 61f0c404-5cb3-11e7-907b-a6006ad3dba0
    const UUID uuid = uuidFromWords(0x61f0c4045cb311e7ULL, 0x907ba6006ad3dba0ULL);
    constexpr CanonicalUUID expected{0x61, 0xf0, 0xc4, 0x04, 0x5c, 0xb3, 0x11, 0xe7, 0x90, 0x7b, 0xa6, 0x00, 0x6a, 0xd3, 0xdb, 0xa0};

    EXPECT_EQ(uuidToCanonicalBytes(uuid), expected);
    EXPECT_EQ(uuidFromCanonicalBytes(expected), uuid);
    EXPECT_EQ(UUIDHelpers::getHighBytes(uuid), 0x61f0c4045cb311e7ULL);
    EXPECT_EQ(UUIDHelpers::getLowBytes(uuid), 0x907ba6006ad3dba0ULL);
    EXPECT_EQ(toHex(sha256(expected)), "e1dea0d4827f190bdbcd53c360d550a083e6de41f4d21b49f830e327b83fa096");
}

TEST(UDTCanonicalHash, UUIDStreamingMatchesCanonicalBytes)
{
    /// 550e8400-e29b-41d4-a716-446655440000
    const UUID uuid = uuidFromWords(0x550e8400e29b41d4ULL, 0xa716446655440000ULL);

    CanonicalHasher by_uuid("ClickHouse UDT UUID V1");
    by_uuid.updateUUID(uuid);

    CanonicalHasher by_bytes("ClickHouse UDT UUID V1");
    by_bytes.update(uuidToCanonicalBytes(uuid));

    EXPECT_EQ(by_uuid.finalize(), by_bytes.finalize());
}

}
