#include <DataTypes/UDT/PhysicalTypeFingerprint.h>

#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesBinaryEncoding.h>

#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/tests/gtest_global_register.h>

#include <gtest/gtest.h>

namespace DB::UDT
{
namespace
{

String toHex(const Digest & digest)
{
    constexpr char digits[] = "0123456789abcdef";
    String result(digest.size() * 2, '0');
    for (size_t index = 0; index < digest.size(); ++index)
    {
        result[2 * index] = digits[digest[index] >> 4];
        result[2 * index + 1] = digits[digest[index] & 0x0f];
    }
    return result;
}

String toHex(std::string_view bytes)
{
    constexpr char digits[] = "0123456789abcdef";
    String result(bytes.size() * 2, '0');
    for (size_t index = 0; index < bytes.size(); ++index)
    {
        const auto byte = static_cast<UInt8>(bytes[index]);
        result[2 * index] = digits[byte >> 4];
        result[2 * index + 1] = digits[byte & 0x0f];
    }
    return result;
}

}

TEST(UDTPhysicalFingerprint, IsDeterministicAndCoversStorageParameters)
{
    const auto & factory = DataTypeFactory::instance();
    const auto first = factory.get("Tuple(DateTime64(3, 'UTC'), Dynamic(max_types=7))");
    const auto identical = factory.get("Tuple(DateTime64(3, 'UTC'), Dynamic(max_types=7))");
    const auto different_precision = factory.get("Tuple(DateTime64(6, 'UTC'), Dynamic(max_types=7))");
    const auto different_dynamic_limit = factory.get("Tuple(DateTime64(3, 'UTC'), Dynamic(max_types=8))");

    EXPECT_EQ(physicalTypeFingerprint(first), physicalTypeFingerprint(identical));
    EXPECT_NE(physicalTypeFingerprint(first), physicalTypeFingerprint(different_precision));
    EXPECT_NE(physicalTypeFingerprint(first), physicalTypeFingerprint(different_dynamic_limit));
}

TEST(UDTPhysicalFingerprint, CurrentStreamingPathIsByteIdenticalToTheFrozenBinaryEncoding)
{
    const auto type = DataTypeFactory::instance().get(
        "Tuple(named Array(Enum8('first' = -1, 'second' = 2)), payload JSON(max_dynamic_paths=17, max_dynamic_types=7))");
    const String binary_encoding = encodeCanonicalDataType(type);

    EXPECT_EQ(physicalTypeFingerprint(type), hashDomainSeparated(physical_type_fingerprint_domain, std::string_view(binary_encoding)));
}

TEST(UDTPhysicalFingerprint, FrozenCurrentFingerprintCoversNestedCanonicalPhysicalSurface)
{
    tryRegisterAggregateFunctions();
    const auto type = DataTypeFactory::instance().get(
        "Tuple(payload JSON(max_dynamic_paths=17, max_dynamic_types=7, z.path Dynamic(max_types=9), a.path Array(UInt16), "
        "SKIP z.skip, SKIP a.skip, SKIP REGEXP '^tmp', SKIP REGEXP '^private'), direct Dynamic(max_types=13), "
        "state AggregateFunction(1, sumMapFiltered([1, 4, 8]), Array(UInt64), Array(UInt64)))");
    const String binary_encoding = encodeCanonicalDataType(type);

    EXPECT_EQ(binary_encoding.size(), 109U);
    EXPECT_EQ(
        toHex(std::string_view(binary_encoding)),
        "2003077061796c6f6164300011070206612e706174681e02067a2e706174682b090206612e736b6970067a2e736b697002085e707269"
        "76617465045e746d70066469726563742b0d05737461746525010e73756d4d617046696c7465726564010d03010101040108021e041e04");
    EXPECT_EQ(toHex(physicalTypeFingerprint(type)), "7be68e2acbede925ce8b16b8490c3aad597d211d437ff14042b8279c29d38614");
}

TEST(UDTPhysicalFingerprint, CurrentStreamingPathDoesNotAllocateForLargeEncodings)
{
    DataTypeEnum16::Values values;
    values.reserve(256);
    for (Int16 value = 0; value < 256; ++value)
        values.emplace_back(String(64, static_cast<char>('a' + value % 26)) + std::to_string(value), value);
    const DataTypePtr type = std::make_shared<DataTypeEnum16>(std::move(values));

    Digest fingerprint{};
    {
        DENY_ALLOCATIONS_IN_SCOPE;
        fingerprint = physicalTypeFingerprint(type);
    }
    EXPECT_NE(fingerprint, Digest{});
}

TEST(UDTPhysicalFingerprint, RejectsNull)
{
    EXPECT_THROW(physicalTypeFingerprint(nullptr), Exception);
}

}
