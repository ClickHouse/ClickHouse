#include <gtest/gtest.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/JSONPathValues.h>

#include <vector>

using namespace DB;

namespace
{

String toHex(std::string_view value)
{
    static constexpr char digits[] = "0123456789abcdef";
    String result(value.size() * 2, '\0');
    for (size_t i = 0; i != value.size(); ++i)
    {
        const auto byte = static_cast<UInt8>(value[i]);
        result[i * 2] = digits[byte >> 4];
        result[i * 2 + 1] = digits[byte & 0x0F];
    }
    return result;
}

}

GTEST_TEST(JSONPathValues, StableTokenBytes)
{
    using namespace JSONPathValues;

    const auto string_type = std::make_shared<DataTypeString>();
    const auto int64_type = std::make_shared<DataTypeInt64>();
    const auto float64_type = std::make_shared<DataTypeFloat64>();
    const auto array_type = std::make_shared<DataTypeArray>(int64_type);
    const auto map_type = std::make_shared<DataTypeMap>(string_type, string_type);

    EXPECT_EQ(toHex(encodePathTypePrefix("a", string_type)), "610000150000");
    EXPECT_EQ(toHex(encodePathTypePrefix("n", int64_type)), "6e00000a0000");
    EXPECT_EQ(toHex(encodePathTypePrefix("u", std::make_shared<DataTypeUInt64>())), "750000040000");
    EXPECT_EQ(toHex(encodePathTypePrefix("b", DataTypeFactory::instance().get("Bool"))), "6200002d0000");
    EXPECT_EQ(toHex(encodePathTypePrefix("array", array_type)), "617272617900001e0a0000");
    EXPECT_EQ(toHex(encodePathTypePrefix("d", nullptr)), "6400000000");

    String long_path_prefix(300, 'p');
    long_path_prefix.append("\0\0\x15\0\0", 5);
    EXPECT_EQ(encodePathTypePrefix(String(300, 'p'), string_type), long_path_prefix);

    const auto complete = encodeValue("a", string_type, "x", 64);
    ASSERT_TRUE(complete);
    EXPECT_TRUE(complete->complete);
    EXPECT_EQ(toHex(complete->token), "6100001500000178");
    EXPECT_EQ(tryGetKind(complete->token), Kind::ScalarComplete);
    EXPECT_FALSE(tryGetKind(std::string_view{"\x01", 1}));

    const auto truncated = encodeValue("a", string_type, "abcdef", 16, false);
    ASSERT_TRUE(truncated);
    EXPECT_FALSE(truncated->complete);
    EXPECT_EQ(truncated->value_prefix_size, 1);
    EXPECT_EQ(toHex(truncated->token), "61000015000002614792385a2986a0bc");
    EXPECT_FALSE(encodeValue("a", string_type, "abcdefg", 14, false));

    const auto positive_zero = encodeValue("f", float64_type, "0", 64);
    const auto negative_zero = encodeValue("f", float64_type, "-0", 64);
    ASSERT_TRUE(positive_zero);
    ASSERT_TRUE(negative_zero);
    EXPECT_EQ(toHex(positive_zero->token), "6600000e00000130");
    EXPECT_EQ(toHex(negative_zero->token), "6600000e0000012d30");

    const String array_prefix = encodePathTypePrefix("a", array_type);
    const auto array_element = encodeValue(
        array_prefix, "1", 64, true, Kind::ArrayElementComplete, Kind::ArrayElementTruncated);
    ASSERT_TRUE(array_element);
    EXPECT_EQ(toHex(array_element->token), "6100001e0a00000331");

    const auto array_json_leaf = encodeValue(
        encodePathTypePrefix("items[].price", array_type),
        "10",
        64,
        true,
        Kind::ScalarComplete,
        Kind::ScalarTruncated);
    ASSERT_TRUE(array_json_leaf);
    EXPECT_EQ(toHex(array_json_leaf->token), "6974656d735b5d2e707269636500001e0a0000013130");

    const auto truncated_array_element = encodeValue(
        array_prefix, "abcdef", 17, false, Kind::ArrayElementComplete, Kind::ArrayElementTruncated);
    ASSERT_TRUE(truncated_array_element);
    EXPECT_EQ(toHex(truncated_array_element->token), "6100001e0a000004614792385a2986a0bc");

    const String map_prefix = encodePathTypePrefix("m", map_type);
    const auto map_entry = encodeMapEntry(map_prefix, std::string_view{"k\0x", 3}, "v", 64);
    ASSERT_TRUE(map_entry);
    EXPECT_EQ(toHex(map_entry->token), "6d00002715150000056b000178000076");
    const auto decoded_map_entry = tryDecodeToken(map_entry->token);
    ASSERT_TRUE(decoded_map_entry);
    ASSERT_TRUE(decoded_map_entry->encoded_map_key);
    EXPECT_EQ(tryDecodeComponent(*decoded_map_entry->encoded_map_key), String("k\0x", 3));

    const auto validation = encodeDynamicValidation("d", 64);
    ASSERT_TRUE(validation);
    EXPECT_EQ(toHex(*validation), "640000000007");
}

GTEST_TEST(JSONPathValues, StableKinds)
{
    using namespace JSONPathValues;

    EXPECT_EQ(static_cast<UInt8>(Kind::ScalarComplete), 1);
    EXPECT_EQ(static_cast<UInt8>(Kind::ScalarTruncated), 2);
    EXPECT_EQ(static_cast<UInt8>(Kind::ArrayElementComplete), 3);
    EXPECT_EQ(static_cast<UInt8>(Kind::ArrayElementTruncated), 4);
    EXPECT_EQ(static_cast<UInt8>(Kind::MapEntryComplete), 5);
    EXPECT_EQ(static_cast<UInt8>(Kind::MapEntryTruncated), 6);
    EXPECT_EQ(static_cast<UInt8>(Kind::DynamicValidation), 7);
}

GTEST_TEST(JSONPathValues, EscapedComponentsPreserveOrdering)
{
    using namespace JSONPathValues;

    const std::vector<String> values{
        "",
        String("\0", 1),
        String("\0\0", 2),
        String("\0\1", 2),
        "a",
        String("a\0", 2),
        "aa",
        "b",
        String(300, 'p'),
    };

    for (const auto & lhs : values)
    {
        String encoded_lhs;
        appendEscapedComponent(encoded_lhs, lhs);
        ASSERT_TRUE(encoded_lhs.ends_with(std::string_view{"\0\0", 2}));
        EXPECT_EQ(tryDecodeComponent(std::string_view(encoded_lhs).substr(0, encoded_lhs.size() - 2)), lhs);
        for (const auto & rhs : values)
        {
            String encoded_rhs;
            appendEscapedComponent(encoded_rhs, rhs);
            EXPECT_EQ(lhs < rhs, encoded_lhs < encoded_rhs);
        }
    }
}

GTEST_TEST(JSONPathValues, RejectsMalformedTokens)
{
    using namespace JSONPathValues;

    EXPECT_FALSE(tryDecodeToken(std::string_view{"\0\2\0\0\1", 5}));
    EXPECT_FALSE(tryDecodeToken(std::string_view{"a\0", 2}));
    EXPECT_FALSE(tryDecodeToken(std::string_view{"\0\0\0\0\0", 5}));
    EXPECT_FALSE(tryDecodeToken(std::string_view{"\0\0\0\0\2short", 10}));
    EXPECT_FALSE(tryDecodeToken(std::string_view{"\0\0\0\0\5x", 6}));
    EXPECT_FALSE(tryDecodeToken(std::string_view{"\0\0x\0\0\10", 7}));
    EXPECT_FALSE(tryDecodeToken(std::string_view{"\0\0\0\0\6key", 8}));
    EXPECT_FALSE(tryDecodeComponent(std::string_view{"\0", 1}));
}
