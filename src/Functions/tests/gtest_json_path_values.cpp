#include <gtest/gtest.h>

#include <Columns/ColumnObject.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/JSONPathValues.h>
#include <Functions/JSONValueEnumerator.h>

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

struct RejectingJSONValueConsumer
{
    bool shouldConsumePath(std::string_view) const { return true; }

    bool shouldConsumeValue(std::string_view, const IDataType &)
    {
        ++value_checks;
        return false;
    }

    void consumeSharedScalar(std::string_view, BinaryTypeIndex, std::string_view) { ++consumed_values; }
    void consumeValue(
        std::string_view,
        const IDataType &,
        std::string_view,
        const ISerialization &,
        const IColumn &,
        size_t,
        bool,
        const FormatSettings &)
    {
        ++consumed_values;
    }
    void consumeNull(std::string_view, bool) {}
    void setRow(size_t) {}
    void finishRows(size_t) {}

    size_t value_checks = 0;
    size_t consumed_values = 0;
};

}

GTEST_TEST(JSONPathValues, SharedValuesAreRejectedBeforeDeserialization)
{
    const auto type = DataTypeFactory::instance().get("JSON(max_dynamic_paths = 0)");
    auto column = type->createColumn();
    column->insert(Object{{"array", Array{Field{1u}, Field{2u}}}, {"string", Field{"ignored"}}});

    RejectingJSONValueConsumer consumer;
    enumerateJSONValues(
        assert_cast<const ColumnObject &>(*column),
        assert_cast<const DataTypeObject &>(*type),
        consumer);

    EXPECT_EQ(consumer.value_checks, 2);
    EXPECT_EQ(consumer.consumed_values, 0);
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
    EXPECT_EQ(tryGetCompleteScalarValue(complete->token), "x");
    EXPECT_FALSE(tryGetCompleteScalarValue(std::string_view{"\x01", 1}));

    const auto truncated = encodeValue("a", string_type, "abcdef", 16, false);
    ASSERT_TRUE(truncated);
    EXPECT_FALSE(truncated->complete);
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
    const auto validation = encodeDynamicValidation("d", 64);
    ASSERT_TRUE(validation);
    EXPECT_EQ(toHex(*validation), "640000000007");
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
        for (const auto & rhs : values)
        {
            String encoded_rhs;
            appendEscapedComponent(encoded_rhs, rhs);
            EXPECT_EQ(lhs < rhs, encoded_lhs < encoded_rhs);
        }
    }
}

GTEST_TEST(JSONPathValues, PathMatcherIncludesExactSubtrees)
{
    using namespace JSONPathValues;

    const PathMatcher matcher(
        {"items[].id", "payload-other", "payload.ids", "request_id", "request_id"},
        {},
        {},
        {});

    EXPECT_TRUE(matcher.shouldIndex("request_id"));
    EXPECT_TRUE(matcher.shouldIndex("payload.ids"));
    EXPECT_TRUE(matcher.shouldIndex("payload.ids.primary"));
    EXPECT_TRUE(matcher.shouldIndex("items[].id"));
    EXPECT_FALSE(matcher.shouldIndex("request"));
    EXPECT_FALSE(matcher.shouldIndex("payload.identity"));
    EXPECT_FALSE(matcher.shouldIndex("items[].name"));
    EXPECT_TRUE(matcher.shouldIndex("payload-other"));

    EXPECT_TRUE(matcher.shouldVisit("payload"));
    EXPECT_TRUE(matcher.shouldVisit("payload.ids"));
    EXPECT_TRUE(matcher.shouldVisit("items"));
    EXPECT_TRUE(matcher.shouldVisit("items[]"));
    EXPECT_FALSE(matcher.shouldVisit("message"));

    EXPECT_EQ(
        matcher.getIncludePaths(),
        (VectorWithMemoryTracking<String>{"items[].id", "payload-other", "payload.ids", "request_id"}));
}

GTEST_TEST(JSONPathValues, PathMatcherIncludesRegexpsAndSkipsWin)
{
    using namespace JSONPathValues;

    const PathMatcher matcher(
        {"payload", "payload!"},
        {"(?:^|\\.)(?:.*_id|.*_at)$", "(?:^|\\.)(?:.*_id|.*_at)$"},
        {"payload.secret"},
        {"private_"});

    EXPECT_TRUE(matcher.shouldIndex("request_id"));
    EXPECT_TRUE(matcher.shouldIndex("nested.created_at"));
    EXPECT_TRUE(matcher.shouldIndex("payload.message"));
    EXPECT_FALSE(matcher.shouldIndex("message"));
    EXPECT_FALSE(matcher.shouldIndex("payload.secret"));
    EXPECT_FALSE(matcher.shouldIndex("payload.secret.value"));
    EXPECT_FALSE(matcher.shouldIndex("private_id"));

    EXPECT_TRUE(matcher.shouldVisit("unmatched_parent"));
    EXPECT_FALSE(matcher.shouldVisit("payload.secret"));
    EXPECT_EQ(matcher.getIncludePathRegexps().size(), 1);

    const PathMatcher ancestor_regexp_matcher({}, {}, {}, {"^container$"});
    EXPECT_FALSE(ancestor_regexp_matcher.shouldIndex("container"));
    EXPECT_TRUE(ancestor_regexp_matcher.shouldVisit("container"));
    EXPECT_TRUE(ancestor_regexp_matcher.shouldIndex("container.value"));
}
