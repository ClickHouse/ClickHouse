#include <Functions/keyvaluepair/impl/KeyValuePairExtractorBuilder.h>
#include <Functions/keyvaluepair/impl/CHKeyValuePairExtractor.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnMap.h>
#include <DataTypes/DataTypeFactory.h>

#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>
#include <initializer_list>


namespace
{
using namespace DB;
using namespace std::literals;

// Print as a map with a single row
auto ToColumnMap(const auto & keys, const auto & values, const ColumnPtr offsets = nullptr)
{
    return ColumnMap::create(
        std::move(keys->clone()),
        std::move(values->clone()),
        offsets ? offsets : ColumnUInt64::create(1, keys->size())
    );
}

// Print as a map with a single row
std::string PrintMap(const auto & keys, const auto & values)
{
    auto map_column = ToColumnMap(keys, values);
    auto serialization = DataTypeFactory::instance().get("Map(String, String)")->getSerialization(ISerialization::Kind::DEFAULT);

    WriteBufferFromOwnString buff;
    serialization->serializeTextJSON(*map_column, 0, buff, FormatSettings{});

    return std::move(buff.str());
}

}

struct KeyValuePairExtractorTestParam
{
    KeyValuePairExtractorBuilder builder;
    std::string input;
    std::vector<std::pair<std::string, std::string>> expected;
};

struct extractKVPairKeyValuePairExtractorTest : public ::testing::TestWithParam<KeyValuePairExtractorTestParam>
{};

TEST_P(extractKVPairKeyValuePairExtractorTest, Match)
{
    const auto & [builder, input, expected] = GetParam();
    SCOPED_TRACE(input);

    auto kv_parser = builder.build();
    SCOPED_TRACE(typeid(kv_parser).name());

    auto keys = ColumnString::create();
    auto values = ColumnString::create();

    auto pairs_found = kv_parser->extract(input, keys, values);
    ASSERT_EQ(expected.size(), pairs_found);

    size_t i = 0;
    for (const auto & expected_kv : expected)
    {
        EXPECT_EQ(expected_kv.first, keys->getDataAt(i));

        EXPECT_EQ(expected_kv.second, values->getDataAt(i));

        ++i;
    }
}

using ExpectedValues = std::vector<std::pair<std::string, std::string>>;
const ExpectedValues neymar_expected{
    {"name","neymar"},
    {"age","31"},
    {"team","psg"},
    {"nationality","brazil"},
    {"last_key","last_value"}
};

INSTANTIATE_TEST_SUITE_P(Simple, extractKVPairKeyValuePairExtractorTest,
        ::testing::ValuesIn(std::initializer_list<KeyValuePairExtractorTestParam>
        {
            {
                KeyValuePairExtractorBuilder().withQuotingCharacter('\''),
                R"in(name:'neymar';'age':31;team:psg;nationality:brazil,last_key:last_value)in",
                neymar_expected
            },
            {
                // Different escaping char
                KeyValuePairExtractorBuilder().withQuotingCharacter('"'),
                R"in(name:"neymar";"age":31;team:psg;nationality:brazil,last_key:last_value)in",
                neymar_expected
            },
            {
                // same as case 1, but with another handler
                KeyValuePairExtractorBuilder().withQuotingCharacter('\'').withEscaping(),
                R"in(name:'neymar';'age':31;team:psg;nationality:brazil,last_key:last_value)in",
                neymar_expected
            }
        }
    )
);

// Perform best-effort parsing for invalid escape sequences
INSTANTIATE_TEST_SUITE_P(InvalidEscapeSeqInValue, extractKVPairKeyValuePairExtractorTest,
        ::testing::ValuesIn(std::initializer_list<KeyValuePairExtractorTestParam>
        {
            {
                // Special case when invalid seq is the last symbol
                KeyValuePairExtractorBuilder().withEscaping(),
                R"in(valid_key:valid_value key:invalid_val\)in",
                ExpectedValues{
                    {"valid_key", "valid_value"},
                    {"key", "invalid_val"}
                }
            },
            // Not handling escape sequences == do not care of broken one, `invalid_val\` must be present
            {
                KeyValuePairExtractorBuilder(),
                R"in(valid_key:valid_value key:invalid_val\ third_key:third_value)in",
                ExpectedValues{
                    {"valid_key", "valid_value"},
                    {"key", "invalid_val\\"},
                    {"third_key", "third_value"}
                }
            },
            {
                // Special case when invalid seq is the last symbol
                KeyValuePairExtractorBuilder(),
                R"in(valid_key:valid_value key:invalid_val\)in",
                ExpectedValues{
                    {"valid_key", "valid_value"},
                    {"key", "invalid_val\\"}
                }
            },
            {
                KeyValuePairExtractorBuilder().withQuotingCharacter('"'),
                R"in(valid_key:valid_value key:"invalid val\ " "third key":"third value")in",
                ExpectedValues{
                    {"valid_key", "valid_value"},
                    {"key", "invalid val\\ "},
                    {"third key", "third value"},
                }
            },
        }
    )
);
