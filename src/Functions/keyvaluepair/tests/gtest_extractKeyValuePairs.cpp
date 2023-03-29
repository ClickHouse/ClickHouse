#include <Functions/keyvaluepair/impl/KeyValuePairExtractorBuilder.h>
#include <Functions/keyvaluepair/impl/CHKeyValuePairExtractor.h>
#include <Functions/keyvaluepair/impl/StateHandler.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnMap.h>
#include <DataTypes/DataTypeFactory.h>

#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>

#include <ostream>
#include <string_view>
#include <gtest/gtest.h>
#include <initializer_list>
#include <string_view>
#include <Core/iostream_debug_helpers.h>

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

template <typename T>
struct Dump
{
    const T & value;

    friend std::ostream & operator<<(std::ostream & ostr, const Dump & d)
    {
        return dumpValue(ostr, d.value);
    }
};

template <typename T>
auto print_with_dump(const T & value)
{
    return Dump<T>{value};
}

}

struct KeyValuePairExtractorTestParam
{
    KeyValuePairExtractorBuilder builder;
    std::string input;
    std::vector<std::pair<std::string, std::string>> expected;
};

struct extractKVPair_KeyValuePairExtractorTest : public ::testing::TestWithParam<KeyValuePairExtractorTestParam>
{};

TEST_P(extractKVPair_KeyValuePairExtractorTest, Match)
{
    const auto & [builder, input, expected] = GetParam();
    SCOPED_TRACE(input);

    auto kv_parser = builder.build();
    SCOPED_TRACE(typeid(kv_parser).name());

    auto keys = ColumnString::create();
    auto values = ColumnString::create();

    auto pairs_found = kv_parser->extract(input, keys, values);
    ASSERT_EQ(expected.size(), pairs_found)
            << "\texpected: " << print_with_dump(expected) << "\n"
            << "\tactual  : " << print_with_dump(*ToColumnMap(keys, values));

    size_t i = 0;
    for (const auto & expected_kv : expected)
    {
        EXPECT_EQ(expected_kv.first,  keys->getDataAt(i))
                << fancyQuote(expected_kv.first) << "\nvs\n"
                << fancyQuote(keys->getDataAt(i));

        EXPECT_EQ(expected_kv.second, values->getDataAt(i))
                << fancyQuote(expected_kv.second) << "\nvs\n"
                << fancyQuote(values->getDataAt(i));

        ++i;
    }
}

const std::vector<std::pair<std::string, std::string>> neymar_expected{
    {"name","neymar"},
    {"age","31"},
    {"team","psg"},
    {"nationality","brazil"},
    {"last_key","last_value"}
};

INSTANTIATE_TEST_SUITE_P(Simple, extractKVPair_KeyValuePairExtractorTest,
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
