#include <DataTypes/DataParsers/JSONDataParser.h>
#include <IO/ReadBufferFromString.h>
#include <gtest/gtest.h>

#if USE_SIMDJSON

using namespace DB;

TEST(JSONDataParser, ReadInto)
{
    String json = R"({"k1" : 1, "k2" : {"k3" : "aa", "k4" : 2}})";
    String json_bad = json + "aaaaaaa";

    JSONDataParser<SimdJSONParser> parser;
    ReadBufferFromString buf(json_bad);
    String res;
    parser.readInto(res, buf);
    ASSERT_EQ(json, res);
}

TEST(JSONDataParser, Parse)
{
    String json = R"({"k1" : 1, "k2" : {"k3" : "aa", "k4" : 2}})";
    JSONDataParser<SimdJSONParser> parser;
    auto res = parser.parse(json.data(), json.size());
    ASSERT_TRUE(res.has_value());

    const auto & [paths, values] = *res;
    ASSERT_EQ(paths, (Strings{"k1", "k2.k3", "k2.k4"}));
    ASSERT_EQ(values, (Strings{"1", "aa", "2"}));
}

#endif
