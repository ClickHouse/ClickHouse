#include <DataTypes/Serializations/JSONDataParser.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <IO/ReadBufferFromString.h>
#include <gtest/gtest.h>

#if USE_SIMDJSON

using namespace DB;

const String json1 = R"({"k1" : 1, "k2" : {"k3" : "aa", "k4" : 2}})";

const String json2 =
R"({"k1" : [
    {
        "k2" : "aaa",
        "k3" : [{ "k4" : "bbb" }, { "k4" : "ccc" }]
    },
    {
        "k2" : "ddd",
        "k3" : [{ "k4" : "eee" }, { "k4" : "fff" }]
    }
    ]
})";

TEST(JSONDataParser, ReadJSON)
{

    {
        String json_bad = json1 + "aaaaaaa";

        JSONDataParser<SimdJSONParser> parser;
        ReadBufferFromString buf(json_bad);
        String res;
        parser.readJSON(res, buf);
        ASSERT_EQ(json1, res);
    }

    {
        String json_bad = json2 + "aaaaaaa";

        JSONDataParser<SimdJSONParser> parser;
        ReadBufferFromString buf(json_bad);
        String res;
        parser.readJSON(res, buf);
        ASSERT_EQ(json2, res);
    }
}

TEST(JSONDataParser, Parse)
{
    {
        JSONDataParser<SimdJSONParser> parser;
        auto res = parser.parse(json1.data(), json1.size());
        ASSERT_TRUE(res.has_value());

        const auto & [paths, values] = *res;
        ASSERT_EQ(paths, (Strings{"k1", "k2.k3", "k2.k4"}));
        ASSERT_EQ(values, (std::vector<Field>{"1", "aa", "2"}));
    }

    {
        JSONDataParser<SimdJSONParser> parser;
        auto res = parser.parse(json2.data(), json2.size());
        ASSERT_TRUE(res.has_value());

        const auto & [paths, values] = *res;
        ASSERT_EQ(paths, (Strings{"k1.k3.k4", "k1.k2"}));

        auto k1k3k4 = Array{Array{"bbb", "ccc"}, Array{"eee", "fff"}};
        auto k1k2 = Array{"aaa", "ddd"};
        ASSERT_EQ(values, (std::vector<Field>{k1k3k4, k1k2}));
    }

}

#endif
