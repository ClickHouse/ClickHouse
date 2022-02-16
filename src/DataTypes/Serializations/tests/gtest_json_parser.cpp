#include <DataTypes/Serializations/JSONDataParser.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <IO/ReadBufferFromString.h>
#include <gtest/gtest.h>
#include <Common/FieldVisitorToString.h>

#if USE_SIMDJSON

using namespace DB;

const String json1 = R"({"k1" : 1, "k2" : {"k3" : "aa", "k4" : 2}})";

/// Nested(k2 String, k3 Nested(k4 String))
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

struct JSONPathAndValue
{
    String path;
    Field value;
    std::vector<bool> is_nested;

    JSONPathAndValue(const String & path_, const Field & value_, const std::vector<bool> & is_nested_)
        : path(path_), value(value_), is_nested(is_nested_)
    {
    }

    JSONPathAndValue(const PathInData & path_, const Field & value_)
        : path(path_.getPath()), value(value_)
    {
        for (const auto & part : path_.getParts())
            is_nested.push_back(part.is_nested);
    }

    bool operator==(const JSONPathAndValue & other) const = default;
    bool operator<(const JSONPathAndValue & other) const { return path < other.path; }
};

using JSONValues = std::vector<JSONPathAndValue>;

static void check(
    const String & json_str,
    const String & tag,
    JSONValues expected_values)
{
    JSONDataParser<SimdJSONParser> parser;
    auto res = parser.parse(json_str.data(), json_str.size());
    ASSERT_TRUE(res.has_value()) << tag;

    const auto & [paths, values] = *res;

    ASSERT_EQ(paths.size(), expected_values.size()) << tag;
    ASSERT_EQ(values.size(), expected_values.size()) << tag;

    JSONValues result_values;
    for (size_t i = 0; i < paths.size(); ++i)
        result_values.emplace_back(paths[i], values[i]);

    std::sort(expected_values.begin(), expected_values.end());
    std::sort(result_values.begin(), result_values.end());

    ASSERT_EQ(result_values, expected_values) << tag;
}

TEST(JSONDataParser, Parse)
{
    {
        check(json1, "json1",
            {
                {"k1", 1, {false}},
                {"k2.k3", "aa", {false, false}},
                {"k2.k4", 2, {false, false}},
            });
    }

    {
        check(json2, "json2",
            {
                {"k1.k2", Array{"aaa", "ddd"}, {true, false}},
                {"k1.k3.k4", Array{Array{"bbb", "ccc"}, Array{"eee", "fff"}}, {true, true, false}},
            });
    }

    {
        /// Nested(k2 Tuple(k3 Array(Int), k4 Array(Int)), k5 String)
        const String json3 =
        R"({"k1": [
            {
                "k2": {
                    "k3": [1, 2],
                    "k4": [3, 4]
                },
                "k5": "foo"
            },
            {
                "k2": {
                    "k3": [5, 6],
                    "k4": [7, 8]
                },
                "k5": "bar"
            }
        ]})";

        Strings paths = {"k1.k2.k4", "k1.k5", "k1.k2.k3"};

        auto k1k2k4 = Array{Array{3, 4}, Array{7, 8}};

        check(json3, "json3",
            {
                {"k1.k5", Array{"foo", "bar"}, {true, false}},
                {"k1.k2.k3", Array{Array{1, 2}, Array{5, 6}}, {true, false, false}},
                {"k1.k2.k4", Array{Array{3, 4}, Array{7, 8}}, {true, false, false}},
            });
    }

    {
        /// Nested(k2 Nested(k3 Int, k4 Int), k5 String)
        const String json4 =
        R"({"k1": [
            {
                "k2": [{"k3": 1, "k4": 3}, {"k3": 2, "k4": 4}],
                "k5": "foo"
            },
            {
                "k2": [{"k3": 5, "k4": 7}, {"k3": 6, "k4": 8}],
                "k5": "bar"
            }
        ]})";

        check(json4, "json4",
            {
                {"k1.k5", Array{"foo", "bar"}, {true, false}},
                {"k1.k2.k3", Array{Array{1, 2}, Array{5, 6}}, {true, true, false}},
                {"k1.k2.k4", Array{Array{3, 4}, Array{7, 8}}, {true, true, false}},
            });
    }

    {
        const String json5 = R"({"k1": [[1, 2, 3], [4, 5], [6]]})";
        check(json5, "json5", {{"k1", Array{Array{1, 2, 3}, Array{4, 5}, Array{6}}, {false}}});
    }

    {
        /// Array(Nested(k2 Int, k3 Int))
        const String json6 = R"({
            "k1": [
                [{"k2": 1, "k3": 2}, {"k2": 3, "k3": 4}],
                [{"k2": 5, "k3": 6}]
            ]
        })";

        Strings paths = {"k1.k2", "k1.k3"};

        auto k1k2 = Array{Array{1, 3}, Array{5}};
        auto k1k3 = Array{Array{2, 4}, Array{6}};

        check(json6, "json6",
            {
                {"k1.k2", Array{Array{1, 3}, Array{5}}, {true, false}},
                {"k1.k3", Array{Array{2, 4}, Array{6}}, {true, false}},
            });
    }

    {
        /// Nested(k2 Array(Int), k3 Array(Int))
        const String json7 = R"({
            "k1": [
                {"k2": [1, 3], "k3": [2, 4]},
                {"k2": [5], "k3": [6]}
            ]
        })";

        check(json7, "json7",
            {
                {"k1.k2", Array{Array{1, 3}, Array{5}}, {true, false}},
                {"k1.k3", Array{Array{2, 4}, Array{6}}, {true, false}},
            });
    }
}

#endif
