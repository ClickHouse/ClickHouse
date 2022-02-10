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

static void check(
    const String & json_str,
    const Strings & expected_paths,
    const std::vector<Field> & expected_values,
    const std::vector<std::vector<bool>> & expected_nested,
    const String & tag)
{
    JSONDataParser<SimdJSONParser> parser;
    auto res = parser.parse(json_str.data(), json_str.size());
    ASSERT_TRUE(res.has_value()) << tag;

    const auto & [paths, values] = *res;

    ASSERT_EQ(paths.size(), expected_paths.size()) << tag;
    ASSERT_EQ(values.size(), expected_values.size()) << tag;

    Strings paths_str;
    std::vector<std::vector<bool>> paths_is_nested;

    for (const auto & path : paths)
    {
        paths_str.push_back(path.getPath());
        paths_is_nested.emplace_back();
        for (size_t i = 0; i < path.getParts().size(); ++i)
            paths_is_nested.back().push_back(path.isNested(i));
    }

    ASSERT_EQ(paths_str, expected_paths) << tag;
    ASSERT_EQ(values, expected_values) << tag;
    ASSERT_EQ(paths_is_nested, expected_nested) << tag;
}

TEST(JSONDataParser, Parse)
{
    {
        check(json1,
            {"k1", "k2.k3", "k2.k4"},
            {1, "aa", 2},
            {{false}, {false, false}, {false, false}},
            "json1");
    }

    {
        Strings paths = {"k1.k3.k4", "k1.k2"};

        auto k1k3k4 = Array{Array{"bbb", "ccc"}, Array{"eee", "fff"}};
        auto k1k2 = Array{"aaa", "ddd"};

        check(json2, paths, {k1k3k4, k1k2}, {{true, true, false}, {true, false}}, "json2");
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

        auto k1k2k3 = Array{Array{1, 2}, Array{5, 6}};
        auto k1k2k4 = Array{Array{3, 4}, Array{7, 8}};
        auto k1k5 = Array{"foo", "bar"};

        check(json3, paths,
            {k1k2k4, k1k5, k1k2k3},
            {{true, false, false}, {true, false}, {true, false, false}},
            "json3");
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

        Strings paths = {"k1.k2.k4", "k1.k5", "k1.k2.k3"};

        auto k1k2k3 = Array{Array{1, 2}, Array{5, 6}};
        auto k1k2k4 = Array{Array{3, 4}, Array{7, 8}};
        auto k1k5 = Array{"foo", "bar"};

        check(json4, paths,
            {k1k2k4, k1k5, k1k2k3},
            {{true, true, false}, {true, false}, {true, true, false}},
            "json4");
    }

    {
        const String json5 = R"({"k1": [[1, 2, 3], [4, 5], [6]]})";
        check(json5, {"k1"}, {Array{Array{1, 2, 3}, Array{4, 5}, Array{6}}}, {{false}}, "json5");
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

        check(json6, paths, {k1k2, k1k3}, {{true, true}, {true, true}}, "json6");
    }

    {
        /// Nested(k2 Array(Int), k3 Array(Int))
        const String json7 = R"({
            "k1": [
                {"k2": [1, 3], "k3": [2, 4]},
                {"k2": [5], "k3": [6]}
            ]
        })";

        Strings paths = {"k1.k2", "k1.k3"};

        auto k1k2 = Array{Array{1, 3}, Array{5}};
        auto k1k3 = Array{Array{2, 4}, Array{6}};

        check(json7, paths, {k1k2, k1k3}, {{true, false}, {true, false}}, "json7");
    }
}

#endif
