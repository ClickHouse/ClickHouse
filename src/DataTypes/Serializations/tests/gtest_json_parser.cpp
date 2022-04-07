#include <DataTypes/Serializations/JSONDataParser.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <IO/ReadBufferFromString.h>
#include <Common/FieldVisitorToString.h>

#include <ostream>
#include <gtest/gtest.h>

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
    PathInData path;
    Field value;

    JSONPathAndValue(const PathInData & path_, const Field & value_)
        : path(path_), value(value_)
    {
    }

    bool operator==(const JSONPathAndValue & other) const = default;
    bool operator<(const JSONPathAndValue & other) const { return path.getPath() < other.path.getPath(); }
};

static std::ostream & operator<<(std::ostream & ostr, const JSONPathAndValue & path_and_value)
{
    ostr << "{ PathInData{";
    bool first = true;
    for (const auto & part : path_and_value.path.getParts())
    {
        ostr << (first ? "{" : ", {") << part.key << ", " << part.is_nested << ", " << part.anonymous_array_level << "}";
        first = false;
    }

    ostr << "}, Field{" << applyVisitor(FieldVisitorToString(), path_and_value.value) << "} }";
    return ostr;
}

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
                { PathInData{{{"k1", false, 0}}}, 1 },
                { PathInData{{{"k2", false, 0}, {"k3", false, 0}}}, "aa" },
                { PathInData{{{"k2", false, 0}, {"k4", false, 0}}}, 2 },
            });
    }

    {
        check(json2, "json2",
            {
                { PathInData{{{"k1", true, 0}, {"k2", false, 0}}}, Array{"aaa", "ddd"} },
                { PathInData{{{"k1", true, 0}, {"k3", true, 0}, {"k4", false, 0}}}, Array{Array{"bbb", "ccc"}, Array{"eee", "fff"}} },
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

        check(json3, "json3",
            {
                { PathInData{{{"k1", true, 0}, {"k5", false, 0}}}, Array{"foo", "bar"} },
                { PathInData{{{"k1", true, 0}, {"k2", false, 0}, {"k3", false, 0}}}, Array{Array{1, 2}, Array{5, 6}} },
                { PathInData{{{"k1", true, 0}, {"k2", false, 0}, {"k4", false, 0}}}, Array{Array{3, 4}, Array{7, 8}} },
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
                { PathInData{{{"k1", true, 0}, {"k5", false, 0}}}, Array{"foo", "bar"} },
                { PathInData{{{"k1", true, 0}, {"k2", true, 0}, {"k3", false, 0}}}, Array{Array{1, 2}, Array{5, 6}} },
                { PathInData{{{"k1", true, 0}, {"k2", true, 0}, {"k4", false, 0}}}, Array{Array{3, 4}, Array{7, 8}} },
            });
    }

    {
        const String json5 = R"({"k1": [[1, 2, 3], [4, 5], [6]]})";
        check(json5, "json5",
            {
                { PathInData{{{"k1", false, 0}}}, Array{Array{1, 2, 3}, Array{4, 5}, Array{6}} }
            });
    }

    {
        /// Array(Nested(k2 Int, k3 Int))
        const String json6 = R"({
            "k1": [
                [{"k2": 1, "k3": 2}, {"k2": 3, "k3": 4}],
                [{"k2": 5, "k3": 6}]
            ]
        })";

        check(json6, "json6",
            {
                { PathInData{{{"k1", true, 0}, {"k2", false, 1}}}, Array{Array{1, 3}, Array{5}} },
                { PathInData{{{"k1", true, 0}, {"k3", false, 1}}}, Array{Array{2, 4}, Array{6}} },
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
                { PathInData{{{"k1", true, 0}, {"k2", false, 0}}}, Array{Array{1, 3}, Array{5}} },
                { PathInData{{{"k1", true, 0}, {"k3", false, 0}}}, Array{Array{2, 4}, Array{6}} },
            });
    }
}

#endif
