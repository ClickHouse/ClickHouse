#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Parsers/queryNormalization.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

template <bool keep_names>
struct Impl
{
    static constexpr auto name = keep_names ? "normalizeQueryKeepNames" : "normalizeQuery";

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        res_offsets.resize(input_rows_count);
        res_data.reserve(data.size());

        ColumnString::Offset prev_src_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ColumnString::Offset curr_src_offset = offsets[i];

            normalizeQueryToPODArray(
                reinterpret_cast<const char *>(&data[prev_src_offset]),
                reinterpret_cast<const char *>(&data[curr_src_offset]),
                res_data, keep_names);

            prev_src_offset = offsets[i];
            res_offsets[i] = res_data.size();
        }
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot apply function normalizeQuery to fixed string.");
    }
};

}

REGISTER_FUNCTION(NormalizeQuery)
{
    FunctionDocumentation::Description normalizeQuery_description = R"(
Replaces literals, sequences of literals and complex aliases (containing whitespace, more than two digits or at least 36 bytes long such as UUIDs) with placeholder `?`.
    )";
    FunctionDocumentation::Syntax normalizeQuery_syntax = "normalizeQuery(x)";
    FunctionDocumentation::Arguments normalizeQuery_arguments = {
        {"x", "Sequence of characters.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue normalizeQuery_returned_value = {"Returns the given sequence of characters with placeholders.", {"String"}};
    FunctionDocumentation::Examples normalizeQuery_examples = {
    {
        "Usage example",
        R"(
SELECT normalizeQuery('[1, 2, 3, x]') AS query
        )",
        R"(
┌─query────┐
│ [?.., x] │
└──────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn normalizeQuery_introduced_in = {20, 8};
    FunctionDocumentation::Category normalizeQuery_category = FunctionDocumentation::Category::Other;
    FunctionDocumentation normalizeQuery_documentation = {normalizeQuery_description, normalizeQuery_syntax, normalizeQuery_arguments, normalizeQuery_returned_value, normalizeQuery_examples, normalizeQuery_introduced_in, normalizeQuery_category};

    FunctionDocumentation::Description normalizeQueryKeepNames_description = R"(
Replaces literals and sequences of literals with placeholder `?` but does not replace complex aliases (containing whitespace, more than two digits or at least 36 bytes long such as UUIDs).
This helps better analyze complex query logs.
    )";
    FunctionDocumentation::Syntax normalizeQueryKeepNames_syntax = "normalizeQueryKeepNames(x)";
    FunctionDocumentation::Arguments normalizeQueryKeepNames_arguments = {
        {"x", "Sequence of characters.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue normalizeQueryKeepNames_returned_value = {"Returns the given sequence of characters with placeholders.", {"String"}};
    FunctionDocumentation::Examples normalizeQueryKeepNames_examples = {
    {
        "Usage example",
        R"(
SELECT normalizeQuery('SELECT 1 AS aComplexName123'), normalizeQueryKeepNames('SELECT 1 AS aComplexName123')
        )",
        R"(
┌─normalizeQuery('SELECT 1 AS aComplexName123')─┬─normalizeQueryKeepNames('SELECT 1 AS aComplexName123')─┐
│ SELECT ? AS `?`                               │ SELECT ? AS aComplexName123                            │
└───────────────────────────────────────────────┴────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn normalizeQueryKeepNames_introduced_in = {21, 2};
    FunctionDocumentation::Category normalizeQueryKeepNames_category = FunctionDocumentation::Category::Other;
    FunctionDocumentation normalizeQueryKeepNames_documentation = {normalizeQueryKeepNames_description, normalizeQueryKeepNames_syntax, normalizeQueryKeepNames_arguments, normalizeQueryKeepNames_returned_value, normalizeQueryKeepNames_examples, normalizeQueryKeepNames_introduced_in, normalizeQueryKeepNames_category};

    factory.registerFunction<FunctionStringToString<Impl<true>, Impl<true>>>(normalizeQueryKeepNames_documentation);
    factory.registerFunction<FunctionStringToString<Impl<false>, Impl<false>>>(normalizeQuery_documentation);
}

}
