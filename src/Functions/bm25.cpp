#include <Functions/bm25.h>

#include <Columns/ColumnConst.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include <cmath>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

BM25Params parseBM25FunctionArguments(const ColumnsWithTypeAndName & arguments)
{
    if (arguments.size() > 2)
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Function bm25 accepts at most two arguments (k1, b), got {}", arguments.size());
    }

    auto get_constant = [&](size_t i, const char * parameter_name)
    {
        const auto & argument = arguments[i];
        if (!isNumber(argument.type))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument {} ({}) of function bm25 must be a number, got {}", i + 1, parameter_name, argument.type->getName());
        }

        if (!argument.column || !isColumnConst(*argument.column))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Argument {} ({}) of function bm25 must be a constant", i + 1, parameter_name);
        }

        Float64 value = applyVisitor(FieldVisitorConvertToNumber<Float64>(), (*argument.column)[0]);
        if (!std::isfinite(value))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument {} ({}) of function bm25 must be finite", i + 1, parameter_name);

        return value;
    };

    BM25Params params;

    if (!arguments.empty())
    {
        params.k1 = get_constant(0, "k1");
        if (params.k1 < 0.0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter k1 of function bm25 must be non-negative, got {}", params.k1);
    }

    if (arguments.size() > 1)
    {
        params.b = get_constant(1, "b");
        if (params.b < 0.0 || params.b > 1.0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter b of function bm25 must be in [0, 1], got {}", params.b);
    }

    return params;
}

namespace
{

/// `bm25([k1[, b]])`: the BM25 relevance score of a row for the text-search predicates of the query.
/// The function is a placeholder for the query planner: `processAndOptimizeTextIndexFunctions`
/// replaces every occurrence by an expression over per-predicate score columns that the text index
/// reader fills, and throws when it cannot. The function itself is never executed.
class FunctionBM25 final : public IFunction
{
public:
    static constexpr auto name = "bm25";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBM25>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// The score is not a function of its (constant) arguments, so it must survive analysis unchanged.
    bool isSuitableForConstantFolding() const override { return false; }
    /// The score of a row depends on the statistics of all parts the query reads, so the result of a condition on it
    /// for a granule cannot be reused across queries (e.g. by the query condition cache).
    bool isDeterministic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        parseBM25FunctionArguments(arguments);
        return std::make_shared<DataTypeFloat32>();
    }

    /// Header computation and constant analysis run the function on zero or one row in dry-run mode
    /// (`ActionsDAG::evaluatePartialResult`): give them a plain column, never a constant to fold.
    ColumnPtr executeImplDryRun(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumn()->cloneResized(input_rows_count);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t) const override
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Function bm25 must be rewritten by the query planner and cannot be executed. It is supported only in a query "
            "that reads a MergeTree table with a text index created with `scoring = 'bm25'` and filters by `hasToken`, `hasAnyTokens` "
            "or `hasAllTokens` on the indexed column with the direct read from the text index, and not above a JOIN or an aggregation");
    }
};

}

REGISTER_FUNCTION(BM25)
{
    FunctionDocumentation::Description description = R"(
Returns the [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) relevance score of a row for the text-search predicates of the query.

The function requires a query that reads a `MergeTree` table with a text index created with `scoring = 'bm25'`
and filters by `hasToken`, `hasAnyTokens` or `hasAllTokens` on the indexed column. The direct read from the text index (`query_plan_direct_read_from_text_index`) must be enabled.
The query planner replaces the function with an expression over the scores of the text-search predicates of the filter, following its boolean structure:
every matching predicate adds its score, a conjunction adds the scores of its predicates only when all of them match, a predicate under `NOT` and any non-text predicate add nothing.

The function is experimental and requires the setting `allow_experimental_bm25_scoring`.
    )";
    FunctionDocumentation::Syntax syntax = "bm25([k1[, b]])";
    FunctionDocumentation::Arguments arguments = {
        {"k1", "Optional. The term-frequency saturation parameter, a non-negative constant. Default: `1.2`.", {"Float64"}},
        {"b", "Optional. The document-length normalization parameter, a constant in `[0, 1]`. Default: `0.75`.", {"Float64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the BM25 score of the row.", {"Float32"}};
    FunctionDocumentation::Examples examples = {
    {
        "Rank the rows matching a text-search predicate",
        R"(
SELECT id, bm25()
FROM tab
WHERE hasAnyTokens(body, ['consensus', 'raft'])
ORDER BY bm25() DESC
LIMIT 3
SETTINGS allow_experimental_bm25_scoring = 1;
        )",
        R"(
┌─id─┬────bm25()─┐
│  1 │ 2.2050254 │
│  7 │ 2.0974927 │
│  3 │ 1.1236567 │
└────┴───────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBM25>(documentation);
}

}
