#include <Storages/TimeSeries/PrometheusQueryToSQL/makeNoDuplicateSeriesPerStepCheck.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// timeSeriesThrowDuplicateSeriesIf(has_duplicates, group) = 0
    ASTPtr makeThrowDuplicateSeriesIf(ASTPtr has_duplicates, ASTPtr group)
    {
        return makeASTFunction(
            "equals",
            makeASTFunction("timeSeriesThrowDuplicateSeriesIf", std::move(has_duplicates), std::move(group)),
            make_intrusive<ASTLiteral>(0u));
    }

    /// arrayExists(c -> c > 1, counts)
    ASTPtr makeAnyCountGreaterThanOne(ASTPtr counts)
    {
        return makeASTFunction(
            "arrayExists",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>("c")),
                makeASTFunction("greater", make_intrusive<ASTIdentifier>("c"), make_intrusive<ASTLiteral>(1u))),
            std::move(counts));
    }
}

ASTPtr makeNoDuplicateSeriesPerStepCheck(ASTPtr values, ASTPtr group)
{
    ASTPtr has_duplicates = makeAnyCountGreaterThanOne(makeASTFunction("countForEach", std::move(values)));
    return makeThrowDuplicateSeriesIf(std::move(has_duplicates), std::move(group));
}

ASTPtr makeNoDuplicateSeriesPerStepValues(ASTPtr values)
{
    return makeASTFunction("anyForEach", std::move(values));
}

ASTPtr makeNoDuplicateSeriesPerStepCheckMatchingCounts(ASTPtr left_side_value_counts, ASTPtr right_side_value_counts, ASTPtr group)
{
    /// arrayExists((l, r) -> greatest(l, r) >= 2 AND least(l, r) >= 1, left_side_value_counts, right_side_value_counts)
    ASTPtr has_duplicates = makeASTFunction(
        "arrayExists",
        makeASTFunction(
            "lambda",
            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("l"), make_intrusive<ASTIdentifier>("r")),
            makeASTFunction(
                "and",
                makeASTFunction(
                    "greaterOrEquals",
                    makeASTFunction("greatest", make_intrusive<ASTIdentifier>("l"), make_intrusive<ASTIdentifier>("r")),
                    make_intrusive<ASTLiteral>(2u)),
                makeASTFunction(
                    "greaterOrEquals",
                    makeASTFunction("least", make_intrusive<ASTIdentifier>("l"), make_intrusive<ASTIdentifier>("r")),
                    make_intrusive<ASTLiteral>(1u)))),
        std::move(left_side_value_counts),
        std::move(right_side_value_counts));

    return makeThrowDuplicateSeriesIf(std::move(has_duplicates), std::move(group));
}

ASTPtr makeNoDuplicateSeriesPerStepCheckMatchingValuesAndCounts(ASTPtr values, ASTPtr other_side_value_counts, ASTPtr group)
{
    /// arrayExists((v, c) -> isNotNull(v) AND c >= 2, values, other_side_value_counts)
    ASTPtr has_duplicates = makeASTFunction(
        "arrayExists",
        makeASTFunction(
            "lambda",
            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("v"), make_intrusive<ASTIdentifier>("c")),
            makeASTFunction(
                "and",
                makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("v")),
                makeASTFunction("greaterOrEquals", make_intrusive<ASTIdentifier>("c"), make_intrusive<ASTLiteral>(2u)))),
        std::move(values),
        std::move(other_side_value_counts));

    return makeThrowDuplicateSeriesIf(std::move(has_duplicates), std::move(group));
}

}
