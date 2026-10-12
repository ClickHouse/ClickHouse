#include <Storages/TimeSeries/PrometheusQueryToSQL/makeNoDuplicateSeriesPerStepCheck.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>


namespace DB::PrometheusQueryToSQL
{

ASTPtr makeNoDuplicateSeriesPerStepCheck(ASTPtr values, ASTPtr group)
{
    /// arrayExists(c -> c > 1, countForEach(values))
    ASTPtr has_duplicates = makeASTFunction(
        "arrayExists",
        makeASTFunction(
            "lambda",
            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("c")),
            makeASTFunction("greater", make_intrusive<ASTIdentifier>("c"), make_intrusive<ASTLiteral>(1u))),
        makeASTFunction("countForEach", std::move(values)));

    return makeASTFunction(
        "equals",
        makeASTFunction("timeSeriesThrowDuplicateSeriesIf", std::move(has_duplicates), std::move(group)),
        make_intrusive<ASTLiteral>(0u));
}

ASTPtr makeNoDuplicateSeriesPerStepValues(ASTPtr values)
{
    return makeASTFunction("anyForEach", std::move(values));
}

}
