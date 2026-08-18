#include <Storages/TimeSeries/PrometheusQueryToSQL/makeSortKeyComponent.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>


namespace DB::PrometheusQueryToSQL
{

ASTPtr makeValueSortKeyComponent(ASTPtr value)
{
    return makeASTFunction("tuple", std::move(value), make_intrusive<ASTLiteral>(UInt64(0)));
}

ASTPtr makeExactSortKeyComponent(ASTPtr value)
{
    return makeASTFunction("tuple", make_intrusive<ASTLiteral>(0.0), std::move(value));
}

ASTPtr makeFallbackSortKey(ASTPtr group_ast)
{
    return makeASTFunction("array", makeExactSortKeyComponent(makeASTFunction("timeSeriesGroupToSamplingKey", std::move(group_ast))));
}

}
