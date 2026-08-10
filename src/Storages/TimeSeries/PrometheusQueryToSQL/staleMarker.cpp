#include <Storages/TimeSeries/PrometheusQueryToSQL/staleMarker.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>


namespace DB::PrometheusQueryToSQL
{

ASTPtr isStaleMarker(const ASTPtr & value)
{
    return makeASTFunction(
        "equals",
        makeASTFunction("reinterpretAsUInt64", makeASTFunction("assumeNotNull", value->clone())),
        make_intrusive<ASTLiteral>(0x7ff0000000000002ULL));
}


ASTPtr keepStaleMarker(const ASTPtr & value, ASTPtr transformed_value)
{
    return makeASTFunction("if", isStaleMarker(value), value->clone(), std::move(transformed_value));
}


ASTPtr replaceStaleMarker(const ASTPtr & value, ASTPtr replacement)
{
    return makeASTFunction("if", isStaleMarker(value), std::move(replacement), value->clone());
}


ASTPtr nullifyStaleMarker(const ASTPtr & value)
{
    return makeASTFunction("if", isStaleMarker(value), make_intrusive<ASTLiteral>(Field{} /* NULL */), value->clone());
}

}
