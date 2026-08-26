#include <Storages/TimeSeries/PrometheusQueryToSQL/dropStaleMarkers.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>


namespace DB::PrometheusQueryToSQL
{

ASTPtr dropStaleMarkers(ASTPtr values)
{
    /// arrayMap(value -> if(isNotNull(value) AND reinterpretAsUInt64(assumeNotNull(value)) = 0x7ff0000000000002, NULL, value), values)
    /// (`0x7ff0000000000002` is Prometheus's staleness NaN bit pattern, see `fromSelector` and `finalizeSQL`.)
    return makeASTFunction(
        "arrayMap",
        makeASTFunction(
            "lambda",
            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("value")),
            makeASTFunction(
                "if",
                makeASTFunction(
                    "and",
                    makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("value")),
                    makeASTFunction(
                        "equals",
                        makeASTFunction("reinterpretAsUInt64", makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>("value"))),
                        make_intrusive<ASTLiteral>(0x7ff0000000000002ULL))),
                make_intrusive<ASTLiteral>(Field{}),
                make_intrusive<ASTIdentifier>("value"))),
        std::move(values));
}

}
