#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/QueryFuzzer.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/parseQuery.h>
#include <Core/Defines.h>

#include "gtest_global_register.h"

using namespace DB;

namespace
{

/// Reproduce the round-trip check executeQueryImpl runs, scoped to a single ASTDataType node:
/// format it, parse it back with ParserDataType, and require an identical tree hash. A false result
/// means the node cannot be reconstructed by parsing, i.e. the #109706 "Inconsistent AST formatting"
/// LOGICAL_ERROR would fire.
::testing::AssertionResult dataTypeRoundTrips(const ASTPtr & data_type_ast)
{
    const String formatted = data_type_ast->formatWithSecretsOneLine();

    ParserDataType parser;
    ASTPtr reparsed;
    try
    {
        reparsed = parseQuery(
            parser,
            formatted.data(),
            formatted.data() + formatted.size(),
            "",
            DBMS_DEFAULT_MAX_QUERY_SIZE,
            DBMS_DEFAULT_MAX_PARSER_DEPTH,
            DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
    catch (const Exception & e)
    {
        return ::testing::AssertionFailure() << "cannot parse data type back: " << e.message() << "\ntype: " << formatted;
    }

    if (data_type_ast->getTreeHash(false) != reparsed->getTreeHash(false))
        return ::testing::AssertionFailure() << "tree hash differs after ParserDataType round-trip\ntype: " << formatted;

    return ::testing::AssertionSuccess();
}

}

/// Deterministic regression test for #109706, independent of the global server-side fuzzer RNG.
///
/// The .sql smoke test (04344) is only probabilistic: the server-side fuzzer is seeded from
/// randomSeed() and keeps global state, so an unfixed build can go green whenever a run simply never
/// turns a type argument into an ASTFunction. This test pins down the bug class directly: an
/// ASTDataType whose argument is an ASTFunction (e.g. `Nullable(multiply(2, 3))`) - exactly what the
/// pre-fix fuzzer produced by descending into a data-type argument list with the generic expression
/// fuzzer - is not round-trippable through ParserDataType, so it trips the consistency check. A
/// structurally valid ASTDataType with the same shape must round-trip.
TEST(QueryFuzzer, MalformedDataTypeArgumentRoundTrip)
{
    /// The malformed shape the fuzzer must never generate: a function expression as a type argument.
    auto with_function_arg = makeASTDataType(
        "Nullable",
        makeASTFunction("multiply", make_intrusive<ASTLiteral>(Field(UInt64(2))), make_intrusive<ASTLiteral>(Field(UInt64(3)))));
    EXPECT_FALSE(dataTypeRoundTrips(with_function_arg));

    /// A structurally valid data type of the same shape (Nullable of a plain type) round-trips.
    EXPECT_TRUE(dataTypeRoundTrips(makeASTDataType("Nullable", makeASTDataType("Int32"))));

    /// The fix makes fuzzDataType own every ASTDataType node and mutate only via the DataType layer,
    /// which yields structurally valid, round-trippable types across all parametric families - the
    /// complex parsers included. Assert those name forms round-trip through ParserDataType.
    const std::vector<String> valid_type_names = {
        "Nullable(Int32)",
        "Array(Nullable(UInt64))",
        "Map(String, Int64)",
        "Tuple(k UInt32, v String)",
        "Variant(UInt64, String)",
        "Dynamic(max_types = 8)",
        "JSON(max_dynamic_paths=8, max_dynamic_types=4, p1 UInt32, p2 Array(String), SKIP s, SKIP REGEXP 'r.*')",
        "QBit(Float32, 16)",
        "SimpleAggregateFunction(sum, UInt64)",
        "SimpleAggregateFunction(max, UInt64)",
        "SimpleAggregateFunction(groupArrayArray, Array(UInt64))",
        "SimpleAggregateFunction(sumMap, Tuple(Array(String), Array(UInt64)))",
        "AggregateFunction(quantileExact(0.5), Float64)",
        "AggregateFunction(topK(10), String)",
        "AggregateFunction(count)",
        "DateTime",
        "DateTime('Asia/Istanbul')",
        "DateTime64(3)",
        "DateTime64(3, 'UTC')",
        "Nested(a UInt32, b String)",
        "Nested(a UInt32, b Array(Nullable(Int64)))",
        "Point",
        "Ring",
        "Polygon",
        "MultiPolygon",
        "LineString",
        "MultiLineString",
        "Geometry",
    };
    for (const auto & type_name : valid_type_names)
    {
        ParserDataType parser;
        ASTPtr parsed = parseQuery(
            parser,
            type_name.data(),
            type_name.data() + type_name.size(),
            "",
            DBMS_DEFAULT_MAX_QUERY_SIZE,
            DBMS_DEFAULT_MAX_PARSER_DEPTH,
            DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        ASSERT_NE(nullptr, parsed) << type_name;
        EXPECT_TRUE(dataTypeRoundTrips(parsed)) << type_name;
    }
}

/// Regression test for the #109713 review point: rebuilding a versioned AggregateFunction state under
/// the fuzzer must preserve the serialization version parsed from the source AST, instead of resetting
/// it to std::nullopt (which normalizes to the function's current default). groupBitmap is versioned
/// with a default version of 1, so an explicit version 0 is a value the default would silently overwrite.
TEST(QueryFuzzer, AggregateFunctionVersionPreserved)
{
    tryRegisterAggregateFunctions();

    QueryFuzzer fuzzer;
    const DataTypes arg_types = {std::make_shared<DataTypeUInt32>()};

    /// An explicit version threaded through makeAggregateFunctionType is kept verbatim on the rebuilt type.
    for (size_t version : {size_t(0), size_t(1)})
    {
        auto rebuilt = fuzzer.makeAggregateFunctionType("groupBitmap", arg_types, Array{}, /*simple=*/false, version);
        ASSERT_NE(nullptr, rebuilt) << "version=" << version;
        const auto * aggr = typeid_cast<const DataTypeAggregateFunction *>(rebuilt.get());
        ASSERT_NE(nullptr, aggr) << "version=" << version;
        EXPECT_EQ(std::optional<size_t>(version), aggr->getVersionIfExplicit()) << "version=" << version;
        EXPECT_EQ(version, aggr->getVersion()) << "version=" << version;
    }

    /// No explicit version stays empty (the type serializes with the function's default version).
    auto rebuilt = fuzzer.makeAggregateFunctionType("groupBitmap", arg_types, Array{}, /*simple=*/false, std::nullopt);
    ASSERT_NE(nullptr, rebuilt);
    const auto * aggr = typeid_cast<const DataTypeAggregateFunction *>(rebuilt.get());
    ASSERT_NE(nullptr, aggr);
    EXPECT_EQ(std::nullopt, aggr->getVersionIfExplicit());
}
