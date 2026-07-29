#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/Exception.h>
#include <Common/QueryFuzzer.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/NullsAction.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/parseQuery.h>
#include <Core/Defines.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserQuery.h>

#include <functional>
#include <map>
#include <vector>

#include <vector>

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

/// The class of a data-type root node, keyed on getID() so it does not depend on the exact-match casts
/// (typeid_cast / IAST::as) that the test exercises.
String rootClassOf(const ASTPtr & type_ast)
{
    const String id = type_ast->getID();
    if (id.starts_with("TupleDataType"))
        return "TupleDataType";
    if (id.starts_with("EnumDataType"))
        return "EnumDataType";
    return "DataType";
}

/// `DataTypes` is a vector of shared pointers, so its `==` compares POINTER identity, not the types. Every
/// rebuild allocates fresh type objects, so a pointer comparison reports "changed" even when the types are
/// identical. Compare the rendered names instead.
bool sameTypes(const DataTypes & left, const DataTypes & right)
{
    if (left.size() != right.size())
        return false;
    for (size_t i = 0; i < left.size(); ++i)
        if (left[i]->getName() != right[i]->getName())
            return false;
    return true;
}

/// (column name, declared type node) for every column of a CREATE query.
std::vector<std::pair<String, ASTPtr>> collectColumnTypes(const ASTPtr & create)
{
    std::vector<std::pair<String, ASTPtr>> result;
    const auto * query = create->as<ASTCreateQuery>();
    if (!query || !query->columns_list || !query->columns_list->columns)
        return result;
    for (const auto & child : query->columns_list->columns->children)
        if (const auto * column = child->as<ASTColumnDeclaration>(); column && column->getType())
            result.emplace_back(column->name, column->getType());
    return result;
}

}

/// Deterministic regression test for #109706: an ASTDataType whose argument is an ASTFunction (e.g.
/// `Nullable(multiply(2, 3))`) is not round-trippable through ParserDataType and trips the consistency check,
/// while a structurally valid ASTDataType of the same shape must round-trip. Pinned here rather than relying on
/// 04344, whose server-side fuzzer is seeded from randomSeed() and can miss the shape entirely.
TEST(QueryFuzzer, MalformedDataTypeArgumentRoundTrip)
{
    /// The malformed shape the fuzzer must never generate: a function expression as a type argument.
    auto with_function_arg = makeASTDataType(
        "Nullable",
        makeASTFunction("multiply", make_intrusive<ASTLiteral>(Field(UInt64(2))), make_intrusive<ASTLiteral>(Field(UInt64(3)))));
    EXPECT_FALSE(dataTypeRoundTrips(with_function_arg));

    /// A structurally valid data type of the same shape (Nullable of a plain type) round-trips.
    EXPECT_TRUE(dataTypeRoundTrips(makeASTDataType("Nullable", makeASTDataType("Int32"))));

    /// Every parametric family must round-trip through ParserDataType.
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
        "MultiPoint",
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

/// Rebuilding a versioned AggregateFunction state must preserve the serialization version parsed from the
/// source AST rather than resetting it to std::nullopt, which normalizes to the function's current default.
/// groupBitmap defaults to version 1, so an explicit version 0 is a value that default would overwrite.
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

/// A serialization version is function-specific: AggregateFunction(2, quantiles(...)) rebuilt as
/// AggregateFunction(2, sumMap, ...) parses back fine but sumMap::serialize throws on version 2. So a rename
/// must drop the explicit version, while an unchanged name keeps it verbatim.
TEST(QueryFuzzer, AggregateFunctionVersionDroppedOnNameChange)
{
    tryRegisterAggregateFunctions();

    /// Source: a versioned single-argument aggregate carrying an explicit version. groupBitmap is in the
    /// arity-1 rename candidate set, so fuzzDataType can rename it to another arity-1 aggregate.
    const DataTypes arg_types = {std::make_shared<DataTypeUInt32>()};
    AggregateFunctionProperties properties;
    auto source_func = AggregateFunctionFactory::instance().get("groupBitmap", NullsAction::EMPTY, arg_types, Array{}, properties);

    bool saw_name_change = false;
    for (UInt64 seed = 0; seed < 4000; ++seed)
    {
        QueryFuzzer fuzzer;
        fuzzer.setSeed(seed);

        /// A fresh source type each iteration with an explicit version 1.
        auto source_type = std::make_shared<DataTypeAggregateFunction>(source_func, arg_types, Array{}, /*version=*/size_t(1));
        DataTypePtr fuzzed;
        try
        {
            fuzzed = fuzzer.fuzzDataType(source_type);
        }
        catch (const Exception &)
        {
            /// Some randomized shapes are rejected at type-construction time (e.g. Map with a Nullable key);
            /// the real fuzzer catches these higher up. Not the arm under test - skip the seed.
            continue;
        }

        const auto * fuzzed_aggr = typeid_cast<const DataTypeAggregateFunction *>(fuzzed.get());
        if (!fuzzed_aggr)
            continue; /// wrapped in Array/Nullable/... - not the arm under test.

        /// fuzzTypeWrapping can replace the source with an unrelated (naturally versionless) aggregate, which
        /// would satisfy the rename assertion without the structured arm ever running. Credit only rebuilds that
        /// kept the source's argument types, i.e. that came out of the aggregate arm.
        if (!sameTypes(fuzzed_aggr->getArgumentsDataTypes(), arg_types))
            continue;

        if (fuzzed_aggr->getFunctionName() != source_type->getFunctionName())
        {
            saw_name_change = true;
            /// Renamed aggregate: the stale source version must have been dropped.
            EXPECT_EQ(std::nullopt, fuzzed_aggr->getVersionIfExplicit())
                << "seed=" << seed << " new_name=" << fuzzed_aggr->getFunctionName();
        }
        else
        {
            /// Same aggregate: the explicit source version is preserved verbatim.
            EXPECT_EQ(std::optional<size_t>(1), fuzzed_aggr->getVersionIfExplicit()) << "seed=" << seed;
        }
    }

    /// Sanity: the chosen seed range actually exercises the rename path (otherwise the test proves nothing).
    EXPECT_TRUE(saw_name_change);

    /// The forwarding itself: a rebuild that KEPT the name but changed the argument types must carry the source
    /// version. Filtered out above (a same-name, same-argument result is just the unchanged source object), so
    /// assert it here or nothing covers the wiring between fuzzDataType and makeAggregateFunctionType.
    bool saw_same_name_rebuild = false;
    for (UInt64 seed = 0; seed < 4000 && !saw_same_name_rebuild; ++seed)
    {
        QueryFuzzer fuzzer;
        fuzzer.setSeed(seed);
        auto source_type = std::make_shared<DataTypeAggregateFunction>(source_func, arg_types, Array{}, size_t(1));
        DataTypePtr fuzzed;
        try
        {
            fuzzed = fuzzer.fuzzDataType(source_type);
        }
        catch (const Exception &)
        {
            continue;
        }

        const auto * aggr = typeid_cast<const DataTypeAggregateFunction *>(fuzzed.get());
        if (!aggr || aggr->getFunctionName() != source_type->getFunctionName()
            || sameTypes(aggr->getArgumentsDataTypes(), arg_types))
            continue;

        saw_same_name_rebuild = true;
        EXPECT_EQ(std::optional<size_t>(1), aggr->getVersionIfExplicit())
            << "seed=" << seed << " lost the source version on a same-name rebuild";
    }
    EXPECT_TRUE(saw_same_name_rebuild) << "no same-name, changed-argument rebuild was observed";
}

/// The aggregate factory accepting a fuzzed parameter is not enough: the parameter must also have an SQL
/// literal form. Decimal and big-integer fields (which fuzzField mints) are accepted by the factory but
/// FieldVisitorToString renders them QUOTED, so reparsing the emitted name turns the parameter into a String
/// the factory then rejects - i.e. the #109706 class this PR prevents. Every type the aggregate arms return
/// must therefore reparse from its own getName().
TEST(QueryFuzzer, AggregateParameterKeepsNameReconstructible)
{
    tryRegisterAggregateFunctions();

    /// quantileExact takes one numeric parameter, so a Decimal substitution is accepted by the factory.
    const DataTypes arg_types = {std::make_shared<DataTypeUInt64>()};
    const Array numeric_parameters = {Field(0.5)};

    /// The direct path: a Decimal parameter must be declined, while its numeric form must be accepted. Without
    /// the emitted-name check the Decimal case returns a type whose name reads quantileExact('0.5').
    QueryFuzzer direct;
    EXPECT_NE(nullptr, direct.makeAggregateFunctionType("quantileExact", arg_types, numeric_parameters, /*simple=*/false))
        << "a numeric parameter must stay acceptable";
    for (const Field & unrepresentable :
         Array{DecimalField<Decimal32>(Int32(5), 1), DecimalField<Decimal64>(Int64(5), 1), Field(Int128(3)), Field(UInt128(3))})
    {
        auto declined = direct.makeAggregateFunctionType("quantileExact", arg_types, Array{unrepresentable}, /*simple=*/false);
        EXPECT_EQ(nullptr, declined) << "accepted a parameter with no SQL literal form: "
                                     << (declined ? declined->getName() : String{});
    }

    /// And the production path: drive fuzzDataType over a parameterized aggregate and require every returned
    /// type to reparse. fuzzAggregateParameters runs unrestricted fuzzField, so Decimal parameters do occur.
    AggregateFunctionProperties properties;
    auto source_func
        = AggregateFunctionFactory::instance().get("quantileExact", NullsAction::EMPTY, arg_types, numeric_parameters, properties);

    bool saw_parameter_change = false;
    for (UInt64 seed = 0; seed < 4000; ++seed)
    {
        QueryFuzzer fuzzer;
        fuzzer.setSeed(seed);
        auto source_type = std::make_shared<DataTypeAggregateFunction>(source_func, arg_types, numeric_parameters);
        DataTypePtr fuzzed;
        try
        {
            fuzzed = fuzzer.fuzzDataType(source_type);
        }
        catch (const Exception &)
        {
            continue; /// a randomized shape rejected at construction time; the real fuzzer catches these too.
        }

        /// Whatever the arms produced - rebuilt aggregate, wrapped, or replaced - it must be reconstructible.
        const String name = fuzzed->getName();
        EXPECT_NE(nullptr, DataTypeFactory::instance().tryGet(name)) << "seed=" << seed << " emitted " << name;

        /// Credit the parameter mutator only for a rebuild of THIS aggregate with THIS argument list whose
        /// parameters changed. Testing "parameters differ" alone also credits the generic replacement tail,
        /// which can hand back an unrelated argument-less aggregate whose empty parameters trivially differ -
        /// so the flag stayed set even with fuzzAggregateParameters removed.
        const auto * aggr = typeid_cast<const DataTypeAggregateFunction *>(fuzzed.get());
        if (aggr && aggr->getFunctionName() == source_type->getFunctionName()
            && sameTypes(aggr->getArgumentsDataTypes(), arg_types) && aggr->getParameters() != numeric_parameters)
            saw_parameter_change = true;
    }
    /// Guard against the loop passing vacuously: the parameter mutator must have fired at least once.
    EXPECT_TRUE(saw_parameter_change) << "no aggregate parameter mutation was observed";
}

/// Every geo alias the factory reports must actually have its storage fuzzed, and every rebuild must stay
/// parser-reconstructible. Enumerating from Geometry's variants covers a newly registered alias for free.
TEST(QueryFuzzer, GeoAliasStorageIsFuzzed)
{
    tryRegisterAggregateFunctions();

    const auto geometry = DataTypeFactory::instance().get("Geometry");
    const auto * geometry_variant = typeid_cast<const DataTypeVariant *>(geometry.get());
    ASSERT_NE(nullptr, geometry_variant) << "Geometry is expected to be a Variant over the geo aliases";

    const auto & selected = QueryFuzzer::geoAliasNames();

    /// Non-geo fixed-name aliases have no nested structure to fuzz and must not be selected.
    EXPECT_FALSE(selected.contains("Bool"));

    std::vector<String> aliases{"Geometry"};
    for (const auto & alternative : geometry_variant->getVariants())
        aliases.push_back(alternative->getName());

    for (const String & alias : aliases)
    {
        /// Every registered geo alias must reach the arm; MultiPoint was missing from the hardcoded list.
        ASSERT_TRUE(selected.contains(alias)) << "geo alias not covered: " << alias;

        /// And the rebuild the arm performs must actually fuzz the nested structure. Drive it directly: going
        /// through fuzzDataType cannot isolate it, because an unselected alias continues into the
        /// wrapping/replacement tail whose output is indistinguishable from a rebuild by name alone.
        const auto plain = DataTypeFactory::instance().get(alias);

        /// The storage rendered with NO child mutation. plain->getName() is the ALIAS ("MultiPoint"), so it
        /// cannot serve as the baseline: every rebuild strips the alias and would differ from it trivially.
        String unmutated;
        if (const auto * v = typeid_cast<const DataTypeVariant *>(plain.get()))
            unmutated = std::make_shared<DataTypeVariant>(v->getVariants())->getName();
        else if (const auto * a = typeid_cast<const DataTypeArray *>(plain.get()))
            unmutated = std::make_shared<DataTypeArray>(a->getNestedType())->getName();
        else if (const auto * t = typeid_cast<const DataTypeTuple *>(plain.get()))
            unmutated = std::make_shared<DataTypeTuple>(t->getElements())->getName();
        ASSERT_FALSE(unmutated.empty()) << alias << " has no fuzzable container storage";

        bool saw_child_mutation = false;
        for (UInt64 seed = 0; seed < 4000 && !saw_child_mutation; ++seed)
        {
            QueryFuzzer fuzzer;
            fuzzer.setSeed(seed);

            DataTypePtr rebuilt;
            try
            {
                rebuilt = fuzzer.fuzzContainerChildren(plain);
            }
            catch (const Exception &)
            {
                continue; /// a fuzzed storage type may violate a container invariant; the arm catches this too.
            }
            ASSERT_NE(nullptr, rebuilt) << alias << " has no fuzzable container storage";

            if (rebuilt->getName() == unmutated)
                continue; /// rebuilt, but every child mutation was a no-op this seed.
            saw_child_mutation = true;

            /// The result must be parseable, else the fuzzer would trip the #109706 consistency check.
            const String rebuilt_name = rebuilt->getName();
            ParserDataType parser;
            ASTPtr parsed = parseQuery(
                parser, rebuilt_name.data(), rebuilt_name.data() + rebuilt_name.size(), "",
                DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
            ASSERT_NE(nullptr, parsed) << alias << " -> " << rebuilt_name;
            EXPECT_TRUE(dataTypeRoundTrips(parsed)) << alias << " -> " << rebuilt_name;
        }

        EXPECT_TRUE(saw_child_mutation) << "fuzzContainerChildren never fuzzed a child of " << alias;

        /// And the production dispatch must route the alias there AND hand the helper's result back. No property
        /// of the output can establish that: the wrapping/replacement tail independently reproduces the storage
        /// kind, the absent custom name, and a name differing from the unmutated storage (it can return
        /// Array(MultiPoint) where the discarded rebuild was based on Array(Point)). So compare against the
        /// helper's recorded return value by POINTER identity, which only the arm returning it can satisfy.
        bool saw_dispatch = false;
        bool saw_production_mutation = false;
        for (UInt64 seed = 0; seed < 4000 && !saw_production_mutation; ++seed)
        {
            QueryFuzzer fuzzer;
            fuzzer.setSeed(seed);
            DataTypePtr fuzzed;
            try
            {
                fuzzed = fuzzer.fuzzDataType(plain);
            }
            catch (const Exception &)
            {
                continue;
            }
            if (fuzzer.getContainerRebuildCount() == 0)
                continue;
            saw_dispatch = true;

            /// Entering the helper is not enough: fuzzDataType must return exactly the object it produced.
            const auto rebuilt = fuzzer.getLastContainerRebuild();
            ASSERT_NE(nullptr, rebuilt) << alias << " seed=" << seed;
            EXPECT_EQ(rebuilt.get(), fuzzed.get())
                << alias << " seed=" << seed << ": dispatched but returned " << fuzzed->getName()
                << " instead of the rebuild " << rebuilt->getName();

            /// A result equal to the unmutated storage only means every child mutation was a no-op for this
            /// seed; keep looking. One differing result proves the production path preserves them.
            if (fuzzed.get() == rebuilt.get() && fuzzed->getName() != unmutated)
                saw_production_mutation = true;
        }
        EXPECT_TRUE(saw_dispatch) << "fuzzDataType never dispatched " << alias << " to the container rebuild";
        EXPECT_TRUE(saw_production_mutation)
            << "fuzzDataType dispatched " << alias << " but never returned a child mutation (always " << unmutated << ")";
    }
}

/// `typeid_cast` and `IAST::as` both match the exact type, so an `ASTDataType` SUBCLASS
/// (`ASTTupleDataType`, `ASTEnumDataType`) is not caught by a check naming only the base and falls through to
/// the generic expression fuzzer - which injects functions into the type's argument list, i.e. the #109706 bug
/// this PR exists to prevent. Fuzz whole CREATE queries carrying those shapes and require every data type in
/// the result to stay parseable.
TEST(QueryFuzzer, DataTypeSubclassesAreNotFuzzedAsExpressions)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    const String query = "CREATE TABLE t ("
                         "a Tuple(x Point, y String), "
                         "b Tuple(UInt8, String), "
                         "c Tuple(m MultiPoint, n Int64), "
                         "d Enum8('a' = 1, 'b' = 2), "
                         "e Array(Nullable(UInt64))"     /// a plain ASTDataType root
                         ") ENGINE = Memory";

    size_t types_checked = 0;
    /// Per class of datatype root, whether that column's declared type was mutated at all - a guard against a
    /// vacuously green run in which the fuzzer happened to change nothing.
    std::map<String, bool> mutated{{"DataType", false}, {"TupleDataType", false}, {"EnumDataType", false}};
    /// The declared type of every column in the UNFUZZED query, keyed by column name, plus the class of its
    /// root node. A mutation is only credited when a column's own type text changed.
    std::map<String, std::pair<String, String>> baseline; /// column -> (class, type text)
    {
        ParserQuery p(query.data() + query.size());
        const ASTPtr base = parseQuery(p, query.data(), query.data() + query.size(), "",
                                       DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH,
                                       DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        for (const auto & col : collectColumnTypes(base))
            baseline[col.first] = {rootClassOf(col.second), col.second->formatWithSecretsOneLine()};
        ASSERT_EQ(5u, baseline.size());
    }
    for (UInt64 seed = 0; seed < 2000; ++seed)
    {
        ParserQuery parser(query.data() + query.size());
        ASTPtr ast;
        try
        {
            ast = parseQuery(
                parser, query.data(), query.data() + query.size(), "",
                DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
            QueryFuzzer fuzzer;
            fuzzer.setSeed(seed);
            fuzzer.fuzzMain(ast);
        }
        catch (const Exception &)
        {
            continue; /// the fuzzer legitimately rejects some shapes; not the arm under test.
        }

        /// Every data-type node left in the tree must be reconstructible by ParserDataType. getID() is the
        /// discriminator that does not depend on the exact-match casts under test.
        std::vector<ASTPtr> stack{ast};
        while (!stack.empty())
        {
            const ASTPtr node = stack.back();
            stack.pop_back();
            if (!node)
                continue;
            const String id = node->getID();
            if (id.starts_with("DataType") || id.starts_with("TupleDataType") || id.starts_with("EnumDataType"))
            {
                ++types_checked;
                EXPECT_TRUE(dataTypeRoundTrips(node)) << "seed=" << seed << " id=" << id;

            }
            for (const auto & child : node->children)
                stack.push_back(child);
        }

        /// Liveness: a column whose own declared type text changed. Note fuzzColumnDeclaration also mutates the
        /// type, so this only guards against a vacuously green run; the guard itself is isolated by the
        /// standalone-root loop below.
        for (const auto & col : collectColumnTypes(ast))
        {
            const auto it = baseline.find(col.first);
            if (it == baseline.end())
                continue; /// the fuzzer renamed/dropped the column - not evidence either way.
            if (col.second->formatWithSecretsOneLine() != it->second.second)
                mutated[it->second.first] = true;
        }
    }

    EXPECT_GT(types_checked, 0u) << "no data-type nodes were inspected - the walk proves nothing";

    for (const auto & [cls, seen] : mutated)
        EXPECT_TRUE(seen) << "no mutation reached any " << cls << " root - the seed range proves nothing";

    /// Isolate the guard: drive a STANDALONE data-type root through fuzzMain, so fuzzColumnDeclaration (which
    /// mutates a column's type independently) is not in the picture and the guarded branch in fuzz() is the only
    /// thing that can act. Each class must be mutated, and every result must still be parseable.
    for (const String & type_name : {"Array(Nullable(UInt64))", "Tuple(a UInt8, b String)", "Tuple(UInt8, String)",
                                     "Enum8('a' = 1, 'b' = 2)", "MultiPoint"})
    {
        size_t mutations = 0;
        for (UInt64 seed = 0; seed < 2000; ++seed)
        {
            ParserDataType type_parser;
            ASTPtr root;
            try
            {
                root = parseQuery(
                    type_parser, type_name.data(), type_name.data() + type_name.size(), "",
                    DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
                QueryFuzzer fuzzer;
                fuzzer.setSeed(seed);
                fuzzer.fuzzMain(root);
            }
            catch (const Exception &)
            {
                continue;
            }

            if (root->formatWithSecretsOneLine() != type_name)
                ++mutations;

            /// Whatever it became must round-trip: this is the #109706 invariant.
            EXPECT_TRUE(dataTypeRoundTrips(root)) << type_name << " seed=" << seed;
        }

        /// A guard whose mutation body is a no-op leaves the root untouched for every seed.
        EXPECT_GT(mutations, 0u) << "no mutation reached the standalone root " << type_name;
    }
}

/// The custom-name block must not become a dead end. `Bool` is a fixed alias over `UInt8` with no children, so
/// it has nothing structural to fuzz - but returning it unchanged there froze it for every seed, silently
/// removing it from fuzzer coverage. It has to reach the wrapping/replacement mutations, which keep the alias
/// intact. `UInt8` is the control: a plain type takes the same tail, so the rates must be comparable.
TEST(QueryFuzzer, CustomNamedLeafAliasIsStillFuzzed)
{
    tryRegisterAggregateFunctions();

    const auto mutation_rate = [](const String & type_name)
    {
        size_t changed = 0;
        for (UInt64 seed = 0; seed < 2000; ++seed)
        {
            QueryFuzzer fuzzer;
            fuzzer.setSeed(seed);
            DataTypePtr fuzzed;
            try
            {
                fuzzed = fuzzer.fuzzDataType(DataTypeFactory::instance().get(type_name));
            }
            catch (const Exception &)
            {
                continue;
            }

            const String fuzzed_name = fuzzed->getName();
            if (fuzzed_name == type_name)
                continue;
            ++changed;

            /// Whatever it became must be a real type: the fuzzer feeds these names back through ParserDataType.
            ParserDataType parser;
            ASTPtr parsed = parseQuery(
                parser, fuzzed_name.data(), fuzzed_name.data() + fuzzed_name.size(), "",
                DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
            EXPECT_NE(nullptr, parsed) << type_name << " -> " << fuzzed_name;
            EXPECT_TRUE(dataTypeRoundTrips(parsed)) << type_name << " -> " << fuzzed_name;
        }
        return changed;
    };

    const size_t control = mutation_rate("UInt8");
    ASSERT_GT(control, 0u) << "the control type was never fuzzed - the measurement proves nothing";

    /// Was 0 before the fix, against ~1000 for the control.
    EXPECT_GT(mutation_rate("Bool"), control / 2) << "Bool is frozen in the custom-name block";
}

/// A deleted structured arm makes its type fall through to the wrapping/replacement tail, which still returns a
/// valid type - so parseability alone cannot tell a live arm from a dead one. Each arm needs a witness the
/// REPLACEMENT path cannot fabricate.
///
/// For `SimpleAggregateFunction`, `Nested` and `JSON` the witness is exact: the first two are absent from
/// `getRandomType`, and the JSON arm is the only producer of a JSON type that still carries the source SKIP
/// list. `DateTime`, `DateTime64` and `QBit` are value-indistinguishable, because `getRandomType` builds them
/// with the very same helpers - but their RATE separates cleanly: the arm fires on 3 of 4 seeds, while the
/// replacement tail lands on the same family only by chance (measured: ~3000/4000 with the arm, 6-18/4000
/// without it). `min_rate` encodes that, far below the live rate and far above the incidental one.
TEST(QueryFuzzer, StructuredTypeArmsMutateTheirOwnType)
{
    tryRegisterAggregateFunctions();

    /// A CARRIER is one mutation an arm performs. Witnesses inspect the rebuilt type STRUCTURALLY (accessors on
    /// the custom name / data type), not its printed name: a name-shaped witness is satisfied by whichever
    /// sibling carrier happens to fire, so removing any single one leaves it green.
    struct Carrier
    {
        String type_name;
        String carrier; /// which mutation of that type must be observed
        std::function<bool(const DataTypePtr &)> observed;
        size_t min_rate; /// minimum count over 4000 seeds (1 = exact witness, 1000 = rate-separated)
    };

    /// The SimpleAggregateFunction custom name, or nullptr if the fuzzer returned something else entirely.
    const auto as_simple_aggr = [](const DataTypePtr & type) -> const DataTypeCustomSimpleAggregateFunction *
    {
        return type->hasCustomName() ? typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(type->getCustomName())
                                     : nullptr;
    };
    const auto as_nested = [](const DataTypePtr & type) -> const DataTypeNestedCustomName *
    { return type->hasCustomName() ? typeid_cast<const DataTypeNestedCustomName *>(type->getCustomName()) : nullptr; };

    const DataTypes simple_aggr_args = {std::make_shared<DataTypeUInt64>()};
    const Names nested_names = {"a", "b"};

    const std::vector<Carrier> carriers = {
        /// SimpleAggregateFunction is absent from getRandomType, so any such result came from its own arm. One
        /// witness per carrier: name, argument types, parameters. `quantileExact` is used where a parameter has
        /// to exist at all.
        {"SimpleAggregateFunction(sum, UInt64)", "aggregate name",
         [&](const DataTypePtr & t)
         { const auto * s = as_simple_aggr(t); return s && s->getFunctionName() != "sum"; }, 1},
        /// `any` is used here rather than `sum`: SimpleAggregateFunction requires the aggregate's return type to
        /// equal the storage type, and `sum` accepts only a narrow numeric set, so nearly every fuzzed argument is
        /// rejected and the arm falls back. `any` returns its own argument type, so any fuzzed type survives and
        /// the carrier is actually observable.
        {"SimpleAggregateFunction(any, UInt64)", "argument types",
         [&](const DataTypePtr & t)
         { const auto * s = as_simple_aggr(t); return s && !sameTypes(s->getArgumentsDataTypes(), simple_aggr_args); }, 1},
        /// SimpleAggregateFunction only accepts a fixed function list (checkSupportedFunctions); groupArrayArray
        /// is the one on it that takes a parameter, so it is the only fixture that can witness this carrier.
        {"SimpleAggregateFunction(groupArrayArray(3), Array(UInt64))", "aggregate parameters",
         [&](const DataTypePtr & t)
         {
             const auto * s = as_simple_aggr(t);
             return s && s->getFunctionName() == "groupArrayArray" && s->getParameters() != Array{Field(UInt64(3))};
         }, 1},
        /// Nested is likewise unreachable from getRandomType; its arm fuzzes the elements and keeps the names.
        {"Nested(a UInt32, b Array(Nullable(Int64)))", "element types",
         [&](const DataTypePtr & t)
         {
             const auto * n = as_nested(t);
             return n && n->getNames() == nested_names
                 && (n->getElements().size() != 2 || n->getElements()[0]->getName() != "UInt32"
                     || n->getElements()[1]->getName() != "Array(Nullable(Int64))");
         }, 1},
        /// The JSON arm recurses into the typed paths and rebuilds via makeRandomObject, which keeps the SKIP
        /// list a random JSON never has. Witness the typed-path TYPE, so randomized numeric parameters alone
        /// (which also change the name) cannot satisfy it.
        {"JSON(max_dynamic_paths=8, p1 UInt32, SKIP s)", "typed-path types",
         [](const DataTypePtr & t)
         {
             const auto * o = typeid_cast<const DataTypeObject *>(t.get());
             if (!o || !o->getPathsToSkip().contains("s"))
                 return false;
             const auto it = o->getTypedPaths().find("p1");
             return it != o->getTypedPaths().end() && it->second->getName() != "UInt32";
         }, 1},
        /// DateTime / DateTime64 / QBit are value-indistinguishable: getRandomType builds them with the very
        /// same helpers. Their RATE separates instead - the arm fires on 3 of 4 seeds, the replacement tail
        /// lands on the family only by chance (measured: ~3000/4000 with the arm, 6-18/4000 without it).
        /// Both timezone branches of makeRandomDateTime / makeRandomDateTime64 must stay reachable: a build that
        /// only ever emitted timezone-less values would still clear a family-and-rate check.
        {"DateTime('Asia/Istanbul')", "explicit timezone",
         [](const DataTypePtr & t)
         {
             const auto * d = typeid_cast<const DataTypeDateTime *>(t.get());
             return d && d->hasExplicitTimeZone();
         }, 200},
        {"DateTime('Asia/Istanbul')", "default timezone",
         [](const DataTypePtr & t)
         {
             const auto * d = typeid_cast<const DataTypeDateTime *>(t.get());
             return d && !d->hasExplicitTimeZone();
         }, 200},
        {"DateTime64(3, 'UTC')", "explicit timezone",
         [](const DataTypePtr & t)
         {
             const auto * d = typeid_cast<const DataTypeDateTime64 *>(t.get());
             return d && d->hasExplicitTimeZone();
         }, 200},
        {"DateTime64(3, 'UTC')", "scale",
         [](const DataTypePtr & t)
         {
             const auto * d = typeid_cast<const DataTypeDateTime64 *>(t.get());
             return d && d->getScale() != 3;
         }, 200},
        {"QBit(Float32, 16)", "element type/dimension",
         [](const DataTypePtr & t) { return typeid_cast<const DataTypeQBit *>(t.get()) != nullptr; }, 1000},
    };

    for (const auto & c : carriers)
    {
        const auto source = DataTypeFactory::instance().get(c.type_name);
        size_t witnessed = 0;
        for (UInt64 seed = 0; seed < 4000; ++seed)
        {
            QueryFuzzer fuzzer;
            fuzzer.setSeed(seed);
            DataTypePtr fuzzed;
            try
            {
                fuzzed = fuzzer.fuzzDataType(source);
            }
            catch (const Exception &)
            {
                continue; /// a randomized shape rejected at construction time; the real fuzzer catches these too.
            }

            const String fuzzed_name = fuzzed->getName();
            if (fuzzed_name == c.type_name || !c.observed(fuzzed))
                continue;
            ++witnessed;

            /// The mutated type is fed back through ParserDataType by the fuzzer, so it must reconstruct.
            EXPECT_NE(nullptr, DataTypeFactory::instance().tryGet(fuzzed_name)) << c.type_name << " -> " << fuzzed_name;
        }
        EXPECT_GE(witnessed, c.min_rate) << "only " << witnessed << " mutations of the " << c.carrier << " of "
                                         << c.type_name << " (expected at least " << c.min_rate << ") - carrier may be dead";
    }
}
