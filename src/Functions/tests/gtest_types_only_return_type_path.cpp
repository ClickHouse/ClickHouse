#include <gtest/gtest.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

namespace DB::ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

using namespace DB;

/// A documentation-only declarative signature must not open a second, weaker resolution path:
/// for a function that does not override `getReturnTypeImpl(const DataTypes &)`, the base
/// `IFunction` implementation applies the signature string on the types-only path, bypassing
/// the authoritative `ColumnsWithTypeAndName` override. `toUTCTimestamp` / `fromUTCTimestamp`
/// (an invariant the DSL cannot express: no explicit timezone on the first argument) and the
/// `JSON_*` functions (a multi-path result type that mirrors the tuple/array shape of the path
/// argument) route the types-only path through a `DataTypes` shim for this reason. These tests
/// pin the shim by resolving through both entry points directly.

namespace
{

DataTypes makeTypes(const std::vector<String> & argument_types)
{
    DataTypes types;
    types.reserve(argument_types.size());
    for (const auto & type_name : argument_types)
        types.push_back(DataTypeFactory::instance().get(type_name));
    return types;
}

const FunctionToOverloadResolverAdaptor & getAdaptor(const FunctionOverloadResolverPtr & resolver)
{
    const auto * adaptor = dynamic_cast<const FunctionToOverloadResolverAdaptor *>(resolver.get());
    if (!adaptor)
        throw std::runtime_error("function is not registered through FunctionToOverloadResolverAdaptor");
    return *adaptor;
}

/// The types-only entry point: `getReturnTypeImpl(const DataTypes &)`.
DataTypePtr typesOnlyReturnType(const String & function_name, const std::vector<String> & argument_types)
{
    tryRegisterFunctions();
    auto resolver = FunctionFactory::instance().get(function_name, getContext().context);
    return getAdaptor(resolver).getReturnTypeImpl(makeTypes(argument_types));
}

/// The column-aware entry point: `getReturnTypeImpl(const ColumnsWithTypeAndName &)`.
DataTypePtr columnPathReturnType(const String & function_name, const std::vector<String> & argument_types)
{
    tryRegisterFunctions();
    auto resolver = FunctionFactory::instance().get(function_name, getContext().context);
    ColumnsWithTypeAndName columns;
    for (const auto & type : makeTypes(argument_types))
        columns.emplace_back(nullptr, type, String{});
    return getAdaptor(resolver).getReturnTypeImpl(columns);
}

}

TEST(TypesOnlyReturnTypePath, UTCTimestampTransformKeepsNoExplicitTimezoneInvariant)
{
    /// Without an explicit timezone on the first argument, both entry points resolve.
    EXPECT_EQ(typesOnlyReturnType("toUTCTimestamp", {"DateTime", "String"})->getName(), "DateTime");
    EXPECT_EQ(typesOnlyReturnType("fromUTCTimestamp", {"DateTime64(3)", "String"})->getName(), "DateTime64(3)");

    /// With an explicit timezone, the types-only path must reject the call exactly like the
    /// column-aware path does, instead of silently accepting it through the signature string.
    for (const auto & [function_name, first_argument] : std::initializer_list<std::pair<String, String>>{
             {"toUTCTimestamp", "DateTime('UTC')"},
             {"fromUTCTimestamp", "DateTime64(3, 'UTC')"}})
    {
        try
        {
            typesOnlyReturnType(function_name, {first_argument, "String"});
            FAIL() << function_name << "(" << first_argument << ", String) must be rejected on the types-only path";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT) << e.message();
        }
    }
}

TEST(TypesOnlyReturnTypePath, SQLJSONMultiPathShapeSurvivesTypesOnlyResolution)
{
    /// A multi-path path argument (a tuple or array of JSONPath strings) produces a result type
    /// that mirrors the path structure. The types-only path must agree with the authoritative
    /// column-aware path instead of rejecting the call or collapsing the result to the scalar
    /// `String` / `UInt8` the documentation signature advertises.
    for (const auto & [function_name, path_type] : std::initializer_list<std::pair<String, String>>{
             {"JSON_EXISTS", "Tuple(String, String)"},
             {"JSON_VALUE", "Tuple(String, String)"},
             {"JSON_QUERY", "Array(String)"},
             {"JSON_VALUE", "String"}})
    {
        auto types_only = typesOnlyReturnType(function_name, {"Dynamic", path_type});
        auto column_path = columnPathReturnType(function_name, {"Dynamic", path_type});
        EXPECT_EQ(types_only->getName(), column_path->getName()) << function_name << " over " << path_type;
    }

    /// The tuple shape is really mirrored, not flattened.
    EXPECT_EQ(typesOnlyReturnType("JSON_EXISTS", {"Dynamic", "Tuple(String, String)"})->getName(), "Tuple(UInt8, UInt8)");
}
