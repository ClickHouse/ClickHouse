#include <gtest/gtest.h>

#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

namespace DB::ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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

/// Both entry points, resolved with a context carrying non-default settings.
std::pair<DataTypePtr, DataTypePtr> bothReturnTypes(
    const ContextPtr & context, const String & function_name, const std::vector<String> & argument_types)
{
    tryRegisterFunctions();
    auto resolver = FunctionFactory::instance().get(function_name, context);
    auto types = makeTypes(argument_types);
    ColumnsWithTypeAndName columns;
    for (const auto & type : types)
        columns.emplace_back(nullptr, type, String{});
    return {getAdaptor(resolver).getReturnTypeImpl(types), getAdaptor(resolver).getReturnTypeImpl(columns)};
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

TEST(TypesOnlyReturnTypePath, ValueDependentDocumentationSignaturesFallBackToNotImplemented)
{
    /// These functions declare documentation-only signatures whose result type is spelled `Any`:
    /// it depends on argument *values* (a setting name, a type name, a dictionary attribute),
    /// which the types-only path does not have. The generic signature fallback in
    /// `IFunction::getReturnTypeImpl(const DataTypes &)` must not surface the internal
    /// `BAD_FUNCTION_SIGNATURE` ("Variable Any was not captured") for them; it falls back to
    /// the legacy `NOT_IMPLEMENTED` of this entry point.
    for (const auto & [function_name, argument_types] : std::initializer_list<std::pair<String, std::vector<String>>>{
             {"getSetting", {"String"}},
             {"getSettingOrDefault", {"String", "UInt64"}},
             {"getServerSetting", {"String"}},
             {"getMergeTreeSetting", {"String"}},
             {"globalVariable", {"String"}},
             {"dynamicElement", {"Dynamic", "String"}},
             {"variantElement", {"Variant(String, UInt64)", "String"}},
             {"accurateCastOrDefault", {"UInt64", "String"}},
             {"dictGet", {"String", "String", "UInt64"}},
             {"dictGetOrDefault", {"String", "String", "UInt64", "UInt64"}},
             {"dictGetOrNull", {"String", "String", "UInt64"}}})
    {
        try
        {
            typesOnlyReturnType(function_name, argument_types);
            FAIL() << function_name << " must not resolve on the types-only path";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED) << function_name << ": " << e.message();
        }
    }

    /// The fallback only covers the non-constructible result type. A genuine mismatch of the
    /// arguments against the signature still propagates with its user-facing code.
    try
    {
        typesOnlyReturnType("getSetting", {"UInt64"});
        FAIL() << "getSetting(UInt64) must be rejected on the types-only path";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT) << e.message();
    }
    try
    {
        typesOnlyReturnType("dictGet", {"String"});
        FAIL() << "dictGet(String) must be rejected on the types-only path";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH) << e.message();
    }
}

TEST(TypesOnlyReturnTypePath, TupleAndVariantTypeUseTheirAuthoritativeResolvers)
{
    /// The signature of `tuple` requires one fixed argument before the trailing
    /// ellipsis, but the implementation also supports `tuple()`.
    EXPECT_EQ(typesOnlyReturnType("tuple", {})->getName(), "Tuple()");
    EXPECT_EQ(typesOnlyReturnType("tuple", {"UInt8", "String"})->getName(), "Tuple(UInt8, String)");

    /// The `variantType` result Enum8 is formed from the input Variant's members;
    /// a documentation-only bare `Enum8` cannot represent that type.
    const std::vector<String> variant_arguments{"Variant(String, UInt64)"};
    EXPECT_EQ(
        typesOnlyReturnType("variantType", variant_arguments)->getName(),
        columnPathReturnType("variantType", variant_arguments)->getName());
    EXPECT_EQ(typesOnlyReturnType("variantType", variant_arguments)->getName(), "Enum8('None' = -1, 'String' = 0, 'UInt64' = 1)");
}

TEST(TypesOnlyReturnTypePath, IfNullAndDecimalArithmeticUseTheirAuthoritativeResolvers)
{
    /// With `use_variant_as_common_type` disabled, the documentation signature would select
    /// `leastSupertype` for this shape. The authoritative resolver must still preserve the
    /// Variant and extend it with the fallback type on both entry points.
    const std::vector<String> if_null_arguments{"Variant(String, UInt64)", "Int8"};
    EXPECT_EQ(
        typesOnlyReturnType("ifNull", if_null_arguments)->getName(),
        columnPathReturnType("ifNull", if_null_arguments)->getName());

    /// Decimal scale is derived by the legacy resolver. A bare `Decimal256` signature result
    /// is not constructible, so the types-only path must instead share the two-argument logic.
    const std::vector<String> decimal_arguments{"Decimal64(3)", "Decimal128(7)"};
    for (const auto & function_name : {"multiplyDecimal", "divideDecimal"})
    {
        EXPECT_EQ(
            typesOnlyReturnType(function_name, decimal_arguments)->getName(),
            columnPathReturnType(function_name, decimal_arguments)->getName()) << function_name;
    }
}

/// `JSONOverloadResolver` builds its result type from `Impl::getReturnType`, which accepts shapes
/// the advertised (documentation-only) signature does not describe: a `Dynamic` JSON argument, and
/// a `Nullable` input whose wrapper the resolver adds itself. Both return-type entry points must
/// therefore route through that same logic, so that a direct `getReturnType` call cannot disagree
/// with the function the analyzer actually builds.
TEST(TypesOnlyReturnTypePath, JSONResolverReturnTypeAgreesWithBuild)
{
    tryRegisterFunctions();

    auto make_arguments = [](const std::vector<String> & argument_types, const std::vector<String> & constant_values)
    {
        ColumnsWithTypeAndName columns;
        for (size_t i = 0; i < argument_types.size(); ++i)
        {
            auto type = DataTypeFactory::instance().get(argument_types[i]);
            if (i < constant_values.size() && !constant_values[i].empty())
                columns.emplace_back(type->createColumnConst(1, Field(constant_values[i])), type, String{});
            else
                columns.emplace_back(type->createColumn(), type, String{});
        }
        return columns;
    };

    for (const auto & [function_name, argument_types, constant_values, expected] :
         std::initializer_list<std::tuple<String, std::vector<String>, std::vector<String>, String>>{
             {"JSONHas", {"String", "String"}, {"", "a"}, "UInt8"},
             /// The signature is rooted at `String`, so without the shim the nullable wrapper
             /// that `build` adds would be dropped.
             {"JSONHas", {"Nullable(String)", "String"}, {"", "a"}, "Nullable(UInt8)"},
             {"JSONLength", {"Nullable(String)"}, {}, "Nullable(UInt64)"},
             /// The signature does not describe a `Dynamic` JSON argument at all.
             {"JSONHas", {"Dynamic", "String"}, {"", "a"}, "UInt8"},
             {"JSONExtract", {"String", "String"}, {"", "UInt64"}, "UInt64"},
             {"JSONExtract", {"Nullable(String)", "String"}, {"", "UInt64"}, "Nullable(UInt64)"}})
    {
        auto resolver = FunctionFactory::instance().get(function_name, getContext().context);
        auto arguments = make_arguments(argument_types, constant_values);

        auto built = resolver->build(arguments)->getResultType();
        EXPECT_EQ(built->getName(), expected) << function_name;
        EXPECT_EQ(resolver->getReturnType(arguments)->getName(), built->getName()) << function_name;
    }
}

/// Under `cast_keep_nullable`, a `Dynamic` or `Variant` argument keeps the conversion result
/// `Nullable` — a conversion-specific rule the declarative signature cannot express. Both
/// return-type entry points of `FunctionConvert` must apply it, or a types-only caller (which
/// only has `DataTypes`) would resolve a non-Nullable type while the analyzer resolves a
/// `Nullable` one.
TEST(TypesOnlyReturnTypePath, ConversionsKeepNullableForDynamicAndVariantOnBothPaths)
{
    tryRegisterFunctions();
    auto context = Context::createCopy(getContext().context);
    context->setSetting("cast_keep_nullable", Field(true));

    for (const auto & [function_name, argument_type, expected] :
         std::initializer_list<std::tuple<String, String, String>>{
             {"toUInt64", "Dynamic", "Nullable(UInt64)"},
             {"toString", "Variant(String, UInt64)", "Nullable(String)"},
             {"toFloat64", "Variant(String, UInt64)", "Nullable(Float64)"},
             /// A plain argument is unaffected by the setting.
             {"toUInt64", "String", "UInt64"}})
    {
        auto [types_only, column_path] = bothReturnTypes(context, function_name, {argument_type});
        EXPECT_EQ(types_only->getName(), expected) << function_name << "(" << argument_type << ")";
        EXPECT_EQ(column_path->getName(), types_only->getName()) << function_name << "(" << argument_type << ")";
    }
}

/// The `naiveBayesClassifier*` signatures are documentation-only because legality depends on the
/// *value* of the first argument: the named dictionary must have the `NAIVE_BAYES` layout. There
/// is no types-only answer, so that entry point must decline instead of accepting any
/// `(String, MaybeNullable(String))` pair through the declarative path.
TEST(TypesOnlyReturnTypePath, NaiveBayesClassifiersDeclineTheTypesOnlyPath)
{
    for (const auto & function_name :
         {"naiveBayesClassifier", "naiveBayesClassifierWithProb", "naiveBayesClassifierWithAllProbs"})
    {
        try
        {
            auto result = typesOnlyReturnType(function_name, {"String", "String"});
            FAIL() << function_name << " must not resolve on the types-only path, got " << result->getName();
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED) << function_name << ": " << e.message();
        }
    }
}

/// `parseDateTime*` derives its result type from argument *values*: the timezone comes from the
/// optional third argument and, for the Joda `DateTime64` variants, the scale is the number of
/// `S` placeholders in the format string. The documentation-only signature can express neither,
/// so the types-only entry point must decline rather than answer with a timezone-less `DateTime`
/// or a scale-0 `DateTime64`.
TEST(TypesOnlyReturnTypePath, ParseDateTimeDeclinesTheTypesOnlyPath)
{
    tryRegisterFunctions();

    for (const auto & function_name :
         {"parseDateTime", "parseDateTimeOrNull", "parseDateTimeOrZero",
          "parseDateTimeInJodaSyntax", "parseDateTime64", "parseDateTime64OrNull",
          "parseDateTime64InJodaSyntax", "parseDateTime64InJodaSyntaxOrNull"})
    {
        try
        {
            auto result = typesOnlyReturnType(function_name, {"String", "String", "String"});
            FAIL() << function_name << " must not resolve on the types-only path, got " << result->getName();
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED) << function_name << ": " << e.message();
        }
    }

    /// The authoritative, column-aware path keeps answering with the explicit timezone and with
    /// the scale derived from the format string.
    auto make_arguments = [](const std::vector<String> & constant_values)
    {
        auto type = DataTypeFactory::instance().get("String");
        ColumnsWithTypeAndName columns;
        columns.emplace_back(type->createColumn(), type, String{});
        for (const auto & value : constant_values)
            columns.emplace_back(type->createColumnConst(1, Field(value)), type, String{});
        return columns;
    };

    auto column_path_return_type = [&](const String & function_name, const std::vector<String> & constant_values)
    {
        auto resolver = FunctionFactory::instance().get(function_name, getContext().context);
        return getAdaptor(resolver).getReturnTypeImpl(make_arguments(constant_values))->getName();
    };

    EXPECT_EQ(column_path_return_type("parseDateTime", {"%Y-%m-%d %H:%i:%s", "UTC"}), "DateTime('UTC')");
    EXPECT_EQ(
        column_path_return_type("parseDateTimeOrNull", {"%Y-%m-%d %H:%i:%s", "UTC"}), "Nullable(DateTime('UTC'))");
    EXPECT_EQ(
        column_path_return_type("parseDateTime64InJodaSyntax", {"yyyy-MM-dd HH:mm:ss.SSS", "UTC"}),
        "DateTime64(3, 'UTC')");
    EXPECT_EQ(
        column_path_return_type("parseDateTime64InJodaSyntax", {"yyyy-MM-dd HH:mm:ss.SSSSSS", "UTC"}),
        "DateTime64(6, 'UTC')");
}
