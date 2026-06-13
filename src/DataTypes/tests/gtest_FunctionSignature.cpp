#include <DataTypes/FunctionSignature.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>


using namespace DB;


namespace
{

ColumnWithTypeAndName makeColumn(const String & type_name)
{
    auto type = DataTypeFactory::instance().get(type_name);
    return {nullptr, type, ""};
}

ColumnWithTypeAndName makeConstColumn(const String & type_name, const Field & value)
{
    auto type = DataTypeFactory::instance().get(type_name);
    return {type->createColumnConst(1, value), type, ""};
}

String checkSignature(const String & signature, const ColumnsWithTypeAndName & args)
{
    FunctionSignature checker(signature);
    String reason;
    auto type = checker.check(args, reason);
    if (!type)
        return "FAIL: " + reason;
    return type->getName();
}

/// The `types_only` path: the arguments carry no column information, so a `const`
/// position cannot be rejected for non-constness and its value cannot be captured.
String checkSignatureTypesOnly(const String & signature, const ColumnsWithTypeAndName & args)
{
    FunctionSignature checker(signature);
    String reason;
    auto type = checker.check(args, reason, /*types_only=*/ true);
    if (!type)
        return "FAIL: " + reason;
    return type->getName();
}

}


GTEST_TEST(FunctionSignature, Identity)
{
    EXPECT_EQ(checkSignature("f(T) -> T", {makeColumn("UInt32")}), "UInt32");
    EXPECT_EQ(checkSignature("f(T) -> T", {makeColumn("String")}), "String");
}

GTEST_TEST(FunctionSignature, ExactTypes)
{
    EXPECT_EQ(checkSignature("f(String) -> UInt8", {makeColumn("String")}), "UInt8");
    EXPECT_THAT(checkSignature("f(String) -> UInt8", {makeColumn("UInt32")}), ::testing::StartsWith("FAIL:"));
}

GTEST_TEST(FunctionSignature, TypeMatchers)
{
    EXPECT_EQ(checkSignature("f(T : UnsignedInteger) -> T", {makeColumn("UInt32")}), "UInt32");
    EXPECT_THAT(checkSignature("f(T : UnsignedInteger) -> T", {makeColumn("Int32")}), ::testing::StartsWith("FAIL:"));
    EXPECT_EQ(checkSignature("f(T : Number) -> T", {makeColumn("Float64")}), "Float64");
}

GTEST_TEST(FunctionSignature, TypeFunctions)
{
    EXPECT_EQ(
        checkSignature("f(T1, T2) -> leastSupertype(T1, T2)", {makeColumn("UInt8"), makeColumn("UInt32")}),
        "UInt32");

    EXPECT_EQ(
        checkSignature("f(T) -> Array(T)", {makeColumn("String")}),
        "Array(String)");
}

GTEST_TEST(FunctionSignature, ConstArguments)
{
    EXPECT_EQ(
        checkSignature("toFixedString(String, const N UnsignedInteger) -> FixedString(N)",
            {makeColumn("String"), makeConstColumn("UInt8", Field(UInt64(5)))}),
        "FixedString(5)");

    /// Non-const second argument: should fail.
    EXPECT_THAT(
        checkSignature("toFixedString(String, const N UnsignedInteger) -> FixedString(N)",
            {makeColumn("String"), makeColumn("UInt8")}),
        ::testing::StartsWith("FAIL:"));
}

GTEST_TEST(FunctionSignature, Ellipsis)
{
    EXPECT_EQ(
        checkSignature("array(T1, ...) -> Array(leastSupertype(T1, ...))",
            {makeColumn("UInt8"), makeColumn("Int8"), makeColumn("UInt16")}),
        "Array(Int32)");

    /// Same argument type repeated.
    EXPECT_EQ(
        checkSignature("f(T, ...) -> T", {makeColumn("UInt32"), makeColumn("UInt32")}),
        "UInt32");
}

GTEST_TEST(FunctionSignature, OptionalGroup)
{
    EXPECT_EQ(
        checkSignature("f(T1, [T2]) -> T1", {makeColumn("UInt32")}),
        "UInt32");

    EXPECT_EQ(
        checkSignature("f(T1, [T2]) -> T1", {makeColumn("UInt32"), makeColumn("String")}),
        "UInt32");

    EXPECT_THAT(
        checkSignature("f(T1, [T2]) -> T1", {}),
        ::testing::StartsWith("FAIL:"));
}

GTEST_TEST(FunctionSignature, Alternatives)
{
    String sig = "reverse(T : StringOrFixedString) -> T OR reverse(T : Array) -> T";
    EXPECT_EQ(checkSignature(sig, {makeColumn("String")}), "String");
    EXPECT_EQ(checkSignature(sig, {makeColumn("FixedString(7)")}), "FixedString(7)");
    EXPECT_EQ(checkSignature(sig, {makeColumn("Array(UInt8)")}), "Array(UInt8)");
    EXPECT_THAT(checkSignature(sig, {makeColumn("UInt8")}), ::testing::StartsWith("FAIL:"));
}

GTEST_TEST(FunctionSignature, ConstStringExtractedAsType)
{
    EXPECT_EQ(
        checkSignature("f(const t String) -> typeFromString(t)",
            {makeConstColumn("String", Field(String("UInt16")))}),
        "UInt16");
}

GTEST_TEST(FunctionSignature, NoFunctionName)
{
    EXPECT_EQ(checkSignature("(T) -> T", {makeColumn("UInt32")}), "UInt32");
    EXPECT_EQ(checkSignature("() -> String", {}), "String");
}

GTEST_TEST(FunctionSignature, NewMatchers)
{
    EXPECT_EQ(checkSignature("(Float) -> Float64", {makeColumn("Float32")}), "Float64");
    EXPECT_EQ(checkSignature("(NativeNumber) -> UInt8", {makeColumn("Int32")}), "UInt8");
    EXPECT_THAT(checkSignature("(NativeNumber) -> UInt8", {makeColumn("UInt128")}), ::testing::StartsWith("FAIL:"));
    EXPECT_EQ(checkSignature("(Decimal) -> Float64", {makeColumn("Decimal(10, 2)")}), "Float64");
    EXPECT_EQ(checkSignature("(UUID) -> String", {makeColumn("UUID")}), "String");
    EXPECT_EQ(checkSignature("(FixedString) -> UInt64", {makeColumn("FixedString(5)")}), "UInt64");
    EXPECT_EQ(checkSignature("(Map) -> UInt64", {makeColumn("Map(String, UInt8)")}), "UInt64");
}

GTEST_TEST(FunctionSignature, OrOperator)
{
    /// Inline alternatives within a single argument.
    String sig = "(NativeNumber | Decimal) -> Float64";
    EXPECT_EQ(checkSignature(sig, {makeColumn("Int32")}), "Float64");
    EXPECT_EQ(checkSignature(sig, {makeColumn("Decimal(10, 2)")}), "Float64");
    EXPECT_THAT(checkSignature(sig, {makeColumn("String")}), ::testing::StartsWith("FAIL:"));

    /// Many alternatives.
    String many = "(String | FixedString | Array | Map | UUID | IPv4 | IPv6) -> UInt64";
    for (const String & accepted : {"String", "FixedString(7)", "Array(UInt8)", "Map(String, UInt8)", "UUID", "IPv4", "IPv6"})
        EXPECT_EQ(checkSignature(many, {makeColumn(accepted)}), "UInt64") << "input: " << accepted;
    EXPECT_THAT(checkSignature(many, {makeColumn("UInt8")}), ::testing::StartsWith("FAIL:"));

    /// `|` binds tighter than `:` so this captures T as the original (Float32 or Float64) input.
    String captured = "(T : Float | Decimal) -> T";
    EXPECT_EQ(checkSignature(captured, {makeColumn("Float32")}), "Float32");
    EXPECT_EQ(checkSignature(captured, {makeColumn("Decimal(5, 2)")}), "Decimal(5, 2)");
}

GTEST_TEST(FunctionSignature, MaybeNullableUnwrap)
{
    /// (MaybeNullable(U)) -> U: U captures the inner non-nullable type, used by `assumeNotNull`.
    EXPECT_EQ(checkSignature("(MaybeNullable(U)) -> U", {makeColumn("UInt32")}), "UInt32");
    EXPECT_EQ(checkSignature("(MaybeNullable(U)) -> U", {makeColumn("Nullable(UInt32)")}), "UInt32");
}

GTEST_TEST(FunctionSignature, NumericLiteral)
{
    /// Numeric literals can appear in return-type expressions as constant Field arguments.
    EXPECT_EQ(checkSignature("(String) -> FixedString(7)", {makeColumn("String")}), "FixedString(7)");
}

GTEST_TEST(FunctionSignature, DateTimeWithOptionalTimezone)
{
    String sig =
        "(DateOrDateTime) -> DateTime"
        " OR (DateOrDateTime, const tz String) -> DateTime(tz)";

    EXPECT_EQ(checkSignature(sig, {makeColumn("Date")}), "DateTime");
    EXPECT_EQ(checkSignature(sig, {makeColumn("DateTime")}), "DateTime");
    EXPECT_EQ(
        checkSignature(sig, {makeColumn("DateTime"), makeConstColumn("String", Field(String("UTC")))}),
        "DateTime('UTC')");
}

GTEST_TEST(FunctionSignature, DateTime64ScaleFromSource)
{
    /// scaleOf(T) returns the source DateTime64 scale (or default 3 for non-DateTime64).
    String sig =
        "(T : DateOrDateTime) -> DateTime64(scaleOf(T))"
        " OR (T : DateOrDateTime, const tz String) -> DateTime64(scaleOf(T), tz)";

    EXPECT_EQ(checkSignature(sig, {makeColumn("Date")}), "DateTime64(3)");
    EXPECT_EQ(checkSignature(sig, {makeColumn("DateTime")}), "DateTime64(3)");
    EXPECT_EQ(checkSignature(sig, {makeColumn("DateTime64(7)")}), "DateTime64(7)");
    EXPECT_EQ(
        checkSignature(sig, {makeColumn("DateTime64(2)"), makeConstColumn("String", Field(String("UTC")))}),
        "DateTime64(2, 'UTC')");
}

GTEST_TEST(FunctionSignature, NamedTuple)
{
    /// Natural shorthand for named tuple elements: `name Type` desugars to NamedField.
    EXPECT_EQ(
        checkSignature("(UInt64) -> Tuple(origin UInt64, destination UInt64)",
                       {makeColumn("UInt64")}),
        "Tuple(origin UInt64, destination UInt64)");

    /// The explicit NamedField form is still accepted and equivalent.
    EXPECT_EQ(
        checkSignature("(UInt64) -> Tuple(NamedField('origin', UInt64), NamedField('destination', UInt64))",
                       {makeColumn("UInt64")}),
        "Tuple(origin UInt64, destination UInt64)");

    /// Unnamed Tuple still works.
    EXPECT_EQ(
        checkSignature("(UInt64) -> Tuple(UInt64, UInt64)", {makeColumn("UInt64")}),
        "Tuple(UInt64, UInt64)");
}

GTEST_TEST(FunctionSignature, DateTime64SubsecondMinScale)
{
    /// max(scaleOf(T), 6) — used by toStartOfMicrosecond etc.
    String sig =
        "(T : DateOrDateTime) -> DateTime64(max(scaleOf(T), 6))"
        " OR (T : DateOrDateTime, const tz String) -> DateTime64(max(scaleOf(T), 6), tz)";

    EXPECT_EQ(checkSignature(sig, {makeColumn("DateTime64(2)")}), "DateTime64(6)");
    EXPECT_EQ(checkSignature(sig, {makeColumn("DateTime64(9)")}), "DateTime64(9)");
}

GTEST_TEST(FunctionSignature, OptionalPairEllipsis)
{
    /// A mandatory prefix followed by zero or more repetitions of a loose
    /// (name, value) pair, written as a bracketed optional group repeated by the
    /// ellipsis. The bracket keeps the preceding argument out of the repeated unit
    /// (a bare `..., T1, V1, ...` would fold the adjacent argument into it). This is
    /// the shape used by `timeSeriesStoreTags`: `(id, tags_array, [name, value]...)`,
    /// accepted arities `2 + 2 * N`.
    String store = "f(I : Any, Array(Tuple(String, String)) | Nothing, "
                   "[T1 : String, V1 : String], ...) -> I";

    /// id + tags_array, no loose pairs.
    EXPECT_EQ(checkSignature(store, {makeColumn("UInt64"), makeColumn("Array(Tuple(String, String))")}), "UInt64");
    /// id + tags_array + one pair.
    EXPECT_EQ(
        checkSignature(store, {makeColumn("UInt128"), makeColumn("Array(Tuple(String, String))"),
            makeColumn("String"), makeColumn("String")}),
        "UInt128");
    /// id + tags_array + two pairs.
    EXPECT_EQ(
        checkSignature(store, {makeColumn("UInt64"), makeColumn("Array(Tuple(String, String))"),
            makeColumn("String"), makeColumn("String"), makeColumn("String"), makeColumn("String")}),
        "UInt64");
    /// A dangling tag name without a value (odd trailing count) is rejected.
    EXPECT_THAT(
        checkSignature(store, {makeColumn("UInt64"), makeColumn("Array(Tuple(String, String))"),
            makeColumn("String")}),
        ::testing::StartsWith("FAIL:"));

    /// The `timeSeriesTagsToGroup` shape: `(tags_array, [name, value]...)`,
    /// accepted arities `1 + 2 * N`.
    String group = "f(Array(Tuple(String, String)) | Nothing, [T1 : String, V1 : String], ...) -> UInt64";

    /// tags_array alone.
    EXPECT_EQ(checkSignature(group, {makeColumn("Array(Tuple(String, String))")}), "UInt64");
    /// tags_array + one pair.
    EXPECT_EQ(
        checkSignature(group, {makeColumn("Array(Tuple(String, String))"), makeColumn("String"), makeColumn("String")}),
        "UInt64");
    /// tags_array + a dangling name (even total) is rejected.
    EXPECT_THAT(
        checkSignature(group, {makeColumn("Array(Tuple(String, String))"), makeColumn("String")}),
        ::testing::StartsWith("FAIL:"));
}

GTEST_TEST(FunctionSignature, AlternativeFallbackOnUnavailableConst)
{
    /// The first alternative needs a const argument's value to build its return
    /// type; on the `types_only` path that value is unavailable, so applying the
    /// return type raises `ILLEGAL_COLUMN`. This must fail only that alternative,
    /// letting a later non-const fallback still match (this is what makes the
    /// setting-compatible timezone signatures usable on the type-only path).
    String sig = "f(const tz String) -> typeFromString(tz) OR f(String) -> DateTime";

    /// Types-only: the const value is unavailable, so the fallback alternative wins.
    EXPECT_EQ(checkSignatureTypesOnly(sig, {makeColumn("String")}), "DateTime");

    /// With an actual constant the first alternative is used.
    EXPECT_EQ(checkSignature(sig, {makeConstColumn("String", Field(String("UInt16")))}), "UInt16");
}
