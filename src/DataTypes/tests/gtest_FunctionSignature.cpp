#include <DataTypes/FunctionSignature.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Core/Field.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>


using namespace DB;

namespace DB::ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
}


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

GTEST_TEST(FunctionSignature, ExplicitArityAlternatives)
{
    /// `arrayROCAUC`: nested Optional groups (`[const Bool, [Array]]`) are not expressible,
    /// so the legal 2..4 argument arities are spelled as explicit alternatives.
    const String sig =
        "(Array, Array) -> Float64"
        " OR (Array, Array, const Bool) -> Float64"
        " OR (Array, Array, const Bool, Array) -> Float64";

    EXPECT_EQ(checkSignature(sig, {makeColumn("Array(Float64)"), makeColumn("Array(UInt8)")}), "Float64");
    EXPECT_EQ(
        checkSignature(sig, {makeColumn("Array(Float64)"), makeColumn("Array(UInt8)"), makeConstColumn("Bool", Field(UInt64(1)))}),
        "Float64");
    EXPECT_EQ(
        checkSignature(sig,
            {makeColumn("Array(Float64)"), makeColumn("Array(UInt8)"), makeConstColumn("Bool", Field(UInt64(1))), makeColumn("Array(Int32)")}),
        "Float64");
    /// Out-of-range arities are rejected.
    EXPECT_THAT(checkSignature(sig, {makeColumn("Array(Float64)")}), ::testing::StartsWith("FAIL:"));
    EXPECT_THAT(
        checkSignature(sig,
            {makeColumn("Array(Float64)"), makeColumn("Array(UInt8)"), makeConstColumn("Bool", Field(UInt64(1))),
             makeColumn("Array(Int32)"), makeColumn("Array(Int32)")}),
        ::testing::StartsWith("FAIL:"));
}

GTEST_TEST(FunctionSignature, ExactFixedStringWidth)
{
    /// `IPv6NumToString` / `UUIDNumToString` encode the exact `FixedString` width directly in
    /// the signature (instead of a separate `getReturnTypeImpl` width override), so a
    /// wrong-width `FixedString` is rejected during analysis on every path.
    const String sig = "(IPv6 | FixedString(16)) -> String";
    EXPECT_EQ(checkSignature(sig, {makeColumn("FixedString(16)")}), "String");
    EXPECT_EQ(checkSignature(sig, {makeColumn("IPv6")}), "String");
    EXPECT_THAT(checkSignature(sig, {makeColumn("FixedString(8)")}), ::testing::StartsWith("FAIL:"));
    EXPECT_THAT(checkSignatureTypesOnly(sig, {makeColumn("FixedString(8)")}), ::testing::StartsWith("FAIL:"));
}

GTEST_TEST(FunctionSignature, ConstArgumentPosition)
{
    /// A `const` position is rejected for a non-constant column on the column path, but is
    /// accepted on the types-only path (where constness cannot be observed).
    const String sig = "(String, const String) -> String";
    EXPECT_EQ(checkSignature(sig, {makeColumn("String"), makeConstColumn("String", Field(String("x")))}), "String");
    EXPECT_THAT(checkSignature(sig, {makeColumn("String"), makeColumn("String")}), ::testing::StartsWith("FAIL:"));
    EXPECT_EQ(checkSignatureTypesOnly(sig, {makeColumn("String"), makeColumn("String")}), "String");
}

GTEST_TEST(FunctionSignature, AdditionMultiplicationResult)
{
    /// `arrayDotProduct`: same-type `Float32`/`BFloat16` accumulate to `Float32` (the two leading
    /// alternatives); every other pair uses `additionMultiplicationResult`, i.e.
    /// `NumberTraits::ResultOfAdditionMultiplication` of the element types.
    const String sig =
        "(Array(Float32), Array(Float32)) -> Float32"
        " OR (Array(BFloat16), Array(BFloat16)) -> Float32"
        " OR (Array(L : NativeNumber | BFloat16), Array(R : NativeNumber | BFloat16))"
        " -> additionMultiplicationResult(L, R)";

    EXPECT_EQ(checkSignature(sig, {makeColumn("Array(Float32)"), makeColumn("Array(Float32)")}), "Float32");
    EXPECT_EQ(checkSignature(sig, {makeColumn("Array(BFloat16)"), makeColumn("Array(BFloat16)")}), "Float32");
    EXPECT_EQ(checkSignature(sig, {makeColumn("Array(BFloat16)"), makeColumn("Array(Float32)")}), "Float64");
    EXPECT_EQ(checkSignature(sig, {makeColumn("Array(Float32)"), makeColumn("Array(Float64)")}), "Float64");
    EXPECT_EQ(checkSignature(sig, {makeColumn("Array(UInt8)"), makeColumn("Array(UInt8)")}), "UInt16");
    EXPECT_EQ(checkSignature(sig, {makeColumn("Array(Int8)"), makeColumn("Array(UInt16)")}), "Int32");
    EXPECT_EQ(checkSignature(sig, {makeColumn("Array(UInt64)"), makeColumn("Array(UInt64)")}), "UInt64");
    /// Non-numeric element types are rejected.
    EXPECT_THAT(checkSignature(sig, {makeColumn("Array(String)"), makeColumn("Array(String)")}), ::testing::StartsWith("FAIL:"));
    EXPECT_THAT(checkSignature(sig, {makeColumn("Array(Decimal(10, 2))"), makeColumn("Array(Decimal(10, 2))")}), ::testing::StartsWith("FAIL:"));
}

GTEST_TEST(FunctionSignature, ThrowIfExplicitPrefixes)
{
    /// `throwIf`'s legal shapes are spelled out as explicit prefixes rather than two independent
    /// optional groups `(NativeNumber, [const String], [const Int8 | ...])`. With the optional-group
    /// form the matcher could skip the message and bind a non-String second argument as the error
    /// code, so `throwIf(cond, toInt8(1))` would match the advertised shape even though the real
    /// validator rejects it. The explicit prefixes keep the metadata honest.
    const String sig =
        "(NativeNumber) -> UInt8"
        " OR (NativeNumber, const String) -> UInt8"
        " OR (NativeNumber, const String, const Int8 | Int16 | Int32) -> UInt8";

    EXPECT_EQ(checkSignature(sig, {makeColumn("UInt8")}), "UInt8");
    EXPECT_EQ(checkSignature(sig, {makeColumn("UInt8"), makeConstColumn("String", Field(String("msg")))}), "UInt8");
    EXPECT_EQ(
        checkSignature(
            sig, {makeColumn("UInt8"), makeConstColumn("String", Field(String("msg"))), makeConstColumn("Int32", Field(Int64(7)))}),
        "UInt8");

    /// `throwIf(cond, toInt8(1))` must NOT match: the second argument is not a String, and the
    /// three-argument alternative requires a message before the error code.
    EXPECT_THAT(checkSignature(sig, {makeColumn("UInt8"), makeConstColumn("Int8", Field(Int64(1)))}), ::testing::StartsWith("FAIL:"));
    /// A non-constant message is rejected on the column path.
    EXPECT_THAT(checkSignature(sig, {makeColumn("UInt8"), makeColumn("String")}), ::testing::StartsWith("FAIL:"));

    /// Without `allow_custom_error_code_in_throwif` the three-argument form is not part of the contract.
    const String sig_no_error_code = "(NativeNumber) -> UInt8 OR (NativeNumber, const String) -> UInt8";
    FunctionSignature checker_no_error_code(sig_no_error_code);
    EXPECT_EQ(checker_no_error_code.minArguments(), 1u);
    EXPECT_EQ(checker_no_error_code.maxArguments(), 2u);
    EXPECT_FALSE(checker_no_error_code.isArgumentCountInRange(3));
}

GTEST_TEST(FunctionSignature, ToStartOfIntervalArgumentShapes)
{
    /// The 2/3/4-argument shapes of `toStartOfInterval` are spelled out explicitly so the
    /// `system.functions.signature` metadata matches the validator, including the four-argument
    /// `(time, interval, origin, timezone)` overload and the constant interval/origin/timezone
    /// positions. The result type is computed by the legacy `getReturnTypeImpl`, so the signature is
    /// documentation-only; here we assert the structural argument-count contract.
    const String sig =
        "(DateOrDateTime, const Interval) -> DateOrDateTime"
        " OR (DateOrDateTime, const Interval, const String) -> DateOrDateTime"
        " OR (DateOrDateTime, const Interval, const DateOrDateTime) -> DateOrDateTime"
        " OR (DateOrDateTime, const Interval, const DateOrDateTime, const String) -> DateOrDateTime";

    FunctionSignature checker(sig);
    EXPECT_EQ(checker.minArguments(), 2u);
    EXPECT_EQ(checker.maxArguments(), 4u);
    EXPECT_FALSE(checker.isArgumentCountInRange(1));
    EXPECT_TRUE(checker.isArgumentCountInRange(2));
    EXPECT_TRUE(checker.isArgumentCountInRange(3));
    EXPECT_TRUE(checker.isArgumentCountInRange(4));
    EXPECT_FALSE(checker.isArgumentCountInRange(5));
}

GTEST_TEST(FunctionSignature, PointInPolygonPointTupleMustBeNumeric)
{
    /// `pointInPolygon` dispatches the point's coordinates at execution through the raw
    /// `CallPointInPolygon` `typeid_cast` over native-number `ColumnVector` types, so the point
    /// tuple is constrained to `NativeNumber` elements (instead of the previous `Tuple(Any, Any)`).
    /// A non-native-number point is then rejected during analysis rather than reaching the terminal
    /// dispatch case that raises a `LOGICAL_ERROR` ("Unknown numeric column type"). The polygon
    /// arguments stay loose; their coordinates are cast to `Float64` in bulk.
    const String sig = "(Tuple(NativeNumber, NativeNumber), Array, ...) -> UInt8";

    EXPECT_EQ(
        checkSignature(sig, {makeColumn("Tuple(Float64, Float64)"), makeColumn("Array(Tuple(Float64, Float64))")}),
        "UInt8");
    EXPECT_EQ(
        checkSignature(sig, {makeColumn("Tuple(UInt32, Int8)"), makeColumn("Array(Array(Tuple(Float64, Float64)))")}),
        "UInt8");
    /// A polygon split across several ring arguments (the variadic tail).
    EXPECT_EQ(
        checkSignature(
            sig,
            {makeColumn("Tuple(Float64, Float64)"),
             makeColumn("Array(Tuple(Float64, Float64))"),
             makeColumn("Array(Tuple(Float64, Float64))")}),
        "UInt8");
    /// Non-native-number point coordinates are rejected.
    EXPECT_THAT(
        checkSignature(sig, {makeColumn("Tuple(String, String)"), makeColumn("Array(Tuple(Float64, Float64))")}),
        ::testing::StartsWith("FAIL:"));
    EXPECT_THAT(
        checkSignature(sig, {makeColumn("Tuple(Decimal(10, 2), Decimal(10, 2))"), makeColumn("Array(Tuple(Float64, Float64))")}),
        ::testing::StartsWith("FAIL:"));
}

GTEST_TEST(FunctionSignature, SparseGramsHashesContract)
{
    /// `sparseGramsHashes` reads its optional `min_ngram_length` / `max_ngram_length` /
    /// `min_cutoff_length` arguments once per block with `getUInt(0)` and only handles a
    /// `ColumnString` input, so the signature spells the precise contract instead of `(Any, ...)`,
    /// which used to accept non-`String` input and row-varying option columns.
    const String sig = "(String, [const NativeInteger], [const NativeInteger], [const NativeInteger]) -> Array(UInt32)";

    EXPECT_EQ(checkSignature(sig, {makeColumn("String")}), "Array(UInt32)");
    EXPECT_EQ(checkSignature(sig, {makeColumn("String"), makeConstColumn("UInt8", Field(UInt64(3)))}), "Array(UInt32)");
    EXPECT_EQ(
        checkSignature(
            sig, {makeColumn("String"), makeConstColumn("UInt8", Field(UInt64(3))), makeConstColumn("UInt16", Field(UInt64(100)))}),
        "Array(UInt32)");
    /// Non-`String` input is rejected.
    EXPECT_THAT(checkSignature(sig, {makeColumn("UInt32")}), ::testing::StartsWith("FAIL:"));
    /// A non-constant option is rejected on the column path.
    EXPECT_THAT(checkSignature(sig, {makeColumn("String"), makeColumn("UInt8")}), ::testing::StartsWith("FAIL:"));
    /// A non-integer option is rejected.
    EXPECT_THAT(
        checkSignature(sig, {makeColumn("String"), makeConstColumn("Float64", Field(Float64(3.0)))}),
        ::testing::StartsWith("FAIL:"));
}

GTEST_TEST(FunctionSignature, BareParametricReturnReportsCleanlyOnTypesOnly)
{
    /// A documentation-only signature whose return type is a bare parametric type function
    /// (`FixedString`, `DateTime64`, `Time64`) is still evaluated by the types-only
    /// `IFunction::getReturnTypeImpl(DataTypes)` fallback, where the size / scale constant is
    /// unavailable. This must surface as a clean, user-facing `ILLEGAL_COLUMN` (the result type
    /// needs a constant argument), not the internal `LOGICAL_ERROR` a bare zero-argument type
    /// function used to raise.
    for (const String & sig : {String("(Any) -> FixedString"), String("(Any) -> DateTime64"), String("(Any) -> Time64")})
    {
        FunctionSignature checker(sig);
        String reason;
        try
        {
            checker.check({makeColumn("UInt32")}, reason, /*types_only=*/ true);
            FAIL() << "expected an exception for " << sig;
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::ILLEGAL_COLUMN) << sig << ": " << e.message();
        }
    }
}

GTEST_TEST(FunctionSignature, BareContainerReturnHazardOnTypesOnly)
{
    /// Several functions carry a documentation-only signature whose return type is a *bare* container
    /// type function — `-> Tuple` (`tuplePlus`, `tupleNegate`, `hilbertDecode` / `mortonDecode`, the
    /// `L*Normalize` family) or `-> Array` (`dictGetKeys`, `nested`, the first `arrayReduceInRanges`).
    /// The exact element / tuple type isn't expressible in the DSL, so those functions keep an
    /// authoritative hand-written `getReturnTypeImpl` and route the types-only path to it as well. This
    /// test documents why that routing is necessary: evaluating a bare container return through the DSL
    /// on the types-only path is wrong. A bare `-> Tuple` silently yields the empty tuple `Tuple()`
    /// instead of the real element-wise type…
    EXPECT_EQ(
        checkSignatureTypesOnly("(Tuple, Tuple) -> Tuple", {makeColumn("Tuple(UInt8, UInt8)"), makeColumn("Tuple(UInt8, UInt8)")}),
        "Tuple()");

    /// … and a bare `-> Array` cannot produce a concrete element type on the types-only path, so it
    /// surfaces as a clean, user-facing `ILLEGAL_COLUMN` (not the internal `LOGICAL_ERROR` a plain
    /// wrong-arity check would raise — which would abort a build with `ABORT_ON_LOGICAL_ERROR`). The
    /// `getReturnTypeImpl(DataTypes)` overrides on those functions never reach this DSL path, so
    /// neither hazard is observable through them.
    try
    {
        checkSignatureTypesOnly("(Any) -> Array", {makeColumn("String")});
        FAIL() << "expected an ILLEGAL_COLUMN for a bare `-> Array` return";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::ILLEGAL_COLUMN) << e.message();
    }
}

GTEST_TEST(FunctionSignature, ConversionScaleAndTimezoneArgumentShapes)
{
    /// `toDateTime64` / `toTime64` require a constant scale (the `FunctionConvert` validator adds it
    /// to the mandatory arguments); `toDateTime64` also accepts an optional trailing timezone that
    /// requires the scale first, while `toTime64` has no timezone. The `FunctionConvert*` overloads
    /// compute the concrete result type via the legacy `getReturnTypeImpl`, so the signatures are
    /// documentation-only and their bare-parametric returns are not resolved here; we assert the
    /// argument-count contract that the explicit prefixes encode instead of two independent optional
    /// groups (which would have advertised `toDateTime64(x)` and `toDateTime64(x, 'UTC')`).
    {
        FunctionSignature checker("(Any, const UInt8) -> DateTime64 OR (Any, const UInt8, const String) -> DateTime64");
        EXPECT_EQ(checker.minArguments(), 2u);
        EXPECT_EQ(checker.maxArguments(), 3u);
        EXPECT_FALSE(checker.isArgumentCountInRange(1));
        EXPECT_TRUE(checker.isArgumentCountInRange(2));
        EXPECT_TRUE(checker.isArgumentCountInRange(3));
        EXPECT_FALSE(checker.isArgumentCountInRange(4));
    }
    {
        FunctionSignature checker("(Any, const UInt8) -> Time64");
        EXPECT_EQ(checker.minArguments(), 2u);
        EXPECT_EQ(checker.maxArguments(), 2u);
        EXPECT_FALSE(checker.isArgumentCountInRange(1));
        EXPECT_TRUE(checker.isArgumentCountInRange(2));
        EXPECT_FALSE(checker.isArgumentCountInRange(3));
    }
    {
        /// The 'OrZero' / 'OrNull' / 'parse*BestEffort' string converters take an optional scale and
        /// (for DateTime64) an optional timezone that requires the scale first, so the scale is part
        /// of the optional prefix rather than mandatory.
        FunctionSignature checker(
            "(MaybeNullable(StringOrFixedString)) -> DateTime64"
            " OR (MaybeNullable(StringOrFixedString), const UInt8) -> DateTime64"
            " OR (MaybeNullable(StringOrFixedString), const UInt8, const String) -> DateTime64");
        EXPECT_EQ(checker.minArguments(), 1u);
        EXPECT_EQ(checker.maxArguments(), 3u);
        EXPECT_TRUE(checker.isArgumentCountInRange(1));
        EXPECT_TRUE(checker.isArgumentCountInRange(3));
        EXPECT_FALSE(checker.isArgumentCountInRange(4));
    }
}

GTEST_TEST(FunctionSignature, ConstantOnlyOptionPositions)
{
    /// `isDecimalOverflow` reads its optional `precision` once via a `ColumnConst<ColumnUInt8>`, so the
    /// signature marks it `const`; a row-varying precision is rejected during analysis.
    {
        const String sig = "(Decimal, [const UInt8]) -> UInt8";
        EXPECT_EQ(checkSignature(sig, {makeColumn("Decimal(10, 2)")}), "UInt8");
        EXPECT_EQ(checkSignature(sig, {makeColumn("Decimal(10, 2)"), makeConstColumn("UInt8", Field(UInt64(9)))}), "UInt8");
        EXPECT_THAT(checkSignature(sig, {makeColumn("Decimal(10, 2)"), makeColumn("UInt8")}), ::testing::StartsWith("FAIL:"));
        EXPECT_THAT(checkSignature(sig, {makeColumn("String")}), ::testing::StartsWith("FAIL:"));
    }

    /// `arrayShuffle` / `arrayPartialShuffle` read the seed / limit once with `getUInt(0)` and mark
    /// them always-constant, so the optional positions are `const`.
    {
        const String shuffle = "(Array(T), [const Integer]) -> Array(T)";
        EXPECT_EQ(checkSignature(shuffle, {makeColumn("Array(UInt32)")}), "Array(UInt32)");
        EXPECT_EQ(checkSignature(shuffle, {makeColumn("Array(String)"), makeConstColumn("UInt64", Field(UInt64(1)))}), "Array(String)");
        EXPECT_THAT(checkSignature(shuffle, {makeColumn("Array(UInt32)"), makeColumn("UInt64")}), ::testing::StartsWith("FAIL:"));

        const String partial = "(Array(T), [const Integer], [const Integer]) -> Array(T)";
        EXPECT_EQ(
            checkSignature(partial, {makeColumn("Array(UInt32)"), makeConstColumn("UInt64", Field(UInt64(1))), makeConstColumn("UInt64", Field(UInt64(2)))}),
            "Array(UInt32)");
        EXPECT_THAT(
            checkSignature(partial, {makeColumn("Array(UInt32)"), makeColumn("UInt64")}),
            ::testing::StartsWith("FAIL:"));
    }

    /// Token generators mark the separator (`splitByChar` / `splitByString` / `splitByRegexp`) and the
    /// `max_substrings` option constant.
    {
        const String sig = "(const String, String, [const NativeInteger]) -> Array(String)";
        EXPECT_EQ(checkSignature(sig, {makeConstColumn("String", Field(String(","))), makeColumn("String")}), "Array(String)");
        EXPECT_EQ(
            checkSignature(sig, {makeConstColumn("String", Field(String(","))), makeColumn("String"), makeConstColumn("UInt64", Field(UInt64(2)))}),
            "Array(String)");
        /// A non-constant separator is rejected.
        EXPECT_THAT(checkSignature(sig, {makeColumn("String"), makeColumn("String")}), ::testing::StartsWith("FAIL:"));
        /// A non-constant `max_substrings` is rejected.
        EXPECT_THAT(
            checkSignature(sig, {makeConstColumn("String", Field(String(","))), makeColumn("String"), makeColumn("UInt64")}),
            ::testing::StartsWith("FAIL:"));

        const String sparse = "(String, [const NativeInteger], [const NativeInteger], [const NativeInteger]) -> Array(String)";
        EXPECT_EQ(checkSignature(sparse, {makeColumn("String")}), "Array(String)");
        EXPECT_THAT(checkSignature(sparse, {makeColumn("String"), makeColumn("UInt8")}), ::testing::StartsWith("FAIL:"));
    }

    /// `ngramSimHash` / `ngramMinHash` read the optional shingle-size / hash-count once, so they are
    /// marked `const` in the (documentation-only) signature that mirrors the executor's contract.
    {
        EXPECT_EQ(checkSignature("(String, [const UInt]) -> UInt64", {makeColumn("String")}), "UInt64");
        EXPECT_EQ(
            checkSignature("(String, [const UInt]) -> UInt64", {makeColumn("String"), makeConstColumn("UInt8", Field(UInt64(4)))}),
            "UInt64");
        EXPECT_THAT(
            checkSignature("(String, [const UInt]) -> UInt64", {makeColumn("String"), makeColumn("UInt8")}),
            ::testing::StartsWith("FAIL:"));
        EXPECT_EQ(
            checkSignature("(String, [const UInt], [const UInt]) -> Tuple(UInt64, UInt64)", {makeColumn("String")}),
            "Tuple(UInt64, UInt64)");
    }
}
