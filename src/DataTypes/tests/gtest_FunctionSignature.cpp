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
