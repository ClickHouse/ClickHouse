#include <gtest/gtest.h>

#include <iostream>
#include <map>

#include <DataTypes/NumberTraits.h>

static const std::map<std::pair<std::string, std::string>, std::string> answer =
{
    {{"UInt8", "UInt8"}, "UInt8"},
    {{"UInt8", "UInt16"}, "UInt16"},
    {{"UInt8", "UInt32"}, "UInt32"},
    {{"UInt8", "UInt64"}, "UInt64"},
    {{"UInt8", "Int8"}, "Int16"},
    {{"UInt8", "Int16"}, "Int16"},
    {{"UInt8", "Int32"}, "Int32"},
    {{"UInt8", "Int64"}, "Int64"},
    {{"UInt8", "Float32"}, "Float32"},
    {{"UInt8", "Float64"}, "Float64"},
    {{"UInt16", "UInt8"}, "UInt16"},
    {{"UInt16", "UInt16"}, "UInt16"},
    {{"UInt16", "UInt32"}, "UInt32"},
    {{"UInt16", "UInt64"}, "UInt64"},
    {{"UInt16", "Int8"}, "Int32"},
    {{"UInt16", "Int16"}, "Int32"},
    {{"UInt16", "Int32"}, "Int32"},
    {{"UInt16", "Int64"}, "Int64"},
    {{"UInt16", "Float32"}, "Float32"},
    {{"UInt16", "Float64"}, "Float64"},
    {{"UInt32", "UInt8"}, "UInt32"},
    {{"UInt32", "UInt16"}, "UInt32"},
    {{"UInt32", "UInt32"}, "UInt32"},
    {{"UInt32", "UInt64"}, "UInt64"},
    {{"UInt32", "Int8"}, "Int64"},
    {{"UInt32", "Int16"}, "Int64"},
    {{"UInt32", "Int32"}, "Int64"},
    {{"UInt32", "Int64"}, "Int64"},
    {{"UInt32", "Float32"}, "Float64"},
    {{"UInt32", "Float64"}, "Float64"},
    {{"UInt64", "UInt8"}, "UInt64"},
    {{"UInt64", "UInt16"}, "UInt64"},
    {{"UInt64", "UInt32"}, "UInt64"},
    {{"UInt64", "UInt64"}, "UInt64"},
    {{"UInt64", "Int8"}, "Error"},
    {{"UInt64", "Int16"}, "Error"},
    {{"UInt64", "Int32"}, "Error"},
    {{"UInt64", "Int64"}, "Error"},
    {{"UInt64", "Float32"}, "Error"},
    {{"UInt64", "Float64"}, "Error"},
    {{"Int8", "UInt8"}, "Int16"},
    {{"Int8", "UInt16"}, "Int32"},
    {{"Int8", "UInt32"}, "Int64"},
    {{"Int8", "UInt64"}, "Error"},
    {{"Int8", "Int8"}, "Int8"},
    {{"Int8", "Int16"}, "Int16"},
    {{"Int8", "Int32"}, "Int32"},
    {{"Int8", "Int64"}, "Int64"},
    {{"Int8", "Float32"}, "Float32"},
    {{"Int8", "Float64"}, "Float64"},
    {{"Int16", "UInt8"}, "Int16"},
    {{"Int16", "UInt16"}, "Int32"},
    {{"Int16", "UInt32"}, "Int64"},
    {{"Int16", "UInt64"}, "Error"},
    {{"Int16", "Int8"}, "Int16"},
    {{"Int16", "Int16"}, "Int16"},
    {{"Int16", "Int32"}, "Int32"},
    {{"Int16", "Int64"}, "Int64"},
    {{"Int16", "Float32"}, "Float32"},
    {{"Int16", "Float64"}, "Float64"},
    {{"Int32", "UInt8"}, "Int32"},
    {{"Int32", "UInt16"}, "Int32"},
    {{"Int32", "UInt32"}, "Int64"},
    {{"Int32", "UInt64"}, "Error"},
    {{"Int32", "Int8"}, "Int32"},
    {{"Int32", "Int16"}, "Int32"},
    {{"Int32", "Int32"}, "Int32"},
    {{"Int32", "Int64"}, "Int64"},
    {{"Int32", "Float32"}, "Float64"},
    {{"Int32", "Float64"}, "Float64"},
    {{"Int64", "UInt8"}, "Int64"},
    {{"Int64", "UInt16"}, "Int64"},
    {{"Int64", "UInt32"}, "Int64"},
    {{"Int64", "UInt64"}, "Error"},
    {{"Int64", "Int8"}, "Int64"},
    {{"Int64", "Int16"}, "Int64"},
    {{"Int64", "Int32"}, "Int64"},
    {{"Int64", "Int64"}, "Int64"},
    {{"Int64", "Float32"}, "Error"},
    {{"Int64", "Float64"}, "Error"},
    {{"Float32", "UInt8"}, "Float32"},
    {{"Float32", "UInt16"}, "Float32"},
    {{"Float32", "UInt32"}, "Float64"},
    {{"Float32", "UInt64"}, "Error"},
    {{"Float32", "Int8"}, "Float32"},
    {{"Float32", "Int16"}, "Float32"},
    {{"Float32", "Int32"}, "Float64"},
    {{"Float32", "Int64"}, "Error"},
    {{"Float32", "Float32"}, "Float32"},
    {{"Float32", "Float64"}, "Float64"},
    {{"Float64", "UInt8"}, "Float64"},
    {{"Float64", "UInt16"}, "Float64"},
    {{"Float64", "UInt32"}, "Float64"},
    {{"Float64", "UInt64"}, "Error"},
    {{"Float64", "Int8"}, "Float64"},
    {{"Float64", "Int16"}, "Float64"},
    {{"Float64", "Int32"}, "Float64"},
    {{"Float64", "Int64"}, "Error"},
    {{"Float64", "Float32"}, "Float64"},
    {{"Float64", "Float64"}, "Float64"}
};

static std::string getTypeString(DB::UInt8) { return "UInt8"; }
static std::string getTypeString(DB::UInt16) { return "UInt16"; }
static std::string getTypeString(DB::UInt32) { return "UInt32"; }
static std::string getTypeString(DB::UInt64) { return "UInt64"; }
static std::string getTypeString(DB::Int8) { return "Int8"; }
static std::string getTypeString(DB::Int16) { return "Int16"; }
static std::string getTypeString(DB::Int32) { return "Int32"; }
static std::string getTypeString(DB::Int64) { return "Int64"; }
static std::string getTypeString(DB::Float32) { return "Float32"; }
static std::string getTypeString(DB::Float64) { return "Float64"; }
static std::string getTypeString(DB::NumberTraits::Error) { return "Error"; }

template <typename T0, typename T1>
[[maybe_unused]] void printTypes()
{
    std::cout << "{{\"";
    std::cout << getTypeString(T0());
    std::cout << "\", \"";
    std::cout << getTypeString(T1());
    std::cout << "\"}, \"";
    std::cout << getTypeString(typename DB::NumberTraits::ResultOfIf<T0, T1>::Type());
    std::cout << "\"},"<< std::endl;
}

template <typename T0, typename T1>
void ifRightType()
{
    auto desired = getTypeString(typename DB::NumberTraits::ResultOfIf<T0, T1>::Type());
    auto left = getTypeString(T0());
    auto right = getTypeString(T1());
    auto expected = answer.find({left, right});
    ASSERT_TRUE(expected != answer.end());
    ASSERT_EQ(expected->second, desired);
}

template <typename T0>
void ifLeftType()
{
    ifRightType<T0, DB::UInt8>();
    ifRightType<T0, DB::UInt16>();
    ifRightType<T0, DB::UInt32>();
    ifRightType<T0, DB::UInt64>();
    ifRightType<T0, DB::Int8>();
    ifRightType<T0, DB::Int16>();
    ifRightType<T0, DB::Int32>();
    ifRightType<T0, DB::Int64>();
    ifRightType<T0, DB::Float32>();
    ifRightType<T0, DB::Float64>();
}


TEST(NumberTraits, ResultOfAdditionMultiplication)
{
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfAdditionMultiplication<DB::UInt8, DB::UInt8>::Type()), "UInt16");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfAdditionMultiplication<DB::UInt8, DB::Int32>::Type()), "Int64");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfAdditionMultiplication<DB::UInt8, DB::Float32>::Type()), "Float64");
}


TEST(NumberTraits, ResultOfSubtraction)
{
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfSubtraction<DB::UInt8, DB::UInt8>::Type()), "Int16");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfSubtraction<DB::UInt16, DB::UInt8>::Type()), "Int32");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfSubtraction<DB::UInt16, DB::Int8>::Type()), "Int32");
}


TEST(NumberTraits, Others)
{
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfFloatingPointDivision<DB::UInt16, DB::Int16>::Type()), "Float64");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfFloatingPointDivision<DB::UInt32, DB::Int16>::Type()), "Float64");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfIntegerDivision<DB::UInt8, DB::Int16>::Type()), "Int8");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfModulo<DB::UInt32, DB::Int8>::Type()), "Int8");
}


TEST(NumberTraits, FunctionIf)
{
    ifLeftType<DB::UInt8>();
    ifLeftType<DB::UInt16>();
    ifLeftType<DB::UInt32>();
    ifLeftType<DB::UInt64>();
    ifLeftType<DB::Int8>();
    ifLeftType<DB::Int16>();
    ifLeftType<DB::Int32>();
    ifLeftType<DB::Int64>();
    ifLeftType<DB::Float32>();
    ifLeftType<DB::Float64>();
}

