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
    {{"UInt8", "UInt256"}, "UInt256"},
    {{"UInt8", "Int8"}, "Int16"},
    {{"UInt8", "Int16"}, "Int16"},
    {{"UInt8", "Int32"}, "Int32"},
    {{"UInt8", "Int64"}, "Int64"},
    {{"UInt8", "Int128"}, "Int128"},
    {{"UInt8", "Int256"}, "Int256"},
    {{"UInt8", "Float32"}, "Float32"},
    {{"UInt8", "Float64"}, "Float64"},
    {{"UInt16", "UInt8"}, "UInt16"},
    {{"UInt16", "UInt16"}, "UInt16"},
    {{"UInt16", "UInt32"}, "UInt32"},
    {{"UInt16", "UInt64"}, "UInt64"},
    {{"UInt16", "UInt256"}, "UInt256"},
    {{"UInt16", "Int8"}, "Int32"},
    {{"UInt16", "Int16"}, "Int32"},
    {{"UInt16", "Int32"}, "Int32"},
    {{"UInt16", "Int64"}, "Int64"},
    {{"UInt16", "Int128"}, "Int128"},
    {{"UInt16", "Int256"}, "Int256"},
    {{"UInt16", "Float32"}, "Float32"},
    {{"UInt16", "Float64"}, "Float64"},
    {{"UInt32", "UInt8"}, "UInt32"},
    {{"UInt32", "UInt16"}, "UInt32"},
    {{"UInt32", "UInt32"}, "UInt32"},
    {{"UInt32", "UInt64"}, "UInt64"},
    {{"UInt32", "UInt256"}, "UInt256"},
    {{"UInt32", "Int8"}, "Int64"},
    {{"UInt32", "Int16"}, "Int64"},
    {{"UInt32", "Int32"}, "Int64"},
    {{"UInt32", "Int64"}, "Int64"},
    {{"UInt32", "Int128"}, "Int128"},
    {{"UInt32", "Int256"}, "Int256"},
    {{"UInt32", "Float32"}, "Float64"},
    {{"UInt32", "Float64"}, "Float64"},
    {{"UInt64", "UInt8"}, "UInt64"},
    {{"UInt64", "UInt16"}, "UInt64"},
    {{"UInt64", "UInt32"}, "UInt64"},
    {{"UInt64", "UInt64"}, "UInt64"},
    {{"UInt64", "UInt256"}, "UInt256"},
    {{"UInt64", "Int8"}, "Int128"},
    {{"UInt64", "Int16"}, "Int128"},
    {{"UInt64", "Int32"}, "Int128"},
    {{"UInt64", "Int64"}, "Int128"},
    {{"UInt64", "Int128"}, "Int128"},
    {{"UInt64", "Int256"}, "Int256"},
    {{"UInt64", "Float32"}, "Error"},
    {{"UInt64", "Float64"}, "Error"},
    {{"UInt256", "UInt8"}, "UInt256"},
    {{"UInt256", "UInt16"}, "UInt256"},
    {{"UInt256", "UInt32"}, "UInt256"},
    {{"UInt256", "UInt64"}, "UInt256"},
    {{"UInt256", "UInt256"}, "UInt256"},
    {{"UInt256", "Int8"}, "Error"},
    {{"UInt256", "Int16"}, "Error"},
    {{"UInt256", "Int32"}, "Error"},
    {{"UInt256", "Int64"}, "Error"},
    {{"UInt256", "Int128"}, "Error"},
    {{"UInt256", "Int256"}, "Error"},
    {{"UInt256", "Float32"}, "Error"},
    {{"UInt256", "Float64"}, "Error"},
    {{"Int8", "UInt8"}, "Int16"},
    {{"Int8", "UInt16"}, "Int32"},
    {{"Int8", "UInt32"}, "Int64"},
    {{"Int8", "UInt64"}, "Int128"},
    {{"Int8", "UInt256"}, "Error"},
    {{"Int8", "Int8"}, "Int8"},
    {{"Int8", "Int16"}, "Int16"},
    {{"Int8", "Int32"}, "Int32"},
    {{"Int8", "Int64"}, "Int64"},
    {{"Int8", "Int128"}, "Int128"},
    {{"Int8", "Int256"}, "Int256"},
    {{"Int8", "Float32"}, "Float32"},
    {{"Int8", "Float64"}, "Float64"},
    {{"Int16", "UInt8"}, "Int16"},
    {{"Int16", "UInt16"}, "Int32"},
    {{"Int16", "UInt32"}, "Int64"},
    {{"Int16", "UInt64"}, "Int128"},
    {{"Int16", "UInt256"}, "Error"},
    {{"Int16", "Int8"}, "Int16"},
    {{"Int16", "Int16"}, "Int16"},
    {{"Int16", "Int32"}, "Int32"},
    {{"Int16", "Int64"}, "Int64"},
    {{"Int16", "Int128"}, "Int128"},
    {{"Int16", "Int256"}, "Int256"},
    {{"Int16", "Float32"}, "Float32"},
    {{"Int16", "Float64"}, "Float64"},
    {{"Int32", "UInt8"}, "Int32"},
    {{"Int32", "UInt16"}, "Int32"},
    {{"Int32", "UInt32"}, "Int64"},
    {{"Int32", "UInt64"}, "Int128"},
    {{"Int32", "UInt256"}, "Error"},
    {{"Int32", "Int8"}, "Int32"},
    {{"Int32", "Int16"}, "Int32"},
    {{"Int32", "Int32"}, "Int32"},
    {{"Int32", "Int64"}, "Int64"},
    {{"Int32", "Int128"}, "Int128"},
    {{"Int32", "Int256"}, "Int256"},
    {{"Int32", "Float32"}, "Float64"},
    {{"Int32", "Float64"}, "Float64"},
    {{"Int64", "UInt8"}, "Int64"},
    {{"Int64", "UInt16"}, "Int64"},
    {{"Int64", "UInt32"}, "Int64"},
    {{"Int64", "UInt64"}, "Int128"},
    {{"Int64", "UInt256"}, "Error"},
    {{"Int64", "Int8"}, "Int64"},
    {{"Int64", "Int16"}, "Int64"},
    {{"Int64", "Int32"}, "Int64"},
    {{"Int64", "Int64"}, "Int64"},
    {{"Int64", "Int128"}, "Int128"},
    {{"Int64", "Int256"}, "Int256"},
    {{"Int64", "Float32"}, "Error"},
    {{"Int64", "Float64"}, "Error"},
    {{"Int128", "UInt8"}, "Int128"},
    {{"Int128", "UInt16"}, "Int128"},
    {{"Int128", "UInt32"}, "Int128"},
    {{"Int128", "UInt64"}, "Int128"},
    {{"Int128", "UInt256"}, "Error"},
    {{"Int128", "Int8"}, "Int128"},
    {{"Int128", "Int16"}, "Int128"},
    {{"Int128", "Int32"}, "Int128"},
    {{"Int128", "Int64"}, "Int128"},
    {{"Int128", "Int128"}, "Int128"},
    {{"Int128", "Int256"}, "Int256"},
    {{"Int128", "Float32"}, "Error"},
    {{"Int128", "Float64"}, "Error"},
    {{"Int256", "UInt8"}, "Int256"},
    {{"Int256", "UInt16"}, "Int256"},
    {{"Int256", "UInt32"}, "Int256"},
    {{"Int256", "UInt64"}, "Int256"},
    {{"Int256", "UInt256"}, "Error"},
    {{"Int256", "Int8"}, "Int256"},
    {{"Int256", "Int16"}, "Int256"},
    {{"Int256", "Int32"}, "Int256"},
    {{"Int256", "Int64"}, "Int256"},
    {{"Int256", "Int128"}, "Int256"},
    {{"Int256", "Int256"}, "Int256"},
    {{"Int256", "Float32"}, "Error"},
    {{"Int256", "Float64"}, "Error"},
    {{"Float32", "UInt8"}, "Float32"},
    {{"Float32", "UInt16"}, "Float32"},
    {{"Float32", "UInt32"}, "Float64"},
    {{"Float32", "UInt64"}, "Error"},
    {{"Float32", "UInt256"}, "Error"},
    {{"Float32", "Int8"}, "Float32"},
    {{"Float32", "Int16"}, "Float32"},
    {{"Float32", "Int32"}, "Float64"},
    {{"Float32", "Int64"}, "Error"},
    {{"Float32", "Int128"}, "Error"},
    {{"Float32", "Int256"}, "Error"},
    {{"Float32", "Float32"}, "Float32"},
    {{"Float32", "Float64"}, "Float64"},
    {{"Float64", "UInt8"}, "Float64"},
    {{"Float64", "UInt16"}, "Float64"},
    {{"Float64", "UInt32"}, "Float64"},
    {{"Float64", "UInt64"}, "Error"},
    {{"Float64", "UInt256"}, "Error"},
    {{"Float64", "Int8"}, "Float64"},
    {{"Float64", "Int16"}, "Float64"},
    {{"Float64", "Int32"}, "Float64"},
    {{"Float64", "Int64"}, "Error"},
    {{"Float64", "Int128"}, "Error"},
    {{"Float64", "Int256"}, "Error"},
    {{"Float64", "Float32"}, "Float64"},
    {{"Float64", "Float64"}, "Float64"}
};

static std::string getTypeString(UInt8) { return "UInt8"; }
static std::string getTypeString(UInt16) { return "UInt16"; }
static std::string getTypeString(UInt32) { return "UInt32"; }
static std::string getTypeString(UInt64) { return "UInt64"; }
static std::string getTypeString(UInt256) { return "UInt256"; }
static std::string getTypeString(Int8) { return "Int8"; }
static std::string getTypeString(Int16) { return "Int16"; }
static std::string getTypeString(Int32) { return "Int32"; }
static std::string getTypeString(Int64) { return "Int64"; }
static std::string getTypeString(Int128) { return "Int128"; }
static std::string getTypeString(Int256) { return "Int256"; }
static std::string getTypeString(Float32) { return "Float32"; }
static std::string getTypeString(Float64) { return "Float64"; }
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
    ifRightType<T0, UInt8>();
    ifRightType<T0, UInt16>();
    ifRightType<T0, UInt32>();
    ifRightType<T0, UInt64>();
    ifRightType<T0, UInt256>();
    ifRightType<T0, Int8>();
    ifRightType<T0, Int16>();
    ifRightType<T0, Int32>();
    ifRightType<T0, Int64>();
    ifRightType<T0, Int128>();
    ifRightType<T0, Int256>();
    ifRightType<T0, Float32>();
    ifRightType<T0, Float64>();
}


TEST(NumberTraits, ResultOfAdditionMultiplication)
{
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfAdditionMultiplication<UInt8, UInt8>::Type()), "UInt16");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfAdditionMultiplication<UInt8, Int32>::Type()), "Int64");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfAdditionMultiplication<UInt8, Float32>::Type()), "Float64");
}


TEST(NumberTraits, ResultOfSubtraction)
{
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfSubtraction<UInt8, UInt8>::Type()), "Int16");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfSubtraction<UInt16, UInt8>::Type()), "Int32");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfSubtraction<UInt16, Int8>::Type()), "Int32");
}


TEST(NumberTraits, Others)
{
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfFloatingPointDivision<UInt16, Int16>::Type()), "Float64");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfFloatingPointDivision<UInt32, Int16>::Type()), "Float64");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfIntegerDivision<UInt8, Int16>::Type()), "Int8");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfModulo<UInt32, Int8>::Type()), "UInt8");
}


TEST(NumberTraits, FunctionIf)
{
    ifLeftType<UInt8>();
    ifLeftType<UInt16>();
    ifLeftType<UInt32>();
    ifLeftType<UInt64>();
    ifLeftType<UInt256>();
    ifLeftType<Int8>();
    ifLeftType<Int16>();
    ifLeftType<Int32>();
    ifLeftType<Int64>();
    ifLeftType<Int128>();
    ifLeftType<Int256>();
    ifLeftType<Float32>();
    ifLeftType<Float64>();
}
