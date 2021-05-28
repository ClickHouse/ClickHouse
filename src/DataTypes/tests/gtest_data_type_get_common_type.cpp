#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/getMostSubtype.h>

#include <sstream>
#pragma GCC diagnostic ignored "-Wmissing-declarations"
#include <gtest/gtest.h>

namespace DB
{

static bool operator==(const IDataType & left, const IDataType & right)
{
    return left.equals(right);
}

}

using namespace DB;

static auto typeFromString(const std::string & str)
{
    auto & data_type_factory = DataTypeFactory::instance();
    return data_type_factory.get(str);
};

static auto typesFromString(const std::string & str)
{
    std::istringstream data_types_stream(str);
    DataTypes data_types;
    std::string data_type;
    while (data_types_stream >> data_type)
        data_types.push_back(typeFromString(data_type));

    return data_types;
};

struct TypesTestCase
{
    const char * from_types;
    const char * expected_type = nullptr;
};

std::ostream & operator<<(std::ostream & ostr, const TypesTestCase & test_case)
{
    ostr << "TypesTestCase{\"" << test_case.from_types << "\", ";
    if (test_case.expected_type)
        ostr << "\"" << test_case.expected_type << "\"";
    else
        ostr << "nullptr";

    return ostr << "}";
}

class TypeTest : public ::testing::TestWithParam<TypesTestCase>
{
public:
    void SetUp() override
    {
        const auto & p = GetParam();
        from_types = typesFromString(p.from_types);

        if (p.expected_type)
            expected_type = typeFromString(p.expected_type);
        else
            expected_type.reset();
    }

    DataTypes from_types;
    DataTypePtr expected_type;
};

class LeastSuperTypeTest : public TypeTest {};

TEST_P(LeastSuperTypeTest, getLeastSupertype)
{
    if (this->expected_type)
    {
        ASSERT_EQ(*this->expected_type, *getLeastSupertype(this->from_types));
    }
    else
    {
        EXPECT_ANY_THROW(getLeastSupertype(this->from_types));
    }
}

class MostSubtypeTest : public TypeTest {};

TEST_P(MostSubtypeTest, getMostSubtype)
{
    if (this->expected_type)
    {
        ASSERT_EQ(*this->expected_type, *getMostSubtype(this->from_types));
    }
    else
    {
        EXPECT_ANY_THROW(getMostSubtype(this->from_types, true));
    }
}

INSTANTIATE_TEST_SUITE_P(data_type,
    LeastSuperTypeTest,
    ::testing::ValuesIn(
        std::initializer_list<TypesTestCase>{
            {"", "Nothing"},
            {"Nothing", "Nothing"},

            {"UInt8", "UInt8"},
            {"UInt8 UInt8", "UInt8"},
            {"Int8 Int8", "Int8"},
            {"UInt8 Int8", "Int16"},
            {"UInt8 Int16", "Int16"},
            {"UInt8 UInt32 UInt64", "UInt64"},
            {"Int8 Int32 Int64", "Int64"},
            {"UInt8 UInt32 Int64", "Int64"},

            {"Float32 Float64", "Float64"},
            {"Float32 UInt16 Int16", "Float32"},
            {"Float32 UInt16 Int32", "Float64"},
            {"Float32 Int16 UInt32", "Float64"},

            {"Date Date", "Date"},
            {"Date DateTime", "DateTime"},
            {"Date DateTime64(3)", "DateTime64(3)"},
            {"DateTime DateTime64(3)", "DateTime64(3)"},
            {"DateTime DateTime64(0)", "DateTime64(0)"},
            {"DateTime64(9) DateTime64(3)", "DateTime64(9)"},

            {"String FixedString(32) FixedString(8)", "String"},

            {"Array(UInt8) Array(UInt8)", "Array(UInt8)"},
            {"Array(UInt8) Array(Int8)", "Array(Int16)"},
            {"Array(Float32) Array(Int16) Array(UInt32)", "Array(Float64)"},
            {"Array(Array(UInt8)) Array(Array(UInt8))", "Array(Array(UInt8))"},
            {"Array(Array(UInt8)) Array(Array(Int8))", "Array(Array(Int16))"},
            {"Array(Date) Array(DateTime)", "Array(DateTime)"},
            {"Array(String) Array(FixedString(32))", "Array(String)"},

            {"Nullable(Nothing) Nothing", "Nullable(Nothing)"},
            {"Nullable(UInt8) Int8", "Nullable(Int16)"},
            {"Nullable(Nothing) UInt8 Int8", "Nullable(Int16)"},

            {"Tuple(Int8,UInt8) Tuple(UInt8,Int8)", "Tuple(Int16,Int16)"},
            {"Tuple(Nullable(Nothing)) Tuple(Nullable(UInt8))", "Tuple(Nullable(UInt8))"},

            {"Int8 String", nullptr},
            {"Int64 UInt64", nullptr},
            {"Float32 UInt64", nullptr},
            {"Float64 Int64", nullptr},
            {"Tuple(Int64) Tuple(UInt64)", nullptr},
            {"Tuple(Int64,Int8) Tuple(UInt64)", nullptr},
            {"Array(Int64) Array(String)", nullptr},
        }
    )
);

INSTANTIATE_TEST_SUITE_P(data_type,
    MostSubtypeTest,
    ::testing::ValuesIn(
        std::initializer_list<TypesTestCase>{
            {"", "Nothing"},
            {"Nothing", "Nothing"},

            {"UInt8", "UInt8"},
            {"UInt8 UInt8", "UInt8"},
            {"Int8 Int8", "Int8"},
            {"UInt8 Int8", "UInt8"},
            {"Int8 UInt16", "Int8"},
            {"UInt8 UInt32 UInt64", "UInt8"},
            {"Int8 Int32 Int64", "Int8"},
            {"UInt8 Int64 UInt64", "UInt8"},

            {"Float32 Float64", "Float32"},
            {"Float32 UInt16 Int16", "UInt16"},
            {"Float32 UInt16 Int32", "UInt16"},
            {"Float32 Int16 UInt32", "Int16"},

            {"DateTime DateTime", "DateTime"},
            {"Date DateTime", "Date"},

            {"String FixedString(8)", "FixedString(8)"},
            {"FixedString(16) FixedString(8)", "Nothing"},

            {"Array(UInt8) Array(UInt8)", "Array(UInt8)"},
            {"Array(UInt8) Array(Int8)", "Array(UInt8)"},
            {"Array(Float32) Array(Int16) Array(UInt32)", "Array(Int16)"},
            {"Array(Array(UInt8)) Array(Array(UInt8))", "Array(Array(UInt8))"},
            {"Array(Array(UInt8)) Array(Array(Int8))", "Array(Array(UInt8))"},
            {"Array(Date) Array(DateTime)", "Array(Date)"},
            {"Array(String) Array(FixedString(32))", "Array(FixedString(32))"},
            {"Array(String) Array(FixedString(32))", "Array(FixedString(32))"},

            {"Nullable(Nothing) Nothing", "Nothing"},
            {"Nullable(UInt8) Int8", "UInt8"},
            {"Nullable(Nothing) UInt8 Int8", "Nothing"},
            {"Nullable(UInt8) Nullable(Int8)", "Nullable(UInt8)"},
            {"Nullable(Nothing) Nullable(Int8)", "Nullable(Nothing)"},

            {"Tuple(Int8,UInt8) Tuple(UInt8,Int8)", "Tuple(UInt8,UInt8)"},
            {"Tuple(Nullable(Nothing)) Tuple(Nullable(UInt8))", "Tuple(Nullable(Nothing))"},

            {"Int8 String", nullptr},
            {"Nothing", nullptr},
            {"FixedString(16) FixedString(8) String", nullptr},
        }
    )
);
