#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>

#include <gtest/gtest.h>

#include <string>
#include <vector>


template <typename T>
inline std::ostream& operator<<(std::ostream & ostr, const std::vector<T> & v)
{
    ostr << "[";
    for (const auto & i : v)
    {
        ostr << i << ", ";
    }
    return ostr << "] (" << v.size() << ") items";
}

using namespace DB;

struct ParseDataTypeTestCase
{
    const char * type_name;
    std::vector<String> values;
    FieldVector expected_values;
};

std::ostream & operator<<(std::ostream & ostr, const ParseDataTypeTestCase & test_case)
{
    return ostr << "ParseDataTypeTestCase{\"" << test_case.type_name << "\", " << test_case.values << "}";
}


class ParseDataTypeTest : public ::testing::TestWithParam<ParseDataTypeTestCase>
{
public:
    void SetUp() override
    {
        const auto & p = GetParam();

        data_type = DataTypeFactory::instance().get(p.type_name);
    }

    DataTypePtr data_type;
};

TEST_P(ParseDataTypeTest, parseStringValue)
{
    const auto & p = GetParam();

    auto col = data_type->createColumn();
    for (const auto & value : p.values)
    {
        ReadBuffer buffer(const_cast<char *>(value.data()), value.size(), 0);
        data_type->getDefaultSerialization()->deserializeWholeText(*col, buffer, FormatSettings{});
    }

    ASSERT_EQ(p.expected_values.size(), col->size());
    for (size_t i = 0; i < col->size(); ++i)
    {
        ASSERT_EQ(p.expected_values[i], (*col)[i]);
    }
}


INSTANTIATE_TEST_SUITE_P(ParseDecimal,
    ParseDataTypeTest,
    ::testing::ValuesIn(
        std::initializer_list<ParseDataTypeTestCase>{
            {
                "Decimal(8, 0)",
                {"0", "5", "8", "-5", "-8", "12345678", "-12345678"},

                std::initializer_list<Field>{
                    DecimalField<Decimal32>(0, 0),
                    DecimalField<Decimal32>(5, 0),
                    DecimalField<Decimal32>(8, 0),
                    DecimalField<Decimal32>(-5, 0),
                    DecimalField<Decimal32>(-8, 0),
                    DecimalField<Decimal32>(12345678, 0),
                    DecimalField<Decimal32>(-12345678, 0)
                }
            }
        }
    )
);
