#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/getMostSubtype.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/HiveTextRowInputFormat.h>
#include <IO/ReadBuffer.h>

#pragma GCC diagnostic ignored "-Wmissing-declarations"
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include <Core/iostream_debug_helpers.h>


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

struct ParseDataTypeFromHiveTestCase
{
    const char * type_name;
    std::vector<String> values;
    FieldVector expected_values;
};

std::ostream & operator<<(std::ostream & ostr, const ParseDataTypeFromHiveTestCase & test_case)
{
    return ostr << "ParseDataTypeFromHiveTestCase{\"" << test_case.type_name << "\", " << test_case.values << "}";
}


class ParseDataTypeFromHiveTest : public ::testing::TestWithParam<ParseDataTypeFromHiveTestCase>
{
public:
    void SetUp() override
    {
        const auto & p = GetParam();

        data_type = DataTypeFactory::instance().get(p.type_name);
    }

    DataTypePtr data_type;
};

TEST_P(ParseDataTypeFromHiveTest, parseStringValue)
{
    const auto & p = GetParam();

    auto col = data_type->createColumn();
    for (const auto & value : p.values)
    {
        ReadBuffer buffer(const_cast<char *>(value.data()), value.size(), 0);
        data_type->getDefaultSerialization()->deserializeTextHiveText(*col, buffer, HiveTextRowInputFormat::updateFormatSettings({}));
    }

    ASSERT_EQ(p.expected_values.size(), col->size()) << "Actual items: " << *col;
    for (size_t i = 0; i < col->size(); ++i)
    {
        // std::cout << "real value:" << toString((*col)[i]);
        ASSERT_EQ(p.expected_values[i], (*col)[i]);
    }
}

INSTANTIATE_TEST_SUITE_P(ParseMapFromHive,
    ParseDataTypeFromHiveTest,
    ::testing::ValuesIn(
        std::initializer_list<ParseDataTypeFromHiveTestCase>{
            {
                "Map(String, String)",
                {"", "a\003b", "a\003b\002c\003d"},
                std::initializer_list<Field>{
                    Map{},
                    Map{Tuple{"a", "b"}},
                    Map{Tuple{"a", "b"}, Tuple{"c", "d"}}
                }
            }
        }
    )
);

INSTANTIATE_TEST_SUITE_P(ParseArrayFromHive,
    ParseDataTypeFromHiveTest,
    ::testing::ValuesIn(
        std::initializer_list<ParseDataTypeFromHiveTestCase>{
            {
                "Array(String)",
                {"", "\002", "a\002b", "a\002b\002c\002d"},
                std::initializer_list<Field>{
                    Array{""},
                    Array{"", ""},
                    Array{"a", "b"},
                    Array{"a", "b", "c", "d"}
                }
            }
        }
    )
);

INSTANTIATE_TEST_SUITE_P(ParseNamedTupleFromHive,
    ParseDataTypeFromHiveTest,
    ::testing::ValuesIn(
        std::initializer_list<ParseDataTypeFromHiveTestCase>{
            {
                "Tuple(a String, b Int64)",
                {"a\003aa\002b\003100", "a\003aa\002b\003bbb", "a\003aa\002b\003100\002"},
                std::initializer_list<Field>{
                    Tuple{"aa", 100},
                    Tuple{"aa", 0},
                    Tuple{"aa", 100},
                }
            }
        }
    )
);

INSTANTIATE_TEST_SUITE_P(ParseTupleFromHive,
    ParseDataTypeFromHiveTest,
    ::testing::ValuesIn(
        std::initializer_list<ParseDataTypeFromHiveTestCase>{
            {
                "Tuple(String, Int64)",
                {"a\002100", "a\002bbb", "a\002100\0021000"},
                std::initializer_list<Field>{
                    Tuple{"a", 100},
                    Tuple{"a", 0},
                    Tuple{"a", 100},
                }
            }
        }
    )
);
