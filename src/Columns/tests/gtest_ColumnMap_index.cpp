#include <Columns/ColumnMap.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>

#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>

#include <IO/WriteBufferFromOStream.h>
#include <Formats/FormatSettings.h>
#include <Core/iostream_debug_helpers.h>

#include <Common/HashTable/HashMap.h>
#include <Common/UInt128.h>

#include <gtest/gtest.h>

#include <unordered_map>
#include <vector>
#include <fmt/format.h>
#include <variant>
using namespace DB;

namespace
{

template <typename ColumnType, typename T>
typename ColumnType::MutablePtr createWithList(const std::initializer_list<T> & vals)
{
    typename ColumnType::MutablePtr res = ColumnType::create();
    res->reserve(vals.size());

    for (const auto & v : vals)
        res->insert(Field(v));

    return res;
}

// createWithValues<ColumnUInt8>(1, 2, 3) => ColumnUInt8(1, 2, 3)
template <typename ColumnType, typename T, typename ...Args>
typename ColumnType::MutablePtr createWithValues(const T & arg1, const Args & ... rest)
{
    return createWithList<ColumnType>({arg1, rest...});
}

template <typename NestedColumnType, typename T>
typename ColumnArray::MutablePtr createArrayWithLists(const std::initializer_list<std::initializer_list<T>> & vals)
{
    ColumnUInt64::MutablePtr offsets = ColumnUInt64::create();

    typename NestedColumnType::MutablePtr data_column = NestedColumnType::create();
    offsets->reserve(vals.size());
    size_t row_offset = 0;
    for (const auto & row : vals)
    {
        for (const auto & v : row)
        {
            data_column->insert(DB::Field(v));
            ++row_offset;
        }

        offsets->insertValue(row_offset);
    }

    return ColumnArray::create(std::move(data_column), std::move(offsets));
}

// createArrayWithValues<ColumnUInt8>({1, 2, 3}, {4, 5, 6}) => ColumnUInt8([1, 2, 3], [4, 5, 6])
template <typename NestedColumnType, typename ...Args>
typename ColumnArray::MutablePtr createArrayWithValues(const std::initializer_list<Args> & ... args)
{
    return createArrayWithLists<NestedColumnType>({args...});
}

template <typename KeyType, typename ValueType>
using ColumnMapLiteral = std::initializer_list<std::initializer_list<std::pair<KeyType, ValueType>>>;

// Example:
// {{{"a", 1}, {"b", 2}, {"c", 3}}, {{"d", 4}, {"e", 5}}} => map(["a"=>1, "b"=>2, "c"=>3], ["d"=>4, "e"=>5])
template <typename KeyColumnType, typename ValueColumnType, typename KeyType = typename KeyColumnType::ValueType, typename ValueType = typename ValueColumnType::ValueType>
typename ColumnMap::Ptr createMap(const std::initializer_list<std::initializer_list<std::pair<KeyType, ValueType>>> & data)
{
    ColumnUInt64::MutablePtr offsets = ColumnUInt64::create();
    offsets->reserve(data.size());

    typename KeyColumnType::MutablePtr keys_column = KeyColumnType::create();
    typename ValueColumnType::MutablePtr vals_column = ValueColumnType::create();

    size_t row_offset = 0;
    for (const auto & row : data)
    {
        for (const auto & kv : row)
        {
            keys_column->insert(DB::Field(kv.first));
            vals_column->insert(DB::Field(kv.second));
            ++row_offset;
        }

        offsets->insertValue(row_offset);
    }

    return ColumnMap::create(std::move(keys_column), std::move(vals_column), std::move(offsets));
}

}

//template <> struct fmt::formatter<DB::Field> : formatter<string_view> {
//  template <typename FormatContext>
//  auto format(const DB::Field & f, FormatContext & ctx) {
//    return formatter<string_view>::format(toString(f), ctx);
//  }
//};

struct ColumnMapFindAllTestCase
{
    ColumnMap::Ptr map;
    IColumn::Ptr needles;
    IColumn::Ptr result;
};

template <typename KeyColumnType, typename ValColumnType, typename KeyType, typename ValueType>
ColumnMapFindAllTestCase makeColumnMapFindAllTestCase(
        std::initializer_list<std::initializer_list<std::pair<KeyType, ValueType>>> && map_literal,
        std::initializer_list<KeyType> needles,
        std::initializer_list<ValueType> result)
{
    using MapLiteralType = std::decay_t<decltype(map_literal)>;
    return {
        createMap<KeyColumnType, ValColumnType, KeyType, ValueType>(std::forward<MapLiteralType>(map_literal)),
        createWithList<KeyColumnType>(needles),
        createWithList<ValColumnType>(result)
    };
}

ColumnMapFindAllTestCase makeColumnMapFindAllTestCase(
        ColumnMap::Ptr map,
        IColumn::Ptr needles,
        IColumn::Ptr result)
{
    return {
        map,
        needles,
        result
    };
}


enum WithException {NO_EXCEPTION, WITH_EXCEPTION};
template <WithException branch = NO_EXCEPTION>
void DoTestFindAll(const char * test_case_name, const ColumnMapFindAllTestCase & test_case)
{
    SCOPED_TRACE(test_case_name);
    const auto & [map, needles, expected_result] = test_case;

    if constexpr (branch == WITH_EXCEPTION)
    {
        (void)expected_result;

        IColumn::Ptr found;
        EXPECT_THROW(found = map->findAll(*needles, needles->size()), Exception);
        EXPECT_EQ(nullptr, found);
    }
    else
    {
        ASSERT_NE(nullptr, expected_result);

        IColumn::Ptr found;
        EXPECT_NO_THROW(found = map->findAll(*needles, needles->size()));
        ASSERT_NE(nullptr, found);

        EXPECT_TRUE(expected_result->structureEquals(*found));
        EXPECT_EQ(expected_result->size(), found->size());
        for (size_t row = 0; row < found->size(); ++row)
        {
            EXPECT_EQ((*expected_result)[row], (*found)[row])
                    << " Row :" << row;
        }
    }
}

// TODO: test against ColumnConst
TEST(ColumnMapFindAllTest, FindAll_String)
{
//    DoTestFindAll("finding value in empty map produces empty result",
//        makeColumnMapFindAllTestCase(
//        ColumnMap::create(Columns{createArrayWithValues<ColumnString>(), createArrayWithValues<ColumnString>()}),
//        createWithValues<ColumnString>("a"),
//        nullptr
//    ));

    DoTestFindAll("finding value corresponding to the first key",
        makeColumnMapFindAllTestCase<ColumnString, ColumnString>(
            {{{"a", "1"}, {"b", "2"}}},
            {"a"},
            {"1"}
    ));

    DoTestFindAll("finding value corresponding to the second key",
        makeColumnMapFindAllTestCase<ColumnString, ColumnString>(
            {{{"a", "1"}, {"b", "2"}}},
            {"b"},
            {"2"}
    ));

    DoTestFindAll("default value when key is not found",
        makeColumnMapFindAllTestCase<ColumnString, ColumnString>(
            {{{"a", "1"}, {"b", "2"}}},
            {"c"},
            {""}
    ));

    DoTestFindAll("findAll produces value corresponding to the first key when there are multiple identical keys",
        makeColumnMapFindAllTestCase<ColumnString, ColumnString>(
            {{{"a", "1"}, {"a", "2"}, {"a", "3"}, {"a", "4"}}},
            {"a"},
            {"1"}
    ));

    DoTestFindAll("map with multiple rows of different sizes",
        makeColumnMapFindAllTestCase<ColumnString, ColumnString>(
            {
                {{"a", "1"}},
                {{"b", "2"}, {"c", "3"}, {"d", "4"}},
                {{"e", "5"}, {"f", "6"}, {"g", "7"}, {"h", "8"}},
                {{"a", "9"}, {"b", "10"}, {"c", "11"}, {"d", "12"}, {"e", "13"}, {"f", "14"}, {"g", "15"}, {"h", "16"}}
            },
            {"a", "b", "g", "d"},
            {"1", "2", "7", "12"}
    ));

    DoTestFindAll("map with longer keys",
        makeColumnMapFindAllTestCase<ColumnString, ColumnString>(
        {
            {{"alpfa", "one"}, {"bravo", "two"}, {"chralie", "three"}},
            {{"delta", "four"}, {"echo", "five"}, {"foxtrot", "six"}, {"golf", "seven"}, {"hotel", "eight"}}
        },
        {"bravo", "foxtrot"},
        {"two",   "six"}
    ));


//    DoTestFindAll<WITH_EXCEPTION>("Keyed by String, lookup with UInt8 causes an error",
//        makeColumnMapFindAllTestCase<ColumnString, ColumnString>(
//            {{{"a", "1"}, {"b", "2"}}},
//            createWithValues<ColumnUInt8>(1, 2),
//            nullptr
//    ));
}


TEST(ColumnMapFindAllTest, FindAll_UInt8)
{
    const auto map = createMap<ColumnUInt8, ColumnString, int, const char *>(
            {
                {{1, "a"}, {2, "b"}, {3, "c"}},
                {{4, "d"}, {5, "e"}, {6, "f"}},
            });

    DoTestFindAll("Map with UInt8 keys, looked up by UInt8 values",
        makeColumnMapFindAllTestCase(
            map,
            createWithValues<ColumnUInt8>(1, 5),
            createWithValues<ColumnString>("a", "e")
    ));

    DoTestFindAll("Map with UInt8 keys, looked up by UInt16 values",
        makeColumnMapFindAllTestCase(
            map,
            createWithValues<ColumnUInt16>(1, 5),
            createWithValues<ColumnString>("a", "e")
    ));

    DoTestFindAll("Map with UInt8 keys, looked up by Int32 values",
        makeColumnMapFindAllTestCase(
            map,
            createWithValues<ColumnInt32>(1, 5),
            createWithValues<ColumnString>("a", "e")
    ));

    DoTestFindAll("Map with UInt8 keys, looked up by non existing Int32 values, should return defaults",
        makeColumnMapFindAllTestCase(
            map,
            createWithValues<ColumnUInt8>(10, 500),
            createWithValues<ColumnString>(std::string_view{}, std::string_view{})
    ));

    DoTestFindAll<WITH_EXCEPTION>("Map with UInt8 keys, look up by String causes error",
        makeColumnMapFindAllTestCase(
            map,
            createWithValues<ColumnString>("a", "e"),
            nullptr
    ));
}

//INSTANTIATE_TEST_SUITE_P(SimpleMatch,
//    ColumnMapFindAllTest,
//    ::testing::Values(
//        TEST_CASE(ColumnString, ColumnString,
//                {{"a"}, {"b", "c", "d"}, {"e", "f", "g", "h"}, {"a", "b",  "c",  "d",  "e",  "f",  "g",   "h"}, {"a",  "a",  "a"}},
//                {{"1"}, {"2", "3", "4"}, {"5", "6", "7", "8"}, {"9", "10", "11", "12", "13", "14", "15", "16"}, {"17", "18", "19"}},
//                {"a", "b", "c", "d", "e"},
//                {"1", "2", "", "12", ""})
//    )
//);
