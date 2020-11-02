#include <Columns/ColumnMap.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>

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
using namespace DB;

namespace
{
template <typename ColumnType, typename T, typename ...Args>
typename ColumnType::MutablePtr createWithValues(const T & arg1, const Args & ... rest)
{
    typename ColumnType::MutablePtr res = ColumnType::create();
    res->reserve(sizeof...(rest) + 1);

    for (const auto & v : {arg1, rest...})
        res->insert(Field(v));

    return res;
}

}

GTEST_TEST(ColumnMap, Index)
{
    const ColumnUInt64::Ptr offsets = createWithValues<ColumnUInt64>(1, 4, 8, 16, 19);
    auto keys = ColumnArray::create(createWithValues<ColumnString>("a", "b", "c", "d", "e", "f", "g", "h", "a", "b",  "c",  "d",  "e",  "f",  "g",  "h",  "a",  "a",  "a"), offsets);
    auto vals = ColumnArray::create(createWithValues<ColumnString>("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19"), offsets);
    std::cerr << "!!! " << *offsets << "\t" << *keys << "\t" << *vals << std::endl;

    const auto map = ColumnMap::create(Columns{std::move(keys), std::move(vals)});

    const auto needles = createWithValues<ColumnString>("a", "b", "c", "d", "e");
    const auto results = map->findAll(*needles, needles->size());

    std::cerr << "!!! " << *results << std::endl;

    ASSERT_EQ(offsets->size(), results->size());
}
