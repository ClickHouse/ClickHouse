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
//    const Field default_value{"NOT-FOUND"};
    const auto results = map->findAll(*needles, needles->size());

    std::cerr << "!!! " << *results << std::endl;


//    {
//        WriteBufferFromOStream writer(std::cerr);
//        const auto datatype = DataTypeMap(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()});
//        std::cerr << "!! created " << datatype.getName() << std::endl;

//        for (size_t i = 0; i < map->size(); ++i)
//        {
//            datatype.serializeAsText(*map, i, writer, FormatSettings{});
//            writer.write(", ", 2);
//        }
//        writer.write("\n", 2);
//    }
//    std::cerr << std::endl;


//    const auto & map_keys = typeid_cast<const ColumnArray &>(*map->getColumns()[0]);
//    const auto & map_values = typeid_cast<const ColumnArray &>(*map->getColumns()[1]);
//    const auto & map_keys_offsets = map_keys.getOffsets();
//    const auto & map_keys_data = map_keys.getData();

//    DUMP(map_keys_offsets, map_keys_data);

//    // Building and using an Index atop of ColumnMap key column.
//    using MapIndex = HashMap2<MapKey, UInt64, MapKeyHash>;
//    MapIndex index;
//    index.reserve(map_keys_data.size());

//    // Build an index, store global key offset as mapped values since that greatly simplifies value extraction.
//    size_t starting_offset = 0;
//    for (size_t row = 0; row < map->size(); ++row)
//    {
//        const size_t final_offset = map_keys_offsets[row];
//        for (size_t i = starting_offset; i < final_offset; ++i)
//        {
//            MapIndex::LookupResult it;
//            bool inserted;
//            const auto key_data = map_keys_data.getDataAt(i);
//            UInt64 value_data = i - starting_offset;
//            std::cerr << row << " " << i << " " << std::string_view(key_data) << " => " << value_data << std::ends;

//            index.emplace(MapKey{row, key_data}, it, inserted);
//            if (inserted)
//                new (&it->getMapped()) UInt64(i);

//            std::cerr << "\tINSERTED: " << inserted << std::endl;
//        }
//        starting_offset = final_offset;
//    }

//    // Test LOOKUP of value by key.
//    const auto & data_items = typeid_cast<const ColumnString &>(map_values.getData());
//    auto tryFind = [&index, &data_items](size_t row, const char * val)
//    {
//        std::cerr << "Finding @row:" << row << " key: \"" << val << "\"" << std::ends;
//        const auto k = MapKey{row, StringRef{val}};
//        auto res = index.find(k);
//        if (res)
//        {
//            const auto value_index = res->getMapped();
//            const auto value = std::string_view(data_items.getDataAt(value_index));
//            std::cerr << "\tfound at pos: " << value_index << " => \"" << value << "\"" << std::endl;
//        }
//        else
//            std::cerr << "\tNOT found!" << std::endl;
//    };

//    for (UInt64 row = 0; row < map->size() + 1; ++row)
//        for (const auto & key : {"a", "c", "h", "z", ""})
//            tryFind(row, key);
}
