#include <Columns/ColumnUnique.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>

#pragma GCC diagnostic ignored "-Wsign-compare"
#ifdef __clang__
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#endif

#include <gtest/gtest.h>

#include <unordered_map>
#include <vector>
using namespace DB;

TEST(column_unique, column_unique_unique_insert_range_Test)
{
    std::unordered_map<String, size_t> ref_map;
    auto data_type = std::make_shared<DataTypeString>();
    auto column_unique = ColumnUnique<ColumnString>::create(*data_type);
    auto column_string = ColumnString::create();

    size_t num_values = 1000000;
    size_t mod_to = 1000;

    std::vector<size_t> indexes(num_values);
    for (size_t i = 0; i < num_values; ++i)
    {
        String str = toString(i % mod_to);
        column_string->insertData(str.data(), str.size());

        if (ref_map.count(str) == 0)
            ref_map[str] = ref_map.size();

        indexes[i]= ref_map[str];
    }

    auto idx = column_unique->uniqueInsertRangeFrom(*column_string, 0, num_values);
    ASSERT_EQ(idx->size(), num_values);

    for (size_t i = 0; i < num_values; ++i)
    {
        ASSERT_EQ(indexes[i] + 1, idx->getUInt(i)) << "Different indexes at position " << i;
    }

    auto & nested = column_unique->getNestedColumn();
    ASSERT_EQ(nested->size(), mod_to + 1);

    for (size_t i = 0; i < mod_to; ++i)
    {
        ASSERT_EQ(std::to_string(i), nested->getDataAt(i + 1).toString());
    }
}

TEST(column_unique, column_unique_unique_insert_range_with_overflow_Test)
{
    std::unordered_map<String, size_t> ref_map;
    auto data_type = std::make_shared<DataTypeString>();
    auto column_unique = ColumnUnique<ColumnString>::create(*data_type);
    auto column_string = ColumnString::create();

    size_t num_values = 1000000;
    size_t mod_to = 1000;

    std::vector<size_t> indexes(num_values);
    for (size_t i = 0; i < num_values; ++i)
    {
        String str = toString(i % mod_to);
        column_string->insertData(str.data(), str.size());

        if (ref_map.count(str) == 0)
            ref_map[str] = ref_map.size();

        indexes[i]= ref_map[str];
    }

    size_t max_val = mod_to / 2;
    size_t max_dict_size = max_val + 1;
    auto idx_with_overflow = column_unique->uniqueInsertRangeWithOverflow(*column_string, 0, num_values, max_dict_size);
    auto & idx = idx_with_overflow.indexes;
    auto & add_keys = idx_with_overflow.overflowed_keys;

    ASSERT_EQ(idx->size(), num_values);

    for (size_t i = 0; i < num_values; ++i)
    {
        ASSERT_EQ(indexes[i] + 1, idx->getUInt(i)) << "Different indexes at position " << i;
    }

    auto & nested = column_unique->getNestedColumn();
    ASSERT_EQ(nested->size(), max_dict_size);
    ASSERT_EQ(add_keys->size(), mod_to - max_val);

    for (size_t i = 0; i < max_val; ++i)
    {
        ASSERT_EQ(std::to_string(i), nested->getDataAt(i + 1).toString());
    }

    for (size_t i = 0; i < mod_to - max_val; ++i)
    {
        ASSERT_EQ(std::to_string(max_val + i), add_keys->getDataAt(i).toString());
    }
}
