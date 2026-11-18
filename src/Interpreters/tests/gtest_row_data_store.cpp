#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/RowDataStore.h>
#include <Common/PODArray.h>

#include <gtest/gtest.h>

#include <functional>

using namespace DB;

namespace
{

void expectRows(const MutableColumns & scattered, const Columns & columns, const std::vector<size_t> & rows)
{
    ASSERT_EQ(scattered.size(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        ASSERT_EQ(scattered[i]->size(), rows.size());
        for (size_t j = 0; j < rows.size(); ++j)
            ASSERT_TRUE((*scattered[i])[j] == (*columns[i])[rows[j]]) << "column " << i << ", row " << rows[j];
    }
}

size_t cacheMissReduction(const std::vector<String> & type_names)
{
    DataTypes types;
    Columns columns;
    for (const auto & name : type_names)
    {
        types.push_back(DataTypeFactory::instance().get(name));
        columns.push_back(types.back()->createColumn());
    }
    return RowDataStore::cacheMissReduction(*RowDataStore::computeLayout(columns, types));
}

}

TEST(RowDataStore, RoundTrip)
{
    const auto null_or = [](UInt64 i, Field value) { return i % 3 == 0 ? Field() : value; };
    const auto string_value = [](UInt64 i) { return i % 2 ? String(20, 'x') + std::to_string(i) : std::to_string(i); };

    const std::vector<std::pair<String, std::function<Field(UInt64)>>> spec{
        {"UInt32", [](UInt64 i) { return Field(i); }},
        {"Nullable(Int64)", [&](UInt64 i) { return null_or(i, Int64(i)); }},
        {"String", [&](UInt64 i) { return Field(string_value(i)); }},
        {"Nullable(String)", [&](UInt64 i) { return null_or(i, string_value(i)); }},
        {"Tuple(UInt32, Tuple(Int64, Nullable(UInt8)))", [&](UInt64 i) { return Field(Tuple{i, Tuple{-Int64(i), null_or(i, i % 256)}}); }},
        {"Nullable(Tuple(UInt32, Int64))", [&](UInt64 i) { return null_or(i, Tuple{i, -Int64(i)}); }},
    };

    constexpr size_t rows = 20000;
    DataTypes types;
    Columns columns;
    for (const auto & [type_name, value] : spec)
    {
        types.push_back(DataTypeFactory::instance().get(type_name));
        auto column = types.back()->createColumn();
        for (size_t i = 0; i < rows; ++i)
            column->insert(value(i));
        columns.push_back(std::move(column));
    }

    const auto store = RowDataStore::create(RowDataStore::computeLayout(columns, types), columns);

    std::vector<size_t> all(rows);
    for (size_t i = 0; i < rows; ++i)
        all[i] = i;
    expectRows(store->scatterRows(0, rows), columns, all);
    expectRows(store->scatterRows(100, 7), columns, {100, 101, 102, 103, 104, 105, 106});
    expectRows(store->scatterRows(PaddedPODArray<UInt64>{19999, 0, 17, 17, 9000}), columns, {19999, 0, 17, 17, 9000});
}

TEST(RowDataStore, CacheMissReduction)
{
    EXPECT_EQ(cacheMissReduction({"UInt64"}), 0u);
    EXPECT_EQ(cacheMissReduction({"Nullable(String)"}), 2u);
    EXPECT_EQ(cacheMissReduction({"UInt64", "UInt64", "UInt64"}), 2u);
    EXPECT_EQ(cacheMissReduction({"FixedString(32)", "FixedString(32)", "FixedString(32)", "FixedString(32)"}), 2u);
    EXPECT_EQ(cacheMissReduction({"Tuple(Int32, Int64, Float64, UInt32, Int16, UInt8)"}), 5u);
    EXPECT_EQ(cacheMissReduction({"Nullable(Tuple(Int32, Tuple(Int8, Nullable(Int16))))"}), 4u);
}
