#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <gtest/gtest.h>
#include <thread>

using namespace DB;

TEST(IColumn, dumpStructure)
{
    auto type_lc = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    ColumnPtr column_lc = type_lc->createColumn();
    String expected_structure = "LowCardinality(size = 0, UInt8(size = 0), Unique(size = 1, String(size = 1)))";

    std::vector<std::thread> threads;
    for (size_t i = 0; i < 6; ++i)
    {
        threads.emplace_back([&]
        {
            for (size_t j = 0; j < 10000; ++j)
                ASSERT_EQ(column_lc->dumpStructure(), expected_structure);
        });
    }

    for (auto & t : threads)
        t.join();
}
