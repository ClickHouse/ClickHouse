#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Storages/VirtualColumnUtils.h>
#include <DataTypes/DataTypeString.h>

using namespace DB;

TEST(VirtualColumnUtils, parseHivePartitioningKeysAndValuesEmptyValue)
{
    static std::string empty_value_path = "/output_data/year=2022/country=/data_0.parquet";

    auto map = VirtualColumnUtils::parseHivePartitioningKeysAndValues(empty_value_path);

    ASSERT_TRUE(map.size() == 2);

    ASSERT_TRUE(map["year"] == "2022");
    ASSERT_TRUE(map["country"].empty());
}

TEST(VirtualColumnUtils, getVirtualsForFileLikeStorageEmptyValue)
{
    static std::string empty_value_path = "/output_data/year=2022/country=/data_0.parquet";

    const auto & context_holder = getContext();

    auto year_column = ColumnDescription("year", std::make_shared<DataTypeString>());
    auto country_column = ColumnDescription("country", std::make_shared<DataTypeString>());
    auto non_partition_column = ColumnDescription("non_partition", std::make_shared<DataTypeString>());

    ColumnsDescription columns;

    columns.add(year_column);
    columns.add(country_column);
    columns.add(non_partition_column);

    auto res = VirtualColumnUtils::getVirtualsForFileLikeStorage(columns, context_holder.context, empty_value_path);

    ASSERT_TRUE(res.has("year"));
    ASSERT_TRUE(res.has("country"));
}
