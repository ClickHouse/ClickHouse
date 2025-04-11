#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Storages/VirtualColumnUtils.h>
#include <DataTypes/DataTypeString.h>

using namespace DB;
using namespace DB::VirtualColumnUtils;

TEST(VirtualColumnUtils, parseHivePartitioningKeysAndValuesEmptyValue)
{
    static std::string empty_value_path = "/output_data/year=2022/country=/data_0.parquet";

    auto map = parseHivePartitioningKeysAndValuesWithRegex(empty_value_path);

    ASSERT_TRUE(map.size() == 2);

    ASSERT_TRUE(map["year"] == "2022");
    ASSERT_TRUE(map["country"].empty());
}

TEST(ParseHivePartitioningTest, ExtractKVP_NormalSinglePartition)
{
    std::string path = "/some/prefix/year=2023/suffix/file.parquet";

    auto result = parseHivePartitioningKeysAndValuesWithExtractKvp(path);
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result["year"], "2023");
}

TEST(ParseHivePartitioningTest, ExtractKVP_MultiplePartitions)
{
    std::string path = "/out/year=2022/country=US/data_0.parquet";

    auto result = parseHivePartitioningKeysAndValuesWithExtractKvp(path);
    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result["year"], "2022");
    EXPECT_EQ(result["country"], "US");
}

TEST(ParseHivePartitioningTest, ExtractKVP_EmptyValue)
{
    // country=  is empty
    std::string path = "/output_data/year=2022/country=/data_0.parquet";

    auto result = parseHivePartitioningKeysAndValuesWithExtractKvp(path);
    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result["year"], "2022");
    EXPECT_EQ(result["country"], ""); // empty string
}

TEST(ParseHivePartitioningTest, ExtractKVP_NoPartitions)
{
    // No key=value segments
    std::string path = "/no/partitions/here/file.parquet";

    auto result = parseHivePartitioningKeysAndValuesWithExtractKvp(path);
    EXPECT_TRUE(result.empty());
}

TEST(ParseHivePartitioningTest, ExtractKVP_DuplicateKeyDifferentValue)
{
    // year=2022 and then year=2023 => that should throw
    std::string path = "/folder/year=2022/year=2023/file.parquet";

    EXPECT_ANY_THROW(parseHivePartitioningKeysAndValuesWithExtractKvp(path));
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

    auto res = getVirtualsForFileLikeStorage(columns, context_holder.context, empty_value_path);

    ASSERT_TRUE(res.has("year"));
    ASSERT_TRUE(res.has("country"));
}
