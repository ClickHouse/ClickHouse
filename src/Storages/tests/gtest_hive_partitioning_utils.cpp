#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Storages/HivePartitioningUtils.h>
#include <DataTypes/DataTypeString.h>

using namespace DB;
using namespace DB::HivePartitioningUtils;

TEST(VirtualColumnUtils, parseHivePartitioningKeysAndValuesEmptyValue)
{
    static std::string empty_value_path = "/output_data/year=2022/country=/data_0.parquet";

    const auto map = parseHivePartitioningKeysAndValues(empty_value_path);

    ASSERT_EQ(map.size(), 2);

    ASSERT_EQ(map.at("year"), "2022");
    ASSERT_EQ(map.at("country"), "");
}

TEST(VirtualColumnUtils, parseHivePartitioningKeysMultiplePartitions)
{
    std::string path = "/out/year=2022/country=US/data_0.parquet";

    const auto map = parseHivePartitioningKeysAndValues(path);
    ASSERT_EQ(map.size(), 2);

    ASSERT_EQ(map.at("year"), "2022");
    ASSERT_EQ(map.at("country"), "US");
}

TEST(VirtualColumnUtils, parseHivePartitioningKeysNoPartition)
{
    std::string path = "/no/partitions/here/file.parquet";

    auto result = parseHivePartitioningKeysAndValues(path);
    EXPECT_TRUE(result.empty());
}

TEST(VirtualColumnUtils, parseHivePartitioningKeysDuplicate)
{
    std::string path = "/folder/year=2022/year=2023/file.parquet";

    EXPECT_ANY_THROW(parseHivePartitioningKeysAndValues(path));
}

TEST(VirtualColumnUtils, parseHivePartitioningKeysMalformed)
{
    std::string path = "/out/year=2022////====US/=//data_0.parquet";

    const auto map = parseHivePartitioningKeysAndValues(path);
    ASSERT_EQ(map.size(), 1);

    ASSERT_EQ(map.at("year"), "2022");
}

TEST(VirtualColumnUtils, parseHivePartitioningKeysFilenameWithPairInIt)
{
    std::string path = "/out/year=2022/country=USA.parquet";

    const auto map = parseHivePartitioningKeysAndValues(path);
    ASSERT_EQ(map.size(), 1);

    ASSERT_EQ(map.at("year"), "2022");
}
