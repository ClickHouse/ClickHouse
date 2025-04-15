#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Storages/VirtualColumnUtils.h>
#include <DataTypes/DataTypeString.h>

using namespace DB;
using namespace DB::VirtualColumnUtils;

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

static std::vector<std::string> test_paths = {
    "/some/folder/key1=val1/key2=val2/file1.txt",
    "/data/keyA=valA/keyB=valB/keyC=valC/file2.txt",
    "/another/dir/x=1/y=2/z=3/file3.txt",
    "/tiny/path/a=b/file4.txt",
    "/yet/another/path/k1=v1/k2=v2/k3=v3/k4=v4/k5=v5/"
};

TEST(VirtualColumnUtils, BenchmarkRegexParser)
{
    static constexpr int iterations = 1000000;

    auto start_extractkv = std::chrono::steady_clock::now();

    for (int i = 0; i < iterations; ++i)
    {
        // Pick from 5 different paths
        const auto & path = test_paths[i % 5];
        auto result = VirtualColumnUtils::parseHivePartitioningKeysAndValues(path);
        ASSERT_TRUE(!result.empty());
    }

    auto end_extractkv = std::chrono::steady_clock::now();
    auto duration_ms_extractkv = std::chrono::duration_cast<std::chrono::milliseconds>(end_extractkv - start_extractkv).count();

    std::cout << "[BenchmarkExtractkvParser] "
              << iterations << " iterations across 5 paths took "
              << duration_ms_extractkv << " ms\n";

    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < iterations; ++i)
    {
        // Pick from 5 different paths
        const auto & path = test_paths[i % 5];
        auto result = VirtualColumnUtils::parseHivePartitioningKeysAndValuesRegex(path);
        ASSERT_TRUE(!result.empty());
    }

    auto end = std::chrono::steady_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::cout << "[BenchmarkRegexParser] "
              << iterations << " iterations across 5 paths took "
              << duration_ms << " ms\n";
}
