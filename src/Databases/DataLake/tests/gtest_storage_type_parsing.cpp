#include <Databases/DataLake/ICatalog.h>
#include <gtest/gtest.h>

namespace DataLake::Test
{

TEST(StorageTypeParsingTest, ParsesHDFS)
{
    EXPECT_EQ(parseStorageTypeFromString("hdfs"), StorageType::HDFS);
    EXPECT_EQ(parseStorageTypeFromString("hdfs://"), StorageType::HDFS);
}

}
