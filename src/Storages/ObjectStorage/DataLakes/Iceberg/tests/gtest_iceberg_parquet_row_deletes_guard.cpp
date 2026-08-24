#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}
}

TEST(IcebergParquetRowDeletesGuard, AcceptsParquet)
{
    EXPECT_NO_THROW(Iceberg::requireParquetDataFileForRowDeletes("parquet", "Deletion vectors"));
    EXPECT_NO_THROW(Iceberg::requireParquetDataFileForRowDeletes("PARQUET", "Position deletes"));
    EXPECT_NO_THROW(Iceberg::requireParquetDataFileForRowDeletes("Parquet", "Deletion vectors"));
}

TEST(IcebergParquetRowDeletesGuard, RejectsNonParquet)
{
    try
    {
        Iceberg::requireParquetDataFileForRowDeletes("ORC", "Deletion vectors");
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED);
        EXPECT_NE(e.message().find("Deletion vectors are only supported"), std::string::npos);
        EXPECT_NE(e.message().find("ORC"), std::string::npos);
    }

    try
    {
        Iceberg::requireParquetDataFileForRowDeletes("AVRO", "Position deletes");
        FAIL() << "Expected exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED);
        EXPECT_NE(e.message().find("Position deletes are only supported"), std::string::npos);
    }
}
