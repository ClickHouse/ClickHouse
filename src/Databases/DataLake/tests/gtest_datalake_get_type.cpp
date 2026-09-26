#include <gtest/gtest.h>

#include <Databases/DataLake/Common.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>

/// DataLake::getType parses the column type strings the Glue and Hive catalogs report for an Iceberg
/// table. Tests for https://github.com/ClickHouse/ClickHouse/issues/121850: Glue reports an Iceberg
/// `binary` column as `varbyte`, and reports a list column with the Iceberg spelling `list<...>`
/// rather than the Hive spelling `array<...>`; both were rejected as unknown types.

TEST(DataLakeGetType, VarbyteIsString)
{
    EXPECT_EQ(DataLake::getType("varbyte", false)->getName(), "String");
    EXPECT_EQ(DataLake::getType("varbyte(16)", false)->getName(), "String");
    EXPECT_EQ(DataLake::getType("varbyte( 16 )", false)->getName(), "String");
    EXPECT_EQ(DataLake::getType("varbyte", true)->getName(), "Nullable(String)");
    EXPECT_EQ(DataLake::getType("array<varbyte>", false)->getName(), "Array(String)");
    EXPECT_EQ(DataLake::getType("map<string,varbyte>", false)->getName(), "Map(String, String)");
    EXPECT_EQ(DataLake::getType("struct<k:string,v:varbyte>", false)->getName(), "Tuple(k String, v String)");
}

TEST(DataLakeGetType, ListIsTheIcebergSpellingOfArray)
{
    EXPECT_EQ(DataLake::getType("list<string>", false)->getName(), "Array(String)");
    /// The type Glue reports for the `headers` field of a Redpanda Iceberg topic.
    EXPECT_EQ(
        DataLake::getType("list<struct<key:string,value:varbyte>>", false)->getName(),
        "Array(Tuple(key String, value String))");
}

TEST(DataLakeGetType, UnknownSpellingsAreStillRejected)
{
    EXPECT_THROW(DataLake::getType("varbytex", false), DB::Exception);
    EXPECT_THROW(DataLake::getType("varbyte(", false), DB::Exception);
    EXPECT_THROW(DataLake::getType("listx<string>", false), DB::Exception);
}
