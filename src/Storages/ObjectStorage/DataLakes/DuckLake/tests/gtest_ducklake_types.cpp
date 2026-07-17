#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakeTypes.h>

#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

using namespace DB;
using namespace DB::DuckLake;

namespace
{

ColumnInfo col(
    Int64 id,
    std::optional<Int64> parent,
    Int64 order,
    String name,
    String type,
    bool nulls_allowed = true,
    Int64 begin = 1,
    std::optional<Int64> end = std::nullopt)
{
    return ColumnInfo{
        .column_id = id,
        .parent_column = parent,
        .column_order = order,
        .name = std::move(name),
        .type = std::move(type),
        .nulls_allowed = nulls_allowed,
        .begin_snapshot = begin,
        .end_snapshot = end,
    };
}

}

TEST(DuckLakeTypes, ScalarTypes)
{
    EXPECT_TRUE(typeid_cast<const DataTypeUInt8 *>(parseScalarType("boolean").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeInt8 *>(parseScalarType("int8").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeInt16 *>(parseScalarType("int16").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeInt32 *>(parseScalarType("int32").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeInt64 *>(parseScalarType("int64").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeInt128 *>(parseScalarType("int128").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeUInt8 *>(parseScalarType("uint8").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeUInt16 *>(parseScalarType("uint16").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeUInt32 *>(parseScalarType("uint32").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeUInt64 *>(parseScalarType("uint64").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeUInt128 *>(parseScalarType("uint128").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeFloat32 *>(parseScalarType("float32").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeFloat64 *>(parseScalarType("float64").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeString *>(parseScalarType("varchar").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeString *>(parseScalarType("json").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeString *>(parseScalarType("blob").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeUUID *>(parseScalarType("uuid").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeDate32 *>(parseScalarType("date").get()));
    EXPECT_TRUE(typeid_cast<const DataTypeTime *>(parseScalarType("time").get()));

    {
        auto t = typeid_cast<const DataTypeTime64 *>(parseScalarType("time_ns").get());
        ASSERT_TRUE(t);
        EXPECT_EQ(t->getScale(), 9);
    }
    {
        auto t = typeid_cast<const DataTypeDateTime64 *>(parseScalarType("timestamp").get());
        ASSERT_TRUE(t);
        EXPECT_EQ(t->getScale(), 6);
        EXPECT_FALSE(t->hasExplicitTimeZone());
    }
    {
        auto t = typeid_cast<const DataTypeDateTime64 *>(parseScalarType("timestamp_us").get());
        ASSERT_TRUE(t);
        EXPECT_EQ(t->getScale(), 6);
    }
    {
        auto t = typeid_cast<const DataTypeDateTime64 *>(parseScalarType("timestamp_ms").get());
        ASSERT_TRUE(t);
        EXPECT_EQ(t->getScale(), 3);
    }
    {
        auto t = typeid_cast<const DataTypeDateTime64 *>(parseScalarType("timestamp_ns").get());
        ASSERT_TRUE(t);
        EXPECT_EQ(t->getScale(), 9);
    }
    {
        auto t = typeid_cast<const DataTypeDateTime64 *>(parseScalarType("timestamp_s").get());
        ASSERT_TRUE(t);
        EXPECT_EQ(t->getScale(), 0);
    }
    {
        auto t = typeid_cast<const DataTypeDateTime64 *>(parseScalarType("timestamptz").get());
        ASSERT_TRUE(t);
        EXPECT_EQ(t->getScale(), 6);
        EXPECT_TRUE(t->hasExplicitTimeZone());
    }
}

TEST(DuckLakeTypes, Decimal)
{
    {
        auto t = typeid_cast<const DataTypeDecimal<Decimal32> *>(parseScalarType("decimal(9,2)").get());
        ASSERT_TRUE(t);
        EXPECT_EQ(t->getPrecision(), 9);
        EXPECT_EQ(t->getScale(), 2);
    }
    {
        auto t = typeid_cast<const DataTypeDecimal<Decimal64> *>(parseScalarType("decimal(18,4)").get());
        ASSERT_TRUE(t);
        EXPECT_EQ(t->getPrecision(), 18);
    }
    {
        auto t = typeid_cast<const DataTypeDecimal<Decimal128> *>(parseScalarType("decimal(38,10)").get());
        ASSERT_TRUE(t);
        EXPECT_EQ(t->getPrecision(), 38);
    }
    EXPECT_THROW(parseScalarType("decimal(10)"), Exception);
    EXPECT_THROW(parseScalarType("decimal(39,1)"), Exception);
}

TEST(DuckLakeTypes, UnsupportedTypesThrow)
{
    for (const auto * type : {"timetz", "interval", "variant", "geometry", "unknown"})
        EXPECT_THROW(parseScalarType(type), Exception) << type;
    EXPECT_THROW(parseScalarType("bogus"), Exception);
}

TEST(DuckLakeTypes, TableSchemaWithNestedTypes)
{
    /// Mirrors the real catalog shape produced by DuckDB:
    ///   (id INTEGER, s STRUCT(x INT, y VARCHAR), l INT[], m MAP(VARCHAR, INT))
    std::vector<ColumnInfo> rows = {
        col(1, std::nullopt, 1, "id", "int32", false),
        col(2, std::nullopt, 2, "s", "struct"),
        col(3, 2, 3, "x", "int32"),
        col(4, 2, 4, "y", "varchar"),
        col(5, std::nullopt, 5, "l", "list"),
        col(6, 5, 6, "element", "int32"),
        col(7, std::nullopt, 7, "m", "map"),
        col(8, 7, 8, "key", "varchar", false),
        col(9, 7, 9, "value", "int32"),
    };

    auto roots = buildColumnTree(rows, 100);
    ASSERT_EQ(roots.size(), 4);
    EXPECT_EQ(roots[0].info.name, "id");
    EXPECT_EQ(roots[1].info.name, "s");
    EXPECT_EQ(roots[1].children.size(), 2);
    EXPECT_EQ(roots[2].info.name, "l");
    EXPECT_EQ(roots[3].info.name, "m");

    auto schema = getTableSchema(roots);
    ASSERT_EQ(schema.size(), 4);
    EXPECT_EQ(schema.tryGetByName("id")->type->getName(), "Int32");
    /// Complex types are never wrapped in Nullable (matches the Parquet reader's conventions);
    /// only scalar leaves honor nulls_allowed.
    EXPECT_EQ(schema.tryGetByName("s")->type->getName(), "Tuple(x Nullable(Int32), y Nullable(String))");
    EXPECT_EQ(schema.tryGetByName("l")->type->getName(), "Array(Nullable(Int32))");
    EXPECT_EQ(schema.tryGetByName("m")->type->getName(), "Map(String, Nullable(Int32))");
}

TEST(DuckLakeTypes, FieldIdMapFollowsIcebergConventions)
{
    std::vector<ColumnInfo> rows = {
        col(1, std::nullopt, 1, "id", "int32"),
        col(2, std::nullopt, 2, "s", "struct"),
        col(3, 2, 3, "x", "int32"),
        col(4, 2, 4, "y", "varchar"),
        col(5, std::nullopt, 5, "l", "list"),
        col(6, 5, 6, "element", "int32"),
        col(7, std::nullopt, 7, "m", "map"),
        col(8, 7, 8, "key", "varchar"),
        col(9, 7, 9, "value", "int32"),
    };

    auto map = buildFieldIdMap(rows, 100);
    EXPECT_EQ(map.size(), 9);
    EXPECT_EQ(map.at("id"), 1);
    EXPECT_EQ(map.at("s"), 2);
    EXPECT_EQ(map.at("s.x"), 3);
    EXPECT_EQ(map.at("s.y"), 4);
    EXPECT_EQ(map.at("l"), 5);
    EXPECT_EQ(map.at("l.element"), 6);
    EXPECT_EQ(map.at("m"), 7);
    EXPECT_EQ(map.at("m.key"), 8);
    EXPECT_EQ(map.at("m.value"), 9);
}

TEST(DuckLakeTypes, SnapshotVisibility)
{
    /// 'name' renamed to 'title' at snapshot 6 (same column_id, new row).
    std::vector<ColumnInfo> rows = {
        col(1, std::nullopt, 1, "id", "int32", true, 1, std::nullopt),
        col(2, std::nullopt, 2, "name", "varchar", true, 1, 6),
        col(2, std::nullopt, 2, "title", "varchar", true, 6, std::nullopt),
        col(3, std::nullopt, 3, "extra", "float64", true, 4, std::nullopt),
    };

    {
        auto roots = buildColumnTree(rows, 5);
        auto schema = getTableSchema(roots);
        /// 'extra' was added at snapshot 4, so it is already visible; 'name' is not yet renamed.
        ASSERT_EQ(schema.size(), 3);
        EXPECT_TRUE(schema.contains("name"));
        EXPECT_TRUE(schema.contains("extra"));
        EXPECT_FALSE(schema.contains("title"));
    }
    {
        auto roots = buildColumnTree(rows, 6);
        auto schema = getTableSchema(roots);
        ASSERT_EQ(schema.size(), 3);
        EXPECT_TRUE(schema.contains("title"));
        EXPECT_TRUE(schema.contains("extra"));

        auto map = buildFieldIdMap(rows, 6);
        /// Renamed column maps to the current name under the same field id.
        EXPECT_EQ(map.at("title"), 2);
        EXPECT_EQ(map.count("name"), 0);
    }
}

TEST(DuckLakeTypes, DroppedColumnGetsSyntheticMapperName)
{
    /// Column 2 dropped at snapshot 6; column 3 is a dropped struct with children 4 and 5.
    std::vector<ColumnInfo> rows = {
        col(1, std::nullopt, 1, "id", "int32", true, 1, std::nullopt),
        col(2, std::nullopt, 2, "name", "varchar", true, 1, 6),
        col(3, std::nullopt, 3, "s", "struct", true, 1, 6),
        col(4, 3, 4, "x", "int32", true, 1, 6),
        col(5, 3, 5, "y", "varchar", true, 1, 6),
    };

    auto roots = buildColumnTree(rows, 6);
    ASSERT_EQ(roots.size(), 1);

    auto map = buildFieldIdMap(rows, 6);
    EXPECT_EQ(map.size(), 5);
    EXPECT_EQ(map.at("id"), 1);
    EXPECT_EQ(map.at("__ducklake_inactive_column_2"), 2);
    EXPECT_EQ(map.at("__ducklake_inactive_column_3"), 3);
    EXPECT_EQ(map.at("__ducklake_inactive_column_3.x"), 4);
    EXPECT_EQ(map.at("__ducklake_inactive_column_3.y"), 5);
}

TEST(DuckLakeTypes, DanglingParentThrows)
{
    std::vector<ColumnInfo> rows = {
        col(1, std::nullopt, 1, "id", "int32"),
        col(2, 99, 2, "orphan", "int32"),
    };
    EXPECT_THROW(buildColumnTree(rows, 100), Exception);
}
