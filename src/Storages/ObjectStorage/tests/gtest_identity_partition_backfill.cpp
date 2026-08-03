#include <gtest/gtest.h>

#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime64.h>

using namespace DB;

/// The helper is exercised directly because `IcebergLocal` cannot write a Decimal partition column,
/// so the per-width `DecimalField` wrapping is not reachable from SQL.
namespace
{
template <typename DecimalT>
void checkDecimal(const DataTypePtr & type, Int64 tick, UInt32 scale)
{
    const size_t num_rows = 3;
    ColumnPtr col = backfillIdentityPartitionColumn(type, Field(tick), num_rows);
    ASSERT_EQ(col->size(), num_rows);
    const auto & dec = assert_cast<const ColumnDecimal<DecimalT> &>(*col);
    for (size_t i = 0; i < num_rows; ++i)
        EXPECT_EQ(dec.getData()[i].value, static_cast<typename DecimalT::NativeType>(tick));
    EXPECT_EQ(dec.getScale(), scale);
}
}

TEST(IdentityPartitionBackfill, DecimalWidthsDoNotThrowBadGet)
{
    /// The regression: Decimal32 previously threw BAD_GET because the helper hard-coded DecimalField<Decimal64>.
    checkDecimal<Decimal32>(std::make_shared<DataTypeDecimal<Decimal32>>(9, 2), 1234, 2);
    checkDecimal<Decimal64>(std::make_shared<DataTypeDecimal<Decimal64>>(18, 4), 987654321, 4);
    checkDecimal<Decimal128>(std::make_shared<DataTypeDecimal<Decimal128>>(38, 6), -55555, 6);
    checkDecimal<Decimal256>(std::make_shared<DataTypeDecimal<Decimal256>>(70, 8), 42, 8);
}

TEST(IdentityPartitionBackfill, DateTime64TickInsertedDirectly)
{
    /// DateTime64 shares Field tag Decimal64; the raw micros tick must be inserted as-is (no value-semantics overflow).
    auto type = std::make_shared<DataTypeDateTime64>(6);
    const Int64 tick = 1756297496000000; /// 2025-08-27 12:34:56 in microseconds
    ColumnPtr col = backfillIdentityPartitionColumn(type, Field(tick), 2);
    const auto & dec = assert_cast<const ColumnDecimal<DateTime64> &>(*col);
    EXPECT_EQ(dec.getData()[0].value, tick);
    EXPECT_EQ(dec.getData()[1].value, tick);
}

TEST(IdentityPartitionBackfill, NullableDecimal32)
{
    /// Nullable target: the inner width must still drive the DecimalField tag.
    const Int64 tick = 777;
    auto type = makeNullable(std::make_shared<DataTypeDecimal<Decimal32>>(9, 3));
    ColumnPtr col = backfillIdentityPartitionColumn(type, Field(tick), 2);
    ASSERT_EQ(col->size(), 2u);
    const auto & nullable = assert_cast<const ColumnNullable &>(*col);
    /// Assert the tick reached the nested column unscaled: a non-null check alone also passes when
    /// the value went through convertFieldToType, which would multiply it by the scale.
    const auto & dec = assert_cast<const ColumnDecimal<Decimal32> &>(nullable.getNestedColumn());
    EXPECT_EQ(dec.getScale(), 3u);
    for (size_t i = 0; i < col->size(); ++i)
    {
        EXPECT_FALSE(nullable.isNullAt(i));
        EXPECT_EQ(dec.getData()[i].value, static_cast<Decimal32::NativeType>(tick));
    }
}

TEST(IdentityPartitionBackfill, NonScaleTypeGoesThroughConversion)
{
    /// A plain integer target does not take the tick path; convertFieldToType applies value semantics.
    auto type = std::make_shared<DataTypeInt64>();
    ColumnPtr col = backfillIdentityPartitionColumn(type, Field(Int64(-123456789)), 2);
    const auto & c = assert_cast<const ColumnInt64 &>(*col);
    EXPECT_EQ(c.getData()[0], -123456789);
    EXPECT_EQ(c.getData()[1], -123456789);
}
