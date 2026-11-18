#include "config.h"

#if USE_ARROW || USE_PARQUET

#include <gtest/gtest.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypeTime64.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Common/Exception.h>

#include <arrow/table.h>

/// `CHColumnToArrowColumn` is not used by the `Arrow` output format anymore (which always uses the
/// native writer), but it is still used by `ArrowFlight`, `StorageArrowFlight` and Delta Lake writes.
/// Arrow `time32`/`time64` values are a time of day and must lie in `[0, units_per_day)`, while
/// ClickHouse `Time`/`Time64` can hold negative values and values greater than or equal to 24 hours;
/// such values must be rejected instead of being written as invalid Arrow data.

namespace DB
{

namespace ErrorCodes
{
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

namespace
{

/// Converts a single-column block holding `value` to an Arrow table via `CHColumnToArrowColumn`.
void convertToArrow(const DataTypePtr & type, const Field & value)
{
    ColumnsWithTypeAndName header_columns{ColumnWithTypeAndName(type->createColumn(), type, "t")};

    auto column = type->createColumn();
    column->insert(value);

    Columns columns;
    columns.emplace_back(std::move(column));

    std::vector<Chunk> chunks;
    chunks.emplace_back(std::move(columns), 1);

    CHColumnToArrowColumn converter(header_columns, "Arrow", CHColumnToArrowColumn::Settings{});
    std::shared_ptr<arrow::Table> arrow_table;
    converter.chChunkToArrowTable(arrow_table, chunks, 1);
}

int convertAndGetErrorCode(const DataTypePtr & type, const Field & value)
{
    try
    {
        convertToArrow(type, value);
    }
    catch (const Exception & e)
    {
        return e.code();
    }
    return 0;
}

}

TEST(CHColumnToArrowColumn, TimeOutOfRangeIsRejected)
{
    const int out_of_range = ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;

    /// Time -> Arrow time32[s]. 90000 seconds is 25 hours.
    auto time_type = std::make_shared<DataTypeTime>();
    EXPECT_EQ(convertAndGetErrorCode(time_type, Field(Int64(90000))), out_of_range);
    EXPECT_EQ(convertAndGetErrorCode(time_type, Field(Int64(-1))), out_of_range);
    /// The upper bound is exclusive.
    EXPECT_EQ(convertAndGetErrorCode(time_type, Field(Int64(86400))), out_of_range);
    EXPECT_EQ(convertAndGetErrorCode(time_type, Field(Int64(86399))), 0);

    /// Nullable(Time) goes through the per-element path because of the null bytemap.
    auto nullable_time_type = std::make_shared<DataTypeNullable>(time_type);
    EXPECT_EQ(convertAndGetErrorCode(nullable_time_type, Field(Int64(90000))), out_of_range);
    EXPECT_EQ(convertAndGetErrorCode(nullable_time_type, Field()), 0);

    /// Time64(3) -> Arrow time32[ms].
    auto time64_3_type = std::make_shared<DataTypeTime64>(3);
    EXPECT_EQ(convertAndGetErrorCode(time64_3_type, DecimalField<Time64>(Time64(90000000), 3)), out_of_range);
    EXPECT_EQ(convertAndGetErrorCode(time64_3_type, DecimalField<Time64>(Time64(-3600000), 3)), out_of_range);
    EXPECT_EQ(convertAndGetErrorCode(time64_3_type, DecimalField<Time64>(Time64(86400000), 3)), out_of_range);
    EXPECT_EQ(convertAndGetErrorCode(time64_3_type, DecimalField<Time64>(Time64(86399999), 3)), 0);

    /// Time64(6) -> Arrow time64[us], the bulk path (no rescale, not nullable).
    auto time64_6_type = std::make_shared<DataTypeTime64>(6);
    EXPECT_EQ(convertAndGetErrorCode(time64_6_type, DecimalField<Time64>(Time64(90000000000), 6)), out_of_range);
    EXPECT_EQ(convertAndGetErrorCode(time64_6_type, DecimalField<Time64>(Time64(86399999999), 6)), 0);

    /// Time64(4) -> Arrow time64[us], the per-element rescale path (scale % 3 != 0).
    auto time64_4_type = std::make_shared<DataTypeTime64>(4);
    EXPECT_EQ(convertAndGetErrorCode(time64_4_type, DecimalField<Time64>(Time64(900000000), 4)), out_of_range);
    EXPECT_EQ(convertAndGetErrorCode(time64_4_type, DecimalField<Time64>(Time64(863999999), 4)), 0);

    /// Nullable(Time64(6)) goes through the per-element path because of the null bytemap.
    auto nullable_time64_6_type = std::make_shared<DataTypeNullable>(time64_6_type);
    EXPECT_EQ(convertAndGetErrorCode(nullable_time64_6_type, DecimalField<Time64>(Time64(90000000000), 6)), out_of_range);
    EXPECT_EQ(convertAndGetErrorCode(nullable_time64_6_type, Field()), 0);
}

}

#endif
