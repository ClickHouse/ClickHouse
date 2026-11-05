#include <Storages/ObjectStorage/DataLakes/Iceberg/DataFileStatistics.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/NaNUtils.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

#if USE_AVRO

DataFileStatistics::DataFileStatistics(Poco::JSON::Array::Ptr schema_)
{
    field_ids.resize(schema_->size());
    for (UInt32 i = 0; i < schema_->size(); ++i)
    {
        auto field = schema_->getObject(i);
        size_t field_id = field->getValue<size_t>(Iceberg::f_id);
        field_ids[i] =  field_id;
    }
}

DataFileStatistics::DataFileStatistics(Poco::JSON::Array::Ptr schema_, std::vector<Int64> written_field_ids)
    : DataFileStatistics(schema_)
{
    if (!written_field_ids.empty())
        field_ids = std::move(written_field_ids);
}

static Range getExtremeRangeFromColumn(const ColumnPtr & column)
{
    Field min_val;
    Field max_val;
    column->getExtremes(min_val, max_val, 0, column->size());
    return Range(min_val, true, max_val, true);
}

namespace
{

/// Counts NaN among a Float32/Float64 column's non-null values; `null_map` is null_map[i] is
/// skipped (a NULL row's nested value is not a real float, so it must not also be tallied as NaN).
Int64 countNaNs(const IColumn & column, ConstNullMapPtr null_map)
{
    auto count = [&](const auto & data) -> Int64
    {
        Int64 result = 0;
        for (size_t i = 0; i < data.size(); ++i)
            if ((!null_map || !(*null_map)[i]) && isNaN(data[i]))
                ++result;
        return result;
    };

    if (const auto * col_f32 = checkAndGetColumn<ColumnFloat32>(&column))
        return count(col_f32->getData());
    if (const auto * col_f64 = checkAndGetColumn<ColumnFloat64>(&column))
        return count(col_f64->getData());
    return 0;
}

}

void DataFileStatistics::update(const Chunk & chunk)
{
    if (!chunk.hasRows())
        return;
    size_t num_columns = chunk.getNumColumns();
    if (ranges.empty())
    {
        null_counts.resize(num_columns, 0);
        nan_counts.resize(num_columns, 0);
        is_float_column.resize(num_columns, false);
        for (size_t i = 0; i < num_columns; ++i)
        {
            ranges.push_back(getExtremeRangeFromColumn(chunk.getColumns()[i]));
        }
    }

    chassert(ranges.size() == num_columns);

    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & col = chunk.getColumns()[i];
        const auto * nullable_col = checkAndGetColumn<ColumnNullable>(col.get());
        ConstNullMapPtr null_map = nullable_col ? &nullable_col->getNullMapData() : nullptr;
        if (nullable_col)
        {
            for (UInt8 v : *null_map)
                null_counts[i] += v;
        }
        const IColumn & nested_col = nullable_col ? nullable_col->getNestedColumn() : *col;
        /// `isNativeFloat()` is exactly Float32/Float64, excluding BFloat16 (which `countNaNs`
        /// below cannot handle - it isn't a `ColumnFloat32`/`ColumnFloat64` - and is out of scope here).
        is_float_column[i] = WhichDataType(nested_col.getDataType()).isNativeFloat();
        nan_counts[i] += countNaNs(nested_col, null_map);
        ranges[i] = uniteRanges(ranges[i], getExtremeRangeFromColumn(col));
    }
}

void DataFileStatistics::addColumnSizesOnDisk(const std::unordered_map<String, size_t> & sizes_by_column_name, const Block & sample_block)
{
    if (sizes_by_column_name.empty())
        return;

    if (sample_block.columns() != field_ids.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Iceberg data file has {} columns while its schema has {} fields",
            sample_block.columns(),
            field_ids.size());

    if (column_sizes.empty())
        column_sizes.resize(field_ids.size(), 0);

    for (size_t i = 0; i < field_ids.size(); ++i)
    {
        const auto & column_name = sample_block.getByPosition(i).name;
        auto it = sizes_by_column_name.find(column_name);
        if (it == sizes_by_column_name.end())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Written data file does not report the on-disk size of column {}", column_name);
        column_sizes[i] += static_cast<Int64>(it->second);
    }
}

void DataFileStatistics::merge(const DataFileStatistics & other)
{
    if (!other.column_sizes.empty())
    {
        if (column_sizes.empty())
        {
            column_sizes = other.column_sizes;
        }
        else
        {
            chassert(column_sizes.size() == other.column_sizes.size());
            for (size_t i = 0; i < column_sizes.size(); ++i)
                column_sizes[i] += other.column_sizes[i];
        }
    }

    if (other.ranges.empty())
        return;

    if (ranges.empty())
    {
        null_counts = other.null_counts;
        nan_counts = other.nan_counts;
        is_float_column = other.is_float_column;
        ranges = other.ranges;
        return;
    }

    chassert(null_counts.size() == other.null_counts.size());
    chassert(nan_counts.size() == other.nan_counts.size());
    chassert(null_counts.size() == nan_counts.size());
    chassert(is_float_column == other.is_float_column);
    for (size_t i = 0; i < null_counts.size(); ++i)
    {
        null_counts[i] += other.null_counts[i];
        nan_counts[i] += other.nan_counts[i];
        ranges[i] = uniteRanges(ranges[i], other.ranges[i]);
    }
}

namespace
{

/// `Field` has no Float32 slot (see Field.h): a Float32 column's values are always stored as
/// Field::Types::Float64, so this one check also covers Float32.
bool isFloatNaN(const Field & value)
{
    return value.getType() == Field::Types::Float64 && isNaN(value.safeGet<Float64>());
}

/// Whether `range` is the "no usable value" sentinel: an all-NaN chunk's own extremes are NaN on
/// both sides at once (see `ColumnVector::getExtremes`), never on just one.
bool isNaNRange(const Range & range)
{
    return isFloatNaN(range.left) && isFloatNaN(range.right);
}

}

Range DataFileStatistics::uniteRanges(const Range & left, const Range & right)
{
    /// `Range::less` (via `accurateLess`) deliberately ranks NaN above every real value, for
    /// generic ordering purposes (sorting, index range analysis). That is the wrong rule here: an
    /// all-NaN chunk/file must not push a real bound out, so a NaN side is excluded instead of
    /// compared.
    bool left_is_nan = isNaNRange(left);
    bool right_is_nan = isNaNRange(right);
    if (left_is_nan && right_is_nan)
        return left;
    if (left_is_nan)
        return right;
    if (right_is_nan)
        return left;

    return Range(
        Range::less(left.left, right.left) ? left.left : right.left,
        true,
        Range::less(right.right, left.right) ? left.right : right.right,
        true);
}

std::vector<std::pair<size_t, size_t>> DataFileStatistics::getColumnSizes() const
{
    std::vector<std::pair<size_t, size_t>> result;
    for (size_t i = 0; i < column_sizes.size(); ++i)
    {
        result.push_back({field_ids[i], column_sizes[i]});
    }
    return result;
}

std::vector<std::pair<size_t, size_t>> DataFileStatistics::getNullCounts() const
{
    std::vector<std::pair<size_t, size_t>> result;
    for (size_t i = 0; i < null_counts.size(); ++i)
    {
        result.push_back({field_ids[i], null_counts[i]});
    }
    return result;
}


std::vector<std::pair<size_t, size_t>> DataFileStatistics::getNanCounts() const
{
    std::vector<std::pair<size_t, size_t>> result;
    for (size_t i = 0; i < nan_counts.size(); ++i)
    {
        if (is_float_column[i])
            result.push_back({field_ids[i], nan_counts[i]});
    }
    return result;
}

std::vector<std::pair<size_t, Field>> DataFileStatistics::getLowerBounds() const
{
    std::vector<std::pair<size_t, Field>> result;
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        result.push_back({field_ids[i], ranges[i].left});
    }
    return result;
}

std::vector<std::pair<size_t, Field>> DataFileStatistics::getUpperBounds() const
{
    std::vector<std::pair<size_t, Field>> result;
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        result.push_back({field_ids[i], ranges[i].right});
    }
    return result;
}

void IcebergStatisticsTransform::transform(Chunk & chunk)
{
    stats->update(chunk);
    cur_chunk = chunk.clone();
}


#endif

}
