#include <Storages/ObjectStorage/DataLakes/Iceberg/DataFileStatistics.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/NaNUtils.h>
#include <Core/Block.h>

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
        if (field->getValue<bool>(Iceberg::f_required))
            required_field_ids.insert(field_id);
    }
}

DataFileStatistics::DataFileStatistics(Poco::JSON::Array::Ptr schema_, std::vector<Int64> written_field_ids)
    : DataFileStatistics(schema_)
{
    if (!written_field_ids.empty())
        field_ids = std::move(written_field_ids);
}

static const ColumnVariant * getVariantColumn(const IColumn & column)
{
    if (const auto * dynamic_col = checkAndGetColumn<ColumnDynamic>(&column))
        return &dynamic_col->getVariantColumn();
    return checkAndGetColumn<ColumnVariant>(&column);
}

/// `getExtremes` compares with IEEE754 `<`, where -0.0 == +0.0, so which zero it returns depends on
/// the scan order. The Iceberg spec orders -0.0 before +0.0: a zero min becomes -0.0 if the column
/// has -0.0, a zero max becomes +0.0 if it has +0.0.
static void fixFloatSignedZeroExtremes(Field & min_val, Field & max_val, const IColumn & column)
{
    bool min_is_zero = isFloatZeroField(min_val);
    bool max_is_zero = isFloatZeroField(max_val);
    if (!min_is_zero && !max_is_zero)
        return;

    const auto * nullable_col = checkAndGetColumn<ColumnNullable>(&column);
    ConstNullMapPtr null_map = nullable_col ? &nullable_col->getNullMapData() : nullptr;
    const IColumn & nested_col = nullable_col ? nullable_col->getNestedColumn() : column;

    auto scan = [&](const auto & data)
    {
        bool has_neg_zero = false;
        bool has_pos_zero = false;
        for (size_t i = 0; i < data.size() && !(has_neg_zero && has_pos_zero); ++i)
        {
            if (null_map && (*null_map)[i])
                continue;
            if (data[i] != 0)
                continue;
            if (signBit(data[i]))
                has_neg_zero = true;
            else
                has_pos_zero = true;
        }
        if (min_is_zero && has_neg_zero)
            min_val = Field(static_cast<Float64>(-0.0));
        if (max_is_zero && has_pos_zero)
            max_val = Field(static_cast<Float64>(0.0));
    };

    if (const auto * col_f32 = checkAndGetColumn<ColumnFloat32>(&nested_col))
        scan(col_f32->getData());
    else if (const auto * col_f64 = checkAndGetColumn<ColumnFloat64>(&nested_col))
        scan(col_f64->getData());
}

static Range getExtremeRangeFromColumn(const ColumnPtr & column)
{
    Field min_val;
    Field max_val;
    column->getExtremes(min_val, max_val, 0, column->size());
    fixFloatSignedZeroExtremes(min_val, max_val, *column);
    return Range(min_val, true, max_val, true);
}

/// Skips rows set in `null_map`: their nested value is a placeholder.
/// `std::nullopt` if `column` is not `Float32`/`Float64`.
static std::optional<Int64> countNaNs(const IColumn & column, ConstNullMapPtr null_map)
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
    return std::nullopt;
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
        can_count_nan.resize(num_columns, true);
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
        if (null_map)
            null_counts[i] += countBytesInFilter(*null_map);
        else if (const auto * variant_col = getVariantColumn(*col); variant_col && !required_field_ids.contains(field_ids[i]))
        {
            for (auto discriminator : variant_col->getLocalDiscriminators())
                null_counts[i] += discriminator == ColumnVariant::NULL_DISCRIMINATOR;
        }
        const IColumn & nested_col = nullable_col ? nullable_col->getNestedColumn() : *col;
        if (auto nans = countNaNs(nested_col, null_map))
            nan_counts[i] += *nans;
        else
            can_count_nan[i] = false;
        ranges[i] = uniteRanges(ranges[i], getExtremeRangeFromColumn(col));
    }
}

void DataFileStatistics::addColumnSizesOnDisk(const UnorderedMapWithMemoryTracking<String, size_t> & sizes_by_column_name, const Block & sample_block)
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
        can_count_nan = other.can_count_nan;
        ranges = other.ranges;
        return;
    }

    chassert(null_counts.size() == other.null_counts.size());
    chassert(nan_counts.size() == other.nan_counts.size());
    chassert(can_count_nan.size() == other.can_count_nan.size());
    for (size_t i = 0; i < null_counts.size(); ++i)
    {
        null_counts[i] += other.null_counts[i];
        nan_counts[i] += other.nan_counts[i];
        can_count_nan[i] = can_count_nan[i] && other.can_count_nan[i];
        ranges[i] = uniteRanges(ranges[i], other.ranges[i]);
    }
}

/// `getExtremes` skips NaN unless the chunk is all NaN, and then returns NaN on both sides.
static bool isNaNRange(const Range & range)
{
    return range.left.isNaN() && range.right.isNaN();
}

bool icebergBoundLess(const Field & lhs, const Field & rhs)
{
    if (isFloatZeroField(lhs) && isFloatZeroField(rhs))
        return signBit(lhs.safeGet<Float64>()) && !signBit(rhs.safeGet<Float64>());
    return Range::less(lhs, rhs);
}

/// `ColumnNullable::getExtremes` returns `POSITIVE_INFINITY` as both ends for a column with only NULL.
static bool isAllNullRange(const Range & range)
{
    return range.left.isPositiveInfinity() && range.right.isPositiveInfinity();
}

Range DataFileStatistics::uniteRanges(const Range & left, const Range & right)
{
    /// An all-NULL range and an all-NaN range (Iceberg bounds exclude NaN) have no bounds to unite:
    /// `Range::less` ranks them above every real value, so they would win the upper bound.
    if (isAllNullRange(right) || isNaNRange(right))
        return left;
    if (isAllNullRange(left) || isNaNRange(left))
        return right;

    return Range(
        icebergBoundLess(left.left, right.left) ? left.left : right.left,
        true,
        icebergBoundLess(right.right, left.right) ? left.right : right.right,
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
        if (can_count_nan[i])
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
