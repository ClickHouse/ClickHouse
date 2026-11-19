#include <Storages/ObjectStorage/DataLakes/Iceberg/DataFileStatistics.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Columns/ColumnNullable.h>
#include <Columns/IColumn.h>
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

void DataFileStatistics::update(const Chunk & chunk)
{
    if (!chunk.hasRows())
        return;
    size_t num_columns = chunk.getNumColumns();
    if (null_counts.empty())
    {
        null_counts.resize(num_columns, 0);
        for (size_t i = 0; i < num_columns; ++i)
        {
            ranges.push_back(getExtremeRangeFromColumn(chunk.getColumns()[i]));
        }
    }

    chassert(ranges.size() == num_columns);

    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & col = chunk.getColumns()[i];
        if (const auto * nullable_col = checkAndGetColumn<ColumnNullable>(col.get()))
        {
            for (UInt8 v : nullable_col->getNullMapData())
                null_counts[i] += v;
        }
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

    if (other.null_counts.empty())
        return;

    if (null_counts.empty())
    {
        null_counts = other.null_counts;
        ranges = other.ranges;
        return;
    }

    chassert(null_counts.size() == other.null_counts.size());
    for (size_t i = 0; i < null_counts.size(); ++i)
    {
        null_counts[i] += other.null_counts[i];
        ranges[i] = uniteRanges(ranges[i], other.ranges[i]);
    }
}

Range DataFileStatistics::uniteRanges(const Range & left, const Range & right)
{
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
