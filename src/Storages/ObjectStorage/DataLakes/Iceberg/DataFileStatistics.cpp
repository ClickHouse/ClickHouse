#include <Storages/ObjectStorage/DataLakes/Iceberg/DataFileStatistics.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Columns/IColumn.h>

namespace DB
{

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

Range getExtremeRangeFromColumn(const ColumnPtr & column)
{
    Field min_val;
    Field max_val;
    column->getExtremes(min_val, max_val, 0, column->size());
    return Range(min_val, true, max_val, true);
}

void DataFileStatistics::update(const Chunk & chunk)
{
    size_t num_columns = chunk.getNumColumns();
    if (column_sizes.empty())
    {
        column_sizes.resize(num_columns, 0);
        null_counts.resize(num_columns, 0);
        for (size_t i = 0; i < num_columns; ++i)
        {
            ranges.push_back(getExtremeRangeFromColumn(chunk.getColumns()[i]));
        }
    }

    chassert(ranges.size() == num_columns);

    for (size_t i = 0; i < num_columns; ++i)
    {
        column_sizes[i] += chunk.getColumns()[i]->byteSize();
        for (size_t j = 0; j < chunk.getNumRows(); ++j)
            null_counts[i] += (chunk.getColumns()[i]->isNullAt(j));
        ranges[i] = uniteRanges(ranges[i], getExtremeRangeFromColumn(chunk.getColumns()[i]));
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

#endif

}
