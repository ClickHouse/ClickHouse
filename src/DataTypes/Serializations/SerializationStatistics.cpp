#include <DataTypes/Serializations/SerializationStatistics.h>

#include <Columns/ColumnSparse.h>
#include <Columns/IColumn.h>

namespace DB
{

void SerializationStatistics::add(const IColumn & column)
{
    size_t rows = column.size();
    double ratio = column.getRatioOfDefaultRows(ColumnSparse::DEFAULT_ROWS_SEARCH_SAMPLE_RATIO);

    num_rows += rows;
    num_defaults += static_cast<size_t>(ratio * static_cast<double>(rows));
}

void SerializationStatistics::add(const SerializationStatistics & other)
{
    num_rows += other.num_rows;
    num_defaults += other.num_defaults;
}

void SerializationStatistics::addDefaults(size_t length)
{
    num_rows += length;
    num_defaults += length;
}

}
