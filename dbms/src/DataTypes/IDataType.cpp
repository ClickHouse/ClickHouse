#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>

#include <Common/Exception.h>
#include <Common/escapeForFileName.h>

#include <Core/Defines.h>

#include <IO/WriteHelpers.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNested.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int MULTIPLE_STREAMS_REQUIRED;
}


void IDataType::updateAvgValueSizeHint(const IColumn & column, double & avg_value_size_hint)
{
    /// Update the average value size hint if amount of read rows isn't too small
    size_t column_size = column.size();
    if (column_size > 10)
    {
        double current_avg_value_size = static_cast<double>(column.byteSize()) / column_size;

        /// Heuristic is chosen so that avg_value_size_hint increases rapidly but decreases slowly.
        if (current_avg_value_size > avg_value_size_hint)
            avg_value_size_hint = std::min(1024., current_avg_value_size); /// avoid overestimation
        else if (current_avg_value_size * 2 < avg_value_size_hint)
            avg_value_size_hint = (current_avg_value_size + avg_value_size_hint * 3) / 4;
    }
}

ColumnPtr IDataType::createConstColumn(size_t size, const Field & field) const
{
    ColumnPtr column = createColumn();
    column->insert(field);
    return std::make_shared<ColumnConst>(column, size);
}


void IDataType::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    throw Exception("Data type " + getName() + " must be serialized with multiple streams", ErrorCodes::MULTIPLE_STREAMS_REQUIRED);
}

void IDataType::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
    throw Exception("Data type " + getName() + " must be deserialized with multiple streams", ErrorCodes::MULTIPLE_STREAMS_REQUIRED);
}


String IDataType::getFileNameForStream(const String & column_name, const IDataType::SubstreamPath & path)
{
    String nested_table_name = DataTypeNested::extractNestedTableName(column_name);
    bool is_sizes_of_nested_type = !path.empty() && path.back().type == IDataType::Substream::ArraySizes
        && nested_table_name != column_name;

    size_t array_level = 0;
    String stream_name = escapeForFileName(is_sizes_of_nested_type ? nested_table_name : column_name);
    for (const IDataType::Substream & elem : path)
    {
        if (elem.type == IDataType::Substream::NullMap)
            stream_name += NULL_MAP_COLUMN_NAME_SUFFIX;
        else if (elem.type == IDataType::Substream::ArraySizes)
            stream_name = DataTypeNested::extractNestedTableName(stream_name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(array_level);
        else if (elem.type == IDataType::Substream::ArrayElements)
            ++array_level;
    }
    return stream_name;
}


void IDataType::insertDefaultInto(IColumn & column) const
{
    column.insertDefault();
}

}
