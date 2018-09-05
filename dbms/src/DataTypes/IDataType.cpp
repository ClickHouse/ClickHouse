#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>

#include <Common/Exception.h>
#include <Common/escapeForFileName.h>

#include <Core/Defines.h>

#include <IO/WriteHelpers.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/NestedUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int MULTIPLE_STREAMS_REQUIRED;
    extern const int LOGICAL_ERROR;
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

ColumnPtr IDataType::createColumnConst(size_t size, const Field & field) const
{
    auto column = createColumn();
    column->insert(field);
    return ColumnConst::create(std::move(column), size);
}


ColumnPtr IDataType::createColumnConstWithDefaultValue(size_t size) const
{
    return createColumnConst(size, getDefault());
}


void IDataType::serializeBinaryBulk(const IColumn &, WriteBuffer &, size_t, size_t) const
{
    throw Exception("Data type " + getName() + " must be serialized with multiple streams", ErrorCodes::MULTIPLE_STREAMS_REQUIRED);
}

void IDataType::deserializeBinaryBulk(IColumn &, ReadBuffer &, size_t, double) const
{
    throw Exception("Data type " + getName() + " must be deserialized with multiple streams", ErrorCodes::MULTIPLE_STREAMS_REQUIRED);
}

size_t IDataType::getSizeOfValueInMemory() const
{
    throw Exception("Value of type " + getName() + " in memory is not of fixed size.", ErrorCodes::LOGICAL_ERROR);
}


String IDataType::getFileNameForStream(const String & column_name, const IDataType::SubstreamPath & path)
{
    /// Sizes of arrays (elements of Nested type) are shared (all reside in single file).
    String nested_table_name = Nested::extractTableName(column_name);

    bool is_sizes_of_nested_type =
        path.size() == 1    /// Nested structure may have arrays as nested elements (so effectively we have multidimensional arrays).
                            /// Sizes of arrays are shared only at first level.
        && path[0].type == IDataType::Substream::ArraySizes
        && nested_table_name != column_name;

    size_t array_level = 0;
    String stream_name = escapeForFileName(is_sizes_of_nested_type ? nested_table_name : column_name);
    for (const Substream & elem : path)
    {
        if (elem.type == Substream::NullMap)
            stream_name += ".null";
        else if (elem.type == Substream::ArraySizes)
            stream_name += ".size" + toString(array_level);
        else if (elem.type == Substream::ArrayElements)
            ++array_level;
        else if (elem.type == Substream::TupleElement)
        {
            /// For compatibility reasons, we use %2E instead of dot.
            /// Because nested data may be represented not by Array of Tuple,
            ///  but by separate Array columns with names in a form of a.b,
            ///  and name is encoded as a whole.
            stream_name += "%2E" + escapeForFileName(elem.tuple_element_name);
        }
    }
    return stream_name;
}


void IDataType::insertDefaultInto(IColumn & column) const
{
    column.insertDefault();
}

}
