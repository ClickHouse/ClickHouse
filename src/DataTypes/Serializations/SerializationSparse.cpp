#include <DataTypes/Serializations/SerializationSparse.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace
{

void serializeOffsetsPositionIndependent(const IColumn::Offsets & offsets, WriteBuffer & ostr)
{
    /// TODO: offset and limit
    size_t size = offsets.size();
    IColumn::Offset prev_offset = 0;
    for (size_t i = 0; i < size; ++i)
    {
        IColumn::Offset current_offset = offsets[i];
        writeIntBinary(current_offset - prev_offset, ostr);
        prev_offset = current_offset;
    }
}

void deserializeOffsetsPositionIndependent(IColumn::Offsets & offsets, ReadBuffer & istr)
{
    IColumn::Offset current_offset = 0;
    while (!istr.eof())
    {
        IColumn::Offset current_size = 0;
        readIntBinary(current_size, istr);
        current_offset += current_size;
        offsets.push_back(current_offset);
    }
}

}

SerializationSparse::SerializationSparse(const SerializationPtr & nested_)
    : SerializationWrapper(nested_)
{
}

void SerializationSparse::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    path.push_back(Substream::SparseOffsets);
    callback(path);
    path.back() = Substream::SparseElements;
    nested->enumerateStreams(callback, path);
    path.pop_back();
}

void SerializationSparse::serializeBinaryBulkStatePrefix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::SparseElements);
    nested->serializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}

void SerializationSparse::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::SparseElements);
    nested->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}

void SerializationSparse::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::SparseElements);
    nested->deserializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}

void SerializationSparse::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    /// TODO: too inefficient.
    /// TODO: use limit and offset
    size_t size = column.size();

    auto offsets_column = DataTypeNumber<IColumn::Offset>().createColumn();
    auto & offsets_data = assert_cast<ColumnVector<IColumn::Offset> &>(*offsets_column).getData();

    column.getIndicesOfNotDefaultValues(offsets_data);
    auto values = column.index(*offsets_column, limit);

    offsets_data.push_back(size);

    settings.path.push_back(Substream::SparseOffsets);
    if (auto * stream = settings.getter(settings.path))
        serializeOffsetsPositionIndependent(offsets_data, *stream);
    
    settings.path.back() = Substream::SparseElements;
    nested->serializeBinaryBulkWithMultipleStreams(*values, offset, limit, settings, state);

    settings.path.pop_back();
}

void SerializationSparse::deserializeBinaryBulkWithMultipleStreamsImpl(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::SparseOffsets);

    auto offsets_column = DataTypeNumber<IColumn::Offset>().createColumn();
    auto & offsets_data = assert_cast<ColumnVector<IColumn::Offset> &>(*offsets_column).getData();

    if (auto * stream = settings.getter(settings.path))
        deserializeOffsetsPositionIndependent(offsets_data, *stream);
    
    settings.path.back() = Substream::SparseElements;

    ColumnPtr values = column.cloneEmpty();
    nested->deserializeBinaryBulkWithMultipleStreams(values, limit, settings, state, cache);

    IColumn::Offset prev_offset = 0;
    for (size_t i = 0; i < offsets_data.size(); ++i)
    {
        size_t num_defaults = offsets_data[i] - prev_offset - 1;
        if (num_defaults)
            column.insertManyDefaults(num_defaults);

        column.insertFrom(*values, i);
        prev_offset = offsets_data[i];
    }
}

}
