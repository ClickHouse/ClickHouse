#include <DataTypes/Serializations/SerializationReplicated.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnReplicated.h>
#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


SerializationReplicated::SerializationReplicated(const SerializationPtr & nested_)
    : nested(nested_)
{
}

ISerialization::KindStack SerializationReplicated::getKindStack() const
{
    auto kind_stack = nested->getKindStack();
    kind_stack.push_back(Kind::REPLICATED);
    return kind_stack;
}

SerializationPtr SerializationReplicated::SubcolumnCreator::create(const SerializationPtr & prev, const DataTypePtr &) const
{
    return std::make_shared<SerializationReplicated>(prev);
}

ColumnPtr SerializationReplicated::SubcolumnCreator::create(const ColumnPtr & prev) const
{
    return ColumnReplicated::create(prev, indexes);
}

void SerializationReplicated::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * column_replicated = data.column ? typeid_cast<const ColumnReplicated *>(data.column.get()) : nullptr;

    settings.path.push_back(Substream::ReplicatedIndexes);
    callback(settings.path);

    settings.path.back() = Substream::ReplicatedElements;
    if (column_replicated)
        settings.path.back().creator = std::make_shared<SubcolumnCreator>(column_replicated->getIndexesColumn());

    auto nested_data = SubstreamData(nested)
        .withType(data.type)
        .withColumn(column_replicated ? column_replicated->getNestedColumn() : nullptr)
        .withSerializationInfo(data.serialization_info);

    nested->enumerateStreams(settings, callback, nested_data);
    settings.path.pop_back();
}

void SerializationReplicated::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::ReplicatedElements);
    if (const auto * column_replicated = typeid_cast<const ColumnReplicated *>(&column))
        nested->serializeBinaryBulkStatePrefix(*column_replicated->getNestedColumn(), settings, state);
    else
        nested->serializeBinaryBulkStatePrefix(column, settings, state);

    settings.path.pop_back();
}

void SerializationReplicated::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    /// For now we support only writing and reading in Native format.

    if (!settings.native_format)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Binary bulk serialization of ColumnReplicated is supported only for Native format");

    if (const size_t size = column.size(); limit == 0 || offset + limit > size)
        limit = size - offset;

    const auto & column_replicated = assert_cast<const ColumnReplicated &>(column);

    settings.path.push_back(Substream::ReplicatedIndexes);
    auto * indexes_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!indexes_stream)
        return;

    /// We write ColumnReplicated data in the following format:
    /// - number of rows in column
    /// - size of indexes type
    /// - indexes data
    /// - number of rows in the nested column
    /// - data of nested column

    writeVarUInt(UInt64(limit), *indexes_stream);
    auto size_of_indexes_type = column_replicated.getIndexes().getSizeOfIndexType();
    writeBinaryLittleEndian(UInt8(size_of_indexes_type), *indexes_stream);

    switch (size_of_indexes_type)
    {
        case sizeof(UInt8):
            SerializationNumber<UInt8>().serializeBinaryBulk(*column_replicated.getIndexesColumn(), *indexes_stream, offset, limit);
            break;
        case sizeof(UInt16):
            SerializationNumber<UInt16>().serializeBinaryBulk(*column_replicated.getIndexesColumn(), *indexes_stream, offset, limit);
            break;
        case sizeof(UInt32):
            SerializationNumber<UInt32>().serializeBinaryBulk(*column_replicated.getIndexesColumn(), *indexes_stream, offset, limit);
            break;
        case sizeof(UInt64):
            SerializationNumber<UInt64>().serializeBinaryBulk(*column_replicated.getIndexesColumn(), *indexes_stream, offset, limit);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for ColumnReplicated: {}", size_of_indexes_type);
    }

    settings.path.push_back(Substream::ReplicatedElements);
    auto * elements_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!elements_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for SerializationReplicated elements.");

    writeVarUInt(UInt64(column_replicated.getNestedColumn()->size()), *elements_stream);
    nested->serializeBinaryBulkWithMultipleStreams(*column_replicated.getNestedColumn(), 0, 0, settings, state);
}

void SerializationReplicated::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::ReplicatedElements);
    nested->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}

void SerializationReplicated::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    settings.path.push_back(Substream::ReplicatedElements);
    nested->deserializeBinaryBulkStatePrefix(settings, state, cache);
    settings.path.pop_back();
}

void SerializationReplicated::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    /// For now we support only writing and reading in Native format.
    /// To be able to write and read in MergeTree part we need to support:
    /// - reading of multiple granules at once
    /// - reading into non-empty column (it requires adjustments of deserialized indexes)
    /// - usage of substreams cache
    /// - reading subcolumns of nested column.
    if (!settings.native_format)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Binary bulk deserialization of ColumnReplicated is supported only for Native format");

    if (rows_offset != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected value of rows_offset in Native format: {}. Expected 0", rows_offset);

    if (!column->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Reading into non-empty column ColumnReplicated is not supported in Native format");

    auto mutable_column = column->assumeMutable();
    auto & column_replicated = assert_cast<ColumnReplicated &>(*mutable_column);

    settings.path.push_back(Substream::ReplicatedIndexes);
    auto * indexes_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!indexes_stream)
        return;

    size_t num_rows;
    readVarUInt(num_rows, *indexes_stream);
    /// In Native format we always read the whole serialized column.
    if (num_rows != limit)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected number of rows in indexes column in ColumnReplicated in Native format: {}. Expected {}", num_rows, limit);

    UInt8 size_of_indexes_type;
    readBinary(size_of_indexes_type, *indexes_stream);

    MutableColumnPtr indexes;

    switch (size_of_indexes_type)
    {
        case sizeof(UInt8):
            indexes = ColumnUInt8::create();
            SerializationNumber<UInt8>().deserializeBinaryBulk(*indexes, *indexes_stream, 0, limit, 0);
            break;
        case sizeof(UInt16):
            indexes = ColumnUInt16::create();
            SerializationNumber<UInt16>().deserializeBinaryBulk(*indexes, *indexes_stream, 0, limit, 0);
            break;
        case sizeof(UInt32):
            indexes = ColumnUInt32::create();
            SerializationNumber<UInt32>().deserializeBinaryBulk(*indexes, *indexes_stream, 0, limit, 0);
            break;
        case sizeof(UInt64):
            indexes = ColumnUInt64::create();
            SerializationNumber<UInt64>().deserializeBinaryBulk(*indexes, *indexes_stream, 0, limit, 0);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for ColumnReplicated: {}", UInt32(size_of_indexes_type));
    }

    column_replicated.getIndexes().attachIndexes(std::move(indexes));

    settings.path.push_back(Substream::ReplicatedElements);
    auto * elements_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!elements_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for SerializationReplicated elements.");

    size_t num_elements;
    readVarUInt(num_elements, *elements_stream);
    nested->deserializeBinaryBulkWithMultipleStreams(column_replicated.getNestedColumn(), 0, num_elements, settings, state, cache);
}

void SerializationReplicated::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeBinary(field, ostr, settings);
}

void SerializationReplicated::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeBinary(field, istr, settings);
}

template <typename Reader>
void SerializationReplicated::deserialize(IColumn & column, Reader && reader) const
{
    auto & column_replicated = assert_cast<ColumnReplicated &>(column);
    reader(*column_replicated.getNestedColumn());
    column_replicated.getIndexes().insertIndex(column_replicated.getNestedColumn()->size() - 1);
}

void SerializationReplicated::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_replicated = assert_cast<const ColumnReplicated &>(column);
    nested->serializeBinary(*column_replicated.getNestedColumn(), column_replicated.getIndexes().getIndexAt(row_num), ostr, settings);
}

void SerializationReplicated::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column)
    {
        nested->deserializeBinary(nested_column, istr, settings);
    });
}

void SerializationReplicated::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_replicated = assert_cast<const ColumnReplicated &>(column);
    nested->serializeTextEscaped(*column_replicated.getNestedColumn(), column_replicated.getIndexes().getIndexAt(row_num), ostr, settings);

}

void SerializationReplicated::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column)
    {
        nested->deserializeTextEscaped(nested_column, istr, settings);
    });
}

void SerializationReplicated::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_replicated = assert_cast<const ColumnReplicated &>(column);
    nested->serializeTextQuoted(*column_replicated.getNestedColumn(), column_replicated.getIndexes().getIndexAt(row_num), ostr, settings);
}

void SerializationReplicated::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column)
    {
        nested->deserializeTextQuoted(nested_column, istr, settings);
    });
}

void SerializationReplicated::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_replicated = assert_cast<const ColumnReplicated &>(column);
    nested->serializeTextCSV(*column_replicated.getNestedColumn(), column_replicated.getIndexes().getIndexAt(row_num), ostr, settings);
}

void SerializationReplicated::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column)
    {
        nested->deserializeTextCSV(nested_column, istr, settings);
    });
}

void SerializationReplicated::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_replicated = assert_cast<const ColumnReplicated &>(column);
    nested->serializeText(*column_replicated.getNestedColumn(), column_replicated.getIndexes().getIndexAt(row_num), ostr, settings);
}

void SerializationReplicated::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column)
    {
        nested->deserializeWholeText(nested_column, istr, settings);
    });
}

void SerializationReplicated::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_replicated = assert_cast<const ColumnReplicated &>(column);
    nested->serializeTextJSON(*column_replicated.getNestedColumn(), column_replicated.getIndexes().getIndexAt(row_num), ostr, settings);
}

void SerializationReplicated::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserialize(column, [&](auto & nested_column)
    {
        nested->deserializeTextJSON(nested_column, istr, settings);
    });
}

void SerializationReplicated::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_replicated = assert_cast<const ColumnReplicated &>(column);
    nested->serializeTextXML(*column_replicated.getNestedColumn(), column_replicated.getIndexes().getIndexAt(row_num), ostr, settings);
}

}
