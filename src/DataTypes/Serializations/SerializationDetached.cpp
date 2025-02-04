#include <DataTypes/Serializations/SerializationDetached.h>

#include <Columns/ColumnBlob.h>
#include <IO/VarInt.h>
#include "Common/assert_cast.h"
#include "Columns/IColumn.h"
#include "DataTypes/Serializations/ISerialization.h"

namespace DB
{
SerializationDetached::SerializationDetached(const SerializationPtr & nested_) : nested(nested_)
{
}

// void SerializationDetached::enumerateStreams(
//     EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
// {
//     nested->enumerateStreams(settings, callback, data);
// }

// void SerializationDetached::serializeBinaryBulkStatePrefix(
//     const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
// {
//     auto nested_column = typeid_cast<const ColumnBlob &>(column).getNestedColumn();
//     nested_column = nested_column->convertToFullColumnIfConst();
//     nested->serializeBinaryBulkStatePrefix(*nested_column, settings, state);
// }

void SerializationDetached::serializeBinaryBulk(
    const IColumn & column, WriteBuffer & ostr, [[maybe_unused]] size_t offset, [[maybe_unused]] size_t limit) const
{
    const auto & blob = typeid_cast<const ColumnBlob &>(column).getBlob();
    writeVarUInt(blob.size(), ostr);
    ostr.write(blob.data(), blob.size());
}

void SerializationDetached::deserializeBinaryBulk(
    IColumn & column, ReadBuffer & istr, [[maybe_unused]] size_t limit, [[maybe_unused]] double avg_value_size_hint) const
{
    auto & blob = typeid_cast<ColumnBlob &>(column).getBlob();
    size_t bytes = 0;
    readVarUInt(bytes, istr);
    blob.resize(bytes);
    istr.readStrict(blob.data(), blob.size());
}

// void SerializationDetached::serializeBinaryBulkWithMultipleStreams(
//     const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
// {
//     auto nested_column = typeid_cast<const ColumnBlob &>(column).getNestedColumn();
//     nested_column = nested_column->convertToFullColumnIfConst();
//     nested->serializeBinaryBulkWithMultipleStreams(*nested_column, offset, limit, settings, state);
// }

// void SerializationDetached::serializeBinaryBulkStateSuffix(
//     SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
// {
//     nested->serializeBinaryBulkStateSuffix(settings, state);
// }

// void SerializationDetached::deserializeBinaryBulkStatePrefix(
//     DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
// {
//     nested->deserializeBinaryBulkStatePrefix(settings, state, cache);
// }

void SerializationDetached::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    ColumnPtr concrete_column(column->cloneEmpty());
    auto task = [concrete_column, nested_serialization = nested, limit](const ColumnBlob::Blob & blob, int)
    {
        // TODO(nickitat): fix format settings
        return ColumnBlob::fromBlob(blob, concrete_column, nested_serialization, limit, std::nullopt);
    };

    auto column_blob = ColumnPtr(ColumnBlob::create(std::move(task), limit));
    ISerialization::deserializeBinaryBulkWithMultipleStreams(column_blob, limit, settings, state, cache);
    column = column_blob;
}
}
