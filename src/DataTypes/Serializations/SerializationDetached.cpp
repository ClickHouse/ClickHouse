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

void SerializationDetached::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    ColumnPtr concrete_column(column->cloneEmpty());
    auto task = [concrete_column, nested_serialization = nested, limit, format_settings = settings.format_settings](
                    const ColumnBlob::Blob & blob, int)
    { return ColumnBlob::fromBlob(blob, concrete_column, nested_serialization, limit, format_settings); };

    auto column_blob = ColumnPtr(ColumnBlob::create(std::move(task), limit));
    ISerialization::deserializeBinaryBulkWithMultipleStreams(column_blob, limit, settings, state, cache);
    column = column_blob;
}
}
