#include <DataTypes/Serializations/SerializationDetached.h>

#include <Columns/ColumnBlob.h>
#include <IO/VarInt.h>
#include "Common/Exception.h"
#include "Common/assert_cast.h"
#include "Common/typeid_cast.h"
#include "Columns/IColumn.h"
#include "Compression/CompressedWriteBuffer.h"
#include "DataTypes/Serializations/ISerialization.h"

namespace DB
{
SerializationDetached::SerializationDetached(const SerializationPtr & nested_) : nested(nested_)
{
}

void SerializationDetached::serializeBinaryBulk(
    const IColumn & column, WriteBuffer & ostr, [[maybe_unused]] size_t offset, [[maybe_unused]] size_t limit) const
{
    // We will write directly into the uncompressed buffer. It also means we don't need to flush the data written by us here.
    WriteBuffer * uncompressed_buf = &ostr;
    if (auto * compressed_buf = typeid_cast<CompressedWriteBuffer *>(&ostr))
    {
        // Flush all pending data
        ostr.next();
        if (ostr.offset() != 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "SerializationDetached: ostr.offset() is not zero");
        uncompressed_buf = &compressed_buf->getImplBuffer();
    }

    const auto & blob = typeid_cast<const ColumnBlob &>(column).getBlob();
    writeVarUInt(blob.size(), *uncompressed_buf);
    (*uncompressed_buf).write(blob.data(), blob.size());
}

void SerializationDetached::deserializeBinaryBulk(
    IColumn & column, ReadBuffer & istr, [[maybe_unused]] size_t limit, [[maybe_unused]] double avg_value_size_hint) const
{
    // We will read directly from the uncompressed buffer.
    ReadBuffer * uncompressed_buf = &istr;
    if (auto * compressed_buf = typeid_cast<CompressedReadBuffer *>(&istr))
    {
        // Each compressed block is prefixed with the its size, so we know that the compressed buffer should be drained at this point.
        if (istr.available() != 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "SerializationDetached: istr.available() is not zero");
        uncompressed_buf = &compressed_buf->getImplBuffer();
    }

    auto & blob = typeid_cast<ColumnBlob &>(column).getBlob();
    size_t bytes = 0;
    readVarUInt(bytes, *uncompressed_buf);
    blob.resize(bytes);
    (*uncompressed_buf).readStrict(blob.data(), blob.size());
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
