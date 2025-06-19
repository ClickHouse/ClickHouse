#include <DataTypes/Serializations/SerializationDetached.h>

#include <Columns/ColumnBLOB.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/VarInt.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

namespace
{

struct DisableCompressionInScope
{
    explicit DisableCompressionInScope(DB::WriteBuffer & buffer_)
        : buffer(buffer_)
    {
        if (auto * compressed_buf = typeid_cast<DB::CompressedWriteBuffer *>(&buffer))
        {
            original_codec = compressed_buf->getCodec();
            compressed_buf->setCodec(DB::CompressionCodecFactory::instance().get("NONE"));
        }
    }

    ~DisableCompressionInScope()
    {
        if (auto * compressed_buf = typeid_cast<DB::CompressedWriteBuffer *>(&buffer))
            compressed_buf->setCodec(original_codec);
    }

    DB::WriteBuffer & buffer;
    DB::CompressionCodecPtr original_codec;
};
}

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

SerializationDetached::SerializationDetached(const SerializationPtr & nested_) : nested(nested_)
{
}

void SerializationDetached::serializeBinaryBulk(
    const IColumn & column, WriteBuffer & ostr, [[maybe_unused]] size_t offset, [[maybe_unused]] size_t limit) const
{
    DisableCompressionInScope codec_switcher(ostr);

    const auto & blob = typeid_cast<const ColumnBLOB &>(column).getBLOB();
    writeVarUInt(blob.size(), ostr);
    ostr.write(blob.data(), blob.size());
}

void SerializationDetached::deserializeBinaryBulk(
    IColumn & column,
    ReadBuffer & istr,
    [[maybe_unused]] size_t rows_offset,
    [[maybe_unused]] size_t limit,
    [[maybe_unused]] double avg_value_size_hint) const
{
    auto & blob = typeid_cast<ColumnBLOB &>(column).getBLOB();
    size_t bytes = 0;
    readVarUInt(bytes, istr);
    blob.resize(bytes);
    istr.readStrict(blob.data(), blob.size());
}

void SerializationDetached::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto task = [wrapped_column = column,
                 nested_serialization = nested,
                 limit,
                 format_settings = settings.format_settings,
                 avg_value_size_hint = settings.avg_value_size_hint](const ColumnBLOB::BLOB & blob)
    {
        // In case of alias columns, `column` might be a reference to the same column for a number of calls to this function.
        // To avoid deserializing into the same column multiple times, we clone the column here one more time.
        return ColumnBLOB::fromBLOB(blob, wrapped_column->cloneEmpty(), nested_serialization, limit, format_settings, avg_value_size_hint);
    };

    auto column_blob = ColumnPtr(ColumnBLOB::create(std::move(task), column, limit));
    ISerialization::deserializeBinaryBulkWithMultipleStreams(column_blob, rows_offset, limit, settings, state, cache);
    column = column_blob;
}

[[noreturn]] void SerializationDetached::throwInapplicable()
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnBLOB should be converted to a regular column before usage");
}
}
