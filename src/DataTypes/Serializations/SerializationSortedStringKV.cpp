#include <DataTypes/Serializations/SerializationSortedStringKV.h>
#include <Storages/MergeTree/SSTFileUtil.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int LOGICAL_ERROR;
}

void SerializationSortedStringKV::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    /// Only enumerate the offsets stream; the SST data stream is
    /// created independently by MergeTreeDataPartWriter.
    settings.path.push_back(Substream::Regular);
    settings.path.back().data = data;
    callback(settings.path);
    settings.path.pop_back();
}

void SerializationSortedStringKV::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (settings.native_format)
    {
        SerializationTuple::serializeBinaryBulkStatePrefix(column, settings, state);
        return;
    }
    state = std::make_shared<SerializeBinaryBulkStateSST>();
}

void SerializationSortedStringKV::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (settings.native_format)
    {
        SerializationTuple::serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
        return;
    }

    const auto & tuple_col = assert_cast<const ColumnTuple &>(column);
    const auto & key_col = assert_cast<const ColumnString &>(tuple_col.getColumn(0));
    const auto & value_col = assert_cast<const ColumnString &>(tuple_col.getColumn(1));

    size_t size = key_col.size();
    size_t end = limit ? std::min(offset + limit, size) : size;

    settings.path.push_back(Substream::Regular);
    WriteBuffer * offsets_stream = settings.getter(settings.path);
    settings.path.pop_back();

    /// Skip when no data to write
    if (offset >= end)
        return;

    auto * sst_state = checkAndGetState<SerializeBinaryBulkStateSST>(state);

    /// Lazy init SSTFileWriter on first actual write
    if (!sst_state->sst_file_writer)
    {
        if (settings.sst_write_stream_getter)
        {
            auto * sst_stream = settings.sst_write_stream_getter(settings.path);
            if (sst_stream)
                sst_state->sst_file_writer = &sst_stream->getSSTWriter();
        }

        if (!sst_state->sst_file_writer)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Missing SST writer for SerializationSortedStringKV "
                "(sst_write_stream_getter not set or returned nullptr)");
    }

    for (size_t i = offset; i < end; ++i)
    {
        const auto key = key_col.getDataAt(i);
        const auto val = value_col.getDataAt(i);

        sst_state->sst_file_writer->put(
            rocksdb::Slice(key),
            rocksdb::Slice(val));
    }

    /// Write row offset at granule boundary for seek
    if (settings.mark_on_start && offsets_stream)
        writeIntBinary(sst_state->sst_file_writer->getWrittenRowCount(), *offsets_stream);

    sst_state->sst_file_writer->addWrittenRowCount(static_cast<UInt64>(end - offset));
}

void SerializationSortedStringKV::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (settings.native_format)
        SerializationTuple::serializeBinaryBulkStateSuffix(settings, state);
}

void SerializationSortedStringKV::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    if (settings.native_format)
    {
        SerializationTuple::deserializeBinaryBulkStatePrefix(settings, state, cache);
        return;
    }
    state = std::make_shared<DeserializeBinaryBulkStateSortedStringKV>();
}

void SerializationSortedStringKV::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (settings.native_format)
    {
        SerializationTuple::deserializeBinaryBulkWithMultipleStreams(column, rows_offset, limit, settings, state, cache);
        return;
    }

    auto mutable_column = column->assumeMutable();
    auto & tuple_col = assert_cast<ColumnTuple &>(*mutable_column);
    auto & key_col = assert_cast<ColumnString &>(tuple_col.getColumn(0));
    auto & value_col = assert_cast<ColumnString &>(tuple_col.getColumn(1));

    auto * sst_state = checkAndGetState<DeserializeBinaryBulkStateSortedStringKV>(state);

    /// Lazy init SST iterator
    if (!sst_state->sst_file_iterator)
    {
        auto * sst_file_read_stream = settings.sst_read_stream_getter ? settings.sst_read_stream_getter(settings.path) : nullptr;
        if (sst_file_read_stream)
        {
            auto * sst_stream = dynamic_cast<ReadBufferFromFileBase *>(sst_file_read_stream->getDataBuffer());
            if (!sst_stream)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "SST data stream must provide a valid ReadBufferFromFileBase");

            uint64_t base_offset = 0;
            auto sst_size = sst_stream->getFileSize();

            /// Handle empty SST file (e.g. part with zero rows after DELETE)
            if (sst_size == 0)
            {
                column = std::move(mutable_column);
                return;
            }

            sst_state->sst_file_reader = std::make_shared<SSTFileReader>(sst_stream, base_offset, sst_size);

            rocksdb::ReadOptions read_opts;
            read_opts.fill_cache = true;
            sst_state->sst_file_iterator = sst_state->sst_file_reader->newIterator(read_opts);
            sst_state->sst_file_iterator->SeekToFirst();
            if (!sst_state->sst_file_iterator->status().ok())
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "SST iterator SeekToFirst error: {}",
                    sst_state->sst_file_iterator->status().ToString());
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No SST read stream available");
        }
    }

    /// New granule: seek to offset from .offsets.bin
    if (!settings.continuous_reading)
    {
        settings.path.push_back(Substream::Regular);
        ReadBuffer * offsets_stream = settings.getter(settings.path);
        settings.path.pop_back();

        if (!offsets_stream)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No offsets stream available");

        UInt64 target_row = 0;
        readIntBinary(target_row, *offsets_stream);

        if (target_row < sst_state->current_row_position)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cannot seek backwards in SST iterator: current_row_position={}, target_row={}",
                sst_state->current_row_position, target_row);

        while (sst_state->current_row_position < target_row && sst_state->sst_file_iterator->Valid())
        {
            sst_state->sst_file_iterator->Next();
            if (!sst_state->sst_file_iterator->status().ok())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "SST iterator seek error: {}", sst_state->sst_file_iterator->status().ToString());
            ++sst_state->current_row_position;
        }

        if (sst_state->current_row_position < target_row)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "SST iterator reached end before target row: current={}, target={}",
                sst_state->current_row_position, target_row);
    }

    size_t rows_read = 0;
    auto & iter = sst_state->sst_file_iterator;
    while (iter->Valid() && rows_read < limit)
    {
        key_col.insertData(iter->key().data(), iter->key().size());
        value_col.insertData(iter->value().data(), iter->value().size());
        ++rows_read;
        ++sst_state->current_row_position;

        iter->Next();
        if (!iter->status().ok())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "SST iterator error: {}", iter->status().ToString());
    }

    column = std::move(mutable_column);
}

}

