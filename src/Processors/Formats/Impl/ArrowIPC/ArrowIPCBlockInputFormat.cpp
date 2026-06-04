#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCBlockInputFormat.h>

#if USE_ARROW

#include <Processors/Port.h>
#include <IO/ReadBuffer.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/castColumn.h>
#include <Common/assert_cast.h>
#include <boost/algorithm/string/case_conv.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
    extern const int THERE_IS_NO_COLUMN;
}

ArrowIPCBlockInputFormat::ArrowIPCBlockInputFormat(
    ReadBuffer & in_, SharedHeader header_, bool stream_, const FormatSettings & format_settings_)
    : IInputFormat(header_, &in_)
    , stream(stream_)
    , message_reader(in_)
    , block_missing_values(getPort().getHeader().columns())
    , format_settings(format_settings_)
{
}

void ArrowIPCBlockInputFormat::prepareReader()
{
    if (!stream)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Native Arrow IPC reader supports only the streaming format so far");

    ArrowIPC::MessageReader::Message msg;
    if (!message_reader.readNextMessage(msg))
        throw Exception(ErrorCodes::INCORRECT_DATA, "The Arrow stream is empty");

    if (msg.header->header_type() != ArrowIPC::flatbuf::MessageHeader_Schema)
        throw Exception(ErrorCodes::INCORRECT_DATA, "The first Arrow IPC message must be the schema");

    arrow_schema = ArrowIPC::parseSchema(*msg.header->header_as_Schema());
    /// The schema message has no body, but be defensive in case a producer emits a (zero-length) one.
    message_reader.skipBody(msg.body_length);

    decoder = std::make_unique<ArrowIPC::RecordBatchDecoder>(*arrow_schema, format_settings);
    prepared = true;
}

Chunk ArrowIPCBlockInputFormat::buildChunk(std::vector<ArrowIPC::RecordBatchDecoder::DecodedColumn> & decoded, size_t num_rows)
{
    const Block & header = getPort().getHeader();

    /// Map decoded column name -> index (lower-cased when case-insensitive matching is on).
    const bool case_insensitive = format_settings.arrow.case_insensitive_column_matching;
    std::unordered_map<String, size_t> name_to_index;
    name_to_index.reserve(decoded.size());
    for (size_t i = 0; i < decoded.size(); ++i)
    {
        String key = decoded[i].name;
        if (case_insensitive)
            boost::to_lower(key);
        name_to_index.emplace(std::move(key), i);
    }

    Columns columns;
    columns.reserve(header.columns());
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(i);
        String key = header_column.name;
        if (case_insensitive)
            boost::to_lower(key);

        auto it = name_to_index.find(key);
        if (it != name_to_index.end())
        {
            auto & src = decoded[it->second];
            ColumnWithTypeAndName arg(src.column, src.type, src.name);
            columns.push_back(castColumn(arg, header_column.type));
        }
        else if (format_settings.arrow.allow_missing_columns)
        {
            auto column = header_column.type->createColumn();
            column->insertManyDefaults(num_rows);
            if (format_settings.defaults_for_omitted_fields)
                block_missing_values.setBits(i, num_rows);
            columns.push_back(std::move(column));
        }
        else
        {
            throw Exception(
                ErrorCodes::THERE_IS_NO_COLUMN,
                "Column '{}' is not present in the Arrow IPC data", header_column.name);
        }
    }

    return Chunk(std::move(columns), num_rows);
}

Chunk ArrowIPCBlockInputFormat::read()
{
    block_missing_values.clear();

    if (!prepared)
        prepareReader();

    if (is_stopped)
        return {};

    const size_t batch_start = in->count();

    while (true)
    {
        ArrowIPC::MessageReader::Message msg;
        if (!message_reader.readNextMessage(msg))
        {
            /// Drain the buffer so the underlying stream is fully consumed (HTTP keepalive, etc.).
            in->eof();
            return {};
        }

        switch (msg.header->header_type())
        {
            case ArrowIPC::flatbuf::MessageHeader_RecordBatch:
            {
                const auto * batch = msg.header->header_as_RecordBatch();
                const size_t num_rows = static_cast<size_t>(batch->length());

                if (need_only_count)
                {
                    message_reader.skipBody(msg.body_length);
                    return getChunkForCount(num_rows);
                }

                message_reader.readBody(msg.body_length, body_buffer);
                auto decoded = decoder->decodeBatch(*batch, body_buffer);
                Chunk chunk = buildChunk(decoded, num_rows);

                const size_t batch_end = in->count();
                if (batch_end > batch_start)
                    approx_bytes_read_for_chunk = batch_end - batch_start;
                return chunk;
            }
            case ArrowIPC::flatbuf::MessageHeader_DictionaryBatch:
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED, "Native Arrow IPC reader does not support dictionaries yet");
            case ArrowIPC::flatbuf::MessageHeader_Schema:
                /// A redundant schema message; it carries no body. Ignore and continue.
                message_reader.skipBody(msg.body_length);
                continue;
            default:
                message_reader.skipBody(msg.body_length);
                continue;
        }
    }
}

void ArrowIPCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();
    prepared = false;
    decoder.reset();
    arrow_schema.reset();
    block_missing_values.clear();
    approx_bytes_read_for_chunk = 0;
}

}

#endif
