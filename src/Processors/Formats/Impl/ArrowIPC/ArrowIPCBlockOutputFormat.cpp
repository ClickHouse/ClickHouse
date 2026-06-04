#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCBlockOutputFormat.h>

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h>
#include <Processors/Port.h>
#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <IO/NetUtils.h>

namespace DB
{

namespace
{
constexpr std::string_view ARROW_MAGIC = "ARROW1";
}

ArrowIPCBlockOutputFormat::ArrowIPCBlockOutputFormat(
    WriteBuffer & out_, SharedHeader header_, bool stream_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_), stream(stream_), format_settings(format_settings_)
{
    const Block & header = *header_;
    column_names.reserve(header.columns());
    column_types.reserve(header.columns());
    for (const auto & column : header)
    {
        column_names.push_back(column.name);
        column_types.push_back(column.type);
    }
    message_writer.emplace(out);
    encoder = std::make_unique<ArrowIPC::RecordBatchEncoder>(format_settings);
}

void ArrowIPCBlockOutputFormat::writeSchemaIfNeeded()
{
    if (schema_written)
        return;

    /// The file format begins with "ARROW1" + 2 padding bytes so the first message body stays aligned.
    if (!stream)
        message_writer->writeRaw("ARROW1\0\0", ARROW_MAGIC.size() + 2);

    flatbuffers::FlatBufferBuilder builder;
    ArrowIPC::buildSchemaMessage(builder, column_names, column_types, format_settings);
    message_writer->writeMessage(builder.GetBufferPointer(), builder.GetSize(), nullptr, 0);
    schema_written = true;
}

void ArrowIPCBlockOutputFormat::consume(Chunk chunk)
{
    writeSchemaIfNeeded();

    const size_t num_rows = chunk.getNumRows();
    auto batch = encoder->encode(chunk.getColumns(), column_types, num_rows);

    flatbuffers::FlatBufferBuilder builder;
    auto nodes_vec = builder.CreateVectorOfStructs(batch.nodes.data(), batch.nodes.size());
    auto buffers_vec = builder.CreateVectorOfStructs(batch.buffers.data(), batch.buffers.size());
    auto record_batch = ArrowIPC::flatbuf::CreateRecordBatch(builder, batch.num_rows, nodes_vec, buffers_vec);
    auto message = ArrowIPC::flatbuf::CreateMessage(
        builder,
        ArrowIPC::flatbuf::MetadataVersion_V5,
        ArrowIPC::flatbuf::MessageHeader_RecordBatch,
        record_batch.Union(),
        static_cast<int64_t>(batch.body.size()));
    builder.Finish(message);

    auto written = message_writer->writeMessage(
        builder.GetBufferPointer(), builder.GetSize(), batch.body.data(), batch.body.size());

    if (!stream)
        record_blocks.push_back({written.offset, written.metadata_length, written.body_length});
}

void ArrowIPCBlockOutputFormat::finalizeImpl()
{
    /// Make sure even an empty result produces a valid stream/file (schema, then EOS or footer).
    writeSchemaIfNeeded();

    if (stream)
    {
        message_writer->writeEOS();
        return;
    }

    /// File format trailer: <footer FlatBuffer> <int32 footer length LE> <"ARROW1">.
    flatbuffers::FlatBufferBuilder builder;
    ArrowIPC::buildFooter(builder, column_names, column_types, format_settings, record_blocks);
    out.write(reinterpret_cast<const char *>(builder.GetBufferPointer()), builder.GetSize());

    int32_t footer_length = DB::toLittleEndian(static_cast<int32_t>(builder.GetSize()));
    out.write(reinterpret_cast<const char *>(&footer_length), sizeof(footer_length));
    out.write(ARROW_MAGIC.data(), ARROW_MAGIC.size());
}

void ArrowIPCBlockOutputFormat::resetFormatterImpl()
{
    message_writer.emplace(out);
    schema_written = false;
    record_blocks.clear();
}

}

#endif
