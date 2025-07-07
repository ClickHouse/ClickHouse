#include "ArrowBlockOutputFormat.h"

#if USE_ARROW

#include <Formats/FormatFactory.h>
#include <Processors/Port.h>

#include "ArrowBufferedStreams.h"
#include "CHColumnToArrowColumn.h"

#include <arrow/ipc/writer.h>
#include <arrow/table.h>
#include <arrow/result.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

namespace
{

arrow::Compression::type getArrowCompression(FormatSettings::ArrowCompression method)
{
    switch (method)
    {
        case FormatSettings::ArrowCompression::NONE:
            return arrow::Compression::type::UNCOMPRESSED;
        case FormatSettings::ArrowCompression::ZSTD:
            return arrow::Compression::type::ZSTD;
        case FormatSettings::ArrowCompression::LZ4_FRAME:
            return arrow::Compression::type::LZ4_FRAME;
    }
}

}

ArrowBlockOutputFormat::ArrowBlockOutputFormat(WriteBuffer & out_, const Block & header_, bool stream_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_)
    , stream{stream_}
    , format_settings{format_settings_}
{
}

void ArrowBlockOutputFormat::consume(Chunk chunk)
{
    const size_t columns_num = chunk.getNumColumns();
    std::shared_ptr<arrow::Table> arrow_table;

    if (!ch_column_to_arrow_column)
    {
        const Block & header = getPort(PortKind::Main).getHeader();
        ch_column_to_arrow_column = std::make_unique<CHColumnToArrowColumn>(
            header,
            "Arrow",
            CHColumnToArrowColumn::Settings
            {
                format_settings.arrow.output_string_as_string,
                format_settings.arrow.output_fixed_string_as_fixed_byte_array,
                format_settings.arrow.low_cardinality_as_dictionary,
                format_settings.arrow.use_signed_indexes_for_dictionary,
                format_settings.arrow.use_64_bit_indexes_for_dictionary
            });
    }

    auto chunks = std::vector<Chunk>();
    chunks.push_back(std::move(chunk));
    ch_column_to_arrow_column->chChunkToArrowTable(arrow_table, chunks, columns_num);

    if (!writer)
        prepareWriter(arrow_table->schema());

    // TODO: calculate row_group_size depending on a number of rows and table size
    auto status = writer->WriteTable(*arrow_table, format_settings.arrow.row_group_size);

    if (!status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
            "Error while writing a table: {}", status.ToString());
}

void ArrowBlockOutputFormat::finalizeImpl()
{
    if (!writer)
    {
        Block header = materializeBlock(getPort(PortKind::Main).getHeader());

        consume(Chunk(header.getColumns(), 0));
    }

    auto status = writer->Close();
    if (!status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
            "Error while closing a table: {}", status.ToString());
}

void ArrowBlockOutputFormat::resetFormatterImpl()
{
    writer.reset();
    arrow_ostream.reset();
}

void ArrowBlockOutputFormat::prepareWriter(const std::shared_ptr<arrow::Schema> & schema)
{
    arrow_ostream = std::make_shared<ArrowBufferedOutputStream>(out);
    arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchWriter>> writer_status;
    arrow::ipc::IpcWriteOptions options = arrow::ipc::IpcWriteOptions::Defaults();
    options.codec = *arrow::util::Codec::Create(getArrowCompression(format_settings.arrow.output_compression_method));
    options.emit_dictionary_deltas = true;

    // TODO: should we use arrow::ipc::IpcOptions::alignment?
    if (stream)
        writer_status = arrow::ipc::MakeStreamWriter(arrow_ostream.get(), schema, options);
    else
        writer_status = arrow::ipc::MakeFileWriter(arrow_ostream.get(), schema,options);

    if (!writer_status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
            "Error while opening a table writer: {}", writer_status.status().ToString());

    writer = *writer_status;
}

void registerOutputFormatArrow(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Arrow",
        [](WriteBuffer & buf,
           const Block & sample,
           const FormatSettings & format_settings)
        {
            return std::make_shared<ArrowBlockOutputFormat>(buf, sample, false, format_settings);
        });
    factory.markFormatHasNoAppendSupport("Arrow");
    factory.markOutputFormatNotTTYFriendly("Arrow");
    factory.setContentType("Arrow", "application/octet-stream");

    factory.registerOutputFormat(
        "ArrowStream",
        [](WriteBuffer & buf,
           const Block & sample,
           const FormatSettings & format_settings)
        {
            return std::make_shared<ArrowBlockOutputFormat>(buf, sample, true, format_settings);
        });
    factory.markFormatHasNoAppendSupport("ArrowStream");
    factory.markOutputFormatPrefersLargeBlocks("ArrowStream");
    factory.markOutputFormatNotTTYFriendly("ArrowStream");
    factory.setContentType("ArrowStream", "application/octet-stream");
}

}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatArrow(FormatFactory &)
{
}
}

#endif
