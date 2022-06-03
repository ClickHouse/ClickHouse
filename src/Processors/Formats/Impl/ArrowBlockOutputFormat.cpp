#include "ArrowBlockOutputFormat.h"

#if USE_ARROW

#include <Formats/FormatFactory.h>
#include <arrow/ipc/writer.h>
#include <arrow/table.h>
#include <arrow/result.h>
#include "ArrowBufferedStreams.h"
#include "CHColumnToArrowColumn.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

ArrowBlockOutputFormat::ArrowBlockOutputFormat(WriteBuffer & out_, const Block & header_, bool stream_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_)
    , stream{stream_}
    , format_settings{format_settings_}
    , arrow_ostream{std::make_shared<ArrowBufferedOutputStream>(out_)}
{
}

void ArrowBlockOutputFormat::consume(Chunk chunk)
{
    const size_t columns_num = chunk.getNumColumns();
    std::shared_ptr<arrow::Table> arrow_table;

    if (!ch_column_to_arrow_column)
    {
        const Block & header = getPort(PortKind::Main).getHeader();
        ch_column_to_arrow_column
            = std::make_unique<CHColumnToArrowColumn>(header, "Arrow", format_settings.arrow.low_cardinality_as_dictionary, format_settings.arrow.output_string_as_string);
    }

    ch_column_to_arrow_column->chChunkToArrowTable(arrow_table, chunk, columns_num);

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
        const Block & header = getPort(PortKind::Main).getHeader();

        consume(Chunk(header.getColumns(), 0));
    }

    auto status = writer->Close();
    if (!status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
            "Error while closing a table: {}", status.ToString());
}

void ArrowBlockOutputFormat::prepareWriter(const std::shared_ptr<arrow::Schema> & schema)
{
    arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchWriter>> writer_status;

    // TODO: should we use arrow::ipc::IpcOptions::alignment?
    if (stream)
        writer_status = arrow::ipc::MakeStreamWriter(arrow_ostream.get(), schema);
    else
        writer_status = arrow::ipc::MakeFileWriter(arrow_ostream.get(), schema);

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
           const RowOutputFormatParams &,
           const FormatSettings & format_settings)
        {
            return std::make_shared<ArrowBlockOutputFormat>(buf, sample, false, format_settings);
        });
    factory.markFormatHasNoAppendSupport("Arrow");

    factory.registerOutputFormat(
        "ArrowStream",
        [](WriteBuffer & buf,
           const Block & sample,
           const RowOutputFormatParams &,
           const FormatSettings & format_settings)
        {
            return std::make_shared<ArrowBlockOutputFormat>(buf, sample, true, format_settings);
        });
    factory.markFormatHasNoAppendSupport("ArrowStream");
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
