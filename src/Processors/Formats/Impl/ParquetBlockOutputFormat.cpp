#include "ParquetBlockOutputFormat.h"

#if USE_PARQUET

#include <Formats/FormatFactory.h>
#include <parquet/arrow/writer.h>
#include "ArrowBufferedStreams.h"
#include "CHColumnToArrowColumn.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

parquet::ParquetVersion::type getParquetVersion(const FormatSettings & settings)
{
    switch (settings.parquet.output_version)
    {
        case FormatSettings::ParquetVersion::V1_0:
            return parquet::ParquetVersion::PARQUET_1_0;
        case FormatSettings::ParquetVersion::V2_4:
            return parquet::ParquetVersion::PARQUET_2_4;
        case FormatSettings::ParquetVersion::V2_6:
            return parquet::ParquetVersion::PARQUET_2_6;
        case FormatSettings::ParquetVersion::V2_LATEST:
            return parquet::ParquetVersion::PARQUET_2_LATEST;
    }
}

parquet::Compression::type getParquetCompression(FormatSettings::ParquetCompression method)
{
    if (method == FormatSettings::ParquetCompression::NONE)
        return parquet::Compression::type::UNCOMPRESSED;

#if USE_SNAPPY
    if (method == FormatSettings::ParquetCompression::SNAPPY)
        return parquet::Compression::type::SNAPPY;
#endif

#if USE_BROTLI
    if (method == FormatSettings::ParquetCompression::BROTLI)
        return parquet::Compression::type::BROTLI;
#endif

    if (method == FormatSettings::ParquetCompression::ZSTD)
        return parquet::Compression::type::ZSTD;

    if (method == FormatSettings::ParquetCompression::LZ4)
        return parquet::Compression::type::LZ4;

    if (method == FormatSettings::ParquetCompression::GZIP)
        return parquet::Compression::type::GZIP;

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported compression method");
}

}

ParquetBlockOutputFormat::ParquetBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_), format_settings{format_settings_}
{
}

void ParquetBlockOutputFormat::consume(Chunk chunk)
{
    /// Do something like SquashingTransform to produce big enough row groups.
    /// Because the real SquashingTransform is only used for INSERT, not for SELECT ... INTO OUTFILE.
    /// The latter doesn't even have a pipeline where a transform could be inserted, so it's more
    /// convenient to do the squashing here.

    appendToAccumulatedChunk(std::move(chunk));

    if (!accumulated_chunk)
        return;

    const size_t target_rows = std::max(static_cast<UInt64>(1), format_settings.parquet.row_group_rows);

    if (accumulated_chunk.getNumRows() < target_rows &&
        accumulated_chunk.bytes() < format_settings.parquet.row_group_bytes)
        return;

    /// Increase row group size slightly (by < 2x) to avoid adding a small row groups for the
    /// remainder of the new chunk.
    /// E.g. suppose input chunks are 70K rows each, and max_rows = 1M. Then we'll have
    /// getNumRows() = 1.05M. We want to write all 1.05M as one row group instead of 1M and 0.05M.
    size_t num_row_groups = std::max(static_cast<UInt64>(1), accumulated_chunk.getNumRows() / target_rows);
    size_t row_group_size = (accumulated_chunk.getNumRows() - 1) / num_row_groups + 1; // round up

    write(std::move(accumulated_chunk), row_group_size);
    accumulated_chunk.clear();
}

void ParquetBlockOutputFormat::finalizeImpl()
{
    if (accumulated_chunk)
        write(std::move(accumulated_chunk), format_settings.parquet.row_group_rows);

    if (!file_writer)
    {
        Block header = materializeBlock(getPort(PortKind::Main).getHeader());
        write(Chunk(header.getColumns(), 0), 1);
    }

    auto status = file_writer->Close();
    if (!status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Error while closing a table: {}", status.ToString());
}

void ParquetBlockOutputFormat::resetFormatterImpl()
{
    file_writer.reset();
}

void ParquetBlockOutputFormat::appendToAccumulatedChunk(Chunk chunk)
{
    if (!accumulated_chunk)
    {
        accumulated_chunk = std::move(chunk);
        return;
    }
    chassert(accumulated_chunk.getNumColumns() == chunk.getNumColumns());
    accumulated_chunk.append(chunk);
}

void ParquetBlockOutputFormat::write(Chunk chunk, size_t row_group_size)
{
    const size_t columns_num = chunk.getNumColumns();
    std::shared_ptr<arrow::Table> arrow_table;

    if (!ch_column_to_arrow_column)
    {
        const Block & header = getPort(PortKind::Main).getHeader();
        ch_column_to_arrow_column = std::make_unique<CHColumnToArrowColumn>(
            header,
            "Parquet",
            false,
            format_settings.parquet.output_string_as_string,
            format_settings.parquet.output_fixed_string_as_fixed_byte_array);
    }

    ch_column_to_arrow_column->chChunkToArrowTable(arrow_table, chunk, columns_num);

    if (!file_writer)
    {
        auto sink = std::make_shared<ArrowBufferedOutputStream>(out);

        parquet::WriterProperties::Builder builder;
        builder.version(getParquetVersion(format_settings));
        builder.compression(getParquetCompression(format_settings.parquet.output_compression_method));

        parquet::ArrowWriterProperties::Builder writer_props_builder;
        if (format_settings.parquet.output_compliant_nested_types)
            writer_props_builder.enable_compliant_nested_types();
        else
            writer_props_builder.disable_compliant_nested_types();

        auto result = parquet::arrow::FileWriter::Open(
            *arrow_table->schema(),
            arrow::default_memory_pool(),
            sink,
            builder.build(),
            writer_props_builder.build());
        if (!result.ok())
            throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Error while opening a table: {}", result.status().ToString());
        file_writer = std::move(result.ValueOrDie());
    }

    auto status = file_writer->WriteTable(*arrow_table, row_group_size);

    if (!status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Error while writing a table: {}", status.ToString());
}

void registerOutputFormatParquet(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Parquet",
        [](WriteBuffer & buf,
           const Block & sample,
           const FormatSettings & format_settings)
        {
            return std::make_shared<ParquetBlockOutputFormat>(buf, sample, format_settings);
        });
    factory.markFormatHasNoAppendSupport("Parquet");
}

}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatParquet(FormatFactory &)
{
}
}

#endif
