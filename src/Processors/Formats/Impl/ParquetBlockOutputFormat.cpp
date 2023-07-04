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

void ParquetBlockOutputFormat::consumeStaged()
{
    const size_t columns_num = staging_chunks.at(0).getNumColumns();
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

    ch_column_to_arrow_column->chChunkToArrowTable(arrow_table, staging_chunks, columns_num);

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

    // TODO: calculate row_group_size depending on a number of rows and table size

    // allow slightly bigger than row_group_size to avoid a very small tail row group
    auto status = file_writer->WriteTable(*arrow_table, std::max<size_t>(format_settings.parquet.row_group_rows, staging_rows));

    if (!status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Error while writing a table: {}", status.ToString());
}

void ParquetBlockOutputFormat::consume(Chunk chunk)
{
    /// Do something like SquashingTransform to produce big enough row groups.
    /// Because the real SquashingTransform is only used for INSERT, not for SELECT ... INTO OUTFILE.
    /// The latter doesn't even have a pipeline where a transform could be inserted, so it's more
    /// convenient to do the squashing here.
    staging_rows += chunk.getNumRows();
    staging_bytes += chunk.bytes();
    staging_chunks.push_back(std::move(chunk));
    chassert(staging_chunks.back().getNumColumns() == staging_chunks.front().getNumColumns());
    if (staging_rows < format_settings.parquet.row_group_rows &&
        staging_bytes < format_settings.parquet.row_group_bytes)
    {
        return;
    }
    else
    {
        consumeStaged();
        staging_chunks.clear();
        staging_rows = 0;
        staging_bytes = 0;
    }
}

void ParquetBlockOutputFormat::finalizeImpl()
{
    if (!file_writer && staging_chunks.empty())
    {
        Block header = materializeBlock(getPort(PortKind::Main).getHeader());

        consume(Chunk(header.getColumns(), 0)); // this will make staging_chunks non-empty
    }

    if (!staging_chunks.empty())
    {
        consumeStaged();
        staging_chunks.clear();
        staging_rows = 0;
        staging_bytes = 0;
    }

    auto status = file_writer->Close();
    if (!status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Error while closing a table: {}", status.ToString());
}

void ParquetBlockOutputFormat::resetFormatterImpl()
{
    file_writer.reset();
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
