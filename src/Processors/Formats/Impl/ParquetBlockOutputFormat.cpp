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
        auto props = builder.build();
        auto result = parquet::arrow::FileWriter::Open(
            *arrow_table->schema(),
            arrow::default_memory_pool(),
            sink,
            props);
        if (!result.ok())
            throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Error while opening a table: {}", result.status().ToString());
        file_writer = std::move(result.ValueOrDie());
    }

    // TODO: calculate row_group_size depending on a number of rows and table size
    auto status = file_writer->WriteTable(*arrow_table, format_settings.parquet.row_group_size);

    if (!status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Error while writing a table: {}", status.ToString());
}

void ParquetBlockOutputFormat::finalizeImpl()
{
    if (!file_writer)
    {
        const Block & header = getPort(PortKind::Main).getHeader();

        consume(Chunk(header.getColumns(), 0));
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
