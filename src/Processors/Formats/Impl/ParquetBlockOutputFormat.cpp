#include "ParquetBlockOutputFormat.h"

#if USE_PARQUET

// TODO: clean includes
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <Core/callOnTypeIndex.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteHelpers.h>
#include <arrow/api.h>
#include <arrow/util/decimal.h>
#include <arrow/util/memory.h>
#include <parquet/arrow/writer.h>
#include <parquet/deprecated_io.h>
#include "CHColumnToArrowColumn.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
    extern const int UNKNOWN_TYPE;
}

ParquetBlockOutputFormat::ParquetBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_), format_settings{format_settings_}
{
}

class OstreamOutputStream : public arrow::io::OutputStream
{
public:
    explicit OstreamOutputStream(WriteBuffer & ostr_) : ostr(ostr_) { is_open = true; }
    ~OstreamOutputStream() override = default;

    // FileInterface
    ::arrow::Status Close() override
    {
        is_open = false;
        return ::arrow::Status::OK();
    }

    ::arrow::Status Tell(int64_t* position) const override
    {
        *position = total_length;
        return ::arrow::Status::OK();
    }

    bool closed() const override { return !is_open; }

    // Writable
    ::arrow::Status Write(const void* data, int64_t length) override
    {
        ostr.write(reinterpret_cast<const char *>(data), length);
        total_length += length;
        return ::arrow::Status::OK();
    }

private:
    WriteBuffer & ostr;
    int64_t total_length = 0;
    bool is_open = false;

    PARQUET_DISALLOW_COPY_AND_ASSIGN(OstreamOutputStream);
};


void ParquetBlockOutputFormat::consume(Chunk chunk)
{
    const Block & header = getPort(PortKind::Main).getHeader();
    const size_t columns_num = chunk.getNumColumns();
    std::shared_ptr<arrow::Table> arrow_table;

    CHColumnToArrowColumn::CHChunkToArrowTable(arrow_table, header, chunk, columns_num, "Parquet");

    auto sink = std::make_shared<OstreamOutputStream>(out);

    if (!file_writer)
    {
        parquet::WriterProperties::Builder builder;
#if USE_SNAPPY
        builder.compression(parquet::Compression::SNAPPY);
#endif
        auto props = builder.build();
        auto status = parquet::arrow::FileWriter::Open(
            *arrow_table->schema(),
            arrow::default_memory_pool(),
            sink,
            props, /*parquet::default_writer_properties(),*/
            &file_writer);
        if (!status.ok())
            throw Exception{"Error while opening a table: " + status.ToString(), ErrorCodes::UNKNOWN_EXCEPTION};
    }

    // TODO: calculate row_group_size depending on a number of rows and table size
    auto status = file_writer->WriteTable(*arrow_table, format_settings.parquet.row_group_size);

    if (!status.ok())
        throw Exception{"Error while writing a table: " + status.ToString(), ErrorCodes::UNKNOWN_EXCEPTION};
}

void ParquetBlockOutputFormat::finalize()
{
    if (file_writer)
    {
        auto status = file_writer->Close();
        if (!status.ok())
            throw Exception{"Error while closing a table: " + status.ToString(), ErrorCodes::UNKNOWN_EXCEPTION};
    }
}


void registerOutputFormatProcessorParquet(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor(
        "Parquet",
        [](WriteBuffer & buf,
           const Block & sample,
           FormatFactory::WriteCallback,
           const FormatSettings & format_settings)
        {
            auto impl = std::make_shared<ParquetBlockOutputFormat>(buf, sample, format_settings);
            /// TODO
            // auto res = std::make_shared<SquashingBlockOutputStream>(impl, impl->getHeader(), format_settings.parquet.row_group_size, 0);
            // res->disableFlush();
            return impl;
        });
}

}


#else

namespace DB
{
class FormatFactory;
void registerOutputFormatProcessorParquet(FormatFactory &)
{
}
}


#endif
