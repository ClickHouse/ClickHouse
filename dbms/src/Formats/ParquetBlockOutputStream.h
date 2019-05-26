#pragma once

#include <Common/config.h>
#if USE_PARQUET
#    include <DataStreams/IBlockOutputStream.h>
#    include <Formats/FormatSettings.h>

namespace arrow
{
class Array;
class DataType;
}

namespace parquet
{
namespace arrow
{
    class FileWriter;
}
}

namespace DB
{
class ParquetBlockOutputStream : public IBlockOutputStream
{
public:
    ParquetBlockOutputStream(WriteBuffer & ostr_, const Block & header_, const FormatSettings & format_settings);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;
    void writeSuffix() override;
    void flush() override;

    String getContentType() const override { return "application/octet-stream"; }

private:
    WriteBuffer & ostr;
    Block header;
    const FormatSettings format_settings;

    std::unique_ptr<parquet::arrow::FileWriter> file_writer;
};

}

#endif
