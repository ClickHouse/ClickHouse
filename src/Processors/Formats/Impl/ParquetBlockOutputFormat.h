#pragma once
#include "config_formats.h"

#if USE_PARQUET
#    include <Processors/Formats/IOutputFormat.h>
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

class CHColumnToArrowColumn;

class ParquetBlockOutputFormat : public IOutputFormat
{
public:
    ParquetBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);

    String getName() const override { return "ParquetBlockOutputFormat"; }
    void consume(Chunk) override;
    void finalize() override;

    String getContentType() const override { return "application/octet-stream"; }

private:
    const FormatSettings format_settings;

    std::unique_ptr<parquet::arrow::FileWriter> file_writer;
    std::unique_ptr<CHColumnToArrowColumn> ch_column_to_arrow_column;
};

}

#endif
