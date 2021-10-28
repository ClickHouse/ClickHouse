#pragma once
#include "config_formats.h"
#if USE_PARQUET

#include <Processors/Formats/IInputFormat.h>
#include <Formats/FormatSettings.h>

namespace parquet::arrow { class FileReader; }

namespace arrow { class Buffer; }

namespace DB
{

class ArrowColumnToCHColumn;

class ParquetBlockInputFormat : public IInputFormat
{
public:
    ParquetBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_);

    void resetParser() override;

    String getName() const override { return "ParquetBlockInputFormat"; }

protected:
    Chunk generate() override;

private:
    void prepareReader();

private:
    std::unique_ptr<parquet::arrow::FileReader> file_reader;
    int row_group_total = 0;
    // indices of columns to read from Parquet file
    std::vector<int> column_indices;
    std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;
    int row_group_current = 0;
    const FormatSettings format_settings;
};

}

#endif
