#pragma once

#include "config_formats.h"
#if USE_PARQUET

#include <Processors/Formats/IInputFormat.h>


namespace parquet { namespace arrow { class FileReader; } }
namespace arrow { class Buffer; }

namespace DB
{
class Context;

class ParquetBlockInputFormat: public IInputFormat
{
public:
    ParquetBlockInputFormat(ReadBuffer & in_, Block header_);

    void resetParser() override;


    String getName() const override { return "ParquetBlockInputFormat"; }

protected:
    Chunk generate() override;

private:
    std::unique_ptr<parquet::arrow::FileReader> file_reader;
    int row_group_total = 0;
    // indices of columns to read from Parquet file
    std::vector<int> column_indices;
    int row_group_current = 0;
};

}

#endif
