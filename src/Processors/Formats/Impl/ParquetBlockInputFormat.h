#pragma once
#include "config.h"
#if USE_PARQUET

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>

namespace parquet::arrow { class FileReader; }

namespace arrow { class Buffer; class RecordBatchReader;}

namespace DB
{

class ArrowColumnToCHColumn;

class ParquetBlockInputFormat : public IInputFormat
{
public:
    ParquetBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_);

    void resetParser() override;

    String getName() const override { return "ParquetBlockInputFormat"; }

    const BlockMissingValues & getMissingValues() const override;

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

private:
    Chunk generate() override;

    void prepareReader();

    void onCancel() override
    {
        is_stopped = 1;
    }

    std::unique_ptr<parquet::arrow::FileReader> file_reader;
    int row_group_total = 0;
    int row_group_current = 0;
    // indices of columns to read from Parquet file
    std::vector<int> column_indices;
    std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;
    BlockMissingValues block_missing_values;
    size_t approx_bytes_read_for_chunk;
    const FormatSettings format_settings;
    const std::unordered_set<int> & skip_row_groups;
    std::shared_ptr<arrow::RecordBatchReader> current_record_batch_reader;

    std::atomic<int> is_stopped{0};
};

class ParquetSchemaReader : public ISchemaReader
{
public:
    ParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;

private:
    const FormatSettings format_settings;
};

}

#endif
