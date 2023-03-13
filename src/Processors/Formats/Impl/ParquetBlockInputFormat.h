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

// Parquet files contain a metadata block with the following information:
//  * list of columns,
//  * list of "row groups",
//  * for each column in each row group:
//     - byte range for the data,
//     - min, max, count.
//
// This information could be used for:
//  (1) Precise reads - only reading the byte ranges we need, instead of filling the whole
//      arbitrarily-sized buffer inside ReadBuffer. We know in advance exactly what ranges we'll
//      need to read.
//  (2) Skipping row groups based on WHERE conditions.
//  (3) Skipping parsing of individual rows based on PREWHERE.
//  (4) Projections. I.e. for queries that only request min/max/count, we can report the
//      min/max/count from metadata. This can be done per row group. I.e. for row groups that
//      fully pass the WHERE conditions we'll use min/max/count from metadata, for row groups that
//      only partially overlap with the WHERE conditions we'll read data.
//  (4a) Before projections are implemented, we should at least be able to do `SELECT count(*)`
//       without reading data.
//
// For (1), we need the IInputFormat to be in control of reading, with its own implementation of
// parallel reading+parsing, instead of using ParallelParsingInputFormat, ParallelReadBuffer,
// AsynchronousReadIndirectBufferFromRemoteFS, ReadBufferFromRemoteFSGather.
// That's what MultistreamInputCreator in FormatFactory is about.

class ParquetBlockInputFormat : public IInputFormat
{
public:
    ParquetBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_);

    void resetParser() override;

    String getName() const override { return "ParquetBlockInputFormat"; }

    const BlockMissingValues & getMissingValues() const override;

private:
    Chunk generate() override;

    void prepareFileReader();
    bool prepareRowGroupReader(); // false if at end

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
    const FormatSettings format_settings;
    const std::unordered_set<int> & skip_row_groups;
    // Reads a single row group.
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
