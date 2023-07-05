#pragma once
#include "config.h"
#if USE_ORC

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>

#include <arrow/adapters/orc/adapter.h>

namespace arrow::adapters::orc
{
    class ORCFileReader;
}

namespace DB
{

class ArrowColumnToCHColumn;

class ORCBlockInputFormat : public IInputFormat
{
public:
    ORCBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_);

    String getName() const override { return "ORCBlockInputFormat"; }

    void resetParser() override;

    const BlockMissingValues & getMissingValues() const override;

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

protected:
    Chunk generate() override;

    void onCancel() override
    {
        is_stopped = 1;
    }

private:
    void prepareFileReader();
    bool prepareStripeReader();

    // TODO: check that this class implements every part of its parent

    std::unique_ptr<arrow::adapters::orc::ORCFileReader> file_reader;
    std::shared_ptr<arrow::RecordBatchReader> stripe_reader;

    std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;

    // indices of columns to read from ORC file
    std::vector<int> include_indices;

    BlockMissingValues block_missing_values;
    size_t approx_bytes_read_for_chunk;

    const FormatSettings format_settings;
    const std::unordered_set<int> & skip_stripes;

    int total_stripes = 0;
    int current_stripe = -1;
    arrow::adapters::orc::StripeInformation current_stripe_info;

    std::atomic<int> is_stopped{0};
};

class ORCSchemaReader : public ISchemaReader
{
public:
    ORCSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;

private:
    const FormatSettings format_settings;
};

}
#endif
