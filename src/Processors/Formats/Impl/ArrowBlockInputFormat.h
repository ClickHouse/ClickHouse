#pragma once
#include "config.h"

#if USE_ARROW

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>

namespace arrow { class RecordBatchReader; }
namespace arrow::ipc { class RecordBatchFileReader; }

namespace DB
{

class ReadBuffer;
class ArrowColumnToCHColumn;

class ArrowBlockInputFormat : public IInputFormat
{
public:
    ArrowBlockInputFormat(ReadBuffer & in_, const Block & header_, bool stream_, const FormatSettings & format_settings_);

    void resetParser() override;

    String getName() const override { return "ArrowBlockInputFormat"; }

    const BlockMissingValues * getMissingValues() const override;

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

private:
    Chunk read() override;

    void onCancel() noexcept override
    {
        is_stopped = 1;
    }

    // Whether to use ArrowStream format
    bool stream;
    // This field is only used for ArrowStream format
    std::shared_ptr<arrow::RecordBatchReader> stream_reader;
    // The following fields are used only for Arrow format
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> file_reader;

    std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;

    int record_batch_total = 0;
    int record_batch_current = 0;

    BlockMissingValues block_missing_values;
    size_t approx_bytes_read_for_chunk = 0;

    const FormatSettings format_settings;

    void prepareReader();

    std::atomic<int> is_stopped{0};
};

class ArrowSchemaReader : public ISchemaReader
{
public:
    ArrowSchemaReader(ReadBuffer & in_, bool stream_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;

    std::optional<size_t> readNumberOrRows() override;

private:
    void initializeIfNeeded();

    bool stream;
    const FormatSettings format_settings;
    std::shared_ptr<arrow::RecordBatchReader> stream_reader;
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> file_reader;
};

}

#endif
