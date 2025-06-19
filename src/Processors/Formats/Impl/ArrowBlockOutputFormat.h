#pragma once
#include "config.h"

#if USE_ARROW

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>
#include "ArrowBufferedStreams.h"

namespace arrow { class Schema; }
namespace arrow::ipc { class RecordBatchWriter; }

namespace DB
{

class CHColumnToArrowColumn;

class ArrowBlockOutputFormat : public IOutputFormat
{
public:
    ArrowBlockOutputFormat(WriteBuffer & out_, const Block & header_, bool stream_, const FormatSettings & format_settings_);

    String getName() const override { return "Arrow"; }

private:
    void consume(Chunk) override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;

    void prepareWriter(const std::shared_ptr<arrow::Schema> & schema);

    bool stream;
    const FormatSettings format_settings;
    std::shared_ptr<ArrowBufferedOutputStream> arrow_ostream;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
    std::unique_ptr<CHColumnToArrowColumn> ch_column_to_arrow_column;
};

}

#endif
