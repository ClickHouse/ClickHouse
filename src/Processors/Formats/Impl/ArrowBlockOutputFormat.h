#pragma once
#include "config_formats.h"
#if USE_ARROW

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IOutputFormat.h>
#include "ArrowBufferedStreams.h"

namespace arrow::ipc { class RecordBatchWriter; }

namespace DB
{

class ArrowBlockOutputFormat : public IOutputFormat
{
public:
    ArrowBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);

    String getName() const override { return "ArrowBlockOutputFormat"; }
    void consume(Chunk) override;
    void finalize() override;

    String getContentType() const override { return "application/octet-stream"; }

private:
    const FormatSettings format_settings;
    std::shared_ptr<ArrowBufferedOutputStream> arrow_ostream;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
};

}

#endif
