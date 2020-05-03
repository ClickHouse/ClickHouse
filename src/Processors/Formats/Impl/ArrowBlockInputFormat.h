#pragma once
#include "config_formats.h"
#if USE_ARROW

#include <string>
#include <memory>
#include <common/types.h>
#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/IInputFormat.h>
#include "ArrowBufferedStreams.h"

namespace arrow { class RecordBatchReader; }

namespace DB
{

class ReadBuffer;

class ArrowBlockInputFormat : public IInputFormat
{
public:
    ArrowBlockInputFormat(ReadBuffer & in_, const Block & header_, const FormatSettings & format_settings_);

    void resetParser() override;

    String getName() const override { return "ArrowBlockInputFormat"; }

protected:
    Chunk generate() override;

private:
    const FormatSettings format_settings;
    std::shared_ptr<ArrowBufferedInputStream> arrow_istream;
    std::shared_ptr<arrow::RecordBatchReader> reader;
};

}

#endif
