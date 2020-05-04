#pragma once
#include "config_formats.h"
#if USE_ARROW

#include <memory>
#include <string>
#include <Core/Block.h>
#include <Core/Types.h>
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
    ArrowBlockInputFormat(ReadBuffer & in_, const Block & header_);

    void resetParser() override;

    String getName() const override { return "ArrowBlockInputFormat"; }

protected:
    Chunk generate() override;

private:
    void prepareReader();

private:
    std::shared_ptr<arrow::RecordBatchReader> reader;
};

}

#endif
