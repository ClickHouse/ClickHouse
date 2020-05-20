#pragma once
#include "config_formats.h"
#if USE_ARROW

#include <Processors/Formats/IInputFormat.h>

namespace arrow::ipc { class RecordBatchFileReader; }

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
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> file_reader;
    int record_batch_total = 0;
    int record_batch_current = 0;
};

}

#endif
