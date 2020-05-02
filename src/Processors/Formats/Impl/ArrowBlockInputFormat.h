#pragma once

#include <string>
#include <memory>
#include <common/types.h>
#include <Core/Block.h>
#include <Processors/Chunk.h>

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
    ArrowBlockInputFormat(ReadBuffer & in_, Block header_);

    void resetParser() override;

    String getName() const override { return "ArrowBlockInputFormat"; }

protected:
    Chunk generate() override;

private:
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> file_reader;
    std::string file_data;
    int row_group_total = 0;
    int row_group_current = 0;
};

}

#endif
