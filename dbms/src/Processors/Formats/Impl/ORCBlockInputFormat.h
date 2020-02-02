#pragma once

#include "config_formats.h"
#include <DataStreams/IBlockInputStream.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/IInputFormat.h>

#if USE_ORC

#include "arrow/adapters/orc/adapter.h"
#include "arrow/io/interfaces.h"

namespace DB
{
class Context;

class ORCBlockInputFormat: public IInputFormat
{
public:
    ORCBlockInputFormat(ReadBuffer & in_, Block header_);

    String getName() const override { return "ORCBlockInputFormat"; }

    void resetParser() override;

protected:
    Chunk generate() override;

private:

    // TODO: check that this class implements every part of its parent

    std::unique_ptr<arrow::adapters::orc::ORCFileReader> file_reader;
    std::string file_data;
    int row_group_total = 0;
    int row_group_current = 0;
};

}
#endif
