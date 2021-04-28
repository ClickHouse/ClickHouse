#pragma once
#include "config_formats.h"
#if USE_ORC

#include <Processors/Formats/IInputFormat.h>

namespace arrow::adapters::orc { class ORCFileReader; }

namespace DB
{
class ORCBlockInputFormat : public IInputFormat
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

    int stripe_total = 0;

    int stripe_current = 0;

    // indices of columns to read from ORC file
    std::vector<int> include_indices;

    void prepareReader();
};

}
#endif
