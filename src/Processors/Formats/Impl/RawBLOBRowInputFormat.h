#pragma once

#include <Processors/Formats/IRowInputFormat.h>


namespace DB
{

class ReadBuffer;

/// This format slurps all input data into single value.
/// This format can only parse a table with single field of type String or similar.

class RawBLOBRowInputFormat : public IRowInputFormat
{
public:
    RawBLOBRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_);

    bool readRow(MutableColumns & columns, RowReadExtension &) override;
    String getName() const override { return "RawBLOBRowInputFormat"; }
};

}

