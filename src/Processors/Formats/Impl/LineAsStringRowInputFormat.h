#pragma once

#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/FormatFactory.h>

namespace DB
{

class ReadBuffer;

/// This format parses a sequence of Line objects separated by newlines, spaces and/or comma.
/// Each Line object is parsed as a whole to string.
/// This format can only parse a table with single field of type String.

class LineAsStringRowInputFormat : public IRowInputFormat
{
public:
    LineAsStringRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_);

    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    String getName() const override { return "LineAsStringRowInputFormat"; }
    void resetParser() override;

private:
    void readLineObject(IColumn & column);
};

}
