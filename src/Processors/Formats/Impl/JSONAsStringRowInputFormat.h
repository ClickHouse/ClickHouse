#pragma once

#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <IO/PeekableReadBuffer.h>

namespace DB
{

class ReadBuffer;

/// This format parses a sequence of JSON objects separated by newlines, spaces and/or comma.
/// Each JSON object is parsed as a whole to string.
/// This format can only parse a table with single field of type String.

class JSONAsStringRowInputFormat : public IRowInputFormat
{
public:
    JSONAsStringRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_);

    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    String getName() const override { return "JSONAsStringRowInputFormat"; }
    void resetParser() override;

private:
    void readJSONObject(IColumn & column);

    PeekableReadBuffer buf;
};

}
