#pragma once

#include <Processors/Formats/IInputFormat.h>
#include <IO/ReadBuffer.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IRowInputFormat.h>

namespace DB
{

class ColumnBinaryInputFormat final : public IInputFormat
{
public:
    ColumnBinaryInputFormat(ReadBuffer & buf, const Block & header,
                            const RowInputFormatParams & params,
                            const FormatSettings & settings);

    String getName() const override { return "ColumnBinary"; }
    Chunk read() override;

private:
    SharedHeader header_;
    bool eof_ = false;
};

}
