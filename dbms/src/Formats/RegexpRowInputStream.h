#pragma once

#include <Core/Block.h>
#include <Formats/IRowInputStream.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/PeekableReadBuffer.h>
#include <Common/OptimizedRegularExpression.h>


namespace DB
{

class RegexpRowInputStream : public IRowInputStream
{
public:
    RegexpRowInputStream(ReadBuffer & istr_, const Block & header_, const FormatSettings & settings_);

    bool read(MutableColumns & columns, RowReadExtension & extra) override;

private:
    ReadBuffer::Position loadDataUntilNewLine();

private:
    PeekableReadBuffer buf;
    Block header;
    const FormatSettings format_settings;

    FormatSettings settings;
    OptimizedRegularExpression regexp;
};

}
