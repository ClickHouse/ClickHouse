#pragma once

#include <Processors/Formats/IRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>

namespace DB
{

class ReadBuffer;

class MarkdownRowOutputFormat : public IRowOutputFormat
{
public:
    MarkdownRowOutputFormat(WriteBuffer & out_, const Block & header_, FormatFactory::WriteCallback callback, const FormatSettings & format_settings_);

    /// Write higher part of markdown table like this:
    /// |columnName1|columnName2|...|columnNameN|
    /// |:-:|:-:|...|:-:|
    void writePrefix() override;

    /// Write '|' before each row
    void writeRowStartDelimiter() override;

    /// Write '|' between values
    void writeFieldDelimiter() override;

    /// Write '|\n' after each row
    void writeRowEndDelimiter() override ;

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    String getName() const override { return "MarkdownRowOutputFormat"; }

protected:
    const FormatSettings format_settings;
};


}
