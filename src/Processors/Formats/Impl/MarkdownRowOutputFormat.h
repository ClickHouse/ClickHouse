#pragma once

#include <Processors/Formats/IRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>

namespace DB
{

class ReadBuffer;

class MarkdownRowOutputFormat final : public IRowOutputFormat
{
public:
    MarkdownRowOutputFormat(WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & format_settings_);

    String getName() const override { return "MarkdownRowOutputFormat"; }

private:
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

    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;

    const FormatSettings format_settings;
};


}
