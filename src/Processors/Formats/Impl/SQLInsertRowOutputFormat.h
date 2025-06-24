#pragma once

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowOutputFormat.h>


namespace DB
{

class WriteBuffer;

class SQLInsertRowOutputFormat : public IRowOutputFormat
{
public:
    SQLInsertRowOutputFormat(
        WriteBuffer & out_,
        const Block & header_,
        const FormatSettings & format_settings_);

    String getName() const override { return "SQLInsertRowOutputFormat"; }

protected:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeRowBetweenDelimiter() override;
    void writeSuffix() override;
    void resetFormatterImpl() override;

    void printLineStart();
    void printColumnNames();

    size_t rows_in_line = 0;
    Names column_names;
    const FormatSettings format_settings;
};

}
