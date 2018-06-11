#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Formats/IRowOutputStream.h>


namespace DB
{

class WriteBuffer;
class Context;


/** Stream to output data in format "each value in separate row".
  * Usable to show few rows with many columns.
  */
class VerticalRowOutputStream : public IRowOutputStream
{
public:
    VerticalRowOutputStream(WriteBuffer & ostr_, const Block & sample_, const FormatSettings & format_settings);

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeRowStartDelimiter() override;
    void writeRowBetweenDelimiter() override;
    void writeSuffix() override;

    void flush() override;

    void setTotals(const Block & totals_) override { totals = totals_; }
    void setExtremes(const Block & extremes_) override { extremes = extremes_; }

protected:
    virtual void writeValue(const IColumn & column, const IDataType & type, size_t row_num) const;

    void writeTotals();
    void writeExtremes();
    /// For totals and extremes.
    void writeSpecialRow(const Block & block, size_t row_num, const char * title);

    WriteBuffer & ostr;
    const Block sample;
    const FormatSettings format_settings;
    size_t field_number = 0;
    size_t row_number = 0;

    using NamesAndPaddings = std::vector<String>;
    NamesAndPaddings names_and_paddings;

    Block totals;
    Block extremes;
};

}

