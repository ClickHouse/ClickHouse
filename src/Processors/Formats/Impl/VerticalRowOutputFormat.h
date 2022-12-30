#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowOutputFormat.h>


namespace DB
{

class WriteBuffer;
class Context;


/** Stream to output data in format "each value in separate row".
  * Usable to show few rows with many columns.
  */
class VerticalRowOutputFormat final : public IRowOutputFormat
{
public:
    VerticalRowOutputFormat(WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & format_settings_);

    String getName() const override { return "VerticalRowOutputFormat"; }

private:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeRowStartDelimiter() override;
    void writeRowBetweenDelimiter() override;
    void writeSuffix() override;

    void writeMinExtreme(const Columns & columns, size_t row_num) override;
    void writeMaxExtreme(const Columns & columns, size_t row_num) override;
    void writeTotals(const Columns & columns, size_t row_num) override;

    void writeBeforeTotals() override;
    void writeBeforeExtremes() override;

    void writeValue(const IColumn & column, const ISerialization & serialization, size_t row_num) const;

    void onRowsReadBeforeUpdate() override { row_number = getRowsReadBefore(); }

    /// For totals and extremes.
    void writeSpecialRow(const Columns & columns, size_t row_num, const char * title);

    const FormatSettings format_settings;
    size_t field_number = 0;
    size_t row_number = 0;

    using NamesAndPaddings = std::vector<String>;
    NamesAndPaddings names_and_paddings;
};

}
