#pragma once

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/OutputFormatWithUTF8ValidationAdaptor.h>
#include <Processors/Formats/RowOutputFormatWithExceptionHandlerAdaptor.h>
#include <Formats/FormatSettings.h>


namespace DB
{

/** The stream for outputting data in JSON format, by JSON array per line.
  */
class JSONCompactEachRowRowOutputFormat final : public RowOutputFormatWithExceptionHandlerAdaptor<RowOutputFormatWithUTF8ValidationAdaptor, bool>
{
public:
    JSONCompactEachRowRowOutputFormat(
        WriteBuffer & out_,
        const Block & header_,
        const FormatSettings & settings_,
        bool with_names_,
        bool with_types_,
        bool yield_strings_);

    String getName() const override { return "JSONCompactEachRowRowOutputFormat"; }

private:
    void writePrefix() override;

    void writeTotals(const Columns & columns, size_t row_num) override;

    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeSuffix() override;

    void resetFormatterImpl() override;

    bool supportTotals() const override { return true; }
    void consumeTotals(Chunk) override;

    void writeLine(const std::vector<String> & values);

    FormatSettings settings;
    bool with_names;
    bool with_types;
    bool yield_strings;

    WriteBuffer * ostr;
};
}
