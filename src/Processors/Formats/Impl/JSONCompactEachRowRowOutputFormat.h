#pragma once

#include <Processors/Formats/OutputFormatWithUTF8ValidationAdaptor.h>
#include <Processors/Formats/RowOutputFormatWithExceptionHandlerAdaptor.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class Block;
class WriteBuffer;

/** The stream for outputting data in JSON format, by JSON array per line.
  */
class JSONCompactEachRowRowOutputFormat : public RowOutputFormatWithExceptionHandlerAdaptor<RowOutputFormatWithUTF8ValidationAdaptor, bool>
{
public:
    JSONCompactEachRowRowOutputFormat(
        WriteBuffer & out_,
        SharedHeader header_,
        const FormatSettings & settings_,
        bool with_names_,
        bool with_types_);

    String getName() const override { return "JSONCompactEachRowRowOutputFormat"; }

    bool supportsSpecialSerializationKinds() const override { return settings.allow_special_serialization_kinds; }

protected:
    void writePrefix() override;

    /// The plain output cannot represent totals and extremes: they would be indistinguishable
    /// from ordinary rows, so they are dropped. Under a framing format the packet boundaries
    /// tell them apart, so they are emitted - as rows of the compact format, into the `totals`
    /// and `extremes` packets.
    bool supportTotals() const override { return getFraming() != nullptr; }
    bool supportExtremes() const override { return getFraming() != nullptr; }

    void writeTotals(const Columns & columns, size_t row_num) override;

    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeSuffix() override;

    void resetFormatterImpl() override;

    void writeLine(const std::vector<String> & values);

    FormatSettings settings;
    bool with_names;
    bool with_types;
    WriteBuffer * ostr;
};

}
