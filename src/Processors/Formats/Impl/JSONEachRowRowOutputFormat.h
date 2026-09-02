#pragma once

#include <Processors/Formats/OutputFormatWithUTF8ValidationAdaptor.h>
#include <Processors/Formats/RowOutputFormatWithExceptionHandlerAdaptor.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class Block;
class WriteBuffer;

/** The stream for outputting data in JSON format, by object per line.
  */
class JSONEachRowRowOutputFormat : public RowOutputFormatWithExceptionHandlerAdaptor<RowOutputFormatWithUTF8ValidationAdaptor, bool>
{
public:
    JSONEachRowRowOutputFormat(
        WriteBuffer & out_,
        SharedHeader header_,
        const FormatSettings & settings_,
        bool pretty_json_ = false);

    String getName() const override { return "JSONEachRowRowOutputFormat"; }

    bool supportsSpecialSerializationKinds() const override { return settings.allow_special_serialization_kinds; }

protected:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeRowBetweenDelimiter() override;
    void writePrefix() override;
    void writeSuffix() override;

    void resetFormatterImpl() override;

    size_t field_number = 0;
    bool pretty_json;

    FormatSettings settings;
    WriteBuffer * ostr;

private:
    Names fields;
};

}
