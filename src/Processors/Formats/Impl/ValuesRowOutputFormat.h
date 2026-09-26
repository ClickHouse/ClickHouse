#pragma once

#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowOutputFormat.h>


namespace DB
{

class WriteBuffer;


/** A stream for outputting data in the VALUES format (as in the INSERT request).
  */
class ValuesRowOutputFormat final : public IRowOutputFormat
{
public:
    ValuesRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_);

    String getName() const override { return "ValuesRowOutputFormat"; }

    bool supportsSpecialSerializationKinds() const override { return format_settings.allow_special_serialization_kinds; }

private:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeRowBetweenDelimiter() override;

    const FormatSettings format_settings;
};

}
