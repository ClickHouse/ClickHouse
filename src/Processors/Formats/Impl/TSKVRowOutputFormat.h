#pragma once

#include <Core/NamesAndTypes.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowOutputFormat.h>


namespace DB
{

/** The stream for outputting data in the TSKV format.
  * TSKV is similar to TabSeparated, but before every value, its name and equal sign are specified: name=value.
  * This format is very inefficient.
  */
class TSKVRowOutputFormat final : public IRowOutputFormat
{
public:
    TSKVRowOutputFormat(WriteBuffer & out_, SharedHeader header, const FormatSettings & format_settings);

    String getName() const override { return "TSKVRowOutputFormat"; }

    bool supportsSpecialSerializationKinds() const override { return format_settings.allow_special_serialization_kinds; }

private:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowEndDelimiter() override;

    const FormatSettings format_settings;
    NamesAndTypes fields;
    size_t field_number = 0;
};

}
