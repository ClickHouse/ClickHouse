#pragma once

#include <Processors/Formats/IRowOutputFormat.h>

namespace DB
{

class Block;
class IColumn;
class IDataType;
class WriteBuffer;


/** A stream for outputting data in a binary line-by-line format.
  */
class BinaryRowOutputFormat final: public IRowOutputFormat
{
public:
    BinaryRowOutputFormat(WriteBuffer & out_, SharedHeader header, bool with_names_, bool with_types_, const FormatSettings & format_settings_);

    String getName() const override { return "BinaryRowOutputFormat"; }

    bool supportsSpecialSerializationKinds() const override { return format_settings.allow_special_serialization_kinds; }

private:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writePrefix() override;

    bool with_names;
    bool with_types;
    const FormatSettings format_settings;
};

}
