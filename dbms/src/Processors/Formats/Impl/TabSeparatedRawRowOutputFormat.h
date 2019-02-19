#pragma once

#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/TabSeparatedRowOutputFormat.h>


namespace DB
{

/** A stream for outputting data in tsv format, but without escaping individual values.
  * (That is, the output is irreversible.)
  */
class TabSeparatedRawRowOutputFormat : public TabSeparatedRowOutputFormat
{
public:
    TabSeparatedRawRowOutputFormat(WriteBuffer & out, Block header, bool with_names, bool with_types, const FormatSettings & format_settings)
        : TabSeparatedRowOutputFormat(out, std::move(header), with_names, with_types, format_settings) {}

    String getName() const override { return "TabSeparatedRawRowOutputFormat"; }

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override
    {
        type.serializeAsText(column, row_num, out, format_settings);
    }
};

}

