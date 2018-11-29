#pragma once

#include <Formats/FormatSettings.h>
#include <Formats/TabSeparatedRowOutputStream.h>


namespace DB
{

/** A stream for outputting data in tsv format, but without escaping individual values.
  * (That is, the output is irreversible.)
  */
class TabSeparatedRawRowOutputStream : public TabSeparatedRowOutputStream
{
public:
    TabSeparatedRawRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool with_names_, bool with_types_, const FormatSettings & format_settings)
        : TabSeparatedRowOutputStream(ostr_, sample_, with_names_, with_types_, format_settings) {}

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override
    {
        type.serializeText(column, row_num, ostr, format_settings);
    }
};

}

