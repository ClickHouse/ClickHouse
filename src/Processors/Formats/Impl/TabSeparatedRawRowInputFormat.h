#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <Processors/Formats/RowInputFormatWithDiagnosticInfo.h>


namespace DB
{

/** A stream to input data in tsv format, but without escaping individual values.
  * It only supports one string column
  */
class TabSeparatedRawRowInputFormat : public TabSeparatedRowInputFormat
{
public:
    /** with_names - the first line is the header with the names of the columns
      * with_types - on the next line header with type names
      */
    TabSeparatedRawRowInputFormat(
        const Block & header_,
        ReadBuffer & in_,
        const Params & params_,
        bool with_names_,
        bool with_types_,
        const FormatSettings & format_settings_)
        : TabSeparatedRowInputFormat(header_, in_, params_, with_names_, with_types_, format_settings_)
    {
    }

    String getName() const override { return "TabSeparatedRawRowInputFormat"; }

    bool readField(IColumn & column, const DataTypePtr & type, bool) override
    {
        // TODO: possible to optimize
        std::string buf;

        while (!in.eof())
        {
            char c = *in.position();

            if (c == '\n' || c == '\t')
                break;

            in.ignore();
            buf.push_back(c);
        }

        ReadBufferFromString line_in(buf);

        type->deserializeAsWholeText(column, line_in, format_settings);

        return true;
    }
};

}
