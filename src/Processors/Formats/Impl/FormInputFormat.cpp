#include <memory>
#include<Processors/Formats/Impl/FormInputFormat.h>
#include <IO/ReadHelpers.h>
#include "Core/NamesAndTypes.h"
#include "Formats/EscapingRuleUtils.h"
#include "Formats/FormatSettings.h"
#include "Processors/Formats/IRowInputFormat.h"
#include "base/find_symbols.h"
#include <Formats/FormatFactory.h>

namespace DB
{

FormInputFormat::FormInputFormat(ReadBuffer & in_, Block header_, Params params_, const FormatSettings & format_settings_) 
    : IRowInputFormat(std::move(header_), in_, params_), format_settings(format_settings_)
{

}

void FormInputFormat::readPrefix()
{
    skipBOMIfExists(*in);
}

static bool readName(ReadBuffer & buf, StringRef & ref, String & tmp)
{
    tmp.clear();

    while (!buf.eof())
    {
        const char * next_pos = find_first_symbols<'='>(buf.position(), buf.buffer().end());
        
        bool have_value = *next_pos == '=';
        if (next_pos == buf.buffer().end())
        {
            tmp.append(buf.position(), next_pos - buf.position());
            buf.position() = buf.buffer().end();
            buf.next();
            continue;
        }

        // names occur before = 
        if (*next_pos == '=')
        {
            ref = StringRef(buf.position(), next_pos - buf.position());

        }

        // data occurs before &
        if (*next_pos == '&')
        {

        }
        return have_value;
    }
    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected end of stream while reading key name from Form format");
}

bool FormInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    size_t num_columns = columns.size();
    if (!num_columns){}
    return false;
}

FormSchemaReader::FormSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : IRowWithNamesSchemaReader(in_, format_settings_,getDefaultDataTypeForEscapingRule(FormatSettings::EscapingRule::Escaped))
{
}

NamesAndTypesList FormSchemaReader::readRowAndGetNamesAndDataTypes(bool & eof)
{
    if(in.eof())
    {
        eof = true;
        return {};
    }

    NamesAndTypesList names_and_types;
    StringRef name_ref;
    String name_buf;
    String value;
    do {
        bool has_value = readName(in, name_ref, name_buf);
        String name = String(name_ref);
        if (has_value)
        {
            readEscapedString(value,in);
            names_and_types.emplace_back(std::move(name), tryInferDataTypeByEscapingRule(value, format_settings, FormatSettings::EscapingRule::Escaped));
        }
        else
        {

        }
        
    }
    while (checkChar('=',in));
    return names_and_types;
}

void registerInputFormatForm(FormatFactory & factory)
{
    factory.registerInputFormat("Form", [](
        ReadBuffer & buf,
        const Block & sample,
        IRowInputFormat::Params params,
        const FormatSettings & settings)
    {
        return std::make_shared<FormInputFormat>(buf, sample, std::move(params),settings);
    });
}

void registerFormSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader("Form", [](ReadBuffer & buffer, const FormatSettings & settings)
    {
        return std::make_shared<FormSchemaReader>(buffer, settings);
    });
}

}


