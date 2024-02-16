#include <memory>
#include<Processors/Formats/Impl/FormInputFormat.h>
#include <IO/ReadHelpers.h>
#include "Core/NamesAndTypes.h"
#include "Core/QueryProcessingStage.h"
#include "DataTypes/IDataType.h"
#include "Formats/EscapingRuleUtils.h"
#include "Formats/FormatSettings.h"
#include "Processors/Formats/IRowInputFormat.h"
#include "base/find_symbols.h"
#include <Formats/FormatFactory.h>

namespace DB
{  

enum
{
    INVALID_INDEX = size_t(-1),
};

FormInputFormat::FormInputFormat(ReadBuffer & in_, Block header_, Params params_, const FormatSettings & format_settings_) 
    : IRowInputFormat(std::move(header_), in_, params_), format_settings(format_settings_)
{
    const auto & header = getPort().getHeader();
    name_map = header.getNamesToIndexesMap();
}

void FormInputFormat::readPrefix()
{
    skipBOMIfExists(*in);
}

const String & FormInputFormat::columnName(size_t i) const
{
    return getPort().getHeader().getByPosition(i).name;
}

/** Read the field name in the `Form` format.
  * Return true if field name is followed by an equal sign,
  * otherwise (field with no value) return false.
  * The reference to the field name is written to `ref`.
  * Temporary buffer `tmp` is used to copy the field name to it.
  */
static bool readName(ReadBuffer & buf, StringRef & ref, String & tmp)
{
    tmp.clear();

    while (!buf.eof())
    {
        const char * next_pos = find_first_symbols<'=','&'>(buf.position(), buf.buffer().end());
        
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
            buf.position() += next_pos + have_value - buf.position();
        }

        return have_value;
    }
    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected end of stream while reading key name from Form format");
}

void FormInputFormat::readField(size_t index, MutableColumns & columns)
{
    if (seen_columns[index])
        throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate field found while parsing Form format: {}", columnName(index));

    seen_columns[index] = true;
    const auto & serialization = serializations[index];
    String str;
    readStringUntilAmpersand(str,*in);

    if (!in->eof())
        ++in->position(); /// skip & 

    ReadBufferFromString buf(str); 
    serialization->deserializeTextRaw(*columns[index], buf, format_settings);
    read_columns[index] = true;
}

bool FormInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{

    if (in->eof())
        return false;

    size_t num_columns = columns.size();
    read_columns.assign(num_columns, false);
    seen_columns.assign(num_columns, false);

    for (size_t i = 0; i < num_columns; i++)
    {
        if(in->eof())
            break;
        
        StringRef name_ref;
        bool has_value = readName(*in, name_ref, name_buf);
        const auto it = name_map.find(String(name_ref));

        if (has_value)
        {
            size_t column_index;
            if (it != name_map.end())
                column_index = it->second;
            else
                column_index = INVALID_INDEX;

            if (column_index == INVALID_INDEX)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: illegal value of column_index");

            readField(column_index, columns);
        }
      
    }
    
    return true;
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
            readStringUntilAmpersand(value,in);
            names_and_types.emplace_back(std::move(name), tryInferDataTypeByEscapingRule(value, format_settings, FormatSettings::EscapingRule::Escaped));
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Found field without value while parsing Form format: {}", name_ref.toString());
        }
    }
    while (checkChar('&',in));
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


