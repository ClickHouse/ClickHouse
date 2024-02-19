#include <memory>
#include<Processors/Formats/Impl/FormInputFormat.h>
#include <IO/ReadHelpers.h>
#include "Core/NamesAndTypes.h"
#include "DataTypes/IDataType.h"
#include "Formats/EscapingRuleUtils.h"
#include "Formats/FormatSettings.h"
#include "Formats/SchemaInferenceUtils.h"
#include "IO/ReadBufferFromString.h"
#include "Processors/Formats/IRowInputFormat.h"
#include "base/find_symbols.h"
#include <Formats/FormatFactory.h>
#include <DataTypes/NestedUtils.h>

namespace DB
{  

enum
{
    UNKNOWN_FIELD = size_t(-1),
    NESTED_FIELD = size_t(-2)
};

/**
  * Recursively check if column_name contains '.' in name
  * and split into separate columns if it does
  */
void FormInputFormat::checkAndSplitIfNested(const StringRef column_name)
{
    while(true)
    {
        const auto split = Nested::splitName(column_name.toView());
        if (!split.second.empty())
        {
            const StringRef table_name(column_name.data, split.first.size());
            name_map[table_name] = NESTED_FIELD;
            const StringRef next_table_name(String(split.second).c_str(), split.second.size());
            checkAndSplitIfNested(next_table_name);
        }
        break;
    }
}

FormInputFormat::FormInputFormat(ReadBuffer & in_, Block header_, Params params_, const FormatSettings & format_settings_) 
    : IRowInputFormat(std::move(header_), in_, params_), format_settings(format_settings_)
{
    const auto & header = getPort().getHeader();
    name_map = header.getNamesToIndexesMap();
    
    /// not sure if this needs to be on a setting or not? 
    for (size_t i=0; i != header.columns(); ++i)
    {
        const StringRef column_name = header.getByPosition(i).name;
        checkAndSplitIfNested(column_name);
    }
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
StringRef readName(ReadBuffer & buf, StringRef & ref, String & tmp)
{
    tmp.clear();

    while (!buf.eof())
    {
        const char * next_pos = find_first_symbols<'=','&'>(buf.position(), buf.buffer().end());
        
        if (next_pos == buf.buffer().end())
        {
            tmp.append(buf.position(), next_pos - buf.position());
            buf.position() = buf.buffer().end();
            buf.next();
            continue;
        }

        /// Column names (keys) occur before '=' 
        if (*next_pos == '=')
        {
            ref = StringRef(buf.position(), next_pos - buf.position());
            buf.position() += next_pos + 1 - buf.position();
        }

        return ref;
    }
    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected end of stream while reading key name from Form format");
}

void FormInputFormat::readField(size_t index, MutableColumns & columns)
{
    if (seen_columns[index])
        throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate field found while parsing Form format: {}", columnName(index));

    seen_columns[index] = read_columns[index] = true;
    const auto & serialization = serializations[index];
    String encoded_str, decoded_str;
    readStringUntilAmpersand(encoded_str,*in);
    Poco::URI::decode(encoded_str, decoded_str);

    /// skip '&' before next key value pair
    if (!in->eof())
        ++in->position(); 

    ReadBufferFromString buf(decoded_str); 
    serialization->deserializeTextRaw(*columns[index], buf, format_settings);
    read_columns[index] = true;
}


String readFieldName(ReadBuffer & in)
{
    String field;
    readStringUntilEquals(field, in);
    assertChar('=', in);
    return field;
}

inline size_t FormInputFormat::columnIndex(StringRef name)
{
    const auto it = name_map.find(name);
    if (it != name_map.end())
    {
        return it->second;
    }
    else 
        return UNKNOWN_FIELD;
}

bool FormInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    size_t num_columns = columns.size();

    read_columns.assign(num_columns, false);
    seen_columns.assign(num_columns, false);

    for (size_t index = 0; index < num_columns; ++index)
    {
        readField(1, columns);
    }

    return true;
}

FormSchemaReader::FormSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : IRowWithNamesSchemaReader(in_, format_settings_,getDefaultDataTypeForEscapingRule(FormatSettings::EscapingRule::Escaped))
{
}

NamesAndTypesList readRowAndGetNamesAndDataTypesForFormRow(ReadBuffer & in, const FormatSettings & settings)
{
    NamesAndTypesList names_and_types;
    String field, value;
    do
    {
        auto name = readFieldName(in);
        readStringUntilAmpersand(value,in);
        auto type = tryInferDataTypeByEscapingRule(value, settings, FormatSettings::EscapingRule::Escaped);
        names_and_types.emplace_back(name, type);
    }
    while(checkChar('&',in));
    return names_and_types;
}

NamesAndTypesList FormSchemaReader::readRowAndGetNamesAndDataTypes(bool & eof)
{
    if(in.eof())
    {
        eof = true;
        return {};
    }

    return readRowAndGetNamesAndDataTypesForFormRow(in, format_settings);
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


