#include <memory>
#include<Processors/Formats/Impl/FormInputFormat.h>
#include <IO/ReadHelpers.h>
#include "Core/NamesAndTypes.h"
#include "DataTypes/IDataType.h"
#include "Formats/EscapingRuleUtils.h"
#include "Formats/FormatSettings.h"
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Formats/JSONUtils.h>
#include "Processors/Formats/IRowInputFormat.h"
#include "base/find_symbols.h"
#include <Formats/FormatFactory.h>
#include <DataTypes/NestedUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}  

namespace 
{

enum
{
    UNKNOWN_FIELD = size_t(-1),
    NESTED_FIELD = size_t(-2)
};

}

FormInputFormat::FormInputFormat(ReadBuffer & in_, Block header_, Params params_, const FormatSettings & format_settings_) 
    : IRowInputFormat(std::move(header_), in_, params_), format_settings(format_settings_)
{
    const auto & header = getPort().getHeader();
    name_map = header.getNamesToIndexesMap();
    
    for (size_t i = 0; i != header.columns(); ++i)
    {
        const StringRef column_name = header.getByPosition(i).name;
        const auto split = Nested::splitName(column_name.toView());
        if (!split.second.empty())
        {
            const StringRef table_name(column_name.data, split.first.size());
            name_map[table_name] = NESTED_FIELD;
        }
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
    const auto & type = getPort().getHeader().getByPosition(index).type;
    const auto & serialization = serializations[index];

    String encoded_str, decoded_str;
    readStringUntilAmpersand(encoded_str,*in);
    Poco::URI::decode(encoded_str, decoded_str);
    ReadBufferFromString buf(decoded_str);
    
    if (format_settings.null_as_default && !isNullableOrLowCardinalityNullable(type))
        read_columns[index] = SerializationNullable::deserializeNullAsDefaultOrNestedTextRaw(*columns[index], buf, format_settings, serialization);
    else
      serialization->deserializeTextRaw(*columns[index], buf, format_settings);    
}


String readFieldName(ReadBuffer & buf)
{
    String field;
    readStringUntilEquals(field, buf);
    assertChar('=', buf);
    return field;
}

void FormInputFormat::skipUnknownFormField(StringRef name_ref)
{
    if (!format_settings.skip_unknown_fields)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown field found while parsing Form format: {}", name_ref.toString());

    /// read name and value but do nothing with them
    readFieldName(*in);
    String value;
    readStringUntilAmpersand(value,*in);
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


void FormInputFormat::readFormData(MutableColumns & columns)
{
    size_t index = 0;
    while (index < columns.size())
    {
        if (in->eof())
            break;

        StringRef name_ref = readFieldName(*in);
        const size_t column_index = columnIndex(name_ref);

        if (ssize_t(column_index) < 0)
        {
            /// copy name_ref to temporary string as name_ref may 
            /// point directly to the input buffer

            current_column_name.assign(name_ref.data, name_ref.size);
            name_ref = StringRef(current_column_name);

            if (column_index == UNKNOWN_FIELD)
                skipUnknownFormField(name_ref);
            else if (column_index == NESTED_FIELD)
                readNestedFormData(name_ref.toString(), columns);
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: illegal value of column_index");
        }
        else
        {
            readField(column_index, columns);    
        }
        ++index;
    }
}

void FormInputFormat::readNestedFormData(const String & name, MutableColumns & columns)
{
    current_column_name = name;
    current_column_name.push_back('.');
    nested_prefix_length = current_column_name.size();
    readFormData(columns);
    nested_prefix_length = 0;
}

bool FormInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (in->eof())
        return false;

    size_t num_columns = columns.size();

    read_columns.assign(num_columns, false);
    seen_columns.assign(num_columns, false);

    readFormData(columns);

    const auto & header = getPort().getHeader();
    /// Non-visited columns get filled with default values
    for (size_t i = 0; i < num_columns; ++i)
        if(!seen_columns[i])
            header.getByPosition(i).type->insertDefaultInto(*columns[i]);

    /// Return info about defaults set.
    /// If defaults_for_omitted_fields is set to 0, then we leave already inserted defaults.
    if (format_settings.defaults_for_omitted_fields)
        ext.read_columns = read_columns;
    else
        ext.read_columns.assign(read_columns.size(), true);

    return true;
}

void FormInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    nested_prefix_length = 0;
    read_columns.clear();
    seen_columns.clear();
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


