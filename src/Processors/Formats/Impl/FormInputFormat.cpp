#include<Processors/Formats/Impl/FormInputFormat.h>
#include "Formats/EscapingRuleUtils.h"
#include <Formats/FormatFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int INCORRECT_DATA;
}

FormInputFormat::FormInputFormat(ReadBuffer & in_, Block header_, Params params_, const FormatSettings & format_settings_) : IRowInputFormat(std::move(header_), in_, params_), format_settings(format_settings_)
{
    const auto & header = getPort().getHeader();
    size_t num_columns = header.columns();
    for (size_t i = 0; i < num_columns; ++i)
        name_map[header.getByPosition(i).name] = i;
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
    const auto & serialization = serializations[index];

    String encoded_str, decoded_str;
    readStringUntilAmpersand(encoded_str,*in);

    if (!in->eof())
        assertChar('&',*in);

    Poco::URI::decode(encoded_str, decoded_str);
    ReadBufferFromString buf(decoded_str);
    serialization->deserializeWholeText(*columns[index], buf, format_settings);
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
    if (!in->eof())
    {
        readFieldName(*in);
        String value;
        readStringUntilAmpersand(value,*in);
    }
}

void FormInputFormat::readFormData(MutableColumns & columns)
{
    size_t index = 0;
    while (true)
    {
        if (in->eof())
            break;

        StringRef name_ref = readFieldName(*in);
        auto * it = name_map.find(name_ref);

        if (!it)
        {
            if (!format_settings.skip_unknown_fields)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown field found while parsing Form format: {}", name_ref.toString());

            /// Skip the value if key is not found.
            NullOutput sink;
            String encoded_str;
            readStringUntilAmpersand(encoded_str,*in);

            if (!in->eof())
                assertChar('&',*in);

            ReadBufferFromString buf(encoded_str);
            readStringInto(sink, buf);
        }
        else
        {
            index = it->getMapped();
            readField(index, columns);
        }
    }
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
        if (!seen_columns[i])
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
        auto type = tryInferDataTypeByEscapingRule(value, settings, FormatSettings::EscapingRule::Raw);
        names_and_types.emplace_back(name, type);
    }
    while (checkChar('&',in));
    return names_and_types;
}

NamesAndTypesList FormSchemaReader::readRowAndGetNamesAndDataTypes(bool & eof)
{
    if (in.eof())
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


