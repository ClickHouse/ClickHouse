#include <Processors/Formats/Impl/FormRowInputFormat.h>
#include "Formats/EscapingRuleUtils.h"
#include <Formats/FormatFactory.h>

#include <Poco/URI.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{
    String readFieldName(ReadBuffer & buf)
    {
        String field;
        readStringUntilEquals(field, buf);
        assertChar('=', buf);
        return field;
    }
}

FormRowInputFormat::FormRowInputFormat(ReadBuffer & in_, Block header_, Params params_, const FormatSettings & format_settings_) : IRowInputFormat(std::move(header_), in_, params_), format_settings(format_settings_)
{
    const auto & header = getPort().getHeader();
    size_t num_columns = header.columns();
    for (size_t i = 0; i < num_columns; ++i)
        name_map[header.getByPosition(i).name] = i;
}

void FormRowInputFormat::readPrefix()
{
    skipBOMIfExists(*in);
}

const String & FormRowInputFormat::columnName(size_t i) const
{
    return getPort().getHeader().getByPosition(i).name;
}

void FormRowInputFormat::readField(size_t index, MutableColumns & columns)
{
    if (seen_columns[index])
        throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate field found while parsing Form format: {}", columnName(index));

    seen_columns[index] = true;
    const auto & serialization = serializations[index];

    String encoded_str, decoded_str;
    readStringUntilAmpersand(encoded_str,*in);

    if (!in->eof())
        assertChar('&', *in);

    Poco::URI::decode(encoded_str, decoded_str);
    ReadBufferFromString buf(decoded_str);
    serialization->deserializeWholeText(*columns[index], buf, format_settings);
}

void FormRowInputFormat::readFormData(MutableColumns & columns)
{
    size_t index = 0;
    StringRef name_ref;
    while (true)
    {
        if (in->eof())
            break;

        auto tmp = readFieldName(*in);
        name_ref = StringRef(tmp);
        auto * it = name_map.find(name_ref);

        if (!it)
        {
            if (!format_settings.skip_unknown_fields)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown field found while parsing Form format: {}", name_ref.toString());

            /// Skip the value if key is not found.
            String encoded_str;
            readStringUntilAmpersand(encoded_str, *in);

            if (!in->eof())
                assertChar('&',*in);

        }
        else
        {
            index = it->getMapped();
            readField(index, columns);
        }
    }
}

bool FormRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (in->eof())
        return false;

    size_t num_columns = columns.size();
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
        ext.read_columns = seen_columns;
    else
        ext.read_columns.assign(seen_columns.size(), true);
    return true;
}

void FormRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    seen_columns.clear();
}

FormSchemaReader::FormSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : IRowWithNamesSchemaReader(in_, format_settings_,getDefaultDataTypeForEscapingRule(FormatSettings::EscapingRule::Escaped))
{
}

NamesAndTypesList readRowAndGetNamesAndDataTypesForFormRow(ReadBuffer & in, const FormatSettings & settings)
{
    NamesAndTypesList names_and_types;
    String value;
    String decoded_value;
    do
    {
        auto name = readFieldName(in);
        readStringUntilAmpersand(value,in);
        Poco::URI::decode(value, decoded_value);
        auto type = tryInferDataTypeByEscapingRule(decoded_value, settings, FormatSettings::EscapingRule::Raw);
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
        return std::make_shared<FormRowInputFormat>(buf, sample, std::move(params),settings);
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
