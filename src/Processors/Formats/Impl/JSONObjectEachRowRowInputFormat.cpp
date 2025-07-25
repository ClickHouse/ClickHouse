#include <Processors/Formats/Impl/JSONObjectEachRowRowInputFormat.h>
#include <Formats/JSONUtils.h>
#include <Formats/FormatFactory.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/SchemaInferenceUtils.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::optional<size_t> getColumnIndexForJSONObjectEachRowObjectName(const Block & header, const FormatSettings & format_settings)
{
    if (format_settings.json_object_each_row.column_for_object_name.empty())
        return std::nullopt;

    if (!header.has(format_settings.json_object_each_row.column_for_object_name))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Column name '{}' from setting format_json_object_each_row_column_for_object_name doesn't exists in header",
            format_settings.json_object_each_row.column_for_object_name);

    size_t index = header.getPositionByName(format_settings.json_object_each_row.column_for_object_name);
    if (!isStringOrFixedString(header.getDataTypes()[index]))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Column '{}' from setting json_object_each_row_column_for_object_name must have String type",
            format_settings.json_object_each_row.column_for_object_name);

    return index;
}

JSONObjectEachRowInputFormat::JSONObjectEachRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_)
    : JSONEachRowRowInputFormat(in_, header_, params_, format_settings_, false), field_index_for_object_name(getColumnIndexForJSONObjectEachRowObjectName(header_, format_settings_))
{
}

void JSONObjectEachRowInputFormat::readPrefix()
{
    JSONUtils::skipObjectStart(*in);
}

void JSONObjectEachRowInputFormat::readRowStart(MutableColumns & columns)
{
    auto object_name = JSONUtils::readFieldName(*in, format_settings.json);
    if (field_index_for_object_name)
    {
        columns[*field_index_for_object_name]->insertData(object_name.data(), object_name.size());
        seen_columns[*field_index_for_object_name] = true;
        read_columns[*field_index_for_object_name] = true;
    }
}

void JSONObjectEachRowInputFormat::skipRowStart()
{
    JSONUtils::readFieldName(*in, format_settings.json);
}

bool JSONObjectEachRowInputFormat::checkEndOfData(bool is_first_row)
{
    if (in->eof() || JSONUtils::checkAndSkipObjectEnd(*in))
        return true;
    if (!is_first_row)
        JSONUtils::skipComma(*in);
    return false;
}

JSONObjectEachRowSchemaReader::JSONObjectEachRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : IRowWithNamesSchemaReader(in_, format_settings_)
{
}

NamesAndTypesList JSONObjectEachRowSchemaReader::readRowAndGetNamesAndDataTypes(bool & eof)
{
    if (first_row)
        JSONUtils::skipObjectStart(in);

    if (in.eof() || JSONUtils::checkAndSkipObjectEnd(in))
    {
        eof = true;
        return {};
    }

    if (first_row)
        first_row = false;
    else
        JSONUtils::skipComma(in);

    JSONUtils::readFieldName(in, format_settings.json);
    return JSONUtils::readRowAndGetNamesAndDataTypesForJSONEachRow(in, format_settings, &inference_info);
}

NamesAndTypesList JSONObjectEachRowSchemaReader::getStaticNamesAndTypes()
{
    if (!format_settings.json_object_each_row.column_for_object_name.empty())
        return {{format_settings.json_object_each_row.column_for_object_name, std::make_shared<DataTypeString>()}};

    return {};
}

void JSONObjectEachRowSchemaReader::transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type)
{
    transformInferredJSONTypesIfNeeded(type, new_type, format_settings, &inference_info);
}

void JSONObjectEachRowSchemaReader::transformFinalTypeIfNeeded(DataTypePtr & type)
{
    transformFinalInferredJSONTypeIfNeeded(type, format_settings, &inference_info);
}

void registerInputFormatJSONObjectEachRow(FormatFactory & factory)
{
    factory.registerInputFormat("JSONObjectEachRow", [](
                ReadBuffer & buf,
                const Block & sample,
                IRowInputFormat::Params params,
                const FormatSettings & settings)
    {
        return std::make_shared<JSONObjectEachRowInputFormat>(buf, sample, std::move(params), settings);
    });

    factory.markFormatSupportsSubsetOfColumns("JSONObjectEachRow");
}

void registerJSONObjectEachRowSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader("JSONObjectEachRow", [](ReadBuffer & buf, const FormatSettings & settings)
    {
        return std::make_unique<JSONObjectEachRowSchemaReader>(buf, settings);
    });
    factory.registerAdditionalInfoForSchemaCacheGetter("JSONObjectEachRow", [](const FormatSettings & settings)
    {
            return getAdditionalFormatInfoByEscapingRule(settings, FormatSettings::EscapingRule::JSON)
                + fmt::format(", format_json_object_each_row_column_for_object_name={}", settings.json_object_each_row.column_for_object_name);
    });
}

}
