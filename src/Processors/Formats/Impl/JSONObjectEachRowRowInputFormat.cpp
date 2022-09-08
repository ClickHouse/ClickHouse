#include <Processors/Formats/Impl/JSONObjectEachRowRowInputFormat.h>
#include <Formats/JSONUtils.h>
#include <Formats/FormatFactory.h>
#include <Formats/EscapingRuleUtils.h>

namespace DB
{

JSONObjectEachRowInputFormat::JSONObjectEachRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_)
    : JSONEachRowRowInputFormat(in_, header_, params_, format_settings_, false)
{
}

void JSONObjectEachRowInputFormat::readPrefix()
{
    JSONUtils::skipObjectStart(*in);
}

void JSONObjectEachRowInputFormat::readRowStart()
{
    JSONUtils::readFieldName(*in);
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

    JSONUtils::readFieldName(in);
    return JSONUtils::readRowAndGetNamesAndDataTypesForJSONEachRow(in, format_settings, false);
}

void JSONObjectEachRowSchemaReader::transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type)
{
    transformInferredJSONTypesIfNeeded(type, new_type, format_settings);
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
        return getAdditionalFormatInfoByEscapingRule(settings, FormatSettings::EscapingRule::JSON);
    });
}

}
