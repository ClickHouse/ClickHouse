#include <Processors/Formats/Impl/JSONColumnsBlockInputFormat.h>
#include <IO/ReadHelpers.h>
#include <Formats/FormatFactory.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/JSONUtils.h>

namespace DB
{

JSONColumnsReader::JSONColumnsReader(ReadBuffer & in_, const FormatSettings & format_settings_) : JSONColumnsReaderBase(in_), format_settings(format_settings_)
{
}

void JSONColumnsReader::readChunkStart()
{
    JSONUtils::skipObjectStart(*in);
}

std::optional<String> JSONColumnsReader::readColumnStart()
{
    auto name = JSONUtils::readFieldName(*in, format_settings.json);
    JSONUtils::skipArrayStart(*in);
    return name;
}

bool JSONColumnsReader::checkChunkEnd()
{
    return JSONUtils::checkAndSkipObjectEnd(*in);
}


void registerInputFormatJSONColumns(FormatFactory & factory)
{
    factory.registerInputFormat(
        "JSONColumns",
        [](ReadBuffer & buf,
           const Block &sample,
           const RowInputFormatParams &,
           const FormatSettings & settings)
        {
            return std::make_shared<JSONColumnsBlockInputFormatBase>(buf, sample, settings, std::make_unique<JSONColumnsReader>(buf, settings));
        }
    );
    factory.markFormatSupportsSubsetOfColumns("JSONColumns");
}

void registerJSONColumnsSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "JSONColumns",
        [](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_shared<JSONColumnsSchemaReaderBase>(buf, settings, std::make_unique<JSONColumnsReader>(buf, settings));
        }
    );
    factory.registerAdditionalInfoForSchemaCacheGetter("JSONColumns", [](const FormatSettings & settings)
    {
        return getAdditionalFormatInfoForAllRowBasedFormats(settings) + getAdditionalFormatInfoByEscapingRule(settings, FormatSettings::EscapingRule::JSON);
    });
}

}
