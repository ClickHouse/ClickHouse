#include <Processors/Formats/Impl/JSONCompactColumnsBlockInputFormat.h>
#include <IO/ReadHelpers.h>
#include <Formats/FormatFactory.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/JSONUtils.h>

namespace DB
{

JSONCompactColumnsReader::JSONCompactColumnsReader(ReadBuffer & in_) : JSONColumnsReaderBase(in_)
{
}

void JSONCompactColumnsReader::readChunkStart()
{
    JSONUtils::skipArrayStart(*in);
}

std::optional<String> JSONCompactColumnsReader::readColumnStart()
{
    JSONUtils::skipArrayStart(*in);
    return std::nullopt;
}

bool JSONCompactColumnsReader::checkChunkEnd()
{
    return JSONUtils::checkAndSkipArrayEnd(*in);
}


void registerInputFormatJSONCompactColumns(FormatFactory & factory)
{
    factory.registerInputFormat(
        "JSONCompactColumns",
        [](ReadBuffer & buf,
           const Block &sample,
           const RowInputFormatParams &,
           const FormatSettings & settings)
        {
            return std::make_shared<JSONColumnsBlockInputFormatBase>(buf, std::make_shared<const Block>(sample), settings, std::make_unique<JSONCompactColumnsReader>(buf));
        }
    );
}

void registerJSONCompactColumnsSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "JSONCompactColumns",
        [](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_shared<JSONColumnsSchemaReaderBase>(buf, settings, std::make_unique<JSONCompactColumnsReader>(buf));
        }
    );
    factory.registerAdditionalInfoForSchemaCacheGetter("JSONCompactColumns", [](const FormatSettings & settings)
    {
        auto result = getAdditionalFormatInfoForAllRowBasedFormats(settings) + getAdditionalFormatInfoByEscapingRule(settings, FormatSettings::EscapingRule::JSON);
        return result + fmt::format(", column_names_for_schema_inference={}", settings.column_names_for_schema_inference);
    });
}

}
