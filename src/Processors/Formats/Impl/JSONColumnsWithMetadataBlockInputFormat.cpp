#include <Processors/Formats/Impl/JSONColumnsWithMetadataBlockInputFormat.h>
#include <IO/ReadHelpers.h>
#include <Formats/FormatFactory.h>
#include <Formats/EscapingRuleUtils.h>
#include <Formats/JSONUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

JSONColumnsWithMetadataReader::JSONColumnsWithMetadataReader(ReadBuffer & in_, const Block & header_, const FormatSettings & format_settings_)
    : JSONColumnsReader(in_, format_settings_), header(header_)
{
}

void JSONColumnsWithMetadataReader::readChunkStart()
{
    skipBOMIfExists(*in);
    JSONUtils::skipObjectStart(*in);
    if (format_settings.json.validate_types_from_metadata)
        JSONUtils::readMetadataAndValidateHeader(*in, header, format_settings.json);
    else
        JSONUtils::readMetadata(*in, format_settings.json);

    JSONUtils::skipComma(*in);
    if (!JSONUtils::skipUntilFieldInObject(*in, "data", format_settings.json))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Expected field \"data\" with table content");

    JSONUtils::skipObjectStart(*in);
}


bool JSONColumnsWithMetadataReader::checkChunkEnd()
{
    if (!JSONUtils::checkAndSkipObjectEnd(*in))
        return false;

    JSONUtils::skipTheRestOfObject(*in, format_settings.json);
    assertEOF(*in);
    return true;
}

JSONColumnsWithMetadataSchemaReader::JSONColumnsWithMetadataSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_) : ISchemaReader(in_), format_settings(format_settings_)
{
}

NamesAndTypesList JSONColumnsWithMetadataSchemaReader::readSchema()
{
    skipBOMIfExists(in);
    JSONUtils::skipObjectStart(in);
    return JSONUtils::readMetadata(in, format_settings.json);
}

void registerInputFormatJSONColumnsWithMetadata(FormatFactory & factory)
{
    factory.registerInputFormat(
        "JSONColumnsWithMetadata",
        [](ReadBuffer & buf,
           const Block & sample,
           const RowInputFormatParams &,
           const FormatSettings & settings)
        {
            return std::make_shared<JSONColumnsBlockInputFormatBase>(buf, sample, settings, std::make_unique<JSONColumnsWithMetadataReader>(buf, sample, settings));
        }
    );
    factory.markFormatSupportsSubsetOfColumns("JSONColumnsWithMetadata");
}

void registerJSONColumnsWithMetadataSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "JSONColumnsWithMetadata",
        [](ReadBuffer & buf, const FormatSettings & format_settings)
        {
            return std::make_shared<JSONColumnsWithMetadataSchemaReader>(buf, format_settings);
        }
    );
}

}
