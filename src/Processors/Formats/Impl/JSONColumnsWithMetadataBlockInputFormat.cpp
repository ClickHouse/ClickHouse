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

JSONColumnsWithMetadataReader::JSONColumnsWithMetadataReader(ReadBuffer & in_, const Block & header_, const FormatSettings & settings)
    : JSONColumnsReader(in_), header(header_), validate_types_from_metadata(settings.json.validate_types_from_metadata)
{
}

void JSONColumnsWithMetadataReader::readChunkStart()
{
    skipBOMIfExists(*in);
    JSONUtils::skipObjectStart(*in);
    if (validate_types_from_metadata)
        JSONUtils::readMetadataAndValidateHeader(*in, header);
    else
        JSONUtils::readMetadata(*in);

    JSONUtils::skipComma(*in);
    if (!JSONUtils::skipUntilFieldInObject(*in, "data"))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Expected field \"data\" with table content");

    JSONUtils::skipObjectStart(*in);
}


bool JSONColumnsWithMetadataReader::checkChunkEnd()
{
    if (!JSONUtils::checkAndSkipObjectEnd(*in))
        return false;

    JSONUtils::skipTheRestOfObject(*in);
    assertEOF(*in);
    return true;
}

JSONColumnsWithMetadataSchemaReader::JSONColumnsWithMetadataSchemaReader(ReadBuffer & in_) : ISchemaReader(in_)
{
}

NamesAndTypesList JSONColumnsWithMetadataSchemaReader::readSchema()
{
    skipBOMIfExists(in);
    JSONUtils::skipObjectStart(in);
    return JSONUtils::readMetadata(in);
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
        [](ReadBuffer & buf, const FormatSettings &)
        {
            return std::make_shared<JSONColumnsWithMetadataSchemaReader>(buf);
        }
    );
}

}
