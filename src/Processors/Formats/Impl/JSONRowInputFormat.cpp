#include <Processors/Formats/Impl/JSONRowInputFormat.h>
#include <Formats/JSONUtils.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

JSONRowInputFormat::JSONRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_)
    : JSONEachRowRowInputFormat(in_, header_, params_, format_settings_, false), validate_types_from_metadata(format_settings_.json.validate_types_from_metadata)
{
}

void JSONRowInputFormat::readPrefix()
{
    skipBOMIfExists(*in);
    JSONUtils::skipObjectStart(*in);
    if (validate_types_from_metadata)
        JSONUtils::readMetadataAndValidateHeader(*in, getPort().getHeader());
    else
        JSONUtils::readMetadata(*in);

    JSONUtils::skipComma(*in);
    if (!JSONUtils::skipUntilFieldInObject(*in, "data"))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Expected field \"data\" with table content");

    JSONUtils::skipArrayStart(*in);
    data_in_square_brackets = true;
}

void JSONRowInputFormat::readSuffix()
{
    JSONUtils::skipArrayEnd(*in);
    JSONUtils::skipTheRestOfObject(*in);
}

JSONRowSchemaReader::JSONRowSchemaReader(ReadBuffer & in_) : ISchemaReader(in_)
{
}

NamesAndTypesList JSONRowSchemaReader::readSchema()
{
    skipBOMIfExists(in);
    JSONUtils::skipObjectStart(in);
    return JSONUtils::readMetadata(in);
}

void registerInputFormatJSON(FormatFactory & factory)
{
    factory.registerInputFormat("JSON", [](
                     ReadBuffer & buf,
                     const Block & sample,
                     IRowInputFormat::Params params,
                     const FormatSettings & settings)
    {
        return std::make_shared<JSONRowInputFormat>(buf, sample, std::move(params), settings);
    });

    factory.markFormatSupportsSubsetOfColumns("JSON");
}

void registerJSONSchemaReader(FormatFactory & factory)
{
    auto register_schema_reader = [&](const String & format)
    {
        factory.registerSchemaReader(
            format, [](ReadBuffer & buf, const FormatSettings &) { return std::make_unique<JSONRowSchemaReader>(buf); });
    };
    register_schema_reader("JSON");
    /// JSONCompact has the same suffix with metadata.
    register_schema_reader("JSONCompact");
}

}
