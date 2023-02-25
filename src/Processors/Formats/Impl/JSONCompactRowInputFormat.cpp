#include <Processors/Formats/Impl/JSONCompactRowInputFormat.h>

#include <Formats/JSONUtils.h>
#include <Formats/FormatFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

JSONCompactRowInputFormat::JSONCompactRowInputFormat(
    const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes(
        header_, in_, params_, false, false, false, format_settings_, std::make_unique<JSONCompactFormatReader>(in_, format_settings_))
    , validate_types_from_metadata(format_settings_.json.validate_types_from_metadata)
{
}

void JSONCompactRowInputFormat::readPrefix()
{
    skipBOMIfExists(*in);
    JSONUtils::skipObjectStart(*in);
    if (validate_types_from_metadata)
    {
        auto names_and_types = JSONUtils::readMetadataAndValidateHeader(*in, getPort().getHeader());
        Names column_names;
        for (const auto & [name, type] : names_and_types)
            column_names.push_back(name);
        column_mapping->addColumns(column_names, column_indexes_by_names, format_settings);
    }
    else
    {
        JSONUtils::readMetadata(*in);
        column_mapping->setupByHeader(getPort().getHeader());
    }

    JSONUtils::skipComma(*in);
    if (!JSONUtils::skipUntilFieldInObject(*in, "data"))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Expected field \"data\" with table content");

    JSONUtils::skipArrayStart(*in);
}

void JSONCompactRowInputFormat::readSuffix()
{
    /// Array end was skipped in JSONCompactFormatReader::checkForSuffix
    JSONUtils::skipTheRestOfObject(*in);
}

void JSONCompactRowInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(*in);
}

JSONCompactFormatReader::JSONCompactFormatReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : JSONCompactEachRowFormatReader(in_, false, format_settings_)
{
}

bool JSONCompactFormatReader::checkForSuffix()
{
    return JSONUtils::checkAndSkipArrayEnd(*in);
}

void registerInputFormatJSONCompact(FormatFactory & factory)
{
    factory.registerInputFormat("JSONCompact", [](
                ReadBuffer & buf,
                const Block & sample,
                IRowInputFormat::Params params,
                const FormatSettings & settings)
    {
        return std::make_shared<JSONCompactRowInputFormat>(sample, buf, std::move(params), settings);
    });

    factory.markFormatSupportsSubsetOfColumns("JSONCompact");
}

}
