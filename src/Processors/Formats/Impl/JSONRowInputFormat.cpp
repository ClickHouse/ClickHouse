#include <Processors/Formats/Impl/JSONRowInputFormat.h>
#include <Formats/JSONUtils.h>
#include <Formats/FormatFactory.h>
#include <Formats/EscapingRuleUtils.h>
#include <IO/ReadHelpers.h>

namespace DB
{

JSONRowInputFormat::JSONRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_)
    : JSONRowInputFormat(std::make_unique<PeekableReadBuffer>(in_), header_, params_, format_settings_)
{
}

JSONRowInputFormat::JSONRowInputFormat(std::unique_ptr<PeekableReadBuffer> buf, const DB::Block & header_, DB::IRowInputFormat::Params params_, const DB::FormatSettings & format_settings_)
    : JSONEachRowRowInputFormat(*buf, header_, params_, format_settings_, false), validate_types_from_metadata(format_settings_.json.validate_types_from_metadata), peekable_buf(std::move(buf))
{
}

void JSONRowInputFormat::readPrefix()
{
    skipBOMIfExists(*peekable_buf);

    PeekableReadBufferCheckpoint checkpoint(*peekable_buf);
    NamesAndTypesList names_and_types_from_metadata;

    /// Try to parse metadata, if failed, try to parse data as JSONEachRow format.
    if (JSONUtils::checkAndSkipObjectStart(*peekable_buf)
        && JSONUtils::tryReadMetadata(*peekable_buf, names_and_types_from_metadata)
        && JSONUtils::checkAndSkipComma(*peekable_buf)
        && JSONUtils::skipUntilFieldInObject(*peekable_buf, "data")
        && JSONUtils::checkAndSkipArrayStart(*peekable_buf))
    {
        data_in_square_brackets = true;
        if (validate_types_from_metadata)
        {
            JSONUtils::validateMetadataByHeader(names_and_types_from_metadata, getPort().getHeader());
        }
    }
    else
    {
        parse_as_json_each_row = true;
        peekable_buf->rollbackToCheckpoint();
        JSONEachRowRowInputFormat::readPrefix();
    }
}

void JSONRowInputFormat::readSuffix()
{
    if (parse_as_json_each_row)
    {
        JSONEachRowRowInputFormat::readSuffix();
    }
    else
    {
        JSONUtils::skipArrayEnd(*peekable_buf);
        JSONUtils::skipTheRestOfObject(*peekable_buf);
    }
}

void JSONRowInputFormat::setReadBuffer(DB::ReadBuffer & in_)
{
    peekable_buf = std::make_unique<PeekableReadBuffer>(in_);
    JSONEachRowRowInputFormat::setReadBuffer(*peekable_buf);
}

void JSONRowInputFormat::resetReadBuffer()
{
    peekable_buf.reset();
    JSONEachRowRowInputFormat::resetReadBuffer();
}

JSONRowSchemaReader::JSONRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : JSONRowSchemaReader(std::make_unique<PeekableReadBuffer>(in_), format_settings_)
{
}

JSONRowSchemaReader::JSONRowSchemaReader(std::unique_ptr<PeekableReadBuffer> buf, const DB::FormatSettings & format_settings_)
    : JSONEachRowSchemaReader(*buf, format_settings_), peekable_buf(std::move(buf))
{
}

NamesAndTypesList JSONRowSchemaReader::readSchema()
{
    skipBOMIfExists(*peekable_buf);
    PeekableReadBufferCheckpoint checkpoint(*peekable_buf);
    /// Try to parse metadata, if failed, try to parse data as JSONEachRow format
    NamesAndTypesList names_and_types;
    if (JSONUtils::checkAndSkipObjectStart(*peekable_buf) && JSONUtils::tryReadMetadata(*peekable_buf, names_and_types))
        return names_and_types;

    peekable_buf->rollbackToCheckpoint(true);
    return JSONEachRowSchemaReader::readSchema();
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
            format, [](ReadBuffer & buf, const FormatSettings & format_settings) { return std::make_unique<JSONRowSchemaReader>(buf, format_settings); });

        factory.registerAdditionalInfoForSchemaCacheGetter(format, [](const FormatSettings & settings)
        {
            return getAdditionalFormatInfoByEscapingRule(settings, FormatSettings::EscapingRule::JSON);
        });
    };
    register_schema_reader("JSON");
    /// JSONCompact has the same suffix with metadata.
    register_schema_reader("JSONCompact");
}

}
