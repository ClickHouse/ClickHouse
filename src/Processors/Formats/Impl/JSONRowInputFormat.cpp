#include <Processors/Formats/Impl/JSONRowInputFormat.h>
#include <Formats/JSONUtils.h>
#include <Formats/FormatFactory.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

JSONRowInputFormat::JSONRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_)
    : JSONEachRowRowInputFormat(in_, header_, params_, format_settings_, false), use_metadata(format_settings_.json.use_metadata)
{
}

void JSONRowInputFormat::readPrefix()
{
    skipBOMIfExists(*in);
    JSONUtils::skipObjectStart(*in);
    auto names_and_types = JSONUtils::readMetadata(*in);
    if (use_metadata)
    {
        auto header = getPort().getHeader();
        for (const auto & [name, type] : names_and_types)
        {
            auto header_type = header.getByName(name).type;
            if (header.has(name) && !type->equals(*header_type))
                throw Exception(
                    ErrorCodes::INCORRECT_DATA, "Type {} of column '{}' from metadata is not the same as type in header {}", type->getName(), name, header_type->getName());
        }
    }
    JSONUtils::skipComma(*in);
    while (!JSONUtils::checkAndSkipObjectEnd(*in))
    {
        auto field_name = JSONUtils::readFieldName(*in);
        LOG_DEBUG(&Poco::Logger::get("JSONRowInputFormat"), "Field {}", field_name);
        if (field_name == "data")
        {
            JSONUtils::skipArrayStart(*in);
            data_in_square_brackets = true;
            return;
        }
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "Expected field \"data\" with table content");
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
