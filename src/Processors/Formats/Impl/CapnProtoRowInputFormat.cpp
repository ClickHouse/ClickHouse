#include "CapnProtoRowInputFormat.h"
#if USE_CAPNP

#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSchemaInfo.h>
#include <capnp/serialize.h>
#include <capnp/dynamic.h>
#include <capnp/common.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

CapnProtoRowInputFormat::CapnProtoRowInputFormat(ReadBuffer & in_, Block header_, Params params_, const CapnProtoSchemaInfo & info, const FormatSettings & format_settings)
    : IRowInputFormat(std::move(header_), in_, std::move(params_))
    , parser(std::make_shared<CapnProtoSchemaParser>())
{
    // Parse the schema and fetch the root object
    schema = parser->getMessageSchema(info.getSchemaInfo());
    const auto & header = getPort().getHeader();
    serializer = std::make_unique<CapnProtoSerializer>(header.getDataTypes(), header.getNames(), schema, format_settings.capn_proto);
}

std::pair<kj::Array<capnp::word>, size_t> CapnProtoRowInputFormat::readMessagePrefix()
{
    uint32_t segment_count;
    in->readStrict(reinterpret_cast<char*>(&segment_count), sizeof(uint32_t));
    /// Don't allow large amount of segments as it's done in capnproto library:
    /// https://github.com/capnproto/capnproto/blob/931074914eda9ca574b5c24d1169c0f7a5156594/c%2B%2B/src/capnp/serialize.c%2B%2B#L181
    /// Large amount of segments can indicate that corruption happened.
    if (segment_count >= 512)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Message has too many segments. Most likely, data was corrupted");

    // one for segmentCount and one because segmentCount starts from 0
    const auto prefix_size = (2 + segment_count) * sizeof(uint32_t);
    const auto words_prefix_size = (segment_count + 1) / 2 + 1;
    auto prefix = kj::heapArray<capnp::word>(words_prefix_size);
    auto prefix_chars = prefix.asChars();
    ::memcpy(prefix_chars.begin(), &segment_count, sizeof(uint32_t));

    // read size of each segment
    for (size_t i = 0; i <= segment_count; ++i)
        in->readStrict(prefix_chars.begin() + ((i + 1) * sizeof(uint32_t)), sizeof(uint32_t));

    return {std::move(prefix), prefix_size};
}

kj::Array<capnp::word> CapnProtoRowInputFormat::readMessage()
{
    auto [prefix, prefix_size] = readMessagePrefix();
    auto prefix_chars = prefix.asChars();

    // calculate size of message
    const auto expected_words = capnp::expectedSizeInWordsFromPrefix(prefix);
    const auto expected_bytes = expected_words * sizeof(capnp::word);
    const auto data_size = expected_bytes - prefix_size;
    auto msg = kj::heapArray<capnp::word>(expected_words);
    auto msg_chars = msg.asChars();

    // read full message
    ::memcpy(msg_chars.begin(), prefix_chars.begin(), prefix_size);
    in->readStrict(msg_chars.begin() + prefix_size, data_size);

    return msg;
}

void CapnProtoRowInputFormat::skipMessage()
{
    auto [prefix, prefix_size] = readMessagePrefix();

    // calculate size of message
    const auto expected_bytes = capnp::expectedSizeInWordsFromPrefix(prefix) * sizeof(capnp::word);
    const auto data_size = expected_bytes - prefix_size;

    // skip full message
    in->ignore(data_size);
}

bool CapnProtoRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    if (in->eof())
        return false;

    try
    {
        auto array = readMessage();
        capnp::FlatArrayMessageReader msg(array);
        auto root_reader = msg.getRoot<capnp::DynamicStruct>(schema);
        serializer->readRow(columns, root_reader);
    }
    catch (const kj::Exception & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read row: {}", e.getDescription().cStr());
    }

    return true;
}

size_t CapnProtoRowInputFormat::countRows(size_t max_block_size)
{
    size_t num_rows = 0;
    while (!in->eof() && num_rows < max_block_size)
    {
        skipMessage();
        ++num_rows;
    }

    return num_rows;
}

CapnProtoSchemaReader::CapnProtoSchemaReader(const FormatSettings & format_settings_) : format_settings(format_settings_)
{
}

NamesAndTypesList CapnProtoSchemaReader::readSchema()
{
    auto schema_info = FormatSchemaInfo(
        format_settings.schema.format_schema,
        "CapnProto",
        true,
        format_settings.schema.is_server,
        format_settings.schema.format_schema_path);

    auto schema_parser = CapnProtoSchemaParser();
    auto schema = schema_parser.getMessageSchema(schema_info);
    return capnProtoSchemaToCHSchema(schema, format_settings.capn_proto.skip_fields_with_unsupported_types_in_schema_inference);
}

void registerInputFormatCapnProto(FormatFactory & factory)
{
    factory.registerInputFormat(
        "CapnProto",
        [](ReadBuffer & buf, const Block & sample, IRowInputFormat::Params params, const FormatSettings & settings)
        {
            return std::make_shared<CapnProtoRowInputFormat>(
                buf,
                sample,
                std::move(params),
                CapnProtoSchemaInfo(settings, "CapnProto", sample, settings.capn_proto.use_autogenerated_schema),
                settings);
        });
    factory.markFormatSupportsSubsetOfColumns("CapnProto");
    factory.registerFileExtension("capnp", "CapnProto");
    factory.registerAdditionalInfoForSchemaCacheGetter(
        "CapnProto",
        [](const FormatSettings & settings)
        {
            return fmt::format(
                "format_schema={}, skip_fields_with_unsupported_types_in_schema_inference={}",
                settings.schema.format_schema,
                settings.capn_proto.skip_fields_with_unsupported_types_in_schema_inference);
        });
}

void registerCapnProtoSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("CapnProto", [](const FormatSettings & settings)
    {
       return std::make_shared<CapnProtoSchemaReader>(settings);
    });
}

}

#else

namespace DB
{
    class FormatFactory;
    void registerInputFormatCapnProto(FormatFactory &) {}
    void registerCapnProtoSchemaReader(FormatFactory &) {}
}

#endif // USE_CAPNP
