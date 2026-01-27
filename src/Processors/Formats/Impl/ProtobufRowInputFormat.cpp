#include <Processors/Formats/Impl/ProtobufRowInputFormat.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPCredentials.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>

#include <IO/ReadHelpers.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromString.h>
#include <IO/PeekableReadBuffer.h>

#include <Common/Exception.h>
#include <Common/CacheBase.h>
#include "IO/VarInt.h"
#include "Interpreters/Context_fwd.h"
#include "base/types.h"
#include <Core/Block_fwd.h>

#include <Processors/Formats/Impl/ConfluentRegistry.h>

#if USE_PROTOBUF
#   include <Columns/IColumn.h>
#   include <Core/Block.h>
#   include <Formats/FormatFactory.h>
#   include <Formats/ProtobufReader.h>
#   include <Formats/ProtobufSchemas.h>
#   include <Formats/ProtobufSerializer.h>

#   include <google/protobuf/descriptor.pb.h>
#   include <google/protobuf/dynamic_message.h>
#   include <google/protobuf/compiler/importer.h>
#   include <google/protobuf/io/zero_copy_stream_impl.h>
#   include <google/protobuf/io/tokenizer.h>

namespace CurrentMetrics
{
    extern const Metric ProtobufSchemaRegistryCacheBytes;
    extern const Metric ProtobufSchemaRegistryCacheCells;
    extern const Metric MarkCacheBytes;
    extern const Metric MarkCacheFiles;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int INCORRECT_DATA;
}

ProtobufRowInputFormat::ProtobufRowInputFormat(
    ReadBuffer & in_,
    SharedHeader header_,
    const Params & params_,
    const ProtobufSchemaInfo & schema_info_,
    bool with_length_delimiter_,
    bool flatten_google_wrappers_,
    bool oneof_presence_,
    const String & google_protos_path)
    : IRowInputFormat(header_, in_, params_)
    , descriptor(ProtobufSchemas::instance().getMessageTypeForFormatSchema(
          schema_info_.getSchemaInfo(), ProtobufSchemas::WithEnvelope::No, google_protos_path))
    , with_length_delimiter(with_length_delimiter_)
    , flatten_google_wrappers(flatten_google_wrappers_)
    , oneof_presence(oneof_presence_)
{
}

void ProtobufRowInputFormat::createReaderAndSerializer()
{
    reader = std::make_unique<ProtobufReader>(*in);
    serializer = ProtobufSerializer::create(
        getPort().getHeader().getNames(),
        getPort().getHeader().getDataTypes(),
        missing_column_indices,
        descriptor,
        with_length_delimiter,
        /* with_envelope = */ false,
        flatten_google_wrappers,
        oneof_presence,
        *reader);
}

void ProtobufRowInputFormat::destroyReaderAndSerializer()
{
    serializer = nullptr;
    reader.reset();
}

bool ProtobufRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & row_read_extension)
try
{
    if (!reader)
        createReaderAndSerializer();

    if (reader->eof())
        return false;

    size_t row_num = columns.empty() ? 0 : columns[0]->size();
    if (!row_num)
        serializer->setColumns(columns.data(), columns.size());

    serializer->readRow(row_num);

    row_read_extension.read_columns.clear();
    row_read_extension.read_columns.resize(columns.size(), true);
    for (size_t column_idx : missing_column_indices)
        row_read_extension.read_columns[column_idx] = false;
    return true;
}
catch (...)
{
    destroyReaderAndSerializer();
    throw;
}

void ProtobufRowInputFormat::setReadBuffer(ReadBuffer & in_)
{
    if (reader)
        reader->setReadBuffer(in_);
    IRowInputFormat::setReadBuffer(in_);
}

bool ProtobufRowInputFormat::allowSyncAfterError() const
{
    return true;
}

void ProtobufRowInputFormat::syncAfterError()
{
    reader->endMessage(true);
}

size_t ProtobufRowInputFormat::countRows(size_t max_block_size)
try
{
    if (!reader)
        createReaderAndSerializer();

    size_t num_rows = 0;
    while (!reader->eof() && num_rows < max_block_size)
    {
        reader->startMessage(with_length_delimiter);
        reader->endMessage(false);
        ++num_rows;
    }

    return num_rows;
}
catch (...)
{
    destroyReaderAndSerializer();
    throw;
}

ProtobufSchemaReader::ProtobufSchemaReader(const FormatSettings & format_settings)
    : schema_info(
          /*format_schema_source=*/format_settings.schema.format_schema_source,
          /*format_schema=*/format_settings.schema.format_schema,
          /*format_schema_message_name=*/format_settings.schema.format_schema_message_name,
          /*format=*/"Protobuf",
          /*require_message=*/true,
          /*is_server=*/format_settings.schema.is_server,
          /*format_schema_path=*/format_settings.schema.format_schema_path)
    , skip_unsupported_fields(format_settings.protobuf.skip_fields_with_unsupported_types_in_schema_inference)
    , oneof_presence(format_settings.protobuf.oneof_presence)
    , google_protos_path(format_settings.protobuf.google_protos_path)
{
}

NamesAndTypesList ProtobufSchemaReader::readSchema()
{
    auto descriptor = ProtobufSchemas::instance().getMessageTypeForFormatSchema(
        schema_info, ProtobufSchemas::WithEnvelope::No, google_protos_path);
    return protobufSchemaToCHSchema(descriptor.message_descriptor, skip_unsupported_fields, oneof_presence);
}

#define SCHEMA_REGISTRY_CACHE_MAX_SIZE 1000
/// Cache of Schema Registry URL -> SchemaRegistry
static CacheBase<std::string, ConfluentSchemaRegistry> schema_registry_cache(CurrentMetrics::ProtobufSchemaRegistryCacheBytes, CurrentMetrics::ProtobufSchemaRegistryCacheCells, SCHEMA_REGISTRY_CACHE_MAX_SIZE);

static std::shared_ptr<ConfluentSchemaRegistry> getConfluentSchemaRegistry(const FormatSettings & format_settings)
{
    const auto & base_url = format_settings.protobuf.schema_registry_url;
    auto [schema_registry, loaded] = schema_registry_cache.getOrSet(
        base_url,
        [base_url]()
        {
            return std::make_shared<ConfluentSchemaRegistry>(base_url, "ProtobufConfluentRowInputFormat");
        }
    );
    return schema_registry;
}

static uint32_t readConfluentSchemaId(ReadBuffer & in, bool first_row)
{
    uint8_t magic = 0x00;
    uint32_t schema_id;

    try
    {
        if (first_row)
             readBinaryBigEndian(magic, in);

        readBinaryBigEndian(schema_id, in);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::CANNOT_READ_ALL_DATA)
        {
            /* empty or incomplete message without Protobuf Confluent magic number or schema id */
            throw Exception(ErrorCodes::INCORRECT_DATA, "Missing ProtobufConfluent magic byte or schema identifier.");
        }
        throw;
    }

    if (magic != 0x00)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid magic byte before ProtobufConfluent schema identifier. "
            "Must be zero byte, found {} instead", int(magic));
    }

    return schema_id;
}

ProtobufConfluentRowInputFormat::ProtobufConfluentRowInputFormat(
    SharedHeader header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, params_)
    , schema_registry(getConfluentSchemaRegistry(format_settings_))
    , format_settings(format_settings_)
    , readers(CurrentMetrics::MarkCacheBytes, CurrentMetrics::MarkCacheFiles, 1024)
    , serializers(CurrentMetrics::MarkCacheBytes, CurrentMetrics::MarkCacheFiles, 1024)
    , descriptor(nullptr)
    , with_length_delimiter(false)
    , flatten_google_wrappers(format_settings_.protobuf.input_flatten_google_wrappers)
{
}

void ProtobufConfluentRowInputFormat::readPrefix()
{
}

void ProtobufConfluentRowInputFormat::createReaderAndSerializer(SchemaId schema_id)
{
    if (readers.contains(schema_id))
        return;

    readers.set(schema_id, std::make_shared<ProtobufReader>(*in));
    serializers.set(schema_id, ProtobufSerializer::create(
        getPort().getHeader().getNames(),
        getPort().getHeader().getDataTypes(),
        missing_column_indices,
        descriptor,
        with_length_delimiter,
        /* with_envelope = */ false,
        flatten_google_wrappers,
        *readers.get(schema_id),
        false,
        true));
}

bool ProtobufConfluentRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & row_read_extension)
{
    if (in->eof())
    {
        return false;
    }
    // skip tombstone records (kafka messages with null value)
    if (in->available() == 0)
    {
        return false;
    }
    SchemaId schema_id = readConfluentSchemaId(*in, first_row);
    first_row = false;
    const google::protobuf::Descriptor * base_descriptor = schema_registry->getProtobufSchema(schema_id);

    std::vector<Int64> indices;

    Int64 first_value;
    readVarInt(first_value, *in);

    if (first_value == 0)
    {
        indices.push_back(0);
    }
    else
    {
        UInt64 indices_size = static_cast<UInt64>(first_value);
        indices.reserve(indices_size);
        for (size_t i = 0; i < indices_size; ++i)
        {
            Int64 index;
            readVarInt(index, *in);
            indices.push_back(index);
        }
    }

    if (base_descriptor)
    {
        const google::protobuf::FileDescriptor * file_descriptor = base_descriptor->file();
        const google::protobuf::Descriptor * target_descriptor = nullptr;
        
        if (indices.size() == 1 && indices[0] == 0)
        {
            target_descriptor = file_descriptor->message_type(0);
        }
        else
        {
            target_descriptor = file_descriptor->message_type(static_cast<int>(indices[0]));
            for (size_t i = 1; i < indices.size(); ++i)
                target_descriptor = target_descriptor->nested_type(static_cast<int>(indices[i]));
        }

        descriptor = target_descriptor;
        createReaderAndSerializer(schema_id);
    }
    size_t row_num = columns.empty() ? 0 : columns[0]->size();

    serializers.get(schema_id)->setColumns(columns.data(), columns.size());
    serializers.get(schema_id)->readRow(row_num);
    
    row_read_extension.read_columns.clear();
    row_read_extension.read_columns.resize(columns.size(), true);
    for (size_t column_idx : missing_column_indices)
        row_read_extension.read_columns[column_idx] = false;
    return true;
}

void ProtobufConfluentRowInputFormat::syncAfterError()
{
    // skip until the end of current kafka message
    in->tryIgnore(in->available());
}

void registerInputFormatProtobuf(FormatFactory & factory)
{
    for (bool with_length_delimiter : {false, true})
    {
        factory.registerInputFormat(
            with_length_delimiter ? "Protobuf" : "ProtobufSingle",
            [with_length_delimiter](ReadBuffer & buf, const Block & sample, IRowInputFormat::Params params, const FormatSettings & settings)
            {
                return std::make_shared<ProtobufRowInputFormat>(
                    buf,
                    std::make_shared<const Block>(sample),
                    std::move(params),
                    ProtobufSchemaInfo(settings, "Protobuf", sample, settings.protobuf.use_autogenerated_schema),
                    with_length_delimiter,
                    settings.protobuf.input_flatten_google_wrappers,
                    true,
                    settings.protobuf.google_protos_path);
            });
        factory.markFormatSupportsSubsetOfColumns(with_length_delimiter ? "Protobuf" : "ProtobufSingle");
    }

    factory.registerInputFormat("ProtobufConfluent",[](
        ReadBuffer & buf,
        const Block & sample,
        const RowInputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<ProtobufConfluentRowInputFormat>(std::make_shared<const Block>(sample), buf, params, settings);
    });
}

void registerProtobufSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("Protobuf", [](const FormatSettings & settings)
    {
        return std::make_shared<ProtobufSchemaReader>(settings);
    });
    factory.registerFileExtension("pb", "Protobuf");

    factory.registerExternalSchemaReader("ProtobufSingle", [](const FormatSettings & settings)
    {
        return std::make_shared<ProtobufSchemaReader>(settings);
    });

    for (const auto & name : {"Protobuf", "ProtobufSingle"})
    {
        factory.registerAdditionalInfoForSchemaCacheGetter(
            name,
            [](const FormatSettings & settings)
            {
                return fmt::format(
                    "format_schema={}, skip_fields_with_unsupported_types_in_schema_inference={}",
                    settings.schema.format_schema,
                    settings.protobuf.skip_fields_with_unsupported_types_in_schema_inference);
            });
    }
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatProtobuf(FormatFactory &) {}
void registerProtobufSchemaReader(FormatFactory &) {}
}

#endif
