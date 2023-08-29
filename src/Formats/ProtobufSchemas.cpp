#include "config_formats.h"

#if USE_PROTOBUF
#    include <Formats/FormatSchemaInfo.h>
#    include <Formats/ProtobufSchemas.h>
#    include <google/protobuf/compiler/importer.h>
#    include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
}

ProtobufSchemas & ProtobufSchemas::instance()
{
    static ProtobufSchemas instance;
    return instance;
}

class ProtobufSchemas::ImporterWithSourceTree : public google::protobuf::compiler::MultiFileErrorCollector
{
public:
    explicit ImporterWithSourceTree(const String & schema_directory, WithEnvelope with_envelope_)
        : importer(&disk_source_tree, this)
        , with_envelope(with_envelope_)
    {
        disk_source_tree.MapPath("", schema_directory);
    }

    ~ImporterWithSourceTree() override = default;

    const google::protobuf::Descriptor * import(const String & schema_path, const String & message_name)
    {
        // Search the message type among already imported ones.
        const auto * descriptor = importer.pool()->FindMessageTypeByName(message_name);
        if (descriptor)
            return descriptor;

        const auto * file_descriptor = importer.Import(schema_path);
        // If there are parsing errors, AddError() throws an exception and in this case the following line
        // isn't executed.
        assert(file_descriptor);

        if (with_envelope == WithEnvelope::No)
        {
            const auto * message_descriptor = file_descriptor->FindMessageTypeByName(message_name);
            if (!message_descriptor)
                throw Exception(
                    "Could not find a message named '" + message_name + "' in the schema file '" + schema_path + "'", ErrorCodes::BAD_ARGUMENTS);

            return message_descriptor;
        }
        else
        {
            const auto * envelope_descriptor = file_descriptor->FindMessageTypeByName("Envelope");
            if (!envelope_descriptor)
                throw Exception(
                    "Could not find a message named 'Envelope' in the schema file '" + schema_path + "'", ErrorCodes::BAD_ARGUMENTS);

            const auto * message_descriptor = envelope_descriptor->FindNestedTypeByName(message_name); // silly protobuf API disallows a restricting the field type to messages
            if (!message_descriptor)
                throw Exception(
                    "Could not find a message named '" + message_name + "' in the schema file '" + schema_path + "'", ErrorCodes::BAD_ARGUMENTS);

            return message_descriptor;
        }
    }

private:
    // Overrides google::protobuf::compiler::MultiFileErrorCollector:
    void AddError(const String & filename, int line, int column, const String & message) override
    {
        throw Exception(
            "Cannot parse '" + filename + "' file, found an error at line " + std::to_string(line) + ", column " + std::to_string(column)
                + ", " + message,
            ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA);
    }

    google::protobuf::compiler::DiskSourceTree disk_source_tree;
    google::protobuf::compiler::Importer importer;
    const WithEnvelope with_envelope;
};


const google::protobuf::Descriptor * ProtobufSchemas::getMessageTypeForFormatSchema(const FormatSchemaInfo & info, WithEnvelope with_envelope)
{
    std::lock_guard lock(mutex);
    auto it = importers.find(info.schemaDirectory());
    if (it == importers.end())
        it = importers.emplace(info.schemaDirectory(), std::make_unique<ImporterWithSourceTree>(info.schemaDirectory(), with_envelope)).first;
    auto * importer = it->second.get();
    return importer->import(info.schemaPath(), info.messageName());
}

}

#endif
