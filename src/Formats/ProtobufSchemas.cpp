#include "config.h"

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

void ProtobufSchemas::clear()
{
    std::lock_guard lock(mutex);
    importers.clear();
}

class ProtobufSchemas::ImporterWithSourceTree : public google::protobuf::compiler::MultiFileErrorCollector
{
public:
    explicit ImporterWithSourceTree(const String & schema_directory, const String & google_protos_path, WithEnvelope with_envelope_)
        : importer(&disk_source_tree, this), with_envelope(with_envelope_)
    {
        disk_source_tree.MapPath("", schema_directory);
        disk_source_tree.MapPath("", google_protos_path);
    }

    ~ImporterWithSourceTree() override = default;

    const google::protobuf::Descriptor * import(const String & schema_path, const String & message_name)
    {
        // Search the message type among already imported ones.
        const auto * descriptor = importer.pool()->FindMessageTypeByName(message_name);
        if (descriptor)
            return descriptor;

        const auto * file_descriptor = importer.Import(schema_path);
        if (error)
        {
            auto info = error.value();
            error.reset();
            throw Exception(
                ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA,
                "Cannot parse '{}' file, found an error at line {}, column {}, {}",
                info.filename,
                std::to_string(info.line),
                std::to_string(info.column),
                info.message);
        }

        assert(file_descriptor);

        if (with_envelope == WithEnvelope::No)
        {
            const auto * message_descriptor = file_descriptor->FindMessageTypeByName(message_name);
            if (!message_descriptor)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Could not find a message named '{}' in the schema file '{}'",
                    message_name, schema_path);

            return message_descriptor;
        }

        const auto * envelope_descriptor = file_descriptor->FindMessageTypeByName("Envelope");
        if (!envelope_descriptor)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Could not find a message named 'Envelope' in the schema file '{}'", schema_path);

        const auto * message_descriptor = envelope_descriptor->FindNestedTypeByName(
            message_name); // silly protobuf API disallows a restricting the field type to messages
        if (!message_descriptor)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Could not find a message named '{}' in the schema file '{}'", message_name, schema_path);

        return message_descriptor;
    }

private:
    // Overrides google::protobuf::compiler::MultiFileErrorCollector:
    void AddError(const String & filename, int line, int column, const String & message) override
    {
        /// Protobuf library code is not exception safe, we should
        /// remember the error and throw it later from our side.
        error = ErrorInfo{filename, line, column, message};
    }

    google::protobuf::compiler::DiskSourceTree disk_source_tree;
    google::protobuf::compiler::Importer importer;
    const WithEnvelope with_envelope;

    struct ErrorInfo
    {
        String filename;
        int line;
        int column;
        String message;
    };

    std::optional<ErrorInfo> error;
};


ProtobufSchemas::DescriptorHolder
ProtobufSchemas::getMessageTypeForFormatSchema(const FormatSchemaInfo & info, WithEnvelope with_envelope, const String & google_protos_path)
{
    std::lock_guard lock(mutex);
    auto it = importers.find(info.schemaDirectory());
    if (it == importers.end())
        it = importers
                 .emplace(
                     info.schemaDirectory(),
                     std::make_shared<ImporterWithSourceTree>(info.schemaDirectory(), google_protos_path, with_envelope))
                 .first;
    auto * importer = it->second.get();
    return DescriptorHolder(it->second, importer->import(info.schemaPath(), info.messageName()));
}

}

#endif
