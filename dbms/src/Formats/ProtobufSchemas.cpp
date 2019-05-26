#include <Common/config.h>
#if USE_PROTOBUF

#include <Formats/FormatSchemaInfo.h>
#include <Formats/ProtobufSchemas.h> // Y_IGNORE
#include <google/protobuf/compiler/importer.h> // Y_IGNORE
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
}


class ProtobufSchemas::ImporterWithSourceTree : public google::protobuf::compiler::MultiFileErrorCollector
{
public:
    ImporterWithSourceTree(const String & schema_directory) : importer(&disk_source_tree, this)
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
        // If there are parsing errors AddError() throws an exception and in this case the following line
        // isn't executed.
        assert(file_descriptor);

        descriptor = file_descriptor->FindMessageTypeByName(message_name);
        if (!descriptor)
            throw Exception(
                "Not found a message named '" + message_name + "' in the schema file '" + schema_path + "'", ErrorCodes::BAD_ARGUMENTS);

        return descriptor;
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
};


ProtobufSchemas::ProtobufSchemas() = default;
ProtobufSchemas::~ProtobufSchemas() = default;

const google::protobuf::Descriptor * ProtobufSchemas::getMessageTypeForFormatSchema(const FormatSchemaInfo & info)
{
    auto it = importers.find(info.schemaDirectory());
    if (it == importers.end())
        it = importers.emplace(info.schemaDirectory(), std::make_unique<ImporterWithSourceTree>(info.schemaDirectory())).first;
    auto * importer = it->second.get();
    return importer->import(info.schemaPath(), info.messageName());
}

}

#endif
