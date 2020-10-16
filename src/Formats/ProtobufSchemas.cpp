#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#    include "config_formats.h"
#endif

#if USE_PROTOBUF
#    include <Formats/FormatSchemaInfo.h>
#    include <Formats/ProtobufHDFSSourceTree.h>
#    include <Formats/ProtobufSchemas.h>
#    include <google/protobuf/compiler/importer.h>
#    include <Common/Exception.h>
#    include <Common/StringUtils/StringUtils.h>


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
    explicit ImporterWithSourceTree(const String & schema_directory)
    {
#if USE_HDFS
        if (startsWith(schema_directory, "hdfs://"))
        {
            importer = std::make_unique<google::protobuf::compiler::Importer>(&hdfs_source_tree, this);
            hdfs_source_tree.init(schema_directory);
        }
        else
#endif
        {
            importer = std::make_unique<google::protobuf::compiler::Importer>(&disk_source_tree, this);
            disk_source_tree.MapPath("", schema_directory);
        }
    }

    ~ImporterWithSourceTree() override = default;

    const google::protobuf::Descriptor * import(const String & schema_path, const String & message_name)
    {
        // Search the message type among already imported ones.
        const auto * descriptor = importer->pool()->FindMessageTypeByName(message_name);
        if (descriptor)
            return descriptor;

        const auto * file_descriptor = importer->Import(schema_path);
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
#if USE_HDFS
    ProtobufHDFSSourceTree hdfs_source_tree;
#endif
    std::unique_ptr<google::protobuf::compiler::Importer> importer;
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
