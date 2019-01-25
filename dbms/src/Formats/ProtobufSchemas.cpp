#include <Common/config.h>
#if USE_PROTOBUF

#include <Common/Exception.h>
#include <Core/Block.h>
#include <Formats/FormatSchemaInfo.h>
#include <Formats/ProtobufSchemas.h>
#include <Poco/Path.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
}

ProtobufSchemas::ProtobufSchemas()
    : disk_source_tree(new google::protobuf::compiler::DiskSourceTree())
    , importer(new google::protobuf::compiler::Importer(disk_source_tree.get(), this))
{
}

ProtobufSchemas::~ProtobufSchemas() = default;

const google::protobuf::Descriptor *
ProtobufSchemas::getMessageTypeForFormatSchema(const FormatSchemaInfo& info)
{
    // Search the message type among already imported ones.
    const auto * descriptor = importer->pool()->FindMessageTypeByName(info.messageName());
    if (descriptor)
        return descriptor;

    // Initialize mapping in protobuf's DiskSourceTree.
    if (proto_directory.has_value())
    {
        assert(*proto_directory == info.schemaDirectory()); // format_schema_path should not be changed!
    }
    else
    {
        proto_directory = info.schemaDirectory();
        disk_source_tree->MapPath("", *proto_directory);
    }

    const auto * file_descriptor = importer->Import(info.schemaPath());

    // If there parsing errors AddError() throws an exception and in this case the following line
    // isn't executed.
    assert(file_descriptor);

    descriptor = file_descriptor->FindMessageTypeByName(info.messageName());
    if (!descriptor)
        throw Exception(
            "Not found a message named '" + info.messageName() + "' in the schema file '" + info.schemaPath() + "'",
            ErrorCodes::BAD_ARGUMENTS);
    return descriptor;
}

const google::protobuf::Descriptor * ProtobufSchemas::getMessageTypeForColumns(const std::vector<ColumnWithTypeAndName> & /*columns*/)
{
    throw Exception("Using the 'Protobuf' format without schema is not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void ProtobufSchemas::AddError(const String & filename, int line, int column, const String & message)
{
    throw Exception(
        "Cannot parse '" + filename + "' file, found an error at line " + std::to_string(line) + ", column " + std::to_string(column) + ", "
            + message,
        ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA);
}

}

#endif
