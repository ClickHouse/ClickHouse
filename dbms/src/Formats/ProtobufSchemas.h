#pragma once

#include <optional>
#include <Core/Types.h>
#include <google/protobuf/compiler/importer.h>
#include <ext/singleton.h>


namespace google
{
namespace protobuf
{
    class Descriptor;
}
}

namespace DB
{
class Block;
class FormatSchemaInfo;
struct ColumnWithTypeAndName;

/** Keeps parsed google protobuf schemas either parsed from files or generated from DB columns.
  * This class is used to handle the "Protobuf" input/output formats.
  */
class ProtobufSchemas : public ext::singleton<ProtobufSchemas>, public google::protobuf::compiler::MultiFileErrorCollector
{
public:
    ProtobufSchemas();
    ~ProtobufSchemas() override;

    /// Parses the format schema, then parses the corresponding proto file, and returns the descriptor of the message type.
    /// The function never returns nullptr, it throws an exception if it cannot load or parse the file.
    const google::protobuf::Descriptor * getMessageTypeForFormatSchema(const FormatSchemaInfo& info);

    /// Generates a message type with suitable types of fields to store a block with |header|, then returns the descriptor
    /// of the generated message type.
    const google::protobuf::Descriptor * getMessageTypeForColumns(const std::vector<ColumnWithTypeAndName> & columns);

private:
    // Overrides google::protobuf::compiler::MultiFileErrorCollector:
    void AddError(const String & filename, int line, int column, const String & message) override;

    std::optional<String> proto_directory;
    std::unique_ptr<google::protobuf::compiler::DiskSourceTree> disk_source_tree;
    std::unique_ptr<google::protobuf::compiler::Importer> importer;
};

}
