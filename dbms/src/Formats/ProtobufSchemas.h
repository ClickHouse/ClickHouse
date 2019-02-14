#pragma once

#include <Common/config.h>
#if USE_PROTOBUF

#include <memory>
#include <unordered_map>
#include <vector>
#include <Core/Types.h>
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
class FormatSchemaInfo;
struct ColumnWithTypeAndName;

/** Keeps parsed google protobuf schemas either parsed from files or generated from DB columns.
  * This class is used to handle the "Protobuf" input/output formats.
  */
class ProtobufSchemas : public ext::singleton<ProtobufSchemas>
{
public:
    ProtobufSchemas();
    ~ProtobufSchemas();

    /// Parses the format schema, then parses the corresponding proto file, and returns the descriptor of the message type.
    /// The function never returns nullptr, it throws an exception if it cannot load or parse the file.
    const google::protobuf::Descriptor * getMessageTypeForFormatSchema(const FormatSchemaInfo & info);

    /// Generates a message type with suitable types of fields to store a block with |header|, then returns the descriptor
    /// of the generated message type.
    const google::protobuf::Descriptor * getMessageTypeForColumns(const std::vector<ColumnWithTypeAndName> & columns);

private:
    class ImporterWithSourceTree;
    std::unordered_map<String, std::unique_ptr<ImporterWithSourceTree>> importers;
};

}

#endif
