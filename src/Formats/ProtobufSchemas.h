#pragma once

#include "config_formats.h"
#if USE_PROTOBUF

#include <memory>
#include <mutex>
#include <unordered_map>
#include <common/types.h>
#include <boost/noncopyable.hpp>


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

/** Keeps parsed google protobuf schemas parsed from files.
  * This class is used to handle the "Protobuf" input/output formats.
  */
class ProtobufSchemas : private boost::noncopyable
{
public:
    static ProtobufSchemas & instance();

    ProtobufSchemas();
    ~ProtobufSchemas();

    /// Parses the format schema, then parses the corresponding proto file, and returns the descriptor of the message type.
    /// The function never returns nullptr, it throws an exception if it cannot load or parse the file.
    const google::protobuf::Descriptor * getMessageTypeForFormatSchema(const FormatSchemaInfo & info);

private:
    class ImporterWithSourceTree;
    std::unordered_map<String, std::unique_ptr<ImporterWithSourceTree>> importers;
    std::mutex mutex;
};

}

#endif
