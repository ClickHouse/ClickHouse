#pragma once

#include "config_formats.h"
#if USE_PROTOBUF

#include <memory>
#include <mutex>
#include <unordered_map>
#include <base/types.h>
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
    enum class WithEnvelope
    {
        // Return descriptor for a top-level message with a user-provided name.
        // Example: In protobuf schema
        //   message MessageType {
        //     string colA = 1;
        //     int32 colB = 2;
        //   }
        // message_name = "MessageType" returns a descriptor. Used by IO
        // formats Protobuf and ProtobufSingle.
        No,
        // Return descriptor for a message with a user-provided name one level
        // below a top-level message with the hardcoded name "Envelope".
        // Example: In protobuf schema
        //   message Envelope {
        //     message MessageType {
        //       string colA = 1;
        //       int32 colB = 2;
        //     }
        //   }
        // message_name = "MessageType" returns a descriptor. Used by IO format
        // ProtobufList.
        Yes
    };

    static ProtobufSchemas & instance();

    /// Parses the format schema, then parses the corresponding proto file, and returns the descriptor of the message type.
    /// The function never returns nullptr, it throws an exception if it cannot load or parse the file.
    const google::protobuf::Descriptor * getMessageTypeForFormatSchema(const FormatSchemaInfo & info, WithEnvelope with_envelope);

private:
    class ImporterWithSourceTree;
    std::unordered_map<String, std::unique_ptr<ImporterWithSourceTree>> importers;
    std::mutex mutex;
};

}

#endif
