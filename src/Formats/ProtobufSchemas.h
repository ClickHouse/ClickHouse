#pragma once

#include "config.h"
#if USE_PROTOBUF

#include <memory>
#include <mutex>
#include <unordered_map>
#include <base/types.h>
#include <boost/noncopyable.hpp>
#include <Common/SipHash.h>


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

/** Keeps google protobuf schemas parsed from files.
  * This class is used to handle the "Protobuf" input/output formats.
  */
class ProtobufSchemas : private boost::noncopyable
{
public:
    enum class WithEnvelope : uint8_t
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
        // If no "Envelope" message exists, the top-level message type
        // specified in format_schema is used directly.
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
    // Clear cached protobuf schemas
    void clear();

    class ImporterWithSourceTree;
    struct DescriptorHolder
    {
        DescriptorHolder(std::shared_ptr<ImporterWithSourceTree> importer_, const google::protobuf::Descriptor * message_descriptor_)
            : importer(std::move(importer_))
            , message_descriptor(message_descriptor_)
        {}
    private:
        std::shared_ptr<ImporterWithSourceTree> importer;
    public:
        const google::protobuf::Descriptor * message_descriptor;
    };

    /// Parses the format schema, then parses the corresponding proto file, and
    /// returns holder (since the descriptor only valid if
    /// ImporterWithSourceTree is valid):
    ///
    ///     {ImporterWithSourceTree, protobuf::Descriptor - descriptor of the message type}.
    ///
    /// The function always return valid message descriptor, it throws an exception if it cannot load or parse the file.
    DescriptorHolder
    getMessageTypeForFormatSchema(const FormatSchemaInfo & info, WithEnvelope with_envelope, const String & google_protos_path);

private:
    struct ImporterKey
    {
        String schema_directory;
        String schema_path;
        WithEnvelope with_envelope;

        bool operator==(const ImporterKey & other) const = default;
    };

    struct ImporterKeyHash
    {
        size_t operator()(const ImporterKey & key) const
        {
            SipHash hash;
            hash.update(key.schema_directory);
            hash.update(key.schema_path);
            hash.update(key.with_envelope);
            return hash.get64();
        }
    };

    std::unordered_map<ImporterKey, std::shared_ptr<ImporterWithSourceTree>, ImporterKeyHash> importers;
    std::mutex mutex;
};

}

#endif
