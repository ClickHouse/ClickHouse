#pragma once
#include <functional>
#include <memory>
#include <unordered_map>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>

namespace DB
{
class IRemoteFileMetadata
{
public:
    virtual ~IRemoteFileMetadata() = default;
    [[nodiscard]] virtual String getName() const = 0; //class name

    // deserialize
    virtual bool fromString(const String & buf) = 0;
    // serialize
    [[nodiscard]] virtual String toString() const = 0;

    // Used for comparing two file metadatas are the same or not.
    [[nodiscard]] virtual String getVersion() const = 0;

    String remote_path;
    size_t file_size = 0;
    UInt64 last_modification_timestamp = 0;
};

using IRemoteFileMetadataPtr = std::shared_ptr<IRemoteFileMetadata>;
}
