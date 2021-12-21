#pragma once
#include <memory>
#include <base/types.h>
#include <functional>
#include <unordered_map>
#include <boost/core/noncopyable.hpp>

namespace DB
{
class IRemoteFileMetadata
{
public:
    IRemoteFileMetadata() = default;
    IRemoteFileMetadata(const String & remote_path_,
            size_t file_size_,
            UInt64 last_modification_timestamp_):
        remote_path(remote_path_)
        ,file_size(file_size_)
        ,last_modification_timestamp(last_modification_timestamp_)
    {
    }
    virtual ~IRemoteFileMetadata() = default;
    virtual String getName() const = 0; //class name
    // methods for basic information
    inline size_t getFileSize() const { return file_size; }
    inline String getRemotePath() const { return remote_path; }
    inline UInt64 getLastModificationTimestamp() const { return last_modification_timestamp; }

    // deserialize
    virtual bool fromString(const String & buf) = 0;
    // serialize
    virtual String toString() const = 0;

    // used for comparing two file metadatas are the same or not.
    virtual String getVersion() const = 0;
protected:
    String remote_path;
    size_t file_size = 0;
    UInt64 last_modification_timestamp = 0;
};

using IRemoteFileMetadataPtr = std::shared_ptr<IRemoteFileMetadata>;
}
