#pragma once
#include <string_view>
#include <base/types.h>
#include <Common/Logger.h>
#include "StoredObject.h"

namespace DB
{
class IDisk;

// Abstraction for storing folder metadata files concatenated in a single file on object storage
struct VFSMetadataStorage
{
    VFSMetadataStorage(IDisk & disk, std::string_view object_key_prefix_);
    bool tryDownloadMetadata(std::string_view from_remote_file, const String & to_folder);
    void uploadMetadata(std::string_view to_remote_file, const String & from_folder);

protected:
    StoredObject getMetadataObject(std::string_view remote) const;

private:
    IDisk & disk;
    String prefix;
    LoggerPtr logger;
};
}
