#pragma once

#include <Disks/IDisk.h>

#include <optional>

namespace DB
{

/// The place where a disk lists its files.
struct DiskRoot
{
    /// Where disk contains it's files: "local" for the local filesystem, or the endpoint and the bucket of an object storage.
    String filesystem;

    /// The directory or the key prefix in `filesystem`.
    String path;

public:
    bool operator==(const DiskRoot &) const = default;
};

std::optional<DiskRoot> getDiskRoot(const DiskPtr & disk);

}
