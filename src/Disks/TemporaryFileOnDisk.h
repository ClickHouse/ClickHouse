#pragma once

#include <Core/Types.h>
#include <memory>
#include <Disks/IDisk.h>
#include <Common/CurrentMetrics.h>


namespace CurrentMetrics
{
    extern const Metric TotalTemporaryFiles;
}

namespace DB
{
using DiskPtr = std::shared_ptr<IDisk>;

/// This class helps with the handling of temporary files or directories.
/// A unique name for the temporary file or directory is automatically chosen based on a specified prefix.
/// Create a directory in the constructor.
/// The destructor always removes the temporary file or directory with all contained files.
class TemporaryFileOnDisk
{
public:
    explicit TemporaryFileOnDisk(const DiskPtr & disk_);
    explicit TemporaryFileOnDisk(const DiskPtr & disk_, CurrentMetrics::Value metric_scope);
    explicit TemporaryFileOnDisk(const DiskPtr & disk_, const String & prefix_);

    ~TemporaryFileOnDisk();

    DiskPtr getDisk() const { return disk; }
    const String & getPath() const { return filepath; }
    const String & path() const { return filepath; }

private:
    DiskPtr disk;

    String filepath;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::TotalTemporaryFiles};
    /// Specified if we know what for file is used (sort/aggregate/join).
    std::optional<CurrentMetrics::Increment> sub_metric_increment = {};
};

using TemporaryFileOnDiskHolder = std::unique_ptr<TemporaryFileOnDisk>;

}
