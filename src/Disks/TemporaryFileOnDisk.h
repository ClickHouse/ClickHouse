#pragma once

#include <Core/Types.h>
#include <memory>
#include <Disks/IDisk.h>
#include <Common/CurrentMetrics.h>

namespace DB
{
using DiskPtr = std::shared_ptr<IDisk>;

class ITemporaryFile
{
public:
    virtual String getPath() const = 0;
    virtual ~ITemporaryFile() = default;
};

using TemporaryFileHolder = std::unique_ptr<ITemporaryFile>;

/// This class helps with the handling of temporary files or directories.
/// A unique name for the temporary file or directory is automatically chosen based on a specified prefix.
/// Create a directory in the constructor.
/// The destructor always removes the temporary file or directory with all contained files.
class TemporaryFileOnDisk : public ITemporaryFile
{
public:
    explicit TemporaryFileOnDisk(const DiskPtr & disk_);
    explicit TemporaryFileOnDisk(const DiskPtr & disk_, CurrentMetrics::Value metric_scope);
    explicit TemporaryFileOnDisk(const DiskPtr & disk_, const String & prefix);

    ~TemporaryFileOnDisk() override;

    DiskPtr getDisk() const { return disk; }
    String getPath() const override;

private:
    DiskPtr disk;

    /// Relative path in disk to the temporary file or directory
    String relative_path;

    CurrentMetrics::Increment metric_increment;

    /// Specified if we know what for file is used (sort/aggregate/join).
    std::optional<CurrentMetrics::Increment> sub_metric_increment = {};
};

using TemporaryFileOnDiskHolder = std::unique_ptr<TemporaryFileOnDisk>;

}
