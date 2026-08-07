#pragma once

#include <Core/Types.h>
#include <memory>
#include <Disks/IDisk.h>
#include <Common/CurrentMetrics.h>

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
    explicit TemporaryFileOnDisk(const DiskPtr & disk_, CurrentMetrics::Metric metric_scope);
    explicit TemporaryFileOnDisk(const DiskPtr & disk_, const String & prefix = "tmp");

    ~TemporaryFileOnDisk();

    DiskPtr getDisk() const { return disk; }
    /// Return absolute path (disk + relative_path)
    String getAbsolutePath() const;
    /// Return relative path (without disk)
    const String & getRelativePath() const { return relative_path; }

    /// Sets whether the destructor should show a warning if the temporary file has been already removed.
    /// By default a warning is shown.
    void setShowWarningIfRemoved(bool show_warning_if_removed_) { show_warning_if_removed = show_warning_if_removed_; }

private:
    DiskPtr disk;

    /// Relative path in disk to the temporary file or directory
    String relative_path;

    /// Whether the destructor should show a warning if the temporary file has been already removed.
    bool show_warning_if_removed = true;

    CurrentMetrics::Increment metric_increment;

    /// Specified if we know what for file is used (sort/aggregate/join).
    std::optional<CurrentMetrics::Increment> sub_metric_increment = {};
};

using TemporaryFileOnDiskHolder = std::unique_ptr<TemporaryFileOnDisk>;

}
