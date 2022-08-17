#pragma once

#include <Core/Types.h>
#include <memory>
#include <Poco/TemporaryFile.h>
#include <Common/CurrentMetrics.h>


namespace CurrentMetrics
{
    extern const Metric TotalTemporaryFiles;
}

namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// This class helps with the handling of temporary files or directories.
/// A unique name for the temporary file or directory is automatically chosen based on a specified prefix.
/// Optionally can create a directory in the constructor.
/// The destructor always removes the temporary file or directory with all contained files.
class TemporaryFileOnDisk
{
public:
    explicit TemporaryFileOnDisk(const DiskPtr & disk_, const String & prefix_ = "tmp", std::unique_ptr<CurrentMetrics::Increment> increment_ = nullptr);
    explicit TemporaryFileOnDisk(const String & prefix_);
    ~TemporaryFileOnDisk();

    DiskPtr getDisk() const { return disk; }
    const String & getPath() const { return filepath; }
    const String & path() const { return filepath; }

private:
    DiskPtr disk;

    /// If disk is not provided, fallback to Poco::TemporaryFile
    /// TODO: it's better to use DiskLocal for that case as well
    std::unique_ptr<Poco::TemporaryFile> tmp_file;

    String filepath;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::TotalTemporaryFiles};
    /// Specified if we know what for file is used (sort/aggregate/join).
    std::unique_ptr<CurrentMetrics::Increment> sub_metric_increment;
};

}
