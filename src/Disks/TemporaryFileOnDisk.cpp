#include <Disks/TemporaryFileOnDisk.h>
#include <Common/CurrentMetrics.h>
#include <Common/logger_useful.h>

#include <filesystem>

namespace ProfileEvents
{
    extern const Event ExternalProcessingFilesTotal;
}

namespace CurrentMetrics
{
    extern const Metric TotalTemporaryFiles;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

TemporaryFileOnDisk::TemporaryFileOnDisk(const DiskPtr & disk_, CurrentMetrics::Metric metric_scope)
    : TemporaryFileOnDisk(disk_)
{
    sub_metric_increment.emplace(metric_scope);
}

TemporaryFileOnDisk::TemporaryFileOnDisk(const DiskPtr & disk_, const String & prefix)
    : disk(disk_)
    , metric_increment(CurrentMetrics::TotalTemporaryFiles)
{
    if (!disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Disk is not specified");

    if (fs::path prefix_path(prefix); prefix_path.has_parent_path())
        disk->createDirectories(prefix_path.parent_path());

    ProfileEvents::increment(ProfileEvents::ExternalProcessingFilesTotal);

    /// A disk can be remote and shared between multiple replicas.
    /// That's why we must not use Poco::TemporaryFile::tempName() here (Poco::TemporaryFile::tempName() can return the same names for different processes on different nodes).
    relative_path = prefix + toString(UUIDHelpers::generateV4());
}

String TemporaryFileOnDisk::getAbsolutePath() const
{
    return std::filesystem::path(disk->getPath()) / relative_path;
}

TemporaryFileOnDisk::~TemporaryFileOnDisk()
{
    try
    {
        if (!disk || relative_path.empty())
            return;

        if (!disk->exists(relative_path))
        {
            LOG_WARNING(getLogger("TemporaryFileOnDisk"), "Temporary path '{}' does not exist in '{}'", relative_path, disk->getPath());
            return;
        }

        disk->removeRecursive(relative_path);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
