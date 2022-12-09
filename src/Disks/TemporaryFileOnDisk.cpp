#include <Disks/TemporaryFileOnDisk.h>
#include <Poco/TemporaryFile.h>
#include <Common/CurrentMetrics.h>

namespace ProfileEvents
{
    extern const Event ExternalProcessingFilesTotal;
}

namespace DB
{

TemporaryFileOnDisk::TemporaryFileOnDisk(const DiskPtr & disk_)
    : TemporaryFileOnDisk(disk_, disk_->getPath())
{}

TemporaryFileOnDisk::TemporaryFileOnDisk(const DiskPtr & disk_, CurrentMetrics::Value metric_scope)
    : TemporaryFileOnDisk(disk_)
{
    sub_metric_increment.emplace(metric_scope);
}

TemporaryFileOnDisk::TemporaryFileOnDisk(const DiskPtr & disk_, const String & prefix_)
    : disk(disk_)
{
    /// is is possible to use with disk other than DickLocal ?
    disk->createDirectories(prefix_);

    ProfileEvents::increment(ProfileEvents::ExternalProcessingFilesTotal);

    /// Do not use default temporaty root path `/tmp/tmpXXXXXX`.
    /// The `dummy_prefix` is used to know what to replace with the real prefix.
    String dummy_prefix = "a/";
    filepath = Poco::TemporaryFile::tempName(dummy_prefix);
    dummy_prefix += "tmp";
    /// a/tmpXXXXX -> <prefix>XXXXX
    assert(filepath.starts_with(dummy_prefix));
    filepath.replace(0, dummy_prefix.length(), prefix_);
}

TemporaryFileOnDisk::~TemporaryFileOnDisk()
{
    try
    {
        if (disk && !filepath.empty() && disk->exists(filepath))
            disk->removeRecursive(filepath);
    }
    catch (...)
    {
    }
}

}
