#include <Disks/TemporaryFileOnDisk.h>
#include <Poco/TemporaryFile.h>


namespace ProfileEvents
{
    extern const Event ExternalProcessingFilesTotal;
}

namespace DB
{

TemporaryFileOnDisk::TemporaryFileOnDisk(const DiskPtr & disk_, std::unique_ptr<CurrentMetrics::Increment> increment_)
    : TemporaryFileOnDisk(disk_, disk_->getPath(), std::move(increment_))
{}

TemporaryFileOnDisk::TemporaryFileOnDisk(const DiskPtr & disk_, const String & prefix_, std::unique_ptr<CurrentMetrics::Increment> increment_)
    : disk(disk_)
    , sub_metric_increment(std::move(increment_))
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
