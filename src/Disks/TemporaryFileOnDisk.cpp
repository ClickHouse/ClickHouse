#include <Disks/TemporaryFileOnDisk.h>
#include <Disks/IDisk.h>
#include <Poco/TemporaryFile.h>


namespace DB
{

TemporaryFileOnDisk::TemporaryFileOnDisk(const DiskPtr & disk_, const String & prefix_)
    : disk(disk_)
{
    String dummy_prefix = "a/";
    filepath = Poco::TemporaryFile::tempName(dummy_prefix);
    dummy_prefix += "tmp";
    assert(filepath.starts_with(dummy_prefix));
    filepath.replace(0, dummy_prefix.length(), prefix_);
}

TemporaryFileOnDisk::~TemporaryFileOnDisk()
{
#if 1
    if (disk && !filepath.empty())
        disk->removeRecursive(filepath);
#endif
}

}
