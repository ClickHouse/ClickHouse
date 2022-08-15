#include <Disks/TemporaryFileOnDisk.h>
#include <Disks/IDisk.h>
#include <Poco/TemporaryFile.h>


namespace DB
{

TemporaryFileOnDisk::TemporaryFileOnDisk(const String & prefix_)
    : tmp_file(std::make_unique<Poco::TemporaryFile>(prefix_))
{
}

TemporaryFileOnDisk::TemporaryFileOnDisk(const DiskPtr & disk_, const String & prefix_)
    : disk(disk_)
{
    /// Do not use default temporaty root path `/tmp`.
    /// The `dummy_prefix` is used to know what to replace with the real prefix.
    String dummy_prefix = "a/";
    filepath = Poco::TemporaryFile::tempName(dummy_prefix);
    dummy_prefix += "tmp";
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
