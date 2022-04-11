#include <Backups/BackupEntryFromSmallFile.h>
#include <Disks/IDisk.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>


namespace DB
{
namespace
{
    String readFile(const String & file_path)
    {
        auto buf = createReadBufferFromFileBase(file_path, /* settings= */ {});
        String s;
        readStringUntilEOF(s, *buf);
        return s;
    }

    String readFile(const DiskPtr & disk, const String & file_path)
    {
        auto buf = disk->readFile(file_path);
        String s;
        readStringUntilEOF(s, *buf);
        return s;
    }
}


BackupEntryFromSmallFile::BackupEntryFromSmallFile(const String & file_path_, const std::optional<UInt128> & checksum_)
    : BackupEntryFromMemory(readFile(file_path_), checksum_), file_path(file_path_)
{
}

BackupEntryFromSmallFile::BackupEntryFromSmallFile(
    const DiskPtr & disk_, const String & file_path_, const std::optional<UInt128> & checksum_)
    : BackupEntryFromMemory(readFile(disk_, file_path_), checksum_), disk(disk_), file_path(file_path_)
{
}
}
