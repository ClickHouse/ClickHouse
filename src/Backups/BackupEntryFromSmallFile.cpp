#include <Backups/BackupEntryFromSmallFile.h>
#include <Common/filesystemHelpers.h>
#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
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

    String readFile(const DiskPtr & disk, const String & file_path, bool copy_encrypted)
    {
        auto buf = copy_encrypted ? disk->readEncryptedFile(file_path, {}) : disk->readFile(file_path);
        String s;
        readStringUntilEOF(s, *buf);
        return s;
    }
}


BackupEntryFromSmallFile::BackupEntryFromSmallFile(const String & file_path_)
    : file_path(file_path_)
    , data_source_description(DiskLocal::getLocalDataSourceDescription(file_path_))
    , data(readFile(file_path_))
{
}

BackupEntryFromSmallFile::BackupEntryFromSmallFile(const DiskPtr & disk_, const String & file_path_, bool copy_encrypted_)
    : disk(disk_)
    , file_path(file_path_)
    , data_source_description(disk_->getDataSourceDescription())
    , copy_encrypted(copy_encrypted_ && data_source_description.is_encrypted)
    , data(readFile(disk_, file_path, copy_encrypted))
{
}

std::unique_ptr<SeekableReadBuffer> BackupEntryFromSmallFile::getReadBuffer(const ReadSettings &) const
{
    return std::make_unique<ReadBufferFromString>(data);
}

}
