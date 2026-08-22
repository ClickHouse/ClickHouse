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
    String readFile(const String & file_path, const ReadSettings & read_settings)
    {
        auto buf = createReadBufferFromFileBase(file_path, read_settings);
        String s;
        readStringUntilEOF(s, *buf);
        return s;
    }

    String readFile(const DiskPtr & disk, const String & file_path, const ReadSettings & read_settings, bool copy_encrypted)
    {
        auto buf = copy_encrypted ? disk->readEncryptedFile(file_path, read_settings) : disk->readFile(file_path, read_settings);
        String s;
        readStringUntilEOF(s, *buf);
        return s;
    }
}


BackupEntryFromSmallFile::BackupEntryFromSmallFile(const String & file_path_, const ReadSettings & read_settings_)
    : file_path(file_path_)
    , data_source_description(DiskLocal::getLocalDataSourceDescription(file_path_))
    , data(readFile(file_path_, read_settings_))
{
}

BackupEntryFromSmallFile::BackupEntryFromSmallFile(const DiskPtr & disk_, const String & file_path_, const ReadSettings & read_settings_, bool copy_encrypted_)
    : disk(disk_)
    , file_path(file_path_)
    , data_source_description(disk_->getDataSourceDescription())
    , copy_encrypted(copy_encrypted_ && data_source_description.is_encrypted)
    , data(readFile(disk_, file_path, read_settings_, copy_encrypted))
{
}

std::unique_ptr<SeekableReadBuffer> BackupEntryFromSmallFile::getReadBuffer(const ReadSettings &) const
{
    return std::make_unique<ReadBufferFromString>(data);
}

}
