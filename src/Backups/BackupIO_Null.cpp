#include <Backups/BackupIO_Null.h>

#include <Backups/BackupFactory.h>
#include <Backups/BackupImpl.h>
#include <Common/Exception.h>
#include <IO/NullWriteBuffer.h>
#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{

namespace ErrorCodes
{
    extern const int BACKUP_ENTRY_NOT_FOUND;
    extern const int BACKUP_NOT_FOUND;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


BackupWriterNull::BackupWriterNull(const ReadSettings & read_settings_, const WriteSettings & write_settings_)
    : BackupWriterDefault(read_settings_, write_settings_, getLogger("BackupWriterNull"))
{
}

std::unique_ptr<WriteBuffer> BackupWriterNull::writeFile(const String & /* file_name */)
{
    return std::make_unique<NullWriteBuffer>();
}

void BackupWriterNull::copyDataToFile(const String & /* path_in_backup */, const CreateReadBufferFunction & /* create_read_buffer */, UInt64 /* start_pos */, UInt64 /* length */)
{
    /// no op
}

void BackupWriterNull::copyFileFromDisk(const String & /* path_in_backup */, DiskPtr /* src_disk */, const String & /* src_path */, bool /* copy_encrypted */, UInt64 /* start_pos */, UInt64 /* length */)
{
    /// no op
}

void BackupWriterNull::copyFile(const String & /* destination */, const String & /* source */, size_t)
{
    /// no op
}

void BackupWriterNull::removeFile(const String & /* file_name */)
{
    /// no op
}

void BackupWriterNull::removeFiles(const Strings & /* file_names */)
{
    /// no op
}

void BackupWriterNull::removeEmptyDirectories()
{
    /// no op
}


bool BackupWriterNull::fileExists(const String & /* file_name */)
{
    return false;
}

UInt64 BackupWriterNull::getFileSize(const String & file_name)
{
    throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup entry {} not found (Null backup is always empty)", file_name);
}

std::unique_ptr<ReadBuffer> BackupWriterNull::readFile(const String & file_name, size_t /* expected_file_size */)
{
    throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup entry {} not found (Null backup is always empty)", file_name);
}

bool BackupWriterNull::fileContentsEqual(const String & file_name, const String & /* expected_file_contents */, String & /* actual_file_contents */)
{
    if (fs::path{file_name}.filename() == ".lock")
        return true; /// To pass the check for the ".lock" file in BackupImpl::checkLockFile().

    throw Exception(ErrorCodes::BACKUP_ENTRY_NOT_FOUND, "Backup entry {} not found (Null backup is always empty)", file_name);
}


void registerBackupEngineNull(BackupFactory & factory)
{
    auto creator_fn = [](const BackupFactory::CreateParams & params) -> std::unique_ptr<IBackup>
    {
        const auto & args = params.backup_info.args;
        if (!args.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Backup engine 'Null' doesn't take arguments");

        if (params.open_mode == IBackup::OpenMode::READ)
            throw Exception(ErrorCodes::BACKUP_NOT_FOUND, "Backup {} not found (cannot restore from Null)", params.backup_info.toStringForLogging());

        auto writer = std::make_shared<BackupWriterNull>(params.read_settings, params.write_settings);

        return std::make_unique<BackupImpl>(params, BackupImpl::ArchiveParams{}, writer);
    };

    factory.registerBackupEngine("Null", creator_fn);
}

}
