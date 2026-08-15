#include <Backups/BackupIO_Memory.h>

#include <Backups/BackupFactory.h>
#include <Backups/BackupImpl.h>
#include <Backups/BackupInMemory.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


BackupReaderMemory::BackupReaderMemory(
    std::shared_ptr<const BackupInMemory> backup_in_memory_, const ReadSettings & read_settings_, const WriteSettings & write_settings_)
    : BackupReaderDefault(read_settings_, write_settings_, getLogger("BackupReaderMemory")), backup_in_memory(backup_in_memory_)
{
}

bool BackupReaderMemory::fileExists(const String & file_name)
{
    return backup_in_memory->fileExists(file_name);
}

UInt64 BackupReaderMemory::getFileSize(const String & file_name)
{
    return backup_in_memory->getFileSize(file_name);
}

std::unique_ptr<ReadBufferFromFileBase> BackupReaderMemory::readFile(const String & file_name)
{
    return backup_in_memory->readFile(file_name);
}


BackupWriterMemory::BackupWriterMemory(
    std::shared_ptr<BackupInMemory> backup_in_memory_, const ReadSettings & read_settings_, const WriteSettings & write_settings_)
    : BackupWriterDefault(read_settings_, write_settings_, getLogger("BackupWriterMemory"))
    , backup_in_memory(backup_in_memory_)
{
}

bool BackupWriterMemory::fileExists(const String & file_name)
{
    return backup_in_memory->fileExists(file_name);
}

UInt64 BackupWriterMemory::getFileSize(const String & file_name)
{
    return backup_in_memory->getFileSize(file_name);
}

std::unique_ptr<WriteBuffer> BackupWriterMemory::writeFile(const String & file_name)
{
    return backup_in_memory->writeFile(file_name);
}

void BackupWriterMemory::copyFile(const String & destination, const String & source, size_t)
{
    backup_in_memory->copyFile(source, destination);
}

void BackupWriterMemory::removeFile(const String & file_name)
{
    backup_in_memory->removeFile(file_name);
}

std::unique_ptr<ReadBuffer> BackupWriterMemory::readFile(const String & file_name, size_t /* expected_file_size */)
{
    return backup_in_memory->readFile(file_name);
}

void BackupWriterMemory::removeEmptyDirectories()
{
    if (backup_in_memory->isEmpty())
        backup_in_memory->drop();
}


void registerBackupEngineMemory(BackupFactory & factory)
{
    auto creator_fn = [](const BackupFactory::CreateParams & params) -> std::unique_ptr<IBackup>
    {
        const auto & args = params.backup_info.args;
        if (args.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Backup engine 'Memory' requires 1 argument (name)");

        String backup_name = args[0].safeGet<String>();

        if (params.open_mode == IBackup::OpenMode::READ)
        {
            const auto & backups_in_memory = params.context->getSessionContext()->getBackupsInMemory();
            auto backup_in_memory = backups_in_memory.getBackup(backup_name);
            auto reader = std::make_shared<BackupReaderMemory>(backup_in_memory, params.read_settings, params.write_settings);

            return std::make_unique<BackupImpl>(params, BackupImpl::ArchiveParams{}, reader);
        }
        else
        {
            auto & backups_in_memory = params.context->getSessionContext()->getBackupsInMemory();
            auto backup_in_memory = backups_in_memory.createBackup(backup_name);
            auto writer = std::make_shared<BackupWriterMemory>(backup_in_memory, params.read_settings, params.write_settings);

            return std::make_unique<BackupImpl>(params, BackupImpl::ArchiveParams{}, writer);
        }
    };

    factory.registerBackupEngine("Memory", creator_fn);
}

}
