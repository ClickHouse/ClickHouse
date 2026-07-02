#include <filesystem>
#include <Backups/BackupEnginesFileAndDiskUtils.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupIO_Disk.h>
#include <Backups/BackupIO_File.h>
#include <Backups/BackupImpl.h>
#include <Core/Settings.h>
#include <Disks/IDisk.h>
#include <IO/Archives/hasRegisteredArchiveFileExtension.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SUPPORT_IS_DISABLED;
}

namespace Setting
{
extern const SettingsUInt64 archive_adaptive_buffer_max_size_bytes;
}


namespace
{
    namespace fs = std::filesystem;
}


void registerBackupEnginesFileAndDisk(BackupFactory &);

void registerBackupEnginesFileAndDisk(BackupFactory & factory)
{
    auto creator_fn = [](const BackupFactory::CreateParams & params) -> std::unique_ptr<IBackup>
    {
        const String & engine_name = params.backup_info.backup_engine_name;

        if (!params.backup_info.id_arg.empty())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Backup engine '{}' requires its first argument to be a string",
                engine_name);
        }

        const auto & args = params.backup_info.args;

        DiskPtr disk;
        fs::path path;
        if (engine_name == "File")
        {
            if (args.size() != 1)
            {
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Backup engine 'File' requires 1 argument (path)");
            }

            path = args[0].safeGet<String>();
            const auto & config = params.context->getConfigRef();
            const auto & data_dir = params.context->getPath();
            checkBackupFilePath(path, config, data_dir);
        }
        else if (engine_name == "Disk")
        {
            if (args.size() != 2)
            {
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Backup engine 'Disk' requires 2 arguments (disk_name, path)");
            }

            String disk_name = args[0].safeGet<String>();
            const auto & config = params.context->getConfigRef();
            checkBackupDiskName(disk_name, config);
            path = args[1].safeGet<String>();
            disk = params.context->getDisk(disk_name);
            checkBackupDiskPath(disk_name, disk, path);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected backup engine '{}'", engine_name);

        BackupImpl::ArchiveParams archive_params;
        if (hasRegisteredArchiveFileExtension(path))
        {
            if (params.is_internal_backup)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Using archives with backups on clusters is disabled");

            archive_params.archive_name = path.filename();
            path = path.parent_path();
            archive_params.compression_method = params.compression_method;
            archive_params.compression_level = params.compression_level;
            archive_params.password = params.password;
            archive_params.adaptive_buffer_max_size = params.context->getSettingsRef()[Setting::archive_adaptive_buffer_max_size_bytes];
        }
        else
        {
            if (!params.password.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Password is not applicable, backup cannot be encrypted");
        }

        if (params.open_mode == IBackup::OpenMode::READ)
        {
            std::shared_ptr<IBackupReader> reader;
            if (engine_name == "File")
                reader = std::make_shared<BackupReaderFile>(path, params.read_settings, params.write_settings);
            else
                reader = std::make_shared<BackupReaderDisk>(disk, path, params.read_settings, params.write_settings);
            return std::make_unique<BackupImpl>(params, archive_params, reader);
        }

        std::shared_ptr<IBackupWriter> writer;
        if (engine_name == "File")
            writer = std::make_shared<BackupWriterFile>(path, params.read_settings, params.write_settings);
        else
            writer = std::make_shared<BackupWriterDisk>(disk, path, params.read_settings, params.write_settings);
        return std::make_unique<BackupImpl>(params, archive_params, writer);
    };

    factory.registerBackupEngine("File", creator_fn);
    factory.registerBackupEngine("Disk", creator_fn);
}

}
