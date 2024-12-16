#include <Backups/BackupFactory.h>
#include <Backups/BackupIO_Disk.h>
#include <Backups/BackupIO_File.h>
#include <Backups/BackupImpl.h>
#include <Common/quoteString.h>
#include <Disks/IDisk.h>
#include <IO/Archives/hasRegisteredArchiveFileExtension.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <filesystem>
#include <Interpreters/Context.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SUPPORT_IS_DISABLED;
}


namespace
{
    namespace fs = std::filesystem;

    /// Checks that a disk name specified as parameters of Disk() is valid.
    void checkDiskName(const String & disk_name, const Poco::Util::AbstractConfiguration & config)
    {
        String key = "backups.allowed_disk";
        if (!config.has(key))
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                            "The 'backups.allowed_disk' configuration parameter "
                            "is not set, cannot use 'Disk' backup engine");

        size_t counter = 0;
        while (config.getString(key) != disk_name)
        {
            key = "backups.allowed_disk[" + std::to_string(++counter) + "]";
            if (!config.has(key))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Disk '{}' is not allowed for backups, see the 'backups.allowed_disk' configuration parameter", quoteString(disk_name));
        }
    }

    /// Checks that a path specified as parameters of Disk() is valid.
    void checkPath(const String & disk_name, const DiskPtr & disk, fs::path & path)
    {
        path = path.lexically_normal();
        if (!path.is_relative() && (disk->getDataSourceDescription().type == DataSourceType::Local))
            path = path.lexically_proximate(disk->getPath());

        bool path_ok = path.empty() || (path.is_relative() && (*path.begin() != ".."));
        if (!path_ok)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path '{}' to backup must be inside the specified disk '{}'",
                            quoteString(path.c_str()), quoteString(disk_name));
    }

    /// Checks that a path specified as parameters of File() is valid.
    void checkPath(fs::path & path, const Poco::Util::AbstractConfiguration & config, const fs::path & data_dir)
    {
        path = path.lexically_normal();
        if (path.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path to backup must not be empty");

        String key = "backups.allowed_path";
        if (!config.has(key))
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                            "The 'backups.allowed_path' configuration parameter is not set, cannot use 'File' backup engine");

        if (path.is_relative())
        {
            auto first_allowed_path = fs::path(config.getString(key));
            if (first_allowed_path.is_relative())
                first_allowed_path = data_dir / first_allowed_path;

            path = first_allowed_path / path;
        }

        size_t counter = 0;
        while (true)
        {
            auto allowed_path = fs::path(config.getString(key));
            if (allowed_path.is_relative())
                allowed_path = data_dir / allowed_path;
            auto rel = path.lexically_proximate(allowed_path);
            bool path_ok = rel.empty() || (rel.is_relative() && (*rel.begin() != ".."));
            if (path_ok)
                break;
            key = "backups.allowed_path[" + std::to_string(++counter) + "]";
            if (!config.has(key))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Path {} is not allowed for backups, see the 'backups.allowed_path' configuration parameter",
                                quoteString(path.c_str()));
        }
    }
}


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
            checkPath(path, config, data_dir);
        }
        else if (engine_name == "Disk")
        {
            if (args.size() != 2)
            {
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Backup engine 'Disk' requires 2 arguments (disk_name, path)");
            }

            String disk_name = args[0].safeGet<String>();
            const auto & config = params.context->getConfigRef();
            checkDiskName(disk_name, config);
            path = args[1].safeGet<String>();
            disk = params.context->getDisk(disk_name);
            checkPath(disk_name, disk, path);
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
            return std::make_unique<BackupImpl>(
                params.backup_info,
                archive_params,
                params.base_backup_info,
                reader,
                params.context,
                params.is_internal_backup,
                params.use_same_s3_credentials_for_base_backup,
                params.use_same_password_for_base_backup);
        }

        std::shared_ptr<IBackupWriter> writer;
        if (engine_name == "File")
            writer = std::make_shared<BackupWriterFile>(path, params.read_settings, params.write_settings);
        else
            writer = std::make_shared<BackupWriterDisk>(disk, path, params.read_settings, params.write_settings);
        return std::make_unique<BackupImpl>(
            params.backup_info,
            archive_params,
            params.base_backup_info,
            writer,
            params.context,
            params.is_internal_backup,
            params.backup_coordination,
            params.backup_uuid,
            params.deduplicate_files,
            params.use_same_s3_credentials_for_base_backup,
            params.use_same_password_for_base_backup);
    };

    factory.registerBackupEngine("File", creator_fn);
    factory.registerBackupEngine("Disk", creator_fn);
}

}
