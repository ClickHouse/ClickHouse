#include <filesystem>
#include <Backups/BackupFactory.h>
#include <Backups/BackupIO_Disk.h>
#include <Backups/BackupIO_File.h>
#include <Backups/BackupImpl.h>
#include <Core/Settings.h>
#include <Disks/IDisk.h>
#include <IO/Archives/ArchiveUtils.h>
#include <IO/Archives/hasRegisteredArchiveFileExtension.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/quoteString.h>


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

namespace Setting
{
extern const SettingsUInt64 archive_adaptive_buffer_max_size_bytes;
}


namespace
{
    namespace fs = std::filesystem;

    struct ResolvedLocalBackupLocation
    {
        DiskPtr disk;
        fs::path path;
        String disk_name;
        String archive_name;
    };

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

    ResolvedLocalBackupLocation resolveLocalBackupLocation(const BackupInfo & backup_info, ContextPtr context)
    {
        const String & engine_name = backup_info.backup_engine_name;
        if (!backup_info.id_arg.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Backup engine '{}' requires its first argument to be a string", engine_name);

        ResolvedLocalBackupLocation location;
        const auto & args = backup_info.args;
        if (engine_name == "File")
        {
            if (args.size() != 1)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Backup engine 'File' requires 1 argument (path)");

            location.path = args[0].safeGet<String>();
            checkPath(location.path, context->getConfigRef(), context->getPath());
        }
        else if (engine_name == "Disk")
        {
            if (args.size() != 2)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Backup engine 'Disk' requires 2 arguments (disk_name, path)");

            location.disk_name = args[0].safeGet<String>();
            checkDiskName(location.disk_name, context->getConfigRef());
            location.path = args[1].safeGet<String>();
            location.disk = context->getDisk(location.disk_name);
            checkPath(location.disk_name, location.disk, location.path);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected backup engine '{}'", engine_name);

        if (hasRegisteredArchiveFileExtension(location.path))
        {
            location.archive_name = location.path.filename();
            location.path = location.path.parent_path();
        }
        return location;
    }

    String normalizeIdentityPath(const fs::path & path)
    {
        if (path == ".")
            return {};

        String str = path.string();
        while (str.size() > 1 && str.back() == '/')
            str.pop_back();
        return str;
    }

    Strings getLocalDestinationIdentity(const BackupInfo & backup_info, ContextPtr context)
    {
        auto location = resolveLocalBackupLocation(backup_info, context);
        Strings components;
        if (backup_info.backup_engine_name == "Disk")
            components.emplace_back("disk=" + location.disk_name);
        components.emplace_back("path=" + normalizeIdentityPath(location.path));
        components.emplace_back("archive=" + location.archive_name);
        return components;
    }
}


void registerBackupEnginesFileAndDisk(BackupFactory &);

void registerBackupEnginesFileAndDisk(BackupFactory & factory)
{
    auto creator_fn = [](const BackupFactory::CreateParams & params) -> std::unique_ptr<IBackup>
    {
        const String & engine_name = params.backup_info.backup_engine_name;
        auto location = resolveLocalBackupLocation(params.backup_info, params.context);

        BackupImpl::ArchiveParams archive_params;
        if (!location.archive_name.empty())
        {
            /// A `Disk` destination can be backed by object storage (e.g. an `s3`-typed disk). In that case a zip
            /// archive is just as problematic as a direct `S3(...)`/`AzureBlobStorage(...)` destination, because zip
            /// requires seeking to read its central directory and every seek turns into a separate HTTP request.
            /// Reject it here too, so the seek-heavy path is not reachable through `BackupReaderDisk`/`BackupWriterDisk`.
            if (location.disk && hasSupportedZipExtension(location.archive_name))
            {
                const auto object_storage_type = location.disk->getDataSourceDescription().object_storage_type;
                if ((object_storage_type == ObjectStorageType::S3) || (object_storage_type == ObjectStorageType::Azure))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Zip archive format is not supported for backups on disk '{}' because it is backed by object storage, "
                        "which does not support seeking efficiently (zip requires seeking). "
                        "Use tar.gz or other tar-based formats instead", location.disk->getName());
            }

            if (params.is_internal_backup)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Using archives with backups on clusters is disabled");

            archive_params.archive_name = location.archive_name;
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
                reader = std::make_shared<BackupReaderFile>(location.path, params.read_settings, params.write_settings);
            else
                reader = std::make_shared<BackupReaderDisk>(location.disk, location.path, params.read_settings, params.write_settings);
            return std::make_unique<BackupImpl>(params, archive_params, reader);
        }

        std::shared_ptr<IBackupWriter> writer;
        if (engine_name == "File")
            writer = std::make_shared<BackupWriterFile>(location.path, params.read_settings, params.write_settings);
        else
            writer = std::make_shared<BackupWriterDisk>(location.disk, location.path, params.read_settings, params.write_settings);
        return std::make_unique<BackupImpl>(params, archive_params, writer);
    };

    factory.registerBackupEngine("File", creator_fn, getLocalDestinationIdentity);
    factory.registerBackupEngine("Disk", creator_fn, getLocalDestinationIdentity);
}

}
