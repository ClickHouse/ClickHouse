#include <Backups/BackupFactory.h>
#include <Backups/DirectoryBackup.h>
#include <Backups/ArchiveBackup.h>
#include <Common/quoteString.h>
#include <IO/Archives/hasRegisteredArchiveFileExtension.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <filesystem>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INVALID_CONFIG_PARAMETER;
}


namespace
{
    namespace fs = std::filesystem;

    [[noreturn]] void throwDiskIsAllowed(const String & disk_name)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk {} is not allowed for backups", disk_name);
    }

    [[noreturn]] void throwPathNotAllowed(const fs::path & path)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path {} is not allowed for backups", quoteString(String{path}));
    }

    void checkAllowedPathInConfigIsValid(const String & key, const fs::path & value)
    {
        if (value.empty() || value.is_relative())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Configuration parameter {} has a wrong value {}", key, String{value});
    }

    /// Checks that a disk name and a path specified as parameters of Disk() are valid.
    void checkDiskNameAndPath(const String & disk_name, fs::path & path, const Poco::Util::AbstractConfiguration & config)
    {
        String key = "backups.allowed_disk";
        bool disk_name_found = false;
        size_t counter = 0;
        while (config.has(key))
        {
            if (config.getString(key) == disk_name)
            {
                disk_name_found = true;
                break;
            }
            key = "backups.allowed_disk[" + std::to_string(++counter) + "]";
        }

        if (!disk_name_found)
            throwDiskIsAllowed(disk_name);

        path = path.lexically_normal();
        if (!path.is_relative() || path.empty() || (*path.begin() == ".."))
            throwPathNotAllowed(path);
    }

    /// Checks that a path specified as a parameter of File() is valid.
    void checkPath(fs::path & path, const Poco::Util::AbstractConfiguration & config)
    {
        String key = "backups.allowed_path";

        path = path.lexically_normal();
        if (path.empty())
            throwPathNotAllowed(path);

        if (path.is_relative())
        {
            if (*path.begin() == "..")
                throwPathNotAllowed(path);

            auto base = fs::path(config.getString(key, ""));
            checkAllowedPathInConfigIsValid(key, base);
            path = base / path;
            return;
        }

        bool path_found_in_config = false;
        size_t counter = 0;
        while (config.has(key))
        {
            auto base = fs::path(config.getString(key));
            checkAllowedPathInConfigIsValid(key, base);
            auto rel = path.lexically_relative(base);
            if (!rel.empty() && (*rel.begin() != ".."))
            {
                path_found_in_config = true;
                break;
            }
            key = "backups.allowed_path[" + std::to_string(++counter) + "]";
        }

        if (!path_found_in_config)
            throwPathNotAllowed(path);
    }
}


void registerBackupEnginesFileAndDisk(BackupFactory & factory)
{
    auto creator_fn = [](const BackupFactory::CreateParams & params) -> std::unique_ptr<IBackup>
    {
        String backup_name = params.backup_info.toString();
        const String & engine_name = params.backup_info.backup_engine_name;
        const auto & args = params.backup_info.args;

        DiskPtr disk;
        fs::path path;
        if (engine_name == "File")
        {
            if (args.size() != 1)
            {
                throw Exception(
                    "Backup engine 'File' requires 1 argument (path)",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }

            path = args[0].safeGet<String>();
            checkPath(path, params.context->getConfigRef());
        }
        else if (engine_name == "Disk")
        {
            if (args.size() != 2)
            {
                throw Exception(
                    "Backup engine 'Disk' requires 2 arguments (disk_name, path)",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }

            String disk_name = args[0].safeGet<String>();
            path = args[1].safeGet<String>();
            checkDiskNameAndPath(disk_name, path, params.context->getConfigRef());
            disk = params.context->getDisk(disk_name);
        }

        std::unique_ptr<IBackup> backup;

        if (!path.has_filename() && !path.empty())
        {
            if (!params.password.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Password is not applicable, backup cannot be encrypted");
            backup = std::make_unique<DirectoryBackup>(backup_name, disk, path, params.context, params.base_backup_info);
        }
        else if (hasRegisteredArchiveFileExtension(path))
        {
            auto archive_backup = std::make_unique<ArchiveBackup>(backup_name, disk, path, params.context, params.base_backup_info);
            archive_backup->setCompression(params.compression_method, params.compression_level);
            archive_backup->setPassword(params.password);
            backup = std::move(archive_backup);
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path to backup must be either a directory or a path to an archive");

        return backup;
    };

    factory.registerBackupEngine("File", creator_fn);
    factory.registerBackupEngine("Disk", creator_fn);
}

}
