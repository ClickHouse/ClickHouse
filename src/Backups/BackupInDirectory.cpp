#include <Backups/BackupInDirectory.h>
#include <Backups/BackupFactory.h>
#include <Common/quoteString.h>
#include <Disks/DiskSelector.h>
#include <Disks/IDisk.h>
#include <Disks/DiskLocal.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    /// Checks multiple keys "key", "key[1]", "key[2]", and so on in the configuration
    /// and find out if some of them have matching value.
    bool findConfigKeyWithMatchingValue(const Poco::Util::AbstractConfiguration & config, const String & key, const std::function<bool(const String & value)> & match_function)
    {
        String current_key = key;
        size_t counter = 0;
        while (config.has(current_key))
        {
            if (match_function(config.getString(current_key)))
                return true;
            current_key = key + "[" + std::to_string(++counter) + "]";
        }
        return false;
    }

    bool isDiskAllowed(const String & disk_name, const Poco::Util::AbstractConfiguration & config)
    {
        return findConfigKeyWithMatchingValue(config, "backups.allowed_disk", [&](const String & value) { return value == disk_name; });
    }

    bool isPathAllowed(const String & path, const Poco::Util::AbstractConfiguration & config)
    {
        return findConfigKeyWithMatchingValue(config, "backups.allowed_path", [&](const String & value) { return path.starts_with(value); });
    }
}


BackupInDirectory::BackupInDirectory(
    const String & backup_name_,
    OpenMode open_mode_,
    const DiskPtr & disk_,
    const String & path_,
    const ContextPtr & context_,
    const std::optional<BackupInfo> & base_backup_info_)
    : BackupImpl(backup_name_, open_mode_, context_, base_backup_info_)
    , disk(disk_), path(path_)
{
    /// Path to backup must end with '/'
    if (path.back() != '/')
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Backup {}: Path to backup must end with '/', but {} doesn't.", getName(), quoteString(path));
    dir_path = fs::path(path).parent_path(); /// get path without terminating slash

    /// If `disk` is not specified, we create an internal instance of `DiskLocal` here.
    if (!disk)
    {
        auto fspath = fs::path{dir_path};
        if (!fspath.has_filename())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Backup {}: Path to a backup must be a directory path.", getName(), quoteString(path));
        path = fspath.filename() / "";
        dir_path = fs::path(path).parent_path(); /// get path without terminating slash
        String disk_path = fspath.remove_filename();
        disk = std::make_shared<DiskLocal>(disk_path, disk_path, 0);
    }

    open();
}


BackupInDirectory::~BackupInDirectory()
{
    close();
}

bool BackupInDirectory::backupExists() const
{
    return disk->isDirectory(dir_path);
}

void BackupInDirectory::startWriting()
{
    disk->createDirectories(dir_path);
}

void BackupInDirectory::removeAllFilesAfterFailure()
{
    if (disk->isDirectory(dir_path))
        disk->removeRecursive(dir_path);
}

std::unique_ptr<ReadBuffer> BackupInDirectory::readFileImpl(const String & file_name) const
{
    String file_path = path + file_name;
    return disk->readFile(file_path);
}

std::unique_ptr<WriteBuffer> BackupInDirectory::addFileImpl(const String & file_name)
{
    String file_path = path + file_name;
    disk->createDirectories(fs::path(file_path).parent_path());
    return disk->writeFile(file_path);
}


void registerBackupEngineFile(BackupFactory & factory)
{
    auto creator_fn = [](const BackupFactory::CreateParams & params)
    {
        String backup_name = params.backup_info.toString();
        const String & engine_name = params.backup_info.backup_engine_name;
        const auto & args = params.backup_info.args;

        DiskPtr disk;
        String path;
        if (engine_name == "File")
        {
            if (args.size() != 1)
            {
                throw Exception(
                    "Backup engine 'File' requires 1 argument (path)",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }

            path = args[0].safeGet<String>();

            if (!isPathAllowed(path, params.context->getConfigRef()))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path {} is not allowed for backups", path);
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
            disk = params.context->getDisk(disk_name);
            path = args[1].safeGet<String>();

            if (!isDiskAllowed(disk_name, params.context->getConfigRef()))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk {} is not allowed for backups", disk_name);
        }

        return std::make_shared<BackupInDirectory>(backup_name, params.open_mode, disk, path, params.context, params.base_backup_info);
    };

    factory.registerBackupEngine("File", creator_fn);
    factory.registerBackupEngine("Disk", creator_fn);
}

}
