#include <Backups/BackupEnginesFileAndDiskUtils.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{
    namespace fs = std::filesystem;
}

void checkBackupDiskName(const String & disk_name, const Poco::Util::AbstractConfiguration & config)
{
    String key = "backups.allowed_disk";
    if (!config.has(key))
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "The 'backups.allowed_disk' configuration parameter is not set, cannot use 'Disk' backup engine");

    size_t counter = 0;
    while (config.getString(key) != disk_name)
    {
        key = "backups.allowed_disk[" + std::to_string(++counter) + "]";
        if (!config.has(key))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Disk '{}' is not allowed for backups, see the 'backups.allowed_disk' configuration parameter",
                quoteString(disk_name));
    }
}

void checkBackupDiskPath(const String & disk_name, const DiskPtr & disk, fs::path & path)
{
    path = path.lexically_normal();
    if (!path.is_relative() && disk->getDataSourceDescription().type == DataSourceType::Local)
        path = path.lexically_proximate(disk->getPath());

    bool path_ok = path.empty() || (path.is_relative() && (*path.begin() != ".."));
    if (!path_ok)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Path '{}' to backup must be inside the specified disk '{}'",
            quoteString(path.c_str()),
            quoteString(disk_name));
}

void checkBackupFilePath(
    fs::path & path,
    const Poco::Util::AbstractConfiguration & config,
    const fs::path & data_dir)
{
    path = path.lexically_normal();
    if (path.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path to backup must not be empty");

    String key = "backups.allowed_path";
    if (!config.has(key))
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
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
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Path {} is not allowed for backups, see the 'backups.allowed_path' configuration parameter",
                quoteString(path.c_str()));
    }
}

}
