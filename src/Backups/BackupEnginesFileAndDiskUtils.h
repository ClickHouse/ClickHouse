#pragma once

#include <Core/Types.h>
#include <Disks/IDisk.h>

#include <filesystem>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

void checkBackupDiskName(const String & disk_name, const Poco::Util::AbstractConfiguration & config);
void checkBackupDiskPath(const String & disk_name, const DiskPtr & disk, std::filesystem::path & path);
void checkBackupFilePath(
    std::filesystem::path & path,
    const Poco::Util::AbstractConfiguration & config,
    const std::filesystem::path & data_dir);

}
