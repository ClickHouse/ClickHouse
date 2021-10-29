#pragma once

#include <Core/Types.h>
#include <boost/noncopyable.hpp>
#include <memory>


namespace DB
{
class IBackup;
using BackupPtr = std::shared_ptr<const IBackup>;
using BackupMutablePtr = std::shared_ptr<IBackup>;
class Context;
using ContextMutablePtr = std::shared_ptr<Context>;
class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;


/// Factory for implementations of the IBackup interface.
class BackupFactory : boost::noncopyable
{
public:
    static BackupFactory & instance();

    /// Must be called to initialize the backup factory.
    void setBackupsVolume(VolumePtr backups_volume_);

    /// Creates a new backup and open it for writing.
    BackupMutablePtr createBackup(const String & backup_name, UInt64 estimated_backup_size, const BackupPtr & base_backup = {}) const;

    /// Opens an existing backup for reading.
    BackupPtr openBackup(const String & backup_name, const BackupPtr & base_backup = {}) const;

private:
    VolumePtr backups_volume;
};

}
