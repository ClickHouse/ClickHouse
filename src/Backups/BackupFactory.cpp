#include <Backups/BackupFactory.h>
#include <Backups/BackupInDirectory.h>
#include <Interpreters/Context.h>
#include <Disks/IVolume.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BACKUP_NOT_FOUND;
    extern const int BACKUP_ALREADY_EXISTS;
    extern const int NOT_ENOUGH_SPACE;
    extern const int LOGICAL_ERROR;
}


BackupFactory & BackupFactory::instance()
{
    static BackupFactory the_instance;
    return the_instance;
}

void BackupFactory::setBackupsVolume(VolumePtr backups_volume_)
{
    backups_volume = backups_volume_;
}

BackupMutablePtr BackupFactory::createBackup(const String & backup_name, UInt64 estimated_backup_size, const BackupPtr & base_backup) const
{
    if (!backups_volume)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No backups volume");

    for (const auto & disk : backups_volume->getDisks())
    {
        if (disk->exists(backup_name))
            throw Exception(ErrorCodes::BACKUP_ALREADY_EXISTS, "Backup {} already exists", quoteString(backup_name));
    }

    auto reservation = backups_volume->reserve(estimated_backup_size);
    if (!reservation)
        throw Exception(
            ErrorCodes::NOT_ENOUGH_SPACE,
            "Couldn't reserve {} bytes of free space for new backup {}",
            estimated_backup_size,
            quoteString(backup_name));

    return std::make_shared<BackupInDirectory>(IBackup::OpenMode::WRITE, reservation->getDisk(), backup_name, base_backup);
}

BackupPtr BackupFactory::openBackup(const String & backup_name, const BackupPtr & base_backup) const
{
    if (!backups_volume)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No backups volume");

    for (const auto & disk : backups_volume->getDisks())
    {
        if (disk->exists(backup_name))
            return std::make_shared<BackupInDirectory>(IBackup::OpenMode::READ, disk, backup_name, base_backup);
    }

    throw Exception(ErrorCodes::BACKUP_NOT_FOUND, "Backup {} not found", quoteString(backup_name));
}

}
