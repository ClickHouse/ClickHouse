#pragma once

#include <Access/Common/AccessFlags.h>
#include <Backups/IBackup.h>

namespace DB
{

/// The SOURCES direction required to open a backup in `open_mode`, for an engine whose `UNLOCK` is
/// served by a reader (`S3`, `AzureBlobStorage`). `UNLOCK` only mutates Keeper, under its own
/// `SYSTEM_UNLOCK_SNAPSHOT` privilege.
inline AccessFlags backupSourceAccessFlagsForReaderUnlock(IBackup::OpenMode open_mode)
{
    return open_mode == IBackup::OpenMode::WRITE ? AccessType::WRITE : AccessType::READ;
}

/// The same, for an engine whose creator routes every non-`READ` mode (including `UNLOCK`) through
/// its writer branch (`File`, `Disk`).
inline AccessFlags backupSourceAccessFlagsForWriterUnlock(IBackup::OpenMode open_mode)
{
    return open_mode == IBackup::OpenMode::READ ? AccessType::READ : AccessType::WRITE;
}

}
