#pragma once

#include <Backups/BackupStatus.h>
#include <Common/ProfileEvents.h>

#include <exception>

namespace DB
{

using BackupOperationID = String;

/// Information about executing a BACKUP or RESTORE operation
struct BackupOperationInfo
{
    /// Operation ID, can be either passed via SETTINGS id=... or be randomly generated UUID.
    BackupOperationID id;

    /// Operation name, a string like "Disk('backups', 'my_backup')"
    String name;

    /// Base Backup Operation name, a string like "Disk('backups', 'my_base_backup')"
    String base_backup_name;

    /// Query ID of a query that started backup
    String query_id;

    /// This operation is internal and should not be shown in system.backups
    bool internal = false;

    /// Status of backup or restore operation.
    BackupStatus status;

    /// The number of files stored in the backup.
    size_t num_files = 0;

    /// The total size of files stored in the backup.
    UInt64 total_size = 0;

    /// The number of entries in the backup, i.e. the number of files inside the folder if the backup is stored as a folder.
    size_t num_entries = 0;

    /// The uncompressed size of the backup.
    UInt64 uncompressed_size = 0;

    /// The compressed size of the backup.
    UInt64 compressed_size = 0;

    /// Returns the number of files read during RESTORE from this backup.
    size_t num_read_files = 0;

    // Returns the total size of files read during RESTORE from this backup.
    UInt64 num_read_bytes = 0;

    /// Set only if there was an error.
    std::exception_ptr exception;
    String error_message;

    /// Profile events collected during the backup.
    std::shared_ptr<ProfileEvents::Counters::Snapshot> profile_counters = nullptr;

    std::chrono::system_clock::time_point start_time;
    std::chrono::system_clock::time_point end_time;
};

}
