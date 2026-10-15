#pragma once

#include <Core/Types.h>


namespace DB
{
class BackupEntriesCollector;
class RestorerFromBackup;

/// Makes backup entries to copy the SQL-created named collections to a backup. Each collection is
/// copied as the bytes the metadata storage holds - `FileEncryption` ciphertext for the
/// `*_encrypted` storage types - so nothing is decrypted here and the collections stay encrypted
/// in the backup. Called by StorageSystemNamedCollections::backupData.
void backupNamedCollections(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup);

/// Restores the SQL-created named collections from a backup. Every entry is read, decrypted with
/// this server's named collections key (the key fingerprint of each entry's header is verified
/// first, so a key mismatch fails with a clear error) and parsed before anything is created, and
/// the collections are re-created through the normal storage path, so they are re-encrypted under
/// this server's key. Called before any table is created, because table engines resolve named
/// collections inside CREATE, see RestorerFromBackup::restoreNamedCollections.
void restoreNamedCollections(RestorerFromBackup & restorer, const String & data_path_in_backup);

/// Makes the named collections restored by another host visible on this host. With a replicated
/// storage only one host writes the restored collections, and the other hosts observe the write
/// through an asynchronous Keeper watch, so every host must reload synchronously after the stage
/// barrier that follows restoreNamedCollections and before table creation starts, where a table
/// CREATE may resolve a restored collection. A no-op for the non-replicated storage types.
void reloadNamedCollectionsAfterRestore();

}
