#pragma once

#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>
#include <Common/NamedCollections/NamedCollections_fwd.h>

#include <unordered_map>


namespace DB
{
class BackupEntriesCollector;
class RestorerFromBackup;
struct RestoreSettings;

/// The `CREATE NAMED COLLECTION` statements of the named collections in a backup, by name.
using NamedCollectionsInBackup = std::unordered_map<String, ASTPtr>;

/// Makes backup entries to copy the SQL-created named collections to a backup. Each collection is
/// copied as the bytes the metadata storage holds - `FileEncryption` ciphertext for the
/// `*_encrypted` storage types - so nothing is decrypted here and the collections stay encrypted
/// in the backup. Called by StorageSystemNamedCollections::backupData.
void backupNamedCollections(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup);

/// Registers with the restore coordination the directory of the backup this host would restore the
/// named collections from. A replicated (Keeper) storage is shared by the hosts using it and only
/// one of them performs the restore, but the hosts of a RESTORE ON CLUSTER query resolve different
/// directories of the backup, and those directories hold different collections when the backup was
/// taken from hosts whose storages were not shared the same way (e.g. a `local` storage on every
/// host) - so the restoring host has to know all of them (see restoreNamedCollections). Must be
/// called with the coordination's stage barrier between the registration and the restore: the
/// restoring host reads the registrations of every host. A no-op for the non-replicated storage
/// types, which each host restores independently from its own directory.
void registerNamedCollectionsDirForRestore(RestorerFromBackup & restorer, const String & data_path_in_backup);

/// Restores the SQL-created named collections from a backup. Every entry is read, decrypted with
/// this server's named collections key (the key fingerprint of each entry's header is verified
/// first, so a key mismatch fails with a clear error) and parsed before anything is created, and
/// the collections are re-created through the normal storage path, so they are re-encrypted under
/// this server's key. Called before any table is created, because table engines resolve named
/// collections inside CREATE, see RestorerFromBackup::restoreNamedCollections.
///
/// With a replicated (Keeper) storage the host acquiring the restore processes the directories
/// registered by every host sharing the storage (see registerNamedCollectionsDirForRestore):
/// identical duplicate definitions of a collection collapse into one, and conflicting definitions
/// of the same name fail the restore before anything has been created. `data_path_in_backup` may
/// be empty when this host's part of the backup holds no named collections.
///
/// Returns whether named collections are restored into this host's storage - by this host or, for
/// a shared storage, possibly by another host, in which case this host must reload them after the
/// stage barrier that follows (see reloadNamedCollectionsAfterRestore).
bool restoreNamedCollections(RestorerFromBackup & restorer, const String & data_path_in_backup);

/// Whether the restore installs the named collection `collection_name` if the backup holds it, as `restoreNamedCollections` decides
/// by `create_named_collection`: false if a definition that the restore creates resolves to the collection that this server has.
[[nodiscard]] bool doesRestoreInstallNamedCollection(const RestoreSettings & restore_settings, const String & collection_name);

/// Reads the named collections that `restoreNamedCollections` would restore from the backup, without restoring them, for the checks
/// of a definition that the restore creates before the collections are restored. Empty if the restore does not restore named
/// collections.
[[nodiscard]] NamedCollectionsInBackup readNamedCollectionsToRestore(RestorerFromBackup & restorer, const String & data_path_in_backup);

/// Returns the named collection `collection_name` of `collections_in_backup` (read by `readNamedCollectionsToRestore`), or nullptr
/// if the backup does not hold it.
[[nodiscard]] NamedCollectionPtr getNamedCollectionInBackup(const NamedCollectionsInBackup & collections_in_backup, const String & collection_name);

/// Makes the named collections restored by another host visible on this host. With a replicated
/// storage only one host writes the restored collections, and the other hosts observe the write
/// through an asynchronous Keeper watch, so every host must reload synchronously after the stage
/// barrier that follows restoreNamedCollections and before table creation starts, where a table
/// CREATE may resolve a restored collection. A no-op for the non-replicated storage types.
void reloadNamedCollectionsAfterRestore();

}
