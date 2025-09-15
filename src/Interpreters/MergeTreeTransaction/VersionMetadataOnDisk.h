#pragma once
#include <atomic>
#include <mutex>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <base/types.h>

namespace DB
{
/// Subclass of `VersionMetadata` that persists transactional metadata to disk alongside the data part.
/// Stores metadata in `txn_version.txt` file within the data part's storage directory.
/// Provides optimizations like deferred persistence for non-transactional parts and atomic file operations.
class VersionMetadataOnDisk : public VersionMetadata
{
public:
    /// Constructs a `VersionMetadataOnDisk` instance for the given data part.
    /// Initializes `can_write_metadata` based on whether the storage supports transactions and the part is writable.
    /// Sets `is_persist_deferrable` to true if no metadata file exists yet (to defer writes for non-transactional parts).
    explicit VersionMetadataOnDisk(IMergeTreeDataPart * merge_tree_data_part_);

    /// Loads transactional metadata from disk with error handling for missing or incomplete files.
    /// Handles special cases:
    /// - If metadata file is missing and part is read-only or has no tmp file: treats as non-transactional part
    /// - If tmp metadata file exists but main file is missing: treats as rolled-back transaction and removes tmp file
    /// - If metadata file exists: reads and returns it
    VersionInfo loadMetadata() override;

    /// Attempts to atomically acquire a removal lock for the given transaction.
    /// Uses compare-and-swap on `removal_tid_lock_hash` to ensure only one transaction can lock the part.
    /// Logs the lock event to `TransactionsInfoLog` if successful.
    /// @param tid Transaction ID requesting the lock
    /// @param context Context information for logging
    /// @param locked_by_id Output parameter: receives TIDHash of the locking transaction if lock acquisition fails
    /// @return true if lock acquired, false if already locked by another transaction
    bool tryLockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context, TIDHash * locked_by_id) override;

    /// Releases the removal lock previously acquired by the given transaction.
    /// Verifies that `tid` matches the transaction holding the lock before unlocking.
    /// Logs the unlock event to `TransactionsInfoLog`.
    /// @param tid Transaction ID that acquired the lock
    /// @param context Context information for logging
    /// @throws Exception if tid does not match the locking transaction
    void unlockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context) override;

    /// Returns true if the data part is currently locked for removal by any transaction.
    /// Checks if `removal_tid_lock_hash` is non-zero.
    bool isRemovalTIDLocked() override;

    /// Returns the TIDHash of the transaction currently holding the removal lock.
    /// Returns 0 if no lock is held.
    TIDHash getRemovalTIDLockHash() override { return removal_tid_lock_hash; }

    /// Returns true if transactional metadata exists in persistent storage or is pending deferred write.
    /// Checks both `deferred_persist_info` and the existence of `txn_version.txt` file.
    bool hasPersistedMetadata() const override;

protected:
    /// Persists the given `VersionInfo` to disk and returns the new storing version number.
    /// Implements deferred persistence optimization: if the part is not involved in a transaction and
    /// no metadata file exists yet, stores metadata in `deferred_persist_info` instead of writing to disk.
    /// Once a transaction touches the part, flushes deferred metadata to disk.
    /// Thread-safe: acquires `persisted_info_mutex` before calling `storeInfoImplUnlock`.
    Int32 storeInfoImpl(const VersionInfo & new_info) override;

    /// Reads transactional metadata directly from `txn_version.txt` file on disk.
    /// If `deferred_persist_info` is set, flushes it to disk first before reading.
    /// Thread-safe: acquires `persisted_info_mutex` before calling `readMetadataUnlock`.
    /// @throws Exception if metadata file does not exist
    VersionInfo readMetadata() override;

private:
    /// Internal implementation of `storeInfoImpl` that requires `persisted_info_mutex` to be held by the caller.
    /// Performs the actual metadata persistence logic:
    /// - For non-transactional parts with no existing file: defers write by storing in `deferred_persist_info`
    /// - Otherwise: validates storing version, increments it, writes to disk via atomic tmp file + rename
    /// - Clears `deferred_persist_info` and sets `is_persist_deferrable` to false after successful write
    Int32 storeInfoImplUnlock(VersionInfo new_info) TSA_REQUIRES(persisted_info_mutex);

    /// Returns the expected storing version number by reading it from disk or from `deferred_persist_info`.
    /// Used for optimistic concurrency control to detect version conflicts.
    /// Requires `persisted_info_mutex` to be held by the caller.
    /// Returns 0 if no metadata file exists yet.
    Int32 getExpectedStoringVersionUnlock() TSA_REQUIRES(persisted_info_mutex);

    /// Reads transactional metadata from `txn_version.txt` without locking.
    /// Requires `persisted_info_mutex` to be held by the caller.
    /// If `deferred_persist_info` is set, flushes it to disk first before reading.
    /// Returns std::nullopt if metadata file does not exist
    std::optional<VersionInfo> readMetadataUnlock() TSA_REQUIRES(persisted_info_mutex);

    /// Removes the temporary metadata file `txn_version.txt.tmp` if it exists.
    /// Called during metadata loading when detecting incomplete transactions.
    /// Logs a warning with the tmp file's content before removal for debugging purposes.
    void removeTmpMetadataFile();

    /// Static helper that writes `VersionInfo` to the data part storage atomically.
    /// Uses a two-phase protocol:
    /// 1. Writes to temporary file `txn_version.txt.tmp`
    /// 2. Atomically renames tmp file to `txn_version.txt`
    /// Includes fsync operations if configured in storage settings.
    /// Cleans up tmp file if any error occurs during the process.
    static void
    storeInfoToDataPartStorage(const MergeTreeData & storage, IDataPartStorage & data_part_storage, const VersionInfo & new_info);

    /// Whether metadata can be written to disk for this part.
    /// True if the storage supports transactions and the part is not on a read-only disk.
    const bool can_write_metadata;

    /// Atomic hash of the transaction ID that currently holds the removal lock.
    /// Zero (0) indicates the part is not locked for removal.
    /// Used for optimistic locking: transactions use compare-and-swap to acquire the lock.
    std::atomic<TIDHash> removal_tid_lock_hash = 0;

    /// Protects access to `is_persist_deferrable` and `deferred_persist_info`.
    /// Also used to serialize disk I/O operations for metadata files.
    mutable std::mutex persisted_info_mutex;

    /// Indicates whether metadata persistence can be deferred for this part.
    /// True initially if no metadata file exists yet (new non-transactional parts).
    /// Set to false once metadata is actually written to disk.
    /// Allows optimization: non-transactional operations don't trigger disk writes immediately.
    mutable bool is_persist_deferrable TSA_GUARDED_BY(persisted_info_mutex) = true;

    /// Holds metadata that is pending persistence to disk.
    /// Used when `is_persist_deferrable` is true and the part hasn't been involved in a transaction yet.
    /// Once a transaction touches the part, this is flushed to disk and cleared.
    mutable std::optional<VersionInfo> deferred_persist_info TSA_GUARDED_BY(persisted_info_mutex);
};

}
