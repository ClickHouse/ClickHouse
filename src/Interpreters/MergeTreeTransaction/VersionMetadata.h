#pragma once
#include <Interpreters/MergeTreeTransaction/VersionInfo.h>
#include <Interpreters/StorageID.h>
#include <base/types.h>
#include <Common/SharedMutex.h>
#include <Common/TransactionID.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class IMergeTreeDataPart;

/**
 * Context information about a data part involved in a transaction operation (creation or removal).
 * Used for logging and generating detailed exception messages.
 */
struct TransactionInfoContext
{
    /// Table to which the data part belongs.
    StorageID table = StorageID::createEmpty();
    /// Name of the data part being created or removed by the transaction.
    String part_name;
    /// Name of the data part that covers `part_name` (set when removing a part that was merged into another).
    String covering_part;

    TransactionInfoContext(StorageID id, String part)
        : table(std::move(id))
        , part_name(std::move(part))
    {
    }

    TransactionInfoContext(StorageID id, String part, String covering_part_)
        : table(std::move(id))
        , part_name(std::move(part))
        , covering_part(std::move(covering_part_))
    {
    }
};

/// Base class for managing transactional metadata of data parts.
/// Stores transaction IDs and commit sequence numbers for both creation and removal operations.
/// Provides visibility checking for MVCC and handles metadata persistence.
class VersionMetadata : public boost::noncopyable
{
public:
    explicit VersionMetadata(IMergeTreeDataPart * merge_tree_data_part_);
    virtual ~VersionMetadata() = default;

    /// Checks if the data part is visible at the given snapshot version and from the perspective of `current_tid`.
    /// Returns true if:
    /// - The part was created before the snapshot and not removed yet, or
    /// - The part was created before the snapshot and removed after it, or
    /// - The current transaction is creating it.
    /// Returns false if:
    /// - The part was created after the snapshot, or
    /// - The part was removed before or at the snapshot, or
    /// - The current transaction is removing it.
    bool isVisible(CSN snapshot_version, TransactionID current_tid = Tx::EmptyTID);

    /// Sets `creation_csn` when a transaction commits.
    /// Gets current info, updates `creation_csn`, persists to storage, then updates in-memory state.
    void setAndStoreCreationCSN(CSN csn);

    /// Sets `removal_csn` when a transaction commits.
    /// Gets current info, updates `removal_csn`, persists to storage, then updates in-memory state.
    void setAndStoreRemovalCSN(CSN csn);

    /// Sets `creation_tid` when a data part is created by a transaction.
    /// Gets current info, updates `creation_tid` (and sets `creation_csn` to `NonTransactionalCSN` if non-transactional),
    /// persists to storage, then updates in-memory state. Optionally logs the event to system log if context is provided.
    void setAndStoreCreationTID(const TransactionID & tid, TransactionInfoContext * context);

    /// Sets `removal_tid` when a transaction starts removing the data part.
    /// Gets current info, updates `removal_tid` (and sets `removal_csn` to `NonTransactionalCSN` if non-transactional),
    /// persists to storage, then updates in-memory state.
    void setAndStoreRemovalTID(const TransactionID & tid);

    /// Locks the data part for removal by the given transaction.
    /// Logic:
    /// 1. Gets current info and checks if already removed (removal_csn != 0) → throws exception if true
    /// 2. Attempts to acquire removal lock via `tryLockRemovalTID` → returns if successful
    /// 3. If lock acquisition fails (another transaction holds the lock) → throws exception with locking transaction info
    /// @param tid Transaction ID requesting the removal lock
    /// @param context Context information for error messages
    void lockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context);

    /// Returns `removal_tid` for logging purposes with validity checking.
    /// If removal is committed (removal_csn set), returns `removal_tid` directly.
    /// If removal_tid is empty or non-transactional, returns it as-is.
    /// Otherwise, verifies the transaction is still running in `TransactionLog`:
    /// - If found, returns the transaction's TID
    /// - If not found (transaction completed/rolled back), returns `EmptyTID`
    TransactionID getRemovalTIDForLogging() const;

    /// Checks if the data part can be safely removed from storage.
    /// Returns true if no running transaction can see this part anymore.
    /// Logic:
    /// - Returns true immediately if removal is non-transactional
    /// - Returns true if creation was rolled back
    /// - Returns false if creation is not yet committed
    /// - Returns false if the part is too new (created after oldest snapshot)
    /// - Returns false if removal is not yet committed
    /// - Returns true if removal was committed before the oldest running snapshot
    bool canBeRemoved() const;

    /// Loads metadata from persistent storage and updates it if transactions have completed.
    /// Logic:
    /// 1. Loads metadata from storage
    /// 2. Adjusts metadata by checking if transactions with missing CSNs have completed:
    ///    - For creation: if creation_tid no longer running, sets creation_csn (or marks as rolled back)
    ///    - For removal: if removal_tid no longer running, sets removal_csn (or clears removal_tid)
    /// 3. If metadata was adjusted, persists it back to storage (with retry logic for version conflicts)
    /// 4. Sets the adjusted metadata in memory
    void loadAndAdjustMetadata();

    /// Validates that in-memory metadata is consistent with persisted metadata.
    /// Checks that key fields match between memory and storage:
    /// - `creation_tid`, `removal_tid`, `creation_csn`, `removal_csn`
    /// - Additional validation: if `removal_csn` is set, `removal_tid` must not be empty
    /// Returns true if valid, false if inconsistent (logs exception without throwing).
    bool hasValidMetadata();

    /// Attempts to lock the data part for removal by the given transaction without throwing exceptions.
    /// Returns true if lock acquired successfully, false if already locked by another transaction.
    /// When returning false, sets `locked_by_id` to the TIDHash of the transaction holding the lock.
    /// @param tid Transaction ID requesting the removal lock
    /// @param context Context information for logging events to system logs
    /// @param locked_by_id Output parameter: receives TIDHash of the locking transaction if lock acquisition fails
    virtual bool tryLockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context, TIDHash * locked_by_id) = 0;

    /// Unlocks the data part previously locked for removal.
    /// Only succeeds if `tid` matches the transaction that originally acquired the lock.
    /// Throws an exception if the transaction does not match.
    /// @param tid Transaction ID that acquired the lock
    /// @param context Context information for logging events to system logs
    virtual void unlockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context) = 0;

    /// Returns true if the data part is currently locked for removal by any transaction.
    virtual bool isRemovalTIDLocked() = 0;

    /// Returns the TIDHash of the transaction that currently holds the removal lock.
    /// Returns 0 if no transaction holds the lock.
    virtual TIDHash getRemovalTIDLockHash() = 0;

    /// Returns true if metadata file exists in persistent storage.
    virtual bool hasPersistedMetadata() const = 0;

    VersionInfo getInfo() const;

    LoggerPtr getLogger() const { return log; }

    inline static constexpr auto TXN_VERSION_METADATA_FILE_NAME = "txn_version.txt";

protected:
    /// Loads `VersionInfo` from persistent storage with error handling.
    /// Handles cases like missing metadata file, creates default values if needed.
    virtual VersionInfo loadMetadata() = 0;

    /// Reads `VersionInfo` directly from persistent storage.
    /// Assumes metadata file exists. Low-level read operation without special case handling.
    virtual VersionInfo readMetadata() = 0;

    /// Validates `new_info` and persists it to storage.
    /// If validation fails with `SERIALIZATION_ERROR`, attempts to adjust `new_info` in-place
    /// by querying CSNs of completed transactions, then re-validates and persists the adjusted version.
    /// It also updates `storing_version` for `new_info` in-place.
    void validateAdjustAndStoreMetadata(VersionInfo & new_info);

    /// Applies an update function to metadata with automatic retry logic for version conflicts.
    /// Logic:
    /// 1. First attempts to apply `update_info_func` to in-memory metadata, then validates, adjusts if needed, and stores
    /// 2. If that fails with `SERIALIZATION_ERROR` (version conflict), refreshes metadata from storage:
    ///    - Loads fresh metadata from storage
    ///    - Re-applies `update_info_func` to the fresh metadata
    ///    - Adjusts metadata by checking if transactions have completed
    ///    - Validates and stores the updated metadata (with retry logic for ZooKeeper `ZBADVERSION` errors)
    /// 3. Sets the updated metadata in memory after successful persistence
    /// Used by `setAndStoreCreationCSN`, `setAndStoreRemovalCSN`, `setAndStoreCreationTID`, and `setAndStoreRemovalTID`.
    void updateStoreAndSetMetadataWithRefresh(std::function<void(VersionInfo & current_info)> update_info_func);

    /// Validates and updates in-memory `version_info` with locking.
    void validateAndSetInfo(const VersionInfo & new_info);
    /// Updates in-memory `version_info` with locking.
    void setInfo(const VersionInfo & new_info);
    /// Updates in-memory `version_info` without locking. Must be called with `version_info_mutex` already held.
    void setInfoUnlocked(const VersionInfo & new_info);

    /// Returns a string identifying this object for logging (typically table name and part name).
    String getObjectName() const;

    /// Checks if transactions in `current_info` have completed and updates their CSNs.
    /// Returns true if `current_info` was modified.
    bool adjustInfo(VersionInfo & current_info);

    /// Static validation of `VersionInfo` fields. Throws on invalid state.
    static void validateInfo(const String & object_name, const VersionInfo & info);

    void writeToBuffer(WriteBuffer & buf, bool one_line) const;
    void readFromBuffer(ReadBuffer & buf, bool one_line);

    /// Persists `new_info` to storage and returns the new `storing_version`.
    virtual Int32 storeInfoImpl(const VersionInfo & new_info) = 0;

    /// Pointer to the data part that this metadata belongs to.
    IMergeTreeDataPart * merge_tree_data_part;

    /// Protects `version_info` from concurrent access.
    mutable SharedMutex version_info_mutex;
    /// In-memory copy of the transactional metadata.
    VersionInfo version_info;

    /// Logger for this version metadata instance.
    LoggerPtr log;
};

DataTypePtr getTransactionIDDataType();
}
