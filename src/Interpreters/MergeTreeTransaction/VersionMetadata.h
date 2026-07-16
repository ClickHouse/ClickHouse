#pragma once
#include <expected>
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

/// Tag type returned by `storeInfo` when the storing version of the metadata to be persisted
/// does not match the persisted version, meaning the in-memory state is outdated.
struct StaleVersion
{
};

/// Sentinel value of type `StaleVersion`. Returned (wrapped in `std::unexpected`) by `storeInfo`
/// when a version conflict is detected.
inline constexpr StaleVersion TOO_OLD_VERSION{};

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

    /// Non-transactionally marks the data part as removed, acquiring and releasing the removal lock inline.
    /// Unlike `setAndStoreRemovalTID` (which assumes the caller already holds the lock), this method
    /// manages the lock itself: lock → set+store `Tx::NonTransactionalTID` → unlock.
    /// Returns immediately if the part is already removed.
    virtual void setAndStoreNonTransactionalRemovalTID(const TransactionInfoContext & transaction_context) = 0;

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

    /// Loads metadata from storage, calls `updateCSNIfNeeded`, and if it modified the info, persists it back.
    /// Sets the loaded (and possibly updated) metadata in memory.
    /// Throws `STALE_VERSION` if storing the updated metadata keeps failing after `MAX_RETRIES` attempts.
    void loadAndUpdateMetadata();

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

    /// Returns a string identifying this object for logging (typically table name and part name).
    String getObjectName() const;

    inline static constexpr auto TXN_VERSION_METADATA_FILE_NAME = "txn_version.txt";

protected:
    /// Loads `VersionInfo` from persistent storage with error handling.
    /// Handles cases like missing metadata file, creates default values if needed.
    virtual VersionInfo loadMetadata() = 0;

    /// Reads `VersionInfo` directly from persistent storage.
    /// Assumes metadata file exists. Low-level read operation without special case handling.
    virtual VersionInfo readMetadata() = 0;

    /// Applies `update_info_func` to metadata and stores it, retrying up to `MAX_RETRIES` times on `TOO_OLD_VERSION`.
    /// On the first attempt uses in-memory metadata; subsequent attempts reload from storage.
    /// Each iteration: loads info → applies `update_info_func` → if it returns `false`, skips storing and returns
    /// immediately; otherwise adjusts CSNs → validates → stores.
    /// Sets the updated metadata in memory after successful persistence.
    /// Used by `setAndStoreCreationCSN`, `setAndStoreRemovalCSN`, `setAndStoreCreationTID`, and `setAndStoreRemovalTID`.
    void updateInfoWithRefreshDataThenStoreAndSetMetadata(std::function<bool(VersionInfo & current_info)> update_info_func);

    /// Validates and updates in-memory `version_info` with locking.
    void validateAndSetInfo(const VersionInfo & new_info);
    /// Updates in-memory `version_info` with locking.
    void setInfo(const VersionInfo & new_info);

    /// For each TID in `current_info` that has a committed CSN in `TransactionLog`, writes the CSN back into `current_info`.
    /// Returns `true` if `current_info` was modified, `false` if no modification was needed.
    /// Returns `std::nullopt` when `current_info` is detected to be stale (the removal lock has been updated by
    /// another transaction since the info was read); the caller must reload metadata and retry.
    std::optional<bool> updateCSNIfNeeded(VersionInfo & current_info);

    /// Looks up the CSN for `tid` in `TransactionLog`.
    /// If no CSN is found, checks whether the transaction is still running.
    /// If it is no longer running and still has no CSN, the transaction was rolled back, and `Tx::RolledBackCSN` is returned.
    /// The CSN is re-checked after the running-transaction lookup to close the race window between the two queries.
    /// Returns 0 if the transaction is still in progress with no committed CSN yet.
    CSN tryGetCSN(TransactionID tid);

    /// Static validation of `VersionInfo` fields. Throws on invalid state.
    static void validateInfo(const String & object_name, const VersionInfo & info);

    void writeToBuffer(WriteBuffer & buf, bool one_line) const;
    void readFromBuffer(ReadBuffer & buf, bool one_line);

    /// Persists `new_info` to storage.
    /// Returns the new `storing_version` on success.
    /// Returns `TOO_OLD_VERSION` if the storing version of `new_info` does not match the persisted version.
    virtual std::expected<Int32, StaleVersion> storeInfo(const VersionInfo & new_info) = 0;

    /// Pointer to the data part that this metadata belongs to.
    IMergeTreeDataPart * merge_tree_data_part;

    /// Protects `version_info` from concurrent access.
    mutable std::mutex version_info_mutex;
    /// In-memory copy of the transactional metadata.
    VersionInfo version_info TSA_GUARDED_BY(version_info_mutex);

    /// Logger for this version metadata instance.
    LoggerPtr log;
};

DataTypePtr getTransactionIDDataType();
}
