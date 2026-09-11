#pragma once
#include <cstdint>
#include <expected>
#include <memory>
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

class IStorage;

class IMergeTreeDataPart;

/// Intent expressed by a part-scoped lock znode. The numeric value is the on-wire
/// identity in `LockContent` and shown in logs; who may take over whom is decided by
/// `canTakeOverLock` in `PartLock.cpp` (cloud-only).
enum class LockKind : uint8_t
{
    NONE = 0,            /// Unset sentinel; real locks set an explicit kind, so it never reaches the takeover decision.
    BG_MERGE = 1,        /// Background merge.
    OPTIMIZE_FINAL = 2,  /// `OPTIMIZE FINAL`.
    MUTATION = 3,        /// `ALTER UPDATE` / `ALTER DELETE` / other mutation.
    REMOVAL = 4,         /// Transactional removal (the `removal_lock` semantic).
    TRUNCATE = 5,        /// `TRUNCATE`.
    DROP = 6,            /// `DROP`.
    OPTIMIZE = 7,        /// `OPTIMIZE` (without FINAL).
};

/// `(version, czxid)` of a `<part>/removal_lock` at acquire time. `unlockRemovalTID`
/// needs `hasFingerprint` to be true — without it the preemption check is skipped and
/// the TID-only path can remove a peer's lock (SMT background ops share the sentinel
/// `NonTransactional` TID).
struct LockFingerprint
{
    Int32 version = -1;
    int64_t czxid = 0;

    bool hasFingerprint() const { return version >= 0 && czxid != 0; }
    bool operator==(const LockFingerprint &) const = default;
};

/// Tag type returned by `storeInfo` when the storing version of the metadata to be persisted
/// does not match the persisted version, meaning the in-memory state is outdated.
struct StaleVersion
{
};

/// Sentinel instance of `StaleVersion`.
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
    VersionMetadata(String part_name_, const IStorage * storage_);
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

    /// Same as the instance method but operates on a free-standing `VersionInfo`.
    /// Resolves `VersionInfo::isVisible`'s `nullopt` (unknown CSN) via `TransactionManager::getCSN`.
    static bool isVisible(const VersionInfo & current_info, CSN snapshot_version, TransactionID current_tid = Tx::EmptyTID);

    /// Sets `creation_csn` when a transaction commits.
    void setAndStoreCreationCSN(CSN csn);

    /// Sets `removal_csn` when a transaction commits.
    void setAndStoreRemovalCSN(CSN csn);

    /// Sets `creation_tid` when a data part is created by a transaction (setting `creation_csn` to
    /// `NonTransactionalCSN` if non-transactional). Optionally logs the event to system log if context is provided.
    void setAndStoreCreationTID(const TransactionID & tid, TransactionInfoContext * context);

    /// Sets `removal_tid` when a transaction starts removing the data part (setting `removal_csn` to
    /// `NonTransactionalCSN` if non-transactional).
    /// When `lock_fingerprint` identifies a held removal lock, the persist is gated on that lock so
    /// a peer that reclaimed the part cannot be overwritten; an empty fingerprint persists unguarded.
    void setAndStoreRemovalTID(const TransactionID & tid, const LockFingerprint & lock_fingerprint = {});

    /// Rollback: clears `removal_tid`. For a part created and removed by the same rolled-back
    /// transaction, also stamps `creation_csn = RolledBackCSN` in the same write, so a peer never
    /// sees it half-reset and mistakes it for a live uncommitted part.
    /// `lock_fingerprint` gates the persist on a held removal lock, as in `setAndStoreRemovalTID`.
    /// Returns whether our `removal_tid` was actually cleared. `false` means it was not ours to clear —
    /// a peer took the lock over, or a removal already committed — so the caller must not restore the
    /// part. A cleared and a peer-cleared `removal_tid` look identical afterwards, so the outcome cannot
    /// be recovered from `VersionInfo`.
    bool resetRemovalTID(const LockFingerprint & lock_fingerprint = {});

    /// Locks the data part for removal by the given transaction. Throws `SERIALIZATION_ERROR` if the
    /// part is already removed or another transaction holds the lock. If the part vanished while
    /// locking (Keeper path: `tryLockRemovalTID` raised `NO_SUCH_DATA_PART`) it returns `false`, so
    /// callers skip removal without a try/catch.
    /// @param tid Transaction ID requesting the removal lock
    /// @param context Context information for error messages
    /// @param acquired Optional out-parameter: receives the acquire-time `LockFingerprint`
    ///        from `tryLockRemovalTID`. Keeper-backed impls fill it; disk-backed impls leave it
    ///        at its default. Pass the same fingerprint to `unlockRemovalTID` so the release
    ///        path can detect preemption / delete-recreate.
    /// @return `true` if the part is now locked for removal; `false` if it already vanished.
    bool lockRemovalTID(const TransactionID & tid, LockKind kind, const TransactionInfoContext & context, LockFingerprint * acquired = nullptr);

    /// Non-transactionally marks the data part as removed, acquiring and releasing the removal lock inline.
    /// Unlike `setAndStoreRemovalTID` (which assumes the caller already holds the lock), this method
    /// manages the lock itself: lock → set+store `Tx::NonTransactionalTID` → unlock.
    /// Returns immediately if the part is already removed.
    virtual void setAndStoreNonTransactionalRemovalTID(LockKind kind, const TransactionInfoContext & transaction_context) = 0;

    /// Returns true only when no running transaction can still see the part: creation was rolled
    /// back, or removal committed at or before the oldest running snapshot.
    bool canBeRemoved() const;

    /// Loads metadata from storage, calls `updateCSNIfNeeded`, and if it modified the info, persists it back.
    /// Sets the loaded (and possibly updated) metadata in memory.
    /// Throws `STALE_VERSION` if storing the updated metadata keeps failing after `MAX_RETRIES` attempts.
    void loadAndUpdateMetadata();

    /// Validates that in-memory metadata is consistent with persisted metadata.
    /// Checks that key fields match between memory and storage:
    /// - `creation_tid`, `removal_tid`, `creation_csn`, `removal_csn`
    /// - Additional validation: if `removal_csn` is set, `removal_tid` must not be empty
    /// Short-circuits to `true` for in-memory rolled-back parts (see implementation comment).
    /// Throws on mismatch — callers like `removeIfNeeded` rely on the part destructor's
    /// outer try/catch to log and swallow.
    /// `VersionMetadataOnDisk` overrides to add disk-existence recovery for `CANNOT_OPEN_FILE`.
    virtual bool hasValidMetadata();

    /// Attempts to lock the data part for removal by the given transaction without throwing exceptions.
    /// Returns true if lock acquired successfully, false if already locked by another transaction.
    /// When returning false, sets `locked_by_id` to the TIDHash of the transaction holding the lock.
    /// @param tid Transaction ID requesting the removal lock
    /// @param context Context information for logging events to system logs
    /// @param locked_by_id Output parameter: receives TIDHash of the locking transaction if lock acquisition fails
    /// @param acquired Optional out-parameter: receives the acquire-time `LockFingerprint`.
    ///        Keeper-backed impls fill it; disk-backed impls leave it untouched.
    virtual bool tryLockRemovalTID(
        const TransactionID & tid,
        LockKind kind,
        const TransactionInfoContext & context,
        TIDHash * locked_by_id,
        LockFingerprint * acquired) = 0;

    /// Unlocks the data part previously locked for removal.
    /// Only succeeds if `tid` matches the transaction that originally acquired the lock.
    /// Throws an exception if the transaction does not match.
    /// @param tid Transaction ID that acquired the lock
    /// @param context Context information for logging events to system logs
    /// @param expected Acquire-time `LockFingerprint`. Keeper-backed impls use it to detect
    ///        preemption / delete-recreate and throw `ABORTED` instead of `LOGICAL_ERROR`
    ///        when ownership has changed since acquire. Pass `{}` to opt out of the
    ///        fingerprint check (for callers that don't track the fingerprint).
    virtual void unlockRemovalTID(
        const TransactionID & tid,
        const TransactionInfoContext & context,
        LockFingerprint expected) = 0;

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

    /// Update the log-only part name on rename. Called only while the part is single-owner, so no lock.
    void setPartName(String new_part_name) { part_name = std::move(new_part_name); }

    inline static constexpr auto TXN_VERSION_METADATA_FILE_NAME = "txn_version.txt";

    /// Temporary file written before the atomic rename to `TXN_VERSION_METADATA_FILE_NAME`.
    /// May legitimately linger on a part (for example, hardlinked onto a mutated part from its
    /// source during a merge/mutation race on object storage), in which case it must be cleaned
    /// up together with the main file. If only this file is present (no final file), the creating
    /// transaction never committed, so the part must be treated as rolled back.
    inline static constexpr auto TMP_TXN_VERSION_METADATA_FILE_NAME = "txn_version.txt.tmp";

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
    /// Returns whether the write landed.
    bool updateInfoWithRefreshDataThenStoreAndSetMetadata(std::function<bool(VersionInfo & current_info)> update_info_func);

    /// Same contract as `updateInfoWithRefreshDataThenStoreAndSetMetadata`, but persists the part
    /// znode in one Multi together with a `Check` on the removal lock identified by `lock_fingerprint`.
    /// If a peer reclaimed the lock the Multi fails on the `Check`, so the part is left untouched and
    /// in-memory state is just refreshed. The base implementation ignores the fingerprint (no lock
    /// backend); `VersionMetadataOnKeeper` overrides it.
    ///
    /// Only for removal state, as the name says: the returned "the write landed" answer relies on
    /// `removal_tid` being writable by the lock holder alone, so the lock can attribute a change we
    /// find already applied. Creation and CSN state has unguarded writers and would break that.
    virtual bool updateRemovalInfoUnderLockThenStore(
        std::function<bool(VersionInfo & current_info)> update_info_func, const LockFingerprint & lock_fingerprint);

    /// Validates and updates in-memory `version_info` with locking.
    void validateAndSetInfo(const VersionInfo & new_info);
    /// Updates in-memory `version_info` with locking.
    void setInfo(const VersionInfo & new_info);

    /// For each TID in `current_info` that has a committed CSN in `TransactionManager`, writes the CSN back into `current_info`.
    /// Returns `true` if `current_info` was modified, `false` if no modification was needed.
    /// Returns `std::nullopt` when `current_info` is detected to be stale (the removal lock has been updated by
    /// another transaction since the info was read); the caller must reload metadata and retry.
    std::optional<bool> updateCSNIfNeeded(VersionInfo & current_info);

    /// Looks up the CSN for `tid` in `TransactionManager`.
    /// `getCSN` plus dead-session fallback: returns `RolledBackCSN` for a TID
    /// whose owning session is dead, `UnknownCSN` if it's still running with no
    /// CSN yet, otherwise the committed CSN.
    CSN tryGetCSN(TransactionID tid) const;

    /// Static validation of `VersionInfo` fields. Throws on invalid state.
    static void validateInfo(const String & object_name, const VersionInfo & info);

    void writeToBuffer(WriteBuffer & buf, bool one_line) const;
    void readFromBuffer(ReadBuffer & buf, bool one_line);

    /// Write the non-transactional removal and finalize its lock together. Caller already holds
    /// the lock. The two must not be separable on Keeper: a higher-priority DROP/TRUNCATE can
    /// take over the lock, so the removal must not be written once we have lost it. Keeper does
    /// it in one atomic Multi — marking the lock committed (may throw `NO_SUCH_DATA_PART` if the
    /// part znode is gone); disk just writes the file then resets its in-process lock.
    virtual void finalizeNonTransactionalRemoval(const TransactionID & tid, LockKind kind, LockFingerprint acquired_fp) = 0;

    /// Persists `new_info` to storage.
    /// Returns the new `storing_version` on success.
    /// Returns `TOO_OLD_VERSION` if the storing version of `new_info` does not match the persisted version.
    virtual std::expected<Int32, StaleVersion> storeInfo(const VersionInfo & new_info) = 0;

    /// Read persisted metadata and validate it matches `current_info`. Throws on mismatch.
    /// Shared by `hasValidMetadata` and its `VersionMetadataOnDisk` override so they can
    /// install different catch blocks without duplicating the comparison logic.
    bool validateAgainstPersistedMetadata(const VersionInfo & current_info);

    /// Part name, used only for log messages via `getObjectName`.
    String part_name;
    /// Raw pointer: storage owns the part, the part owns us, so storage always outlives
    /// us. A `shared_ptr` here would form a cycle and hang `DROP TABLE`.
    const IStorage * storage;

    /// Protects `version_info` from concurrent access.
    mutable std::mutex version_info_mutex;
    /// In-memory copy of the transactional metadata.
    VersionInfo version_info TSA_GUARDED_BY(version_info_mutex);

    /// Logger for this version metadata instance.
    LoggerPtr log;
};

DataTypePtr getTransactionIDDataType();

/// Is a part's creation committed? Answers from live state, the same on every replica: the stamped
/// `creation_csn` when present, otherwise resolved via `TransactionManager::getCSN(creation_tid)`.
/// A non-transactional creation counts as committed. Lets callers decide from the current commit
/// state instead of a value recorded when the part was dropped.
bool isCreationCommitted(const VersionInfo & version_info);
}
