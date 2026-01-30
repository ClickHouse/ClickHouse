#pragma once
#include <atomic>
#include <Interpreters/MergeTreeTransaction/VersionInfo.h>
#include <Interpreters/StorageID.h>
#include <Common/TransactionID.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class IMergeTreeDataPart;

/**
 * @brief Contains additional information about a part that a transaction is attempting to create or remove.
 *
 * This structure is useful for logging purposes and generating detailed exception messages.
 */
struct TransactionInfoContext
{
    /// To which table a part belongs
    StorageID table = StorageID::createEmpty();
    /// Name of a part that transaction is trying to create/remove
    String part_name;
    /// Optional: name of part that covers `part_name` if transaction is trying to remove `part_name`
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

/**
* @brief VersionMetadata stores metadata identifying the transaction responsible for creating the object, along with the transaction creation time.
* It is responsible to persist the metadata on a storage.
*/
class VersionMetadata
{
public:
    explicit VersionMetadata(IMergeTreeDataPart * merge_tree_data_part_);
    virtual ~VersionMetadata() = default;

    /**
    * Checks if an object is visible for transaction or not.
    */
    bool isVisible(const MergeTreeTransaction & txn);
    bool isVisible(CSN snapshot_version, TransactionID current_tid = Tx::EmptyTID);

    /**
    * @brief Set creation_csn and store it to the stored data.
    */
    void storeCreationCSN(CSN csn);
    void setCreationCSN(CSN csn) { creation_csn.store(csn, std::memory_order_relaxed); }
    CSN getCreationCSN() const { return creation_csn.load(); }

    /**
    * @brief Set removal_csn and store it to the stored data.
    */
    void storeRemovalCSN(CSN csn);
    void setRemovalCSN(CSN csn);
    CSN getRemovalCSN() const { return removal_csn.load(); }

    /**
    * @brief Set removal_tid and store it to the stored data.
    */
    void storeRemovalTID(const TransactionID & tid);

    /**
    * @brief Set removal_tid a removal ID.
    *
    * @param tid `Tx::EmptyTID` indicates that the transaction is rolled back.
    */
    void setRemovalTID(const TransactionID & tid);

    TransactionID getRemovalTID() const;
    TransactionID getRemovalTIDForLogging() const;

    /**
    * @brief Locks the object for removal.
    *
    * Attempts to lock the object for removal. If the object is already locked
    * by another transaction, an exception is thrown.
    *
    * @param tid If successfully locked, tid is used as the removal TID.
    * @param context Used to provide some info when throwing an exception.
    */
    void lockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context);

    /// It can be called only from MergeTreeTransaction or on server startup
    void setCreationTID(const TransactionID & tid, TransactionInfoContext * context);
    TransactionID getCreationTID() const;

    /**
    * @brief check if the object is safe to removed
    */
    bool canBeRemoved();

    LoggerPtr getLogger() const { return log; }
    String toString(bool one_line = true) const;

    /**
    * @brief Load and verify metadata from persistent storage.
    *
    * @param logger For trace logging
    */
    void loadAndVerifyMetadata(LoggerPtr logger);

    bool wasInvolvedInTransaction();

    /**
    * @brief Validate if the info stored on persistent storage matches the info stored in this object.
    */
    bool assertHasValidMetadata();

    /**
    * @brief Stores the metadata to persistent storage.
    */
    virtual void storeMetadata(bool force) = 0;

    /**
    * @brief Locks the object for removal. Return true if successfully locked, otherwise, return false.
    *
    * @param tid If successfully locked, tid is used as the removal TID.
    * @param context Transaction info which is used to write events to system logs
    * @param locked_by_id The TIDHash of the current transaction hold the lock
    */
    virtual bool tryLockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context, TIDHash * locked_by_id) = 0;

    /**
    * @brief Unlocks the object previously locked for removal.
    *
    * Unlocking is successful only if the given transaction ID (`tid`) matches
    * the transaction that originally locked the object for removal.
    * If the transaction does not match, the unlock operation will throw an exception.
    *
    * @param tid The unlocking transaction ID
    * @param context Transaction info which is used to write events to system logs
    */
    virtual void unlockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context) = 0;

    /**
    * @brief Check if the object is currently locked for removal
    */
    virtual bool isRemovalTIDLocked() = 0;
    /**
    * @brief Retrieves the TIDHash of the transaction that locked the object for removal.
    */
    virtual TIDHash getRemovalTIDLock() = 0;

    /**
    * @brief Check if the object has metadata stored in storage.
    */
    virtual bool hasStoredMetadata() const = 0;
    /**

    * @brief Remove metadata stored in storage.
    */
    virtual void removeStoredMetadata() = 0;

    VersionInfo getInfo() const
    {
        return VersionInfo{
            .creation_tid = getCreationTID(),
            .removal_tid = getRemovalTID(),
            .creation_csn = getCreationCSN(),
            .removal_csn = getRemovalCSN(),
            .removal_tid_lock = removal_tid_lock,
        };
    }

    inline static constexpr auto TXN_VERSION_METADATA_FILE_NAME = "txn_version.txt";


protected:
    /**
    * @brief Store `creation_csn` to the stored metadata
    */
    void storeCreationCSNToStoredMetadata();

    /**
    * @brief Store `removal_csn` to the stored metadata
    */
    void storeRemovalCSNToStoredMetadata();

    /**
    * @brief Store removal_tid to the stored data.
    *
    * This function appends a removal transaction ID to the metadata being persisted.
    * The removal ID can be either `removal_tid` or `Tx::EmptyTID`.
    *
    * If the `creation_tid` is pre-historic and tid is not `Tx::EmptyTID`, the metadata is not yet
    * stored in persistent storage. This can occur in the following cases:
    * - The data has not been written to storage yet.
    * - The data was created without an associated transaction.
    *
    * In such cases, the function will first persist the data to storage,
    * then append the removal ID.
    */
    void storeRemovalTIDToStoredMetadata();

    /**
    * @brief Get the current removal TID hash.
    * If the object is locked, return the locking transaction.
    * If unlocked, return the has of `removal_tid`.
    * Returns 0 if there is no removal TID.
    */
    TIDHash getCurrentRemovalTIDHash();

    String getObjectName() const;

    bool canBeRemovedImpl(CSN oldest_snapshot_version);

    /**
    * @brief Adjust information from metadata
    *
    * @param logger For trace logging
    * @return true Version info is updated, need to re-store metadata in storage
    * @return false Version info is not updated.
    */
    bool adjustMetadata(LoggerPtr logger);

    /**
    * @brief Verify information from metadata
    */
    void verifyMetadata() const;

    void updateFromInfo(const VersionInfo & info);

    static void verifyMetadata(const String & object_name, const VersionInfo & info);

    void writeToBuffer(WriteBuffer & buf, bool one_line) const;
    void readFromBuffer(ReadBuffer & buf, bool one_line);

    /**
    * @brief Load metadata from persistent storage.
    */
    virtual void loadMetadata() = 0;

    /**
    * @brief Set removal_tid the hash of the TID which locks the object for removal.
    *
    * @param removal_tid_lock_hash The target TID hash
    */
    void setRemovalTIDLock(TIDHash removal_tid_lock_hash);

    /**
    * @brief The implementation to store `creation_csn` to the stored metadata. Called by `storeCreationCSNToStoredMetadata`
    */
    virtual void storeCreationCSNToStoredMetadataImpl() = 0;

    /**
    * @brief The implementation to store `removal_csn` to the stored metadata. Called by `storeRemovalCSNToStoredMetadata`
    */
    virtual void storeRemovalCSNToStoredMetadataImpl() = 0;

    /**
    * @brief The implementation to store a removal ID to the stored data. Called by `storeRemovalTIDToStoredMetadata`.
    *
    */
    virtual void storeRemovalTIDToStoredMetadataImpl() = 0;

    /**
    * @brief Read info from the stored metadata
    */
    virtual VersionInfo readStoredMetadata(String & content) = 0;

    IMergeTreeDataPart * merge_tree_data_part;

    mutable std::mutex creation_and_removal_tid_mutex;
    /// ID of transaction that has created/is trying to create this object stored in the storage.
    TransactionID creation_tid TSA_GUARDED_BY(creation_and_removal_tid_mutex) = Tx::EmptyTID;
    /// ID of transaction that has removed/is trying to remove this object stored in the storage.
    TransactionID removal_tid TSA_GUARDED_BY(creation_and_removal_tid_mutex) = Tx::EmptyTID;

    /// CSN of transaction that has created this object stored in the storage.
    std::atomic<CSN> creation_csn = Tx::UnknownCSN;
    /// CSN of transaction that has removed this object stored in the storage.
    std::atomic<CSN> removal_csn = Tx::UnknownCSN;

    /// Hash of the TID locking the object for removal
    /// Zero if unlocked
    std::atomic<TIDHash> removal_tid_lock = 0;

    LoggerPtr log;
};

DataTypePtr getTransactionIDDataType();
}
