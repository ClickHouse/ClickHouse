#pragma once
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
};

/**
* @brief VersionMetadata stores metadata identifying the transaction responsible for creating the object, along with the transaction creation time.
* It is responsible to persist the metadata on a storage.
*/
class VersionMetadata
{
public:
    explicit VersionMetadata(IMergeTreeDataPart * merge_tree_data_part_)
        : merge_tree_data_part(merge_tree_data_part_)
    {
    }
    virtual ~VersionMetadata() = default;

    /**
    * Checks if an object is visible for transaction or not.
    */
    bool isVisible(const MergeTreeTransaction & txn);
    bool isVisible(CSN snapshot_version, TransactionID current_tid = Tx::EmptyTID);

    void setCreationCSN(CSN csn) { creation_csn.store(csn); }
    CSN getCreationCSN() const { return creation_csn.load(); }

    void setRemovalCSN(CSN csn) { removal_csn.store(csn); }
    CSN getRemovalCSN() const { return removal_csn.load(); }

    /**
    * @brief Append `creation_csn` to the stored metadata
    */
    void appendCreationCSNToStoredMetadata();
    /**
    * @brief Append `removal_csn` to the stored metadata
    */
    void appendRemovalCSNToStoredMetadata();

    /**
    * @brief Appends a removal ID to the stored data.
    *
    * This function appends a removal transaction ID to the metadata being persisted.
    * The removal ID can be either `removal_tid` or `Tx::EmptyTID`.
    *
    * If the `creation_tid` is pre-historic and `clear` is false, the metadata is not yet
    * stored in persistent storage. This can occur in the following cases:
    * - The data has not been written to storage yet.
    * - The data was created without an associated transaction.
    *
    * In such cases, the function will first persist the data to storage,
    * then append the removal ID.
    *
    * @param clear If enabled,`Tx::EmptyTID` is used, otherwise, `removal_tid` is used
    */
    void appendRemovalTIDToStoredMetadata(bool clear = false);

    TransactionID getRemovalTID() const { return removal_tid; }
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

    TransactionID getCreationTID() const { return creation_tid; }
    /// It can be called only from MergeTreeTransaction or on server startup
    void setCreationTID(const TransactionID & tid, TransactionInfoContext * context);

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

    /**
    * @brief Stores the metadata to persistent storage.
    */
    virtual void storeMetadata(bool force) = 0;
    /**
    * @brief Validate if the info stored on persistent storage matches the info stored in this object.
    */
    virtual bool assertHasValidMetadata() const = 0;

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
    * @param tid The unlocking tranction ID
    * @param context Transaction info which is used to write events to system logs
    */
    virtual void unlockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context) = 0;

    /**
    * @brief Check if the object is currently locked for removal
    */
    virtual bool isRemovalTIDLocked() const = 0;
    /**
    * @brief Retrieves the TIDHash of the transaction that locked the object for removal.
    */
    virtual TIDHash getRemovalTIDLock() const = 0;

    /**
    * @brief Check if the object has metadata stored in storage.
    */
    virtual bool hasStoredMetadata() const = 0;

protected:
    bool canBeRemovedImpl(CSN oldest_snapshot_version);

    /**
    * @brief Write the metadata to a buffer
    * @param buf The writing buffer
    */
    void writeToBuffer(WriteBuffer & buf) const;

    struct Info
    {
        TransactionID creation_tid = Tx::EmptyTID;
        TransactionID removal_tid = Tx::EmptyTID;
        CSN creation_csn = Tx::UnknownCSN;
        CSN removal_csn = Tx::UnknownCSN;
    };
    static Info readFromBufferHelper(ReadBuffer & buf);

    /**
    * @brief Read the metadata from a buffer
    * @param buf The reading buffer
    */
    void readFromBuffer(ReadBuffer & buf);

    /**
    * @brief Write `creation_csn` to `buf`.
    * @param buf The writing buffer
    * @param throw_if_csn_unknown If true, it throws an exception if `creation_csn` is `UnknownCSN`
    */
    void writeCreationCSNToBuffer(WriteBuffer & buf, bool throw_if_csn_unknown = false) const;

    /**
    * @brief Write `removal_csn` to `buf`.
    * @param buf The writing buffer
    * @param throw_if_csn_unknown If true, it throws an exception if `removal_csn` is `UnknownCSN`
    */
    void writeRemovalCSNToBuffer(WriteBuffer & buf, bool throw_if_csn_unknown = false) const;

    /**
    * @brief Write `removal_csn` to `Tx::EmptyTID` to `buf`.
    *
    * @param clear If enabled,`Tx::EmptyTID` is written, otherwise, `removal_tid` is written.
    */
    void writeRemovalTIDToBuffer(WriteBuffer & buf, bool clear) const;

    /**
    * @brief Load metadata from persistent storage.
    */
    virtual void loadMetadata() = 0;

    /**
    * @brief Set the hash of the TID which locks the object for removal.
    *
    * @param removal_tid_hash The target TID hash
    */
    virtual void setRemovalTIDLock(TIDHash removal_tid_hash) = 0;

    /**
    * @brief The implementation to append `creation_csn` to the stored metadata. Called by `appendCreationCSNToStoredMetadata`
    */
    virtual void appendCreationCSNToStoredMetadataImpl() = 0;

    /**
    * @brief The implementation to append `removal_csn` to the stored metadata. Called by `appendRemovalCSNToStoredMetadata`
    */
    virtual void appendRemovalCSNToStoredMetadataImpl() = 0;

    /**
    * @brief The implementation to append a removal ID to the stored data.. Called by `appendRemovalTIDToStoredMetadata`.
    *
    * @param clear If enabled,`Tx::EmptyTID` is used, otherwise, `removal_tid` is used
    */
    virtual void appendRemovalTIDToStoredMetadataImpl(bool clear) = 0;

    static inline constexpr char CREATION_TID_STR[] = "creation_tid: ";
    static inline constexpr char CREATION_CSN_STR[] = "creation_csn: ";
    static inline constexpr char REMOVAL_TID_STR[] = "removal_tid:  ";
    static inline constexpr char REMOVAL_CSN_STR[] = "removal_csn:  ";

    const IMergeTreeDataPart * merge_tree_data_part;

    /// ID of transaction that has created/is trying to create this object
    TransactionID creation_tid = Tx::EmptyTID;
    /// ID of transaction that has removed/is trying to remove this object
    TransactionID removal_tid = Tx::EmptyTID;

    /// CSN of transaction that has created this object
    std::atomic<CSN> creation_csn = Tx::UnknownCSN;
    /// CSN of transaction that has removed this object
    std::atomic<CSN> removal_csn = Tx::UnknownCSN;

    LoggerPtr log;
};

DataTypePtr getTransactionIDDataType();

using VersionMetadataPtr = std::unique_ptr<VersionMetadata>;

}
