#pragma once

#include <base/types.h>
#include <Common/TransactionID.h>

namespace DB
{
struct VersionInfo
{
    /// Transaction ID that created this object.
    TransactionID creation_tid = Tx::EmptyTID;
    /// Transaction ID that removed or is removing this object
    TransactionID removal_tid = Tx::EmptyTID;
    /// Commit sequence number when creation was committed (UnknownCSN if creation not committed yet).
    CSN creation_csn = Tx::UnknownCSN;
    /// Commit sequence number when removal was committed (UnknownCSN if removal not committed yet).
    CSN removal_csn = Tx::UnknownCSN;
    /// Monotonically increasing version generated when storing `VersionInfo` to storage.
    /// Used to detect concurrent modifications and prevent lost updates.
    Int32 storing_version{UNSTORED_VERSION};

    /// Initial value for storing_version indicating that the metadata has not been persisted to storage yet.
    static constexpr Int32 UNSTORED_VERSION = -1;

    /// Hard upper bound on a serialized txn_version.txt. Real metadata is a few dozen bytes; a larger
    /// file is corrupt. Used to cap reads without trusting a possibly-corrupt on-disk file size.
    static constexpr size_t MAX_METADATA_SIZE = 4096;

    /// Reads at most MAX_METADATA_SIZE + 1 bytes from `buf`. A returned length of MAX_METADATA_SIZE + 1
    /// proves (from bytes actually read) that the content exceeds the cap.
    static String readMetadataString(ReadBuffer & buf);

    /// Checks if the object is visible to a transaction with `snapshot_version` and `current_tid`.
    /// Returns true if visible (created before snapshot and not removed, or being created by current transaction).
    /// Returns false if not visible (removed before snapshot, created after snapshot, or being removed by current transaction).
    /// Returns std::nullopt if visibility cannot be determined because the part is involved in an ongoing transaction
    /// that is neither creating nor removing it from the perspective of `current_tid`.
    std::optional<bool> isVisible(CSN snapshot_version, TransactionID current_tid) const;
    /// Returns true if the object creation is finalized (either non-transactional or committed).
    bool isCreated() const;
    /// Returns true if the object removal is finalized (either non-transactional, committed, or creation rolled back).
    bool isRemoved() const;
    /// Returns true if the object was created, is being removed, or was removed by a transaction (not non-transactionally).
    bool wasInvolvedInTransaction() const;

    String toString(bool one_line) const;
    void fromString(const String & content, bool one_line);
    void readFromBuffer(ReadBuffer & buf, bool one_line);
    void writeToBuffer(WriteBuffer & buf, bool one_line) const;

    bool operator==(const VersionInfo & other) const noexcept = default;

private:
    void readFromMultiLineBuffer(ReadBuffer & buf);
};
}
