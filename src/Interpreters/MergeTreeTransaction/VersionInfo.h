#pragma once

#include <Common/TransactionID.h>

namespace DB
{
struct VersionInfo
{
    TransactionID creation_tid = Tx::EmptyTID;
    TransactionID removal_tid = Tx::EmptyTID;
    CSN creation_csn = Tx::UnknownCSN;
    CSN removal_csn = Tx::UnknownCSN;
    UInt64 removal_tid_lock{0};

    /**
    * @brief Check if the version info is visible to `snapshot_version` with `current_tid`.
    *
    * @param snapshot_version the snapshot version of the current transaction
    * @param current_tid current TID
    * @return std::optional<bool> return std::nullopt if unable to check. It happens when both following conditions occur:
    *   - Data part has creation_tid/removal_tid, but does not have creation_csn/removal_csn.
    *   - `current_tid` is not the same removal_tid or currently hold the removal lock
    */
    std::optional<bool> isVisible(CSN snapshot_version, TransactionID current_tid) const;
    bool isCreated() const;
    bool isRemoved() const;

    TIDHash getCurrentRemovalTIDHash() const;
    String toString(bool one_line) const;
    void fromString(const String & content, bool one_line);
    void readFromBuffer(ReadBuffer & buf, bool one_line);
    void writeToBuffer(WriteBuffer & buf, bool one_line) const;

    static void writeCreationCSNToBuffer(const char * separator, WriteBuffer & buf, UInt64 creation_csn);
    static void writeRemovalCSNToBuffer(const char * separator, WriteBuffer & buf, UInt64 removal_csn);
    static void writeRemovalTIDToBuffer(const char * separator, WriteBuffer & buf, const TransactionID & removal_tid);

    inline static const char ONE_LINE_SEPARATOR[] = "|";
    inline static const char MULTI_LINE_SEPARATOR[] = "\n";

    bool operator==(const VersionInfo & other) const noexcept = default;

private:
    void readFromMultiLineBuffer(ReadBuffer & buf);
};
}
