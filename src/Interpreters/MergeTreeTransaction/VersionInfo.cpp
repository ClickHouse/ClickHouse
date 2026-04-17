#include <optional>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/MergeTreeTransaction/VersionInfo.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <fmt/format.h>

namespace DB
{

static constexpr char STORING_VERSION_STR[] = "storing_version: ";
static constexpr char CREATION_TID_STR[] = "creation_tid: ";
static constexpr char CREATION_CSN_STR[] = "creation_csn: ";
static constexpr char REMOVAL_TID_STR[] = "removal_tid: ";
static constexpr char REMOVAL_CSN_STR[] = "removal_csn: ";

static constexpr char ONE_LINE_SEPARATOR[] = "|";
static constexpr char MULTI_LINE_SEPARATOR[] = "\n";

String VersionInfo::toString(bool one_line) const
{
    WriteBufferFromOwnString buf;
    writeToBuffer(buf, one_line);
    return buf.str();
}

void VersionInfo::fromString(const String & content, bool one_line)
{
    if (one_line)
    {
        String line = content;
        std::replace(line.begin(), line.end(), ONE_LINE_SEPARATOR[0], '\n');
        ReadBufferFromString str_buf(line);
        readFromMultiLineBuffer(str_buf);
    }
    else
    {
        ReadBufferFromString str_buf(content);
        readFromMultiLineBuffer(str_buf);
    }
}

void VersionInfo::readFromBuffer(ReadBuffer & buf, bool one_line)
{
    if (one_line)
    {
        String line;
        readStringUntilNewlineInto(line, buf);
        fromString(line, true);
    }
    else
    {
        readFromMultiLineBuffer(buf);
    }
}

bool VersionInfo::wasInvolvedInTransaction() const
{
    bool created_by_transaction = !creation_tid.isNonTransactional();
    bool removing_by_transaction = removal_csn == 0 && !removal_tid.isNonTransactional() && !removal_tid.isEmpty();
    bool removed_by_transaction = removal_csn != Tx::NonTransactionalCSN && removal_csn != 0;
    return created_by_transaction || removed_by_transaction || removing_by_transaction;
}

template <typename T>
static void writeIntegerToBuffer(const char * separator, WriteBuffer & buf, const char * tag, T val)
{
    writeCString(separator, buf);
    writeCString(tag, buf);
    writeText(val, buf);
}

static void writeTIDToBuffer(const char * separator, WriteBuffer & buf, const char * tag, const TransactionID & tid)
{
    writeCString(separator, buf);
    writeCString(tag, buf);
    TransactionID::write(tid, buf);
}

void VersionInfo::writeToBuffer(WriteBuffer & buf, bool one_line) const
{
    const auto & separator = one_line ? ONE_LINE_SEPARATOR : MULTI_LINE_SEPARATOR;
    writeCString("version: 1", buf);
    writeIntegerToBuffer(separator, buf, STORING_VERSION_STR, storing_version);
    writeTIDToBuffer(separator, buf, CREATION_TID_STR, creation_tid);
    writeIntegerToBuffer(separator, buf, CREATION_CSN_STR, creation_csn);
    writeTIDToBuffer(separator, buf, REMOVAL_TID_STR, removal_tid);
    writeIntegerToBuffer(separator, buf, REMOVAL_CSN_STR, removal_csn);
}

void VersionInfo::readFromMultiLineBuffer(ReadBuffer & buf)
{
    Int32 current_storing_version{0};
    TransactionID current_creation_tid = Tx::EmptyTID;
    CSN current_creation_csn = Tx::UnknownCSN;
    TransactionID current_removal_tid = Tx::EmptyTID;
    CSN current_removal_csn = Tx::UnknownCSN;

    assertString("version: 1", buf);

    assertString(String("\n") + STORING_VERSION_STR, buf);
    readText(current_storing_version, buf);

    assertString(String("\n") + CREATION_TID_STR, buf);
    current_creation_tid = TransactionID::read(buf);

    assertString(String("\n") + CREATION_CSN_STR, buf);
    readText(current_creation_csn, buf);

    assertString(String("\n") + REMOVAL_TID_STR, buf);
    current_removal_tid = TransactionID::read(buf);

    assertString(String("\n") + REMOVAL_CSN_STR, buf);
    readText(current_removal_csn, buf);

    storing_version = current_storing_version;
    creation_tid = current_creation_tid;
    removal_tid = current_removal_tid;
    creation_csn = current_creation_csn;
    removal_csn = current_removal_csn;
}

std::optional<bool> VersionInfo::isVisible(CSN snapshot_version, TransactionID current_tid) const
{
    chassert(!creation_tid.isEmpty());

    if (removal_tid.isNonTransactional())
        return false;

    if (current_tid.isNonTransactional())
        return removal_tid.isEmpty();

    [[maybe_unused]]
    bool had_creation_csn
        = creation_csn != Tx::UnknownCSN;
    [[maybe_unused]] bool had_removal_csn = removal_csn != Tx::UnknownCSN;

    chassert(!had_removal_csn || had_creation_csn);
    chassert(creation_csn == Tx::UnknownCSN || creation_csn == Tx::NonTransactionalCSN || Tx::MaxReservedCSN < creation_csn);
    chassert(removal_csn == Tx::UnknownCSN || removal_csn == Tx::NonTransactionalCSN || Tx::MaxReservedCSN < removal_csn);

    /// Special snapshot for introspection purposes
    if (unlikely(snapshot_version == Tx::EverythingVisibleCSN))
        return true;

    /// Fast path:

    /// Part is definitely not visible if:
    /// - creation was committed after we took the snapshot
    /// - removal was committed before we took the snapshot
    /// - current transaction is removing it
    if (creation_csn && snapshot_version < creation_csn)
        return false;
    if (removal_csn && removal_csn <= snapshot_version)
        return false;
    if (!current_tid.isEmpty() && removal_tid == current_tid)
        return false;

    /// Otherwise, part is definitely visible if:
    /// - creation was committed before we took the snapshot and nobody tried to remove the part
    /// - creation was committed before and removal was committed after
    /// - current transaction is creating it
    if (creation_csn && creation_csn <= snapshot_version && removal_tid.isEmpty())
        return true;
    if (creation_csn && creation_csn <= snapshot_version && removal_csn && snapshot_version < removal_csn)
        return true;
    if (!current_tid.isEmpty() && creation_tid == current_tid)
        return true;

    /// Data part has creation_tid/removal_tid, but does not have creation_csn/removal_csn.
    /// It means that some transaction is creating/removing the part right now or has done it recently
    /// and we don't know if it was already committed or not.
    chassert(!had_creation_csn || !had_removal_csn);
    chassert(current_tid.isEmpty() || (creation_tid != current_tid && removal_tid != current_tid));

    return std::nullopt;
}

bool VersionInfo::isCreated() const
{
    return creation_tid == Tx::NonTransactionalTID || (creation_csn > Tx::MaxReservedCSN && creation_csn != Tx::RolledBackCSN);
}

bool VersionInfo::isRemoved() const
{
    return removal_tid == Tx::NonTransactionalTID || creation_csn == Tx::RolledBackCSN || removal_csn != Tx::UnknownCSN;
};
}
