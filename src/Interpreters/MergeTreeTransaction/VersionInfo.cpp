#include <optional>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/MergeTreeTransaction/VersionInfo.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

static constexpr char CREATION_TID_STR[] = "creation_tid: ";
static constexpr char CREATION_CSN_STR[] = "creation_csn: ";
static constexpr char REMOVAL_TID_STR[] = "removal_tid:  ";
static constexpr char REMOVAL_TID_LOCK_STR[] = "removal_lock: ";
static constexpr char REMOVAL_CSN_STR[] = "removal_csn:  ";

namespace ErrorCodes
{
extern const int CANNOT_PARSE_TEXT;
}

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

void VersionInfo::writeCreationCSNToBuffer(const char * separator, WriteBuffer & buf, UInt64 creation_csn)
{
    writeCString(separator, buf);
    writeCString(CREATION_CSN_STR, buf);
    writeText(creation_csn, buf);
}

void VersionInfo::writeRemovalCSNToBuffer(const char * separator, WriteBuffer & buf, UInt64 removal_csn)
{
    writeCString(separator, buf);
    writeCString(REMOVAL_CSN_STR, buf);
    writeText(removal_csn, buf);
}

void VersionInfo::writeRemovalTIDToBuffer(const char * separator, WriteBuffer & buf, const TransactionID & removal_tid)
{
    writeCString(separator, buf);
    writeCString(REMOVAL_TID_STR, buf);
    TransactionID::write(removal_tid, buf);
}


void VersionInfo::writeToBuffer(WriteBuffer & buf, bool one_line) const
{
    const auto & separator = one_line ? ONE_LINE_SEPARATOR : MULTI_LINE_SEPARATOR;
    writeCString("version: 1", buf);
    writeCString(separator, buf);
    writeCString(CREATION_TID_STR, buf);

    TransactionID::write(creation_tid, buf);
    if (creation_csn)
        writeCreationCSNToBuffer(separator, buf, creation_csn);

    if (!removal_tid.isEmpty())
        writeRemovalTIDToBuffer(separator, buf, removal_tid);

    if (removal_tid_lock)
    {
        writeCString(separator, buf);
        writeCString(REMOVAL_TID_LOCK_STR, buf);
        writeText(removal_tid_lock, buf);
    }

    if (removal_csn)
    {
        chassert(!removal_tid.isEmpty());
        writeRemovalCSNToBuffer(separator, buf, removal_csn);
    }
}

void VersionInfo::readFromMultiLineBuffer(ReadBuffer & buf)
{
    constexpr size_t size = sizeof(CREATION_TID_STR) - 1;
    static_assert(sizeof(CREATION_CSN_STR) - 1 == size);
    static_assert(sizeof(REMOVAL_TID_STR) - 1 == size);
    static_assert(sizeof(REMOVAL_CSN_STR) - 1 == size);

    assertString("version: 1", buf);
    assertString(String("\n") + CREATION_TID_STR, buf);
    creation_tid = TransactionID::read(buf);
    if (buf.eof())
        return;

    String name;
    name.resize(size);

    auto read_uint64 = [&]()
    {
        UInt64 val;
        readText(val, buf);
        return val;
    };

    while (!buf.eof())
    {
        assertChar('\n', buf);
        buf.readStrict(name.data(), size);

        if (name == CREATION_CSN_STR)
        {
            auto new_val = read_uint64();
            chassert(!creation_csn || (creation_csn == new_val && creation_csn == Tx::PrehistoricCSN));
            creation_csn = new_val;
        }
        else if (name == REMOVAL_TID_STR)
        {
            /// NOTE Metadata file may actually contain multiple creation TIDs, we need the last one.
            removal_tid = TransactionID::read(buf);
        }
        else if (name == REMOVAL_TID_LOCK_STR)
        {
            removal_tid_lock = read_uint64();
        }
        else if (name == REMOVAL_CSN_STR)
        {
            auto reading_csn = read_uint64();
            if (removal_tid.isEmpty())
                throw Exception(
                    ErrorCodes::CANNOT_PARSE_TEXT,
                    "Found removal_csn {} in metadata file, but removal_tid is {}",
                    reading_csn,
                    removal_tid);
            chassert(!removal_csn);
            removal_csn = reading_csn;
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Got unexpected content: {}", name);
        }
    }
}

std::optional<bool> VersionInfo::isVisible(CSN snapshot_version, TransactionID current_tid) const
{
    chassert(!creation_tid.isEmpty());

    if (removal_tid.isPrehistoric())
        return false;

    TIDHash current_removal_tid_hash = getCurrentRemovalTIDHash();

    [[maybe_unused]] bool had_creation_csn = creation_csn != Tx::UnknownCSN;
    [[maybe_unused]] bool had_removal_tid_lock = current_removal_tid_hash;
    [[maybe_unused]] bool had_removal_csn = removal_csn != Tx::UnknownCSN;

    chassert(!had_removal_csn || had_creation_csn);
    chassert(creation_csn == Tx::UnknownCSN || creation_csn == Tx::PrehistoricCSN || Tx::MaxReservedCSN < creation_csn);
    chassert(removal_csn == Tx::UnknownCSN || removal_csn == Tx::PrehistoricCSN || Tx::MaxReservedCSN < removal_csn);

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
    if (!current_tid.isEmpty() && current_removal_tid_hash && current_removal_tid_hash == current_tid.getHash())
        return false;

    /// Otherwise, part is definitely visible if:
    /// - creation was committed before we took the snapshot and nobody tried to remove the part
    /// - creation was committed before and removal was committed after
    /// - current transaction is creating it
    if (creation_csn && creation_csn <= snapshot_version && !current_removal_tid_hash)
        return true;
    if (creation_csn && creation_csn <= snapshot_version && removal_csn && snapshot_version < removal_csn)
        return true;
    if (!current_tid.isEmpty() && creation_tid == current_tid)
        return true;

    /// Data part has creation_tid/removal_tid, but does not have creation_csn/removal_csn.
    /// It means that some transaction is creating/removing the part right now or has done it recently
    /// and we don't know if it was already committed or not.
    chassert(!had_creation_csn || (had_removal_tid_lock && !had_removal_csn));
    chassert(current_tid.isEmpty() || (creation_tid != current_tid && current_removal_tid_hash != current_tid.getHash()));

    return std::nullopt;
}

bool VersionInfo::isCreated() const
{
    return creation_tid == Tx::PrehistoricTID || (creation_csn > Tx::MaxReservedCSN && creation_csn != Tx::RolledBackCSN);
}

bool VersionInfo::isRemoved() const
{
    return removal_tid == Tx::PrehistoricTID || creation_csn == Tx::RolledBackCSN || removal_csn != Tx::UnknownCSN;
};

TIDHash VersionInfo::getCurrentRemovalTIDHash() const
{
    // Check if the object is locked by any transaction first
    TIDHash current_removal_tid_hash = removal_tid_lock;
    if (!removal_tid_lock && !removal_tid.isEmpty())
    {
        // The object might be unlocked, check removal_tid
        current_removal_tid_hash = removal_tid.getHash();
    }

    return current_removal_tid_hash;
}
}
