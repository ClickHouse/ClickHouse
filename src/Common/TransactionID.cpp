#include <Common/TransactionID.h>
#include <Common/SipHash.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

TIDHash TransactionID::getHash() const
{
    SipHash hash;
    hash.update(start_csn);
    hash.update(local_tid);
    hash.update(host_id);
    hash.update(session_node_version);
    return hash.get64();
}


void TransactionID::write(const TransactionID & tid, WriteBuffer & buf)
{
    writeChar('(', buf);
    writeText(tid.start_csn, buf);
    writeCString(", ", buf);
    writeText(tid.local_tid, buf);
    writeCString(", ", buf);
    writeText(tid.host_id, buf);
    /// Emit the 4th field only when set, so the common (never-restarted) case stays byte-identical
    /// to the old 3-field form. Once a session version is assigned it is always written.
    if (tid.session_node_version != 0)
    {
        writeCString(", ", buf);
        writeText(tid.session_node_version, buf);
    }
    writeChar(')', buf);
}

TransactionID TransactionID::read(ReadBuffer & buf)
{
    TransactionID tid = Tx::EmptyTID;
    assertChar('(', buf);
    readText(tid.start_csn, buf);
    assertString(", ", buf);
    readText(tid.local_tid, buf);
    assertString(", ", buf);
    readText(tid.host_id, buf);
    /// session_node_version is optional for backward compatibility with pre-feature TIDs.
    /// Old format: (start_csn, local_tid, host_id)
    /// New format: (start_csn, local_tid, host_id, session_node_version)
    if (!buf.eof() && *buf.position() == ',')
    {
        assertString(", ", buf);
        readText(tid.session_node_version, buf);
    }
    assertChar(')', buf);
    return tid;
}

}
