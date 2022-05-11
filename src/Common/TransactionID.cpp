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
    assertChar(')', buf);
    return tid;
}

}
