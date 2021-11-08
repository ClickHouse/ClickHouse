#include <Common/TransactionID.h>
#include <Common/SipHash.h>

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

}
