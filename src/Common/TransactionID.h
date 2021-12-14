#pragma once
#include <Core/Types.h>
#include <Core/UUID.h>
#include <fmt/format.h>
#include <IO/WriteHelpers.h>

namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
class MergeTreeTransaction;

/// FIXME Transactions: Sequential node numbers in ZooKeeper are Int32, but 31 bit is not enough
using CSN = UInt64;
using Snapshot = CSN;
using LocalTID = UInt64;
using TIDHash = UInt64;

namespace Tx
{
    const CSN UnknownCSN = 0;
    const CSN PrehistoricCSN = 1;
    const CSN CommittingCSN = 2; /// TODO do we really need it?
    const CSN MaxReservedCSN = 16;

    const LocalTID PrehistoricLocalTID = 1;
    const LocalTID MaxReservedLocalTID = 16;
}

struct TransactionID
{
    CSN start_csn = 0;
    LocalTID local_tid = 0;
    UUID host_id = UUIDHelpers::Nil;

    bool operator == (const TransactionID & rhs) const
    {
        return start_csn == rhs.start_csn && local_tid == rhs.local_tid && host_id == rhs.host_id;
    }

    bool operator != (const TransactionID & rhs) const
    {
        return !(*this == rhs);
    }

    TIDHash getHash() const;

    bool isEmpty() const
    {
        assert((local_tid == 0) == (start_csn == 0 && host_id == UUIDHelpers::Nil));
        return local_tid == 0;
    }

    bool isPrehistoric() const
    {
        assert((local_tid == Tx::PrehistoricLocalTID) == (start_csn == Tx::PrehistoricCSN));
        return local_tid == Tx::PrehistoricLocalTID;
    }
};

namespace Tx
{
    const TransactionID EmptyTID = {0, 0, UUIDHelpers::Nil};
    const TransactionID PrehistoricTID = {PrehistoricCSN, PrehistoricLocalTID, UUIDHelpers::Nil};

    /// So far, that changes will never become visible
    const CSN RolledBackCSN = std::numeric_limits<CSN>::max();
}

}

template<>
struct fmt::formatter<DB::TransactionID>
{
    template<typename ParseContext>
    constexpr auto parse(ParseContext & context)
    {
        return context.begin();
    }

    template<typename FormatContext>
    auto format(const DB::TransactionID & tid, FormatContext & context)
    {
        return fmt::format_to(context.out(), "({}, {}, {})", tid.start_csn, tid.local_tid, tid.host_id);
    }
};
