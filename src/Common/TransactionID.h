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

/// This macro is useful for places where a pointer to current transaction should be passed,
/// but transactions are not supported yet (e.g. when calling MergeTreeData's methods from StorageReplicatedMergeTree)
/// or transaction object is not needed and not passed intentionally.
#ifndef NO_TRANSACTION_PTR
#define NO_TRANSACTION_PTR std::shared_ptr<MergeTreeTransaction>(nullptr)
#define NO_TRANSACTION_RAW static_cast<MergeTreeTransaction *>(nullptr)
#endif

/// Commit Sequence Number
using CSN = UInt64;
/// Local part of TransactionID
using LocalTID = UInt64;
/// Hash of TransactionID that fits into 64-bit atomic
using TIDHash = UInt64;

namespace Tx
{
    /// For transactions that are probably not committed (yet)
    const CSN UnknownCSN = 0;
    /// For changes were made without creating a transaction
    const CSN PrehistoricCSN = 1;
    /// Special reserved values
    const CSN CommittingCSN = 2;
    const CSN EverythingVisibleCSN = 3;
    const CSN MaxReservedCSN = 32;

    /// So far, that changes will never become visible
    const CSN RolledBackCSN = std::numeric_limits<CSN>::max();

    const LocalTID PrehistoricLocalTID = 1;
    const LocalTID DummyLocalTID = 2;
    const LocalTID MaxReservedLocalTID = 32;
}

struct TransactionID
{
    /// Global sequential number, the newest commit timestamp the we saw when this transaction began
    CSN start_csn = 0;
    /// Local sequential that is unique for each transaction started by this host within specific start_csn
    LocalTID local_tid = 0;
    /// UUID of host that has started this transaction
    UUID host_id = UUIDHelpers::Nil;

    /// NOTE Maybe we could just generate UUIDv4 for each transaction, but it would be harder to debug.
    /// Partial order is defined for this TransactionID structure:
    /// (tid1.start_csn <= tid2.start_csn)    <==>    (tid1 <= tid2)
    /// (tid1.start_csn == tid2.start_csn && tid1.host_id == tid2.host_id && tid1.local_tid < tid2.local_tid)    ==>    (tid1 < tid2)
    /// If two transaction have the same start_csn, but were started by different hosts, then order is undefined.

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


    static void write(const TransactionID & tid, WriteBuffer & buf);
    static TransactionID read(ReadBuffer & buf);
};

namespace Tx
{
    const TransactionID EmptyTID = {0, 0, UUIDHelpers::Nil};
    const TransactionID PrehistoricTID = {PrehistoricCSN, PrehistoricLocalTID, UUIDHelpers::Nil};
    const TransactionID DummyTID = {PrehistoricCSN, DummyLocalTID, UUIDHelpers::Nil};
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
