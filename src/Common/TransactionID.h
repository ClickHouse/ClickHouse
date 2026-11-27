#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <IO/WriteHelpers.h>
#include <fmt/format.h>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
class MergeTreeTransaction;
class ReadBuffer;
class WriteBuffer;

/// This macro is useful for places where a pointer to current transaction should be passed,
/// but transactions are not supported yet (e.g. when calling MergeTreeData's methods from StorageReplicatedMergeTree)
/// or transaction object is not needed and not passed intentionally.
#ifndef NO_TRANSACTION_PTR
#define NO_TRANSACTION_PTR std::shared_ptr<MergeTreeTransaction>(nullptr)
#define NO_TRANSACTION_RAW static_cast<MergeTreeTransaction *>(nullptr) /// NOLINT(bugprone-macro-parentheses)
#endif

/// Commit Sequence Number
using CSN = UInt64;
/// Local part of TransactionID
using LocalTID = UInt64;
/// Hash of TransactionID that fits into 64-bit atomic
using TIDHash = UInt64;
/// Identifies a background operation (merge/mutation) of a transaction over its own uncommitted
/// parts, distinguishing that operation's part-locks and marker from the transaction's foreground
/// work and from its sibling operations. It is carried alongside the `TransactionID` (in lock
/// content and the marker), NOT encoded into it; the operation's part version metadata still uses
/// the plain transaction TID. `0` is the transaction itself (`Tx::MainJobId`).
using JobId = UInt32;

namespace Tx
{
    const JobId MainJobId = 0;
}

namespace Tx
{
    /// For transactions that are probably not committed (yet)
    const CSN UnknownCSN = 0;
    /// For changes made without creating a transaction.
    const CSN NonTransactionalCSN = 1;
    /// Special reserved values
    const CSN CommittingCSN = 2;
    const CSN EverythingVisibleCSN = 3;
    const CSN MaxReservedCSN = 32;

    /// So far, that changes will never become visible
    const CSN RolledBackCSN = std::numeric_limits<CSN>::max();

    /// Maximum possible CSN for committed transactions (used for visibility checks)
    const CSN MaxCommittedCSN = RolledBackCSN - 1;

    const LocalTID NonTransactionalLocalTID = 1;
    const LocalTID DummyLocalTID = 2;
    const LocalTID MaxReservedLocalTID = 32;

    /// True for a CSN of a committed transaction: past the reserved range and not the rolled-back marker.
    inline bool isCommittedCSN(CSN csn)
    {
        return csn > MaxReservedCSN && csn != RolledBackCSN;
    }
}

struct TransactionID
{
    /// Global sequential number, the newest commit timestamp we had seen when this transaction began
    CSN start_csn = 0;
    /// Local sequential that is unique for each transaction started by this host within specific start_csn
    LocalTID local_tid = 0;
    /// UUID of host that has started this transaction
    UUID host_id = UUIDHelpers::Nil;
    /// Version of the host's `_session` Keeper node when the transaction started.
    /// Detects ghost commits from a session that peers declared dead. Zero means absent
    /// (a bare TID without a session stamp, or a freshly-created `_session`, which also starts at 0).
    ///
    /// `TransactionManager::isTIDInvalid` compares with strict `<`: since both a bare
    /// TID and a fresh session are 0, `0 < 0` is false, so they read as "alive" until the
    /// session is bumped once — the safe direction. Do NOT use `<=`: it would mark TIDs from a
    /// brand-new cluster as dead.
    Int64 session_node_version = 0;

    /// NOTE Maybe we could just generate UUIDv4 for each transaction, but it would be harder to debug.
    /// Partial order is defined for this TransactionID structure:
    /// (tid1.start_csn <= tid2.start_csn)    <==>    (tid1 <= tid2)
    /// (tid1.start_csn == tid2.start_csn && tid1.host_id == tid2.host_id && tid1.local_tid < tid2.local_tid)    ==>    (tid1 < tid2)
    /// If two transaction have the same start_csn, but were started by different hosts, then order is undefined.

    bool operator == (const TransactionID & rhs) const
    {
        return start_csn == rhs.start_csn && local_tid == rhs.local_tid && host_id == rhs.host_id
            && session_node_version == rhs.session_node_version;
    }

    bool operator != (const TransactionID & rhs) const
    {
        return !(*this == rhs);
    }

    TIDHash getHash() const;

    bool isEmpty() const
    {
        chassert((local_tid == 0) == (start_csn == 0 && host_id == UUIDHelpers::Nil));
        return local_tid == 0;
    }

    bool isNonTransactional() const
    {
        /// Non-transactional changes carry `start_csn == NonTransactionalCSN`. We discriminate on
        /// `start_csn`, not `local_tid`, so a non-transactional operation can carry a unique
        /// `local_tid` (a per-operation counter above `MaxReservedLocalTID`) for lock ownership and
        /// future per-TID invalidation, while still being recognized as non-transactional.
        ///
        /// `Tx::DummyTID` (`{NonTransactionalCSN, DummyLocalTID, Nil}`) is excluded: it marks a
        /// rolled-back interrupted `.tmp` part and must NOT count as non-transactional, matching the
        /// behavior `wasInvolvedInTransaction` / `validateInfo` rely on during part loading.
        ///
        /// Invariant: the reserved non-transactional `local_tid` sentinels only ever pair with
        /// `NonTransactionalCSN`; a reserved sentinel with any other `start_csn` is corrupted.
        chassert(
            (local_tid != Tx::NonTransactionalLocalTID && local_tid != Tx::DummyLocalTID)
            || start_csn == Tx::NonTransactionalCSN);
        return start_csn == Tx::NonTransactionalCSN && local_tid != Tx::DummyLocalTID;
    }

    static void write(const TransactionID & tid, WriteBuffer & buf);
    static TransactionID read(ReadBuffer & buf);
};

namespace Tx
{
    /// Designated initialisers so a field reorder or insertion in `TransactionID`
    /// cannot silently shift values across these sentinels.
    const TransactionID EmptyTID = {
        .start_csn = 0,
        .local_tid = 0,
        .host_id = UUIDHelpers::Nil,
        .session_node_version = 0,
    };
    const TransactionID NonTransactionalTID = {
        .start_csn = NonTransactionalCSN,
        .local_tid = NonTransactionalLocalTID,
        .host_id = UUIDHelpers::Nil,
        .session_node_version = 0,
    };
    const TransactionID DummyTID = {
        .start_csn = NonTransactionalCSN,
        .local_tid = DummyLocalTID,
        .host_id = UUIDHelpers::Nil,
        .session_node_version = 0,
    };
}

}

template<>
struct fmt::formatter<DB::TransactionID>
{
    template <typename ParseContext>
    constexpr auto parse(ParseContext & context)
    {
        return context.begin();
    }

    template <typename FormatContext>
    auto format(const DB::TransactionID & tid, FormatContext & context) const
    {
        return fmt::format_to(context.out(), "({}, {}, {}, {})", tid.start_csn, tid.local_tid, tid.host_id, tid.session_node_version);
    }
};
