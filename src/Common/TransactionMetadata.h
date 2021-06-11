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

struct TransactionID
{
    CSN start_csn = 0;
    LocalTID local_tid = 0;
    UUID host_id = UUIDHelpers::Nil;   /// Depends on #17278, leave it Nil for now.

    static DataTypePtr getDataType();

    bool operator == (const TransactionID & rhs) const
    {
        return start_csn == rhs.start_csn && local_tid == rhs.local_tid && host_id == rhs.host_id;
    }

    bool operator != (const TransactionID & rhs) const
    {
        return !(*this == rhs);
    }

    operator bool() const
    {
        assert(local_tid || (start_csn == 0 && host_id == UUIDHelpers::Nil));
        return local_tid;
    }

    TIDHash getHash() const;
};

namespace Tx
{

const CSN UnknownCSN = 0;
const CSN PrehistoricCSN = 1;

const LocalTID PrehistoricLocalTID = 1;

const TransactionID EmptyTID = {0, 0, UUIDHelpers::Nil};
const TransactionID PrehistoricTID = {PrehistoricCSN, PrehistoricLocalTID, UUIDHelpers::Nil};

/// So far, that changes will never become visible
const CSN RolledBackCSN = std::numeric_limits<CSN>::max();

}

struct VersionMetadata
{
    const TransactionID mintid = Tx::EmptyTID;
    TransactionID maxtid = Tx::EmptyTID;

    std::atomic<TIDHash> maxtid_lock = 0;

    std::atomic<CSN> mincsn = Tx::UnknownCSN;
    std::atomic<CSN> maxcsn = Tx::UnknownCSN;

    bool isVisible(const MergeTreeTransaction & txn);
    bool isVisible(Snapshot snapshot_version, TransactionID current_tid = Tx::EmptyTID);

    TransactionID getMinTID() const { return mintid; }
    TransactionID getMaxTID() const;

    void lockMaxTID(const TransactionID & tid, const String & error_context = {});
    void unlockMaxTID(const TransactionID & tid);

    bool isMaxTIDLocked() const;

    /// It can be called only from MergeTreeTransaction or on server startup
    void setMinTID(const TransactionID & tid);

    bool canBeRemoved(Snapshot oldest_snapshot_version);
};

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
