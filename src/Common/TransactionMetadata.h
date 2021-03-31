#pragma once
#include <Core/Types.h>
#include <Core/UUID.h>

namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

/// FIXME Transactions: Sequential node numbers in ZooKeeper are Int32, but 31 bit is not enough
using CSN = UInt64;
using Snapshot = CSN;
using LocalTID = UInt64;

struct TransactionID
{
    CSN start_csn = 0;
    LocalTID local_tid = 0;
    UUID host_id = UUIDHelpers::Nil;   /// Depends on #17278, leave it Nil for now.

    static DataTypePtr getDataType();
};

namespace Tx
{

const CSN UnknownCSN = 0;
const CSN PrehistoricCSN = 1;

const LocalTID PrehistoricLocalTID = 1;

const TransactionID EmptyTID = {0, 0, UUIDHelpers::Nil};
const TransactionID PrehistoricTID = {0, PrehistoricLocalTID, UUIDHelpers::Nil};

/// So far, that changes will never become visible
const CSN RolledBackCSN = std::numeric_limits<CSN>::max();

}

}
