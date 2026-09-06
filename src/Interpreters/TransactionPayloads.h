#pragma once

#include <vector>

#include <Interpreters/MergeTreeTransaction.h>
#include <base/types.h>
#include <Common/TransactionID.h>

namespace DB::Tx
{

/// Payloads of the transaction znodes in Keeper. `format_version` is filled by
/// `deserialize`; writers always emit version 1.

/// `/clickhouse/txn/log/csn-<N>`. A header block, then one line per affected table:
///
///   version: 1
///   (42, 7, 3f1b0c9e-0000-0000-0000-000000000001, 5)
///   replica_id: 9c2a77d4-0000-0000-0000-000000000002
///   smt_count: 1
///   6442450944 P=/clickhouse/tables/db/hits A=all_1_1_0 R=all_0_0_0
///
/// A table line is `cross_replica_id`, then P=`zk_path`, A=added parts, R=removed parts,
/// in that order. Each is omitted when empty. Values must not contain a space, comma
/// or newline - the parser splits on those.
struct CSNEntryData
{
    UInt64 format_version = 1;
    TransactionID tid = Tx::EmptyTID;
    UUID replica_id;
    std::vector<MergeTreeTransaction::AffectedSMTTable> smt;

    String serialize() const;
    /// Also reads pre-version-1 bodies, which hold a bare TID and no SMT fan-out.
    static CSNEntryData deserialize(const String & data);
};

/// `/clickhouse/txn/tables/stamp_csn/<cross_replica_id>` - the table's bounded-snapshot stamp.
struct StampData
{
    UInt64 format_version = 1;
    TransactionID tid = Tx::EmptyTID;
    String zk_path;

    String serialize() const;
    static StampData deserialize(const String & data);
};

/// `/clickhouse/txn/tables/processed_csn/<cross_replica_id>`, written by `processPartsUpdate`.
/// `csn` is what the `_tail_ptr` clamp reads; `virtual_parts_version` gates monotonic writes.
struct ProcessedData
{
    UInt64 format_version = 1;
    CSN csn = 0;
    TransactionID tid = Tx::EmptyTID;
    Int64 virtual_parts_version = 0;
    UUID writing_replica_id;

    String serialize() const;
    static ProcessedData deserialize(const String & data);
};

/// Name of a CSN log entry node, `csn-<N>`.
String serializeCSN(CSN csn);
UInt64 deserializeCSN(const String & csn_node_name);

}
