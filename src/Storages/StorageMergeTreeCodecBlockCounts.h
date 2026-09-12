#pragma once

#include <Interpreters/StorageID.h>
#include <Storages/IStorage.h>

namespace DB
{

/// Backing storage for `mergeTreeCodecBlockCounts(database, table)`. One row per (part, column, substream).
/// Counts compressed blocks per codec by reading each stream's `.bin` header. Selecting `part_name`/`column`/`substream` is metadata-only.
class StorageMergeTreeCodecBlockCounts final : public IStorage
{
public:
    /// Holds only the name of the source table. It is resolved and checked on every read, under the context of the
    /// user who reads, because the storage may be created under the global context: `CREATE TABLE ... AS
    /// mergeTreeCodecBlockCounts(...)` stores the function and materialises it lazily on the first read of the created
    /// table. Resolving the source there would let a user who may read the created table but not the source tell
    /// a hidden source from a missing one, and learn its engine once it is recreated under the same name.
    StorageMergeTreeCodecBlockCounts(const StorageID & table_id_, StorageID source_table_id_, const ColumnsDescription & columns_);

    std::string getName() const override { return "MergeTreeCodecBlockCounts"; }

    /// Every column of this function is derived from the source table's data, so reading any of them requires
    /// `SELECT` on all of the source table's columns. Called both when the function's structure is resolved and
    /// when it is read, so that resolving the structure cannot reveal anything about a table the user cannot select from.
    static void checkSourceTableAccess(const StoragePtr & source_table, const ContextPtr & context);

    /// Resolves the source table for `context`'s user, in the order that discloses nothing the user may not learn:
    /// `SHOW TABLES` on the name, before the catalog is consulted, so that an inaccessible table and a missing one
    /// answer alike; then `SELECT` on every column, before the engine is examined, so that a user without it cannot
    /// learn the engine from the `BAD_ARGUMENTS` that rejects a table that is not a `MergeTree`.
    static StoragePtr resolveSourceTable(const StorageID & source_table_id, const ContextPtr & context);

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    StorageID source_table_id;
};

}
