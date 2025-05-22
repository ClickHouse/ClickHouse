#pragma once

#include <Common/RWLock.h>
#include <Storages/StorageSet.h>
#include <Storages/TableLockHolder.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Interpreters/JoinUtils.h>


namespace DB
{

class TableJoin;
class HashJoin;
using HashJoinPtr = std::shared_ptr<HashJoin>;

/** Allows you save the state for later use on the right side of the JOIN.
  * When inserted into a table, the data will be inserted into the state,
  *  and also written to the backup file, to restore after the restart.
  * Reading from the table is not possible directly - only specifying on the right side of JOIN is possible.
  *
  * When using, JOIN must be of the appropriate type (ANY|ALL LEFT|INNER ...).
  */
class StorageJoin final : public StorageSetOrJoinBase
{
public:
    StorageJoin(
        DiskPtr disk_,
        const String & relative_path_,
        const StorageID & table_id_,
        const Names & key_names_,
        bool use_nulls_,
        SizeLimits limits_,
        JoinKind kind_,
        JoinStrictness strictness_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        bool overwrite,
        bool persistent_);

    String getName() const override { return "Join"; }

    void truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, TableExclusiveLockHolder &) override;

    /// Only delete is supported.
    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;
    void mutate(const MutationCommands & commands, ContextPtr context) override;

    /// Return instance of HashJoin holding lock that protects from insertions to StorageJoin.
    /// HashJoin relies on structure of hash table that's why we need to return it with locked mutex.
    HashJoinPtr getJoinLocked(std::shared_ptr<TableJoin> analyzed_join, ContextPtr context, const Names & required_columns_names) const;

    /// Get result type for function "joinGet(OrNull)"
    DataTypePtr joinGetCheckAndGetReturnType(const DataTypes & data_types, const String & column_name, bool or_null) const;

    /// Execute function "joinGet(OrNull)" on data block.
    /// Takes rwlock for read to prevent parallel StorageJoin updates during processing data block
    /// (but not during processing whole query, it's safe for joinGet that doesn't involve `used_flags` from HashJoin)
    ColumnWithTypeAndName joinGet(const Block & block, const Block & block_with_columns_to_add, ContextPtr context) const;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override;

    bool optimize(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const ASTPtr & /*partition*/,
        bool /*final*/,
        bool /*deduplicate*/,
        const Names & /* deduplicate_by_columns */,
        bool /*cleanup*/,
        ContextPtr /*context*/) override;

    void optimizeUnlocked();

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    std::optional<UInt64> totalRows(const Settings & settings) const override;
    std::optional<UInt64> totalBytes(const Settings & settings) const override;

    Block getRightSampleBlock() const
    {
        auto metadata_snapshot = getInMemoryMetadataPtr();
        Block block = metadata_snapshot->getSampleBlock();
        convertRightBlock(block);
        return block;
    }

    bool useNulls() const { return use_nulls; }

    const Names & getKeyNames() const { return key_names; }

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

private:
    Block sample_block;
    const Names key_names;
    bool use_nulls;
    SizeLimits limits;
    JoinKind kind;                    /// LEFT | INNER ...
    JoinStrictness strictness;        /// ANY | ALL
    bool overwrite;

    std::shared_ptr<TableJoin> table_join;
    HashJoinPtr join;

    /// Protect state for concurrent use in insertFromBlock and joinBlock.
    /// Lock is stored in HashJoin instance during query and blocks concurrent insertions.
    mutable RWLock rwlock = RWLockImpl::create();

    mutable std::mutex mutate_mutex;

    void insertBlock(const Block & block, ContextPtr context) override;
    void finishInsert() override {}
    size_t getSize(ContextPtr context) const override;
    RWLockImpl::LockHolder tryLockTimedWithContext(const RWLock & lock, RWLockImpl::Type type, ContextPtr context) const;
    /// Same as tryLockTimedWithContext, but returns `nullptr` if lock is already acquired by current query.
    static RWLockImpl::LockHolder tryLockForCurrentQueryTimedWithContext(const RWLock & lock, RWLockImpl::Type type, ContextPtr context);

    void convertRightBlock(Block & block) const;
};

}
