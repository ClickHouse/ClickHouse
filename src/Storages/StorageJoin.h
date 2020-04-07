#pragma once

#include <ext/shared_ptr_helper.h>

#include <Storages/StorageSet.h>
#include <Parsers/ASTTablesInSelectQuery.h>


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
class StorageJoin final : public ext::shared_ptr_helper<StorageJoin>, public StorageSetOrJoinBase
{
    friend struct ext::shared_ptr_helper<StorageJoin>;
public:
    String getName() const override { return "Join"; }

    void truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &) override;

    /// Access the innards.
    HashJoinPtr & getJoin() { return join; }
    HashJoinPtr getJoin(std::shared_ptr<TableJoin> analyzed_join) const;

    /// Verify that the data structure is suitable for implementing this type of JOIN.
    void assertCompatible(ASTTableJoin::Kind kind_, ASTTableJoin::Strictness strictness_) const;

    Pipes read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    Block sample_block;
    const Names key_names;
    bool use_nulls;
    SizeLimits limits;
    ASTTableJoin::Kind kind;                    /// LEFT | INNER ...
    ASTTableJoin::Strictness strictness;        /// ANY | ALL
    bool overwrite;

    std::shared_ptr<TableJoin> table_join;
    HashJoinPtr join;

    void insertBlock(const Block & block) override;
    void finishInsert() override {}
    size_t getSize() const override;

protected:
    StorageJoin(
        const String & relative_path_,
        const StorageID & table_id_,
        const Names & key_names_,
        bool use_nulls_,
        SizeLimits limits_,
        ASTTableJoin::Kind kind_, ASTTableJoin::Strictness strictness_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        bool overwrite,
        const Context & context_);
};

}
