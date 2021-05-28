#pragma once

#include <ext/shared_ptr_helper.h>

#include <Storages/IStorage.h>


namespace DB
{

class Set;
using SetPtr = std::shared_ptr<Set>;


/** Common part of StorageSet and StorageJoin.
  */
class StorageSetOrJoinBase : public IStorage
{
    friend class SetOrJoinBlockOutputStream;

public:
    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;

    Strings getDataPaths() const override { return {path}; }

protected:
    StorageSetOrJoinBase(
        const String & relative_path_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const Context & context_);

    String base_path;
    String path;

    std::atomic<UInt64> increment = 0;    /// For the backup file names.

    /// Restore from backup.
    void restore();

private:
    void restoreFromFile(const String & file_path);

    /// Insert the block into the state.
    virtual void insertBlock(const Block & block) = 0;
    /// Call after all blocks were inserted.
    virtual void finishInsert() = 0;
    virtual size_t getSize() const = 0;
};


/** Lets you save the set for later use on the right side of the IN statement.
  * When inserted into a table, the data will be inserted into the set,
  *  and also written to a file-backup, for recovery after a restart.
  * Reading from the table is not possible directly - it is possible to specify only the right part of the IN statement.
  */
class StorageSet final : public ext::shared_ptr_helper<StorageSet>, public StorageSetOrJoinBase
{
friend struct ext::shared_ptr_helper<StorageSet>;

public:
    String getName() const override { return "Set"; }

    /// Access the insides.
    SetPtr & getSet() { return set; }

    void truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, const Context &, TableExclusiveLockHolder &) override;

private:
    SetPtr set;

    void insertBlock(const Block & block) override;
    void finishInsert() override;
    size_t getSize() const override;

protected:
    StorageSet(
        const String & relative_path_,
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const Context & context_);
};

}
