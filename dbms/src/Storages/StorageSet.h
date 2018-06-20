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
    String getTableName() const override { return table_name; }

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    String getDataPath() const override { return path; }

protected:
    StorageSetOrJoinBase(
        const String & path_,
        const String & table_name_,
        const ColumnsDescription & columns_);

    String path;
    String table_name;

    UInt64 increment = 0;    /// For the backup file names.

    /// Restore from backup.
    void restore();

private:
    void restoreFromFile(const String & file_path);

    /// Insert the block into the state.
    virtual void insertBlock(const Block & block) = 0;
    virtual size_t getSize() const = 0;
};


/** Lets you save the set for later use on the right side of the IN statement.
  * When inserted into a table, the data will be inserted into the set,
  *  and also written to a file-backup, for recovery after a restart.
  * Reading from the table is not possible directly - it is possible to specify only the right part of the IN statement.
  */
class StorageSet : public ext::shared_ptr_helper<StorageSet>, public StorageSetOrJoinBase
{
friend struct ext::shared_ptr_helper<StorageSet>;

public:
    String getName() const override { return "Set"; }

    /// Access the insides.
    SetPtr & getSet() { return set; }

    void truncate(const ASTPtr &) override;

private:
    SetPtr set;

    void insertBlock(const Block & block) override;
    size_t getSize() const override;

protected:
    StorageSet(
        const String & path_,
        const String & name_,
        const ColumnsDescription & columns_);
};

}
