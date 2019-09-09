#pragma once

#include <mutex>

#include <ext/shared_ptr_helper.h>

#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

/** Implements storage in the RAM.
  * Suitable for temporary data.
  * It does not support keys.
  * Data is stored as a set of blocks and is not stored anywhere else.
  */
class StorageMemory : public ext::shared_ptr_helper<StorageMemory>, public IStorage
{
friend class MemoryBlockInputStream;
friend class MemoryBlockOutputStream;
friend struct ext::shared_ptr_helper<StorageMemory>;

public:
    String getName() const override { return "Memory"; }
    String getTableName() const override { return table_name; }
    String getDatabaseName() const override { return database_name; }

    size_t getSize() const { return data.size(); }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Context & context) override;

    void drop(TableStructureWriteLockHolder &) override;

    void truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &) override;

    void rename(const String & /*new_path_to_db*/, const String & new_database_name, const String & new_table_name, TableStructureWriteLockHolder &) override
    {
        table_name = new_table_name;
        database_name = new_database_name;
    }

private:
    String database_name;
    String table_name;

    /// The data itself. `list` - so that when inserted to the end, the existing iterators are not invalidated.
    BlocksList data;

    std::mutex mutex;

protected:
    StorageMemory(String database_name_, String table_name_, ColumnsDescription columns_description_, ConstraintsDescription constraints_);
};

}
