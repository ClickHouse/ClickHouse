#pragma once

#include <mutex>

#include <ext/shared_ptr_helper.h>

#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/IStorage.h>


namespace DB
{
/** Implements storage in the RAM.
  * Suitable for temporary data.
  * It does not support keys.
  * Data is stored as a set of blocks and is not stored anywhere else.
  */
class StorageWindow : public ext::shared_ptr_helper<StorageWindow>, public IStorage
{
    friend class StorageWindowWrapper;
    friend class WindowBlockInputStream;
    friend class WindowBlockOutputStream;
    friend struct ext::shared_ptr_helper<StorageWindow>;

public:
    String getName() const override { return "Window"; }
    String getTableName() const override { return table_name; }
    String getDatabaseName() const override { return database_name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Context & context) override;

    void rename(
        const String & /*new_path_to_db*/,
        const String & new_database_name,
        const String & new_table_name,
        TableStructureWriteLockHolder &) override
    {
        table_name = new_table_name;
        database_name = new_database_name;
    }

private:
    String database_name;
    String table_name;
    String source_table;
    String dest_table;
    UInt64 window_size;
    Context global_context;

    BlocksList data;

protected:
    StorageWindow(
        String database_name_,
        String table_name_,
        ColumnsDescription columns_description_,
        String source_tables_,
        String dest_table_,
        UInt64 window_size_,
        const Context & context_);
};

class StorageWindowWrapper : public ext::shared_ptr_helper<StorageWindowWrapper>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageWindowWrapper>;

public:
    String getName() const override { return "WindowWrapper"; }
    String getTableName() const override { return name; }
    String getDatabaseName() const override { return storage.getDatabaseName(); }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    String name;
    StorageWindow & storage;

protected:
    StorageWindowWrapper(const String & name_, StorageWindow & storage_);
};
}
