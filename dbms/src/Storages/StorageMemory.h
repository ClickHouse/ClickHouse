#pragma once

#include <mutex>

#include <ext/shared_ptr_helper.hpp>

#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

class StorageMemory;


/** Implements storage in the RAM.
  * Suitable for temporary data.
  * It does not support keys.
  * Data is stored as a set of blocks and is not stored anywhere else.
  */
class StorageMemory : private ext::shared_ptr_helper<StorageMemory>, public IStorage
{
friend class ext::shared_ptr_helper<StorageMemory>;
friend class MemoryBlockInputStream;
friend class MemoryBlockOutputStream;

public:
    static StoragePtr create(
        const std::string & name_,
        NamesAndTypesListPtr columns_);

    static StoragePtr create(
        const std::string & name_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_);

    std::string getName() const override { return "Memory"; }
    std::string getTableName() const override { return name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

    size_t getSize() const { return data.size(); }

    BlockInputStreams read(
        const Names & column_names,
        ASTPtr query,
        const Context & context,
        const Settings & settings,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size = DEFAULT_BLOCK_SIZE,
        unsigned threads = 1) override;

    BlockOutputStreamPtr write(ASTPtr query, const Settings & settings) override;

    void drop() override;
    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override { name = new_table_name; }

private:
    String name;
    NamesAndTypesListPtr columns;

    /// The data itself. `list` - so that when inserted to the end, the existing iterators are not invalidated.
    BlocksList data;

    std::mutex mutex;

    StorageMemory(
        const std::string & name_,
        NamesAndTypesListPtr columns_);

    StorageMemory(
        const std::string & name_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_);
};

}
