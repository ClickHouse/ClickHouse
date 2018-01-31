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

public:
    std::string getName() const override { return "Memory"; }
    std::string getTableName() const override { return name; }

    size_t getSize() const { return data.size(); }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    void drop() override;
    void rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & new_table_name) override { name = new_table_name; }

private:
    String name;

    /// The data itself. `list` - so that when inserted to the end, the existing iterators are not invalidated.
    BlocksList data;

    std::mutex mutex;

protected:
    StorageMemory(
        const std::string & name_,
        const NamesAndTypesList & columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_);
};

}
