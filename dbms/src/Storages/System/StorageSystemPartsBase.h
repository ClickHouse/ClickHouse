#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

class Context;


/** Implements system table 'parts' which allows to get information about data parts for tables of MergeTree family.
  */
class StorageSystemPartsBase : public IStorage
{
public:
    std::string getTableName() const override { return name; }

    NameAndTypePair getColumn(const String & column_name) const override;

    bool hasColumn(const String & column_name) const override;

    BlockInputStreams read(
            const Names & column_names,
            const SelectQueryInfo & query_info,
            const Context & context,
            QueryProcessingStage::Enum & processed_stage,
            size_t max_block_size,
            unsigned num_streams) override;

    struct StoragesInfo
    {
        StoragePtr storage;
        TableStructureReadLockPtr table_lock;

        String database;
        String table;
        String engine;

        MergeTreeData * data;
        MergeTreeData::DataPartStateVector all_parts_state;
        MergeTreeData::DataPartsVector all_parts;

        operator bool() const { return storage != nullptr; }
    };

private:
    const std::string name;


    bool hasStateColumn(const Names & column_names);

protected:
    StorageSystemPartsBase(std::string name_, NamesAndTypesList && columns_);

    virtual void processNextStorage(MutableColumns & columns, const StoragesInfo & info, bool has_state_column) = 0;
};

}
