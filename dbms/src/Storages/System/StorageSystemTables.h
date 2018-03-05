#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements the system table `tables`, which allows you to get information about all tables.
  */
class StorageSystemTables : public ext::shared_ptr_helper<StorageSystemTables>, public IStorage
{
public:
    std::string getName() const override { return "SystemTables"; }
    std::string getTableName() const override { return name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool hasColumn(const String & column_name) const override;

    NameAndTypePair getColumn(const String & column_name) const override;

private:
    const std::string name;

    ColumnsWithTypeAndName virtual_columns;

protected:
    StorageSystemTables(const std::string & name_);
};

}
