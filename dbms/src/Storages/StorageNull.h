#pragma once

#include <ext/shared_ptr_helper.h>

#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/NullBlockOutputStream.h>


namespace DB
{

/** When writing, does nothing.
  * When reading, returns nothing.
  */
class StorageNull : public ext::shared_ptr_helper<StorageNull>, public IStorage
{
public:
    std::string getName() const override { return "Null"; }
    std::string getTableName() const override { return table_name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo &,
        const Context & /*context*/,
        QueryProcessingStage::Enum /*processing_stage*/,
        size_t,
        unsigned) override
    {
        return { std::make_shared<NullBlockInputStream>(getSampleBlockForColumns(column_names)) };
    }

    BlockOutputStreamPtr write(const ASTPtr &, const Settings &) override
    {
        return std::make_shared<NullBlockOutputStream>(getSampleBlock());
    }

    void rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & new_table_name) override
    {
        table_name = new_table_name;
    }

    void alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context) override;

private:
    String table_name;

protected:
    StorageNull(String table_name_, ColumnsDescription columns_description_)
        : IStorage{std::move(columns_description_)}, table_name(std::move(table_name_))
    {
    }
};

}
