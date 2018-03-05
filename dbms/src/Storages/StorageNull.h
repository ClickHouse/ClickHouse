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
    std::string getTableName() const override { return name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo &,
        const Context &,
        QueryProcessingStage::Enum &,
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
        name = new_table_name;
    }

    void alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context) override;

private:
    String name;

protected:
    StorageNull(
        const std::string & name_,
        const NamesAndTypesList & columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_)
        : IStorage{columns_, materialized_columns_, alias_columns_, column_defaults_}, name(name_)  {}
};

}
