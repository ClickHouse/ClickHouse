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

    const NamesAndTypesList & getColumnsListImpl() const override { return columns; }

    BlockInputStreams read(
        const Names &,
        const SelectQueryInfo &,
        const Context &,
        QueryProcessingStage::Enum &,
        size_t,
        unsigned) override
    {
        return { std::make_shared<NullBlockInputStream>() };
    }

    BlockOutputStreamPtr write(const ASTPtr &, const Settings &) override
    {
        return std::make_shared<NullBlockOutputStream>();
    }

    void rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & new_table_name) override
    {
        name = new_table_name;
    }

private:
    String name;
    NamesAndTypesList columns;

protected:
    StorageNull(
        const std::string & name_,
        const NamesAndTypesList & columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_)
        : IStorage{materialized_columns_, alias_columns_, column_defaults_}, name(name_), columns(columns_) {}
};

}
