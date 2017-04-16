#pragma once

#include <ext/shared_ptr_helper.hpp>

#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/NullBlockOutputStream.h>


namespace DB
{

/** When writing, does nothing.
  * When reading, returns nothing.
  */
class StorageNull : private ext::shared_ptr_helper<StorageNull>, public IStorage
{
friend class ext::shared_ptr_helper<StorageNull>;

public:
    static StoragePtr create(
        const std::string & name_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_)
    {
        return make_shared(name_, columns_, materialized_columns_, alias_columns_, column_defaults_);
    }

    std::string getName() const override { return "Null"; }
    std::string getTableName() const override { return name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

    BlockInputStreams read(
        const Names & column_names,
        ASTPtr query,
        const Context & context,
        const Settings & settings,
        QueryProcessingStage::Enum & processed_stage,
        const size_t max_block_size = DEFAULT_BLOCK_SIZE,
        const unsigned threads = 1) override
    {
        return { std::make_shared<NullBlockInputStream>() };
    }

    BlockOutputStreamPtr write(ASTPtr query, const Settings & settings) override
    {
        return std::make_shared<NullBlockOutputStream>();
    }

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override
    {
        name = new_table_name;
    }

private:
    String name;
    NamesAndTypesListPtr columns;

    StorageNull(
        const std::string & name_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_)
        : IStorage{materialized_columns_, alias_columns_, column_defaults_}, name(name_), columns(columns_) {}
};

}
