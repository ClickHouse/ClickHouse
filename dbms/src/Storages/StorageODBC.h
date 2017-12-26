#pragma once

#include <Storages/IStorage.h>

#include <Poco/Data/SessionPool.h>

#include <sparsehash/dense_hash_map>

namespace DB
{
class StorageODBC : public IStorage
{
public:
    StorageODBC(const std::string & table_name_,
        const std::string & database_name_,
        const std::string & odbc_table_name_,
        const NamesAndTypesListPtr & columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        const Context & context_);

    static StoragePtr create(const std::string & table_name,
        const std::string & database_name,
        const std::string & odbc_table_name,
        const NamesAndTypesListPtr & columns,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        const Context & context)
    {
        return std::make_shared<StorageODBC>(
            table_name, database_name, odbc_table_name, columns, materialized_columns_, alias_columns_, column_defaults_, context);
    }

    std::string getName() const override
    {
        return "ODBC";
    }

    std::string getTableName() const override
    {
        return table_name;
    }

    const NamesAndTypesList & getColumnsListImpl() const override
    {
        return *columns;
    }

    BlockInputStreams read(const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    std::string table_name;
    std::string database_name;
    std::string odbc_table_name;
    Block sample_block;
    NamesAndTypesListPtr columns;
    const Context & context_global;
    google::dense_hash_map<std::string, DataTypePtr> column_map;
    Poco::Data::SessionPool pool;
};
}
