#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;


class StorageView : public ext::shared_ptr_helper<StorageView>, public IStorage
{
public:
    std::string getName() const override { return "View"; }
    std::string getTableName() const override { return table_name; }

    /// It is passed inside the query and solved at its level.
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    String select_database_name;
    String select_table_name;
    String table_name;
    String database_name;
    ASTPtr inner_query;

protected:
    StorageView(
        const String & table_name_,
        const String & database_name_,
        const ASTCreateQuery & query,
        const NamesAndTypesList & columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_);
};

}
