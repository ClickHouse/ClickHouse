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
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & new_table_name) override
    {
        table_name = new_table_name;
    }

private:
    String table_name;
    ASTPtr inner_query;

protected:
    StorageView(
        const String & table_name_,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns_);
};

}
