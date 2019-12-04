#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>

#include <ext/shared_ptr_helper.h>


namespace DB
{

class StorageView : public ext::shared_ptr_helper<StorageView>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageView>;
public:
    std::string getName() const override { return "View"; }

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

private:
    ASTPtr inner_query;

    void replaceTableNameWithSubquery(ASTSelectQuery * select_query, ASTPtr & subquery);

protected:
    StorageView(
        const StorageID & table_id_,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns_);
};

}
