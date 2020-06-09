#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>

#include <ext/shared_ptr_helper.h>


namespace DB
{

class StorageView final : public ext::shared_ptr_helper<StorageView>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageView>;
public:
    std::string getName() const override { return "View"; }
    bool isView() const override { return true; }

    /// It is passed inside the query and solved at its level.
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }

    Pipes read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void replaceWithSubquery(ASTSelectQuery & select_query, ASTPtr & view_name) const
    {
        replaceWithSubquery(select_query, inner_query->clone(), view_name);
    }

    static void replaceWithSubquery(ASTSelectQuery & outer_query, ASTPtr view_query, ASTPtr & view_name);
    static ASTPtr restoreViewName(ASTSelectQuery & select_query, const ASTPtr & view_name);

private:
    ASTPtr inner_query;

protected:
    StorageView(
        const StorageID & table_id_,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns_);
};

}
