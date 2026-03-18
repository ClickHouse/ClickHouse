#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>


namespace DB
{

class StorageView final : public IStorage
{
public:
    StorageView(
        const StorageID & table_id_,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns_,
        const String & comment,
        bool is_parameterized_view_ = false);

    std::string getName() const override { return "View"; }
    bool isView() const override { return true; }
    bool isParameterizedView() const { return is_parameterized_view; }

    /// It is passed inside the query and solved at its level.
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    static void replaceQueryParametersIfParametrizedView(ASTPtr & outer_query, const NameToNameMap & parameter_values);

    static void replaceWithSubquery(ASTSelectQuery & select_query, ASTPtr & view_name, const StorageMetadataPtr & metadata_snapshot, const bool parameterized_view)
    {
        replaceWithSubquery(select_query, metadata_snapshot->getSelectQuery().inner_query->clone(), view_name, parameterized_view);
    }

    static void replaceWithSubquery(ASTSelectQuery & outer_query, ASTPtr view_query, ASTPtr & view_name, bool parameterized_view);
    static ASTPtr restoreViewName(ASTSelectQuery & select_query, const ASTPtr & view_name);

protected:
    bool is_parameterized_view;
};

}
