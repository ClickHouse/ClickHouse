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
        const String & comment);

    std::string getName() const override { return "View"; }
    bool isView() const override { return true; }
    bool isParameterizedView() const { return is_parameterized_view; }

    /// It is passed inside the query and solved at its level.
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    void replaceQueryParametersIfParametrizedView(ASTPtr & outer_query);

    static void replaceWithSubquery(ASTSelectQuery & select_query, ASTPtr & view_name, const StorageMetadataPtr & metadata_snapshot, const bool parameterized_view)
    {
        replaceWithSubquery(select_query, metadata_snapshot->getSelectQuery().inner_query->clone(), view_name, parameterized_view);
    }

    static void replaceWithSubquery(ASTSelectQuery & outer_query, ASTPtr view_query, ASTPtr & view_name, const bool parameterized_view);
    static ASTPtr restoreViewName(ASTSelectQuery & select_query, const ASTPtr & view_name);
    static String replaceQueryParameterWithValue (const String & column_name, const NameToNameMap & parameter_values, const NameToNameMap & parameter_types);
    static String replaceValueWithQueryParameter (const String & column_name, const NameToNameMap & parameter_values);

    void setParameterValues (NameToNameMap parameter_values_)
    {
        parameter_values = parameter_values_;
    }

    NameToNameMap getParameterValues() const
    {
        return parameter_types;
    }

protected:
    bool is_parameterized_view;
    NameToNameMap parameter_values;
    NameToNameMap parameter_types;
};

}
