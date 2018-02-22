#pragma once

#include <ext/shared_ptr_helper.h>

#include <Storages/IStorage.h>


namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;


class StorageMaterializedView : public ext::shared_ptr_helper<StorageMaterializedView>, public IStorage
{
public:
    std::string getName() const override { return "MaterializedView"; }
    std::string getTableName() const override { return table_name; }
    const NamesAndTypesList & getColumnsListImpl() const override { return columns; }
    ASTPtr getInnerQuery() const { return inner_query->clone(); };
    StoragePtr getTargetTable() const;

    NameAndTypePair getColumn(const String & column_name) const override;
    bool hasColumn(const String & column_name) const override;

    bool supportsSampling() const override { return getTargetTable()->supportsSampling(); }
    bool supportsPrewhere() const override { return getTargetTable()->supportsPrewhere(); }
    bool supportsFinal() const override { return getTargetTable()->supportsFinal(); }
    bool supportsIndexForIn() const override { return getTargetTable()->supportsIndexForIn(); }

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;
    void drop() override;
    bool optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context) override;
    void shutdown() override;
    bool checkTableCanBeDropped() const override;

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    String getDataPath() const override { return getTargetTable()->getDataPath(); }

private:
    String select_database_name;
    String select_table_name;
    String target_database_name;
    String target_table_name;
    String table_name;
    String database_name;
    ASTPtr inner_query;
    Context & global_context;
    bool has_inner_table = false;

protected:
    StorageMaterializedView(
        const String & table_name_,
        const String & database_name_,
        Context & local_context,
        const ASTCreateQuery & query,
        const NamesAndTypesList & columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        bool attach_);
};

}
