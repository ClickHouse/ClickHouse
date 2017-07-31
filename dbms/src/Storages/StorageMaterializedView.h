#pragma once

#include <ext/shared_ptr_helper.h>

#include <Storages/IStorage.h>


namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;


class StorageMaterializedView : public ext::shared_ptr_helper<StorageMaterializedView>, public IStorage
{
friend class ext::shared_ptr_helper<StorageMaterializedView>;

public:
    std::string getName() const override { return "MaterializedView"; }
    std::string getTableName() const override { return table_name; }
    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }
    std::string getInnerTableName() const { return  ".inner." + table_name; }
    ASTPtr getInnerQuery() const { return inner_query->clone(); };
    StoragePtr getInnerTable() const;

    NameAndTypePair getColumn(const String & column_name) const override;
    bool hasColumn(const String & column_name) const override;

    bool supportsSampling() const override { return getInnerTable()->supportsSampling(); }
    bool supportsPrewhere() const override { return getInnerTable()->supportsPrewhere(); }
    bool supportsFinal() const override { return getInnerTable()->supportsFinal(); }
    bool supportsParallelReplicas() const override { return getInnerTable()->supportsParallelReplicas(); }
    bool supportsIndexForIn() const override { return getInnerTable()->supportsIndexForIn(); }

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;
    void drop() override;
    bool optimize(const ASTPtr & query, const String & partition, bool final, bool deduplicate, const Settings & settings) override;

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
    Context & context;
    NamesAndTypesListPtr columns;

    StorageMaterializedView(
        const String & table_name_,
        const String & database_name_,
        Context & context_,
        ASTPtr & query_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        bool attach_);
};

}
