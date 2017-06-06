#pragma once

#include <ext/shared_ptr_helper.h>

#include <Parsers/ASTSelectQuery.h>
#include <Storages/IStorage.h>


namespace DB
{

class StorageView : public ext::shared_ptr_helper<StorageView>, public IStorage
{
friend class ext::shared_ptr_helper<StorageView>;

public:
    std::string getName() const override { return "View"; }
    std::string getTableName() const override { return table_name; }
    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }
    ASTPtr getInnerQuery() const { return inner_query.clone(); };

    /// It is passed inside the query and solved at its level.
    bool supportsSampling() const override { return true; }
    bool supportsFinal()     const override { return true; }

    bool supportsParallelReplicas() const override { return true; }

    BlockInputStreams read(
        const Names & column_names,
        const ASTPtr & query,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void drop() override;

protected:
    String select_database_name;
    String select_table_name;
    String table_name;
    String database_name;
    ASTSelectQuery inner_query;
    Context & context;
    NamesAndTypesListPtr columns;

    StorageView(
        const String & table_name_,
        const String & database_name_,
        Context & context_,
        ASTPtr & query_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_);

private:
    /// extract the name of the database and the table from the most internal subquery: `select_database_name, select_table_name`.
    void extractDependentTable(const ASTSelectQuery & query);
};

}
