#pragma once

#include <ext/shared_ptr_helper.hpp>

#include <Common/OptimizedRegularExpression.h>
#include <Storages/IStorage.h>


namespace DB
{

/** A table that represents the union of an arbitrary number of other tables.
  * All tables must have the same structure.
  */
class StorageMerge : private ext::shared_ptr_helper<StorageMerge>, public IStorage
{
friend class ext::shared_ptr_helper<StorageMerge>;

public:
    static StoragePtr create(
        const std::string & name_,            /// The name of the table.
        NamesAndTypesListPtr columns_,        /// List of columns.
        const String & source_database_,      /// In which database to look for source tables.
        const String & table_name_regexp_,    /// Regex names of source tables.
        const Context & context_);            /// Known tables.

    static StoragePtr create(
        const std::string & name_,            /// The name of the table.
        NamesAndTypesListPtr columns_,        /// List of columns.
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        const String & source_database_,    /// In which database to look for source tables.
        const String & table_name_regexp_,    /// Regex names of source tables.
        const Context & context_);            /// Known tables.

    std::string getName() const override { return "Merge"; }
    std::string getTableName() const override { return name; }

    /// The check is delayed to the read method. It checks the support of the tables used.
    bool supportsSampling() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsIndexForIn() const override { return true; }

    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }
    NameAndTypePair getColumn(const String & column_name) const override;
    bool hasColumn(const String & column_name) const override;

    BlockInputStreams read(
        const Names & column_names,
        ASTPtr query,
        const Context & context,
        const Settings & settings,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size = DEFAULT_BLOCK_SIZE,
        unsigned threads = 1) override;

    void drop() override {}
    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override { name = new_table_name; }

    /// you need to add and remove columns in the sub-tables manually
    /// the structure of sub-tables is not checked
    void alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context) override;

private:
    String name;
    NamesAndTypesListPtr columns;
    String source_database;
    OptimizedRegularExpression table_name_regexp;
    const Context & context;

    StorageMerge(
        const std::string & name_,
        NamesAndTypesListPtr columns_,
        const String & source_database_,
        const String & table_name_regexp_,
        const Context & context_);

    StorageMerge(
        const std::string & name_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        const String & source_database_,
        const String & table_name_regexp_,
        const Context & context_);

    using StorageListWithLocks = std::list<std::pair<StoragePtr, TableStructureReadLockPtr>>;

    StorageListWithLocks getSelectedTables() const;

    Block getBlockWithVirtualColumns(const StorageListWithLocks & selected_tables) const;
};

}
