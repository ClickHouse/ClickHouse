#pragma once

#include <Interpreters/IInterpreter.h>
#include <Storages/ColumnDefault.h>


class ThreadPool;

namespace DB
{

class Context;
class ASTCreateQuery;
class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;


/** Allows to create new table or database,
  *  or create an object for existing table or database.
  */
class InterpreterCreateQuery : public IInterpreter
{
public:
    InterpreterCreateQuery(const ASTPtr & query_ptr_, Context & context_);

    BlockIO execute() override;

    /// List of columns and their types in AST.
    static ASTPtr formatColumns(const NamesAndTypesList & columns);
    static ASTPtr formatColumns(
        NamesAndTypesList columns,
        const NamesAndTypesList & materialized_columns,
        const NamesAndTypesList & alias_columns,
        const ColumnDefaults & column_defaults);

    void setDatabaseLoadingThreadpool(ThreadPool & thread_pool_)
    {
        thread_pool = &thread_pool_;
    }

    void setForceRestoreData(bool has_force_restore_data_flag_)
    {
        has_force_restore_data_flag = has_force_restore_data_flag_;
    }

    struct ColumnsInfo
    {
        NamesAndTypesListPtr columns = std::make_shared<NamesAndTypesList>();
        NamesAndTypesList materialized_columns;
        NamesAndTypesList alias_columns;
        ColumnDefaults column_defaults;
    };

    /// Obtain information about columns, their types and default values, for case when columns in CREATE query is specified explicitly.
    static ColumnsInfo getColumnsInfo(const ASTPtr & columns, const Context & context);

private:
    BlockIO createDatabase(ASTCreateQuery & create);
    BlockIO createTable(ASTCreateQuery & create);

    /// Calculate list of columns of table and return it.
    ColumnsInfo setColumns(ASTCreateQuery & create, const Block & as_select_sample, const StoragePtr & as_storage) const;
    String setEngine(ASTCreateQuery & create, const StoragePtr & as_storage) const;

    ASTPtr query_ptr;
    Context & context;

    /// Using while loading database.
    ThreadPool * thread_pool = nullptr;

    /// Skip safety threshold when loading tables.
    bool has_force_restore_data_flag = false;
};


}
