#pragma once

#include <Interpreters/IInterpreter.h>
#include <Storages/ColumnsDescription.h>


class ThreadPool;

namespace DB
{
class Context;
class ASTCreateQuery;
class ASTExpressionList;
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
    static ASTPtr formatColumns(const ColumnsDescription & columns);

    void setDatabaseLoadingThreadpool(ThreadPool & thread_pool_)
    {
        thread_pool = &thread_pool_;
    }

    void setForceRestoreData(bool has_force_restore_data_flag_)
    {
        has_force_restore_data_flag = has_force_restore_data_flag_;
    }

    void setInternal(bool internal_)
    {
        internal = internal_;
    }

    /// Obtain information about columns, their types and default values, for case when columns in CREATE query is specified explicitly.
    static ColumnsDescription getColumnsDescription(const ASTExpressionList & columns, const Context & context);

private:
    BlockIO createDatabase(ASTCreateQuery & create);
    BlockIO createTable(ASTCreateQuery & create);

    /// Calculate list of columns of table and return it.
    ColumnsDescription setColumns(ASTCreateQuery & create, const Block & as_select_sample, const StoragePtr & as_storage) const;
    void setEngine(ASTCreateQuery & create) const;
    void checkAccess(const ASTCreateQuery & create);

    ASTPtr query_ptr;
    Context & context;

    /// Using while loading database.
    ThreadPool * thread_pool = nullptr;

    /// Skip safety threshold when loading tables.
    bool has_force_restore_data_flag = false;
    /// Is this an internal query - not from the user.
    bool internal = false;
};
}
