#pragma once

#include <Interpreters/IInterpreter.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageSnapshot.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

struct ASTTableExpression;

/** Return names, types and other information about columns in specified table.
  */
class InterpreterDescribeQuery : public IInterpreter, WithContext
{
public:
    InterpreterDescribeQuery(const ASTPtr & query_ptr_, ContextPtr context_);

    BlockIO execute() override;

    static Block getSampleBlock(bool include_subcolumns, bool include_virtuals, bool compact);

private:
    void fillColumnsFromSubquery(const ASTTableExpression & table_expression);
    void fillColumnsFromTableFunction(const ASTTableExpression & table_expression);
    void fillColumnsFromTable(const ASTTableExpression & table_expression);

    void addColumn(const ColumnDescription & column, bool is_virtual, MutableColumns & res_columns);
    void addSubcolumns(const ColumnDescription & column, bool is_virtual, MutableColumns & res_columns);

    ASTPtr query_ptr;
    const Settings & settings;

    std::vector<ColumnDescription> columns;
    std::vector<ColumnDescription> virtual_columns;
    StorageSnapshotPtr storage_snapshot;
};


}
