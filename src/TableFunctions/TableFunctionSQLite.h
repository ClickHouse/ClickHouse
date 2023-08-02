#pragma once
#include "config.h"

#if USE_SQLITE
#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageSQLite.h>


namespace DB
{

class TableFunctionSQLite : public ITableFunction
{
public:
    static constexpr auto name = "sqlite";
    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, ContextPtr context,
            const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "SQLite"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String database_path, remote_table_name;
    std::shared_ptr<sqlite3> sqlite_db;
};

}

#endif
