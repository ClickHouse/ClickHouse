#pragma once
#include "config.h"

#if USE_DUCKDB
#include <TableFunctions/ITableFunction.h>
#include <Storages/DuckDB/StorageDuckDB.h>


namespace DB
{

class TableFunctionDuckDB : public ITableFunction
{
public:
    static constexpr auto name = "duckdb";
    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, ContextPtr context,
            const std::string & table_name, ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "DuckDB"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String database_path, remote_table_name;
    std::shared_ptr<duckdb::DuckDB> duckdb_instance;
};

}

#endif
