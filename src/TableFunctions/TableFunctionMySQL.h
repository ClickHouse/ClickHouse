#pragma once
#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL
#include <TableFunctions/ITableFunction.h>
#include <mysqlxx/Pool.h>


namespace DB
{

/* mysql ('host:port', database, table, user, password) - creates a temporary StorageMySQL.
 * The structure of the table is taken from the mysql query DESCRIBE table.
 * If there is no such table, an exception is thrown.
 */
class TableFunctionMySQL : public ITableFunction
{
public:
    static constexpr auto name = "mysql";
    std::string getName() const override
    {
        return name;
    }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "MySQL"; }

    ColumnsDescription getActualTableStructure(const Context & context) const override;
    void parseArguments(const ASTPtr & ast_function, const Context & context) override;

    std::pair<std::string, UInt16> parsed_host_port;
    String remote_database_name;
    String remote_table_name;
    String user_name;
    String password;
    bool replace_query = false;
    String on_duplicate_clause;

    mutable std::optional<mysqlxx::Pool> pool;
};

}

#endif
