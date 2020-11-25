#pragma once
#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <TableFunctions/ITableFunction.h>
#include "pqxx/pqxx"

namespace DB
{

class TableFunctionPostgreSQL : public ITableFunction
{
public:
    static constexpr auto name = "postgresql";
    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, const Context & context,
            const std::string & table_name, ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "PostgreSQL"; }

    ColumnsDescription getActualTableStructure(const Context & context) const override;
    void parseArguments(const ASTPtr & ast_function, const Context & context) override;

    DataTypePtr getDataType(std::string & type, bool is_nullable, uint16_t dimensions) const;

    String connection_str;
    String remote_table_name;
    std::shared_ptr<pqxx::connection> connection;
};

}

#endif
