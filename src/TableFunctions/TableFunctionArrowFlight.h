#pragma once

#include "config.h"

#if USE_ARROWFLIGHT
#include <Storages/StorageArrowFlight.h>
#include <Storages/ArrowFlight/ArrowFlightConnection.h>
#include <TableFunctions/ITableFunction.h>

namespace DB
{

class TableFunctionArrowFlight : public ITableFunction
{
public:
    static constexpr auto name = "arrowFlight";
    String getName() const override { return name; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const String & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return "ArrowFlight"; }
    const String & getFunctionURI() const override { return config.host; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    StorageArrowFlight::Configuration config;
    std::shared_ptr<ArrowFlightConnection> connection;
};

}

#endif
