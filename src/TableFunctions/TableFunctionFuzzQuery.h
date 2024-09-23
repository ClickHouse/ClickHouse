#pragma once

#include <optional>

#include <TableFunctions/ITableFunction.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/StorageFuzzQuery.h>

#include "config.h"

namespace DB
{

class TableFunctionFuzzQuery : public ITableFunction
{
public:
    static constexpr auto name = "fuzzQuery";
    std::string getName() const override { return name; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const override
    {
        return ColumnsDescription{{"query", std::make_shared<DataTypeString>()}};
    }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "fuzzQuery"; }

    String source;
    std::optional<UInt64> random_seed;
    StorageFuzzQuery::Configuration configuration;
};

}
