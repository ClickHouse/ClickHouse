#pragma once

#include <optional>

#include <TableFunctions/ITableFunction.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/StorageMemory.h>
#include <Storages/MemorySettings.h>

namespace DB
{

class TableFunctionPredict : public ITableFunction
{
public:
    static constexpr auto name = "predict";
    std::string getName() const override { return name; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const override
    {
        return ColumnsDescription{{"predict", std::make_shared<DataTypeString>()}};
    }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "Values"; }

    String model_name;
    String table_name;
};

}
