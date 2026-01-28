#pragma once

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <TableFunctions/ITableFunction.h>


namespace DB
{

/** filesystem('path') - recursively iterates the path and represents the result as a table,
  * allowing to get files' metadata and, optionally, contents.
  */
class TableFunctionFilesystem : public ITableFunction
{
public:
    static constexpr auto name = "filesystem";
    std::string getName() const override { return name; }

    bool hasStaticStructure() const override { return true; }

protected:
    String path;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const override;

private:
    const char * getStorageTypeName() const override { return "Filesystem"; }

    StoragePtr executeImpl(
        const ASTPtr & /* ast_function */,
        ContextPtr /* context */,
        const std::string & table_name,
        ColumnsDescription /* cached_columns */,
        bool is_insert_query) const override;
};

}
