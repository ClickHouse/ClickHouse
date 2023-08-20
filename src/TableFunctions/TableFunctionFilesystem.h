#pragma once

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <TableFunctions/ITableFunction.h>

namespace DB
{
/* directory(path, TODO) - creates a temporary storage from TODO
 *
 * TODO
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

    ColumnsDescription getActualTableStructure(ContextPtr /* context */) const override { return structure; }

private:
    const char * getStorageTypeName() const override { return "Directory"; }

    ColumnsDescription structure
    {
        {
            {"type", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
            {"symlink", DataTypeFactory::instance().get("Bool")},
            {"path", std::make_shared<DataTypeString>()},
            {"size", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>())},
            {"modification_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>())},
            {"name", std::make_shared<DataTypeString>()},
            {"owner_read", DataTypeFactory::instance().get("Bool")},
            {"owner_write", DataTypeFactory::instance().get("Bool")},
            {"owner_exec", DataTypeFactory::instance().get("Bool")},
            {"group_read", DataTypeFactory::instance().get("Bool")},
            {"group_write", DataTypeFactory::instance().get("Bool")},
            {"group_exec", DataTypeFactory::instance().get("Bool")},
            {"others_read", DataTypeFactory::instance().get("Bool")},
            {"others_write", DataTypeFactory::instance().get("Bool")},
            {"others_exec", DataTypeFactory::instance().get("Bool")},
            {"set_gid", DataTypeFactory::instance().get("Bool")},
            {"set_uid", DataTypeFactory::instance().get("Bool")},
            {"sticky_bit", DataTypeFactory::instance().get("Bool")}
        }
    };

    StoragePtr executeImpl(
        const ASTPtr & /* ast_function */,
        ContextPtr /* context */,
        const std::string & table_name,
        ColumnsDescription /* cached_columns */) const override;
};
}
