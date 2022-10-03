#pragma once

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <TableFunctions/ITableFunction.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}
/* directory(path, TODO) - creates a temporary storage from TODO
 *
 * TODO
 */
class TableFunctionDirectory : public ITableFunction
{
public:
    static constexpr auto name = "directory";
    std::string getName() const override { return name; }

    bool hasStaticStructure() const override { return true; }

protected:
    String path;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr /* context */) const override { return structure; }

private:
    const char * getStorageTypeName() const override { return "Directory"; }
    ColumnsDescription structure{NamesAndTypesList{
        NameAndTypePair{"type", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        NameAndTypePair{"symlink", DataTypeFactory::instance().get("Bool")},
        NameAndTypePair{"path", std::make_shared<DataTypeString>()},
        NameAndTypePair{"size", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>())},
        NameAndTypePair{"last_write_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>())},
        NameAndTypePair{"name", std::make_shared<DataTypeString>()},
        NameAndTypePair{"owner_read", DataTypeFactory::instance().get("Bool")},
        NameAndTypePair{"owner_write", DataTypeFactory::instance().get("Bool")},
        NameAndTypePair{"owner_exec", DataTypeFactory::instance().get("Bool")},
        NameAndTypePair{"group_read", DataTypeFactory::instance().get("Bool")},
        NameAndTypePair{"group_write", DataTypeFactory::instance().get("Bool")},
        NameAndTypePair{"group_exec", DataTypeFactory::instance().get("Bool")},
        NameAndTypePair{"others_read", DataTypeFactory::instance().get("Bool")},
        NameAndTypePair{"others_write", DataTypeFactory::instance().get("Bool")},
        NameAndTypePair{"others_exec", DataTypeFactory::instance().get("Bool")},
        NameAndTypePair{"set_gid", DataTypeFactory::instance().get("Bool")},
        NameAndTypePair{"set_uid", DataTypeFactory::instance().get("Bool")},
        NameAndTypePair{"sticky_bit", DataTypeFactory::instance().get("Bool")}}};
    StoragePtr executeImpl(
        const ASTPtr & /* ast_function */,
        ContextPtr /* context */,
        const std::string & table_name,
        ColumnsDescription /* cached_columns */) const override;
};
}
