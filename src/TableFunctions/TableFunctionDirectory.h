#pragma once

#include <TableFunctions/ITableFunction.h>

#include <TableFunctions/ITableFunctionFileLike.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <Common/Exception.h>

#include <Storages/Distributed/DirectoryMonitor.h>
#include <Storages/StorageFile.h>

#include <Interpreters/evaluateConstantExpression.h>

#include <Processors/ISource.h>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Formats/FormatFactory.h>
#include <Storages/StorageDirectory.h>

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
    bool content_column = false;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override
    {
        printf("parse1");

        /// Parse args
        ASTs & args_func = ast_function->children;

        if (args_func.size() != 1)
            throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

        ASTs & args = args_func.at(0)->children;

        if (args.empty())
            throw Exception(
                "Table function '" + getName() + "' requires at least 1 argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & arg : args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

        path = args[0]->as<ASTLiteral &>().value.safeGet<String>();

        // TODO: may be redutant
        if (args.size() == 2)
        {
            auto content_arg = args[0]->as<ASTLiteral &>().value.safeGet<String>();
            content_column = content_arg == "true" || content_arg == "TRUE" || content_arg == "1";
            structure.add({"content", std::make_shared<DataTypeString>()});
        }
        //

        if (args.size() > 1)
            throw Exception("Table function '" + getName() + "' requires path", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    ColumnsDescription getActualTableStructure(ContextPtr /* context */) const override { return structure; }

private:
    const char * getStorageTypeName() const override { return "Directory"; }
    ColumnsDescription structure{NamesAndTypesList{
//        NameAndTypePair{"type", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        NameAndTypePair{"symlink", DataTypeFactory::instance().get("Bool")},
        NameAndTypePair{"path", std::make_shared<DataTypeString>()}
//        NameAndTypePair{"size", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>())},
//        NameAndTypePair{"last_write_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>())},
//        NameAndTypePair{"name", std::make_shared<DataTypeString>()},
//        NameAndTypePair{"owner_read", DataTypeFactory::instance().get("Bool")},
//        NameAndTypePair{"owner_write", DataTypeFactory::instance().get("Bool")},
//        NameAndTypePair{"owner_exec", DataTypeFactory::instance().get("Bool")},
//        NameAndTypePair{"group_read", DataTypeFactory::instance().get("Bool")},
//        NameAndTypePair{"group_write", DataTypeFactory::instance().get("Bool")},
//        NameAndTypePair{"group_exec", DataTypeFactory::instance().get("Bool")},
//        NameAndTypePair{"others_read", DataTypeFactory::instance().get("Bool")},
//        NameAndTypePair{"others_write", DataTypeFactory::instance().get("Bool")},
//        NameAndTypePair{"others_exec", DataTypeFactory::instance().get("Bool")},
//        NameAndTypePair{"set_gid", DataTypeFactory::instance().get("Bool")},
//        NameAndTypePair{"set_uid", DataTypeFactory::instance().get("Bool")},
//        NameAndTypePair{"sticky_bit", DataTypeFactory::instance().get("Bool")}
        }};
    StoragePtr executeImpl(
        const ASTPtr & /* ast_function */,
        ContextPtr /* context */,
        const std::string & table_name,
        ColumnsDescription /* cached_columns */) const override
    {
        printf("%s\n", path.c_str());
        StoragePtr res
            = StorageDirectory::create(StorageID(getDatabaseName(), table_name), structure, path, ConstraintsDescription(), String{});
        printf("execute2");
        res->startup();
        printf("execute3");
        return res;
    }
};
};
