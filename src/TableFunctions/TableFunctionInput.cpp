#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>
#include <Storages/StorageInput.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

namespace
{

/* input(structure) - allows to make INSERT SELECT from incoming stream of data
 */
class TableFunctionInput : public ITableFunction
{
public:
    static constexpr auto name = "input";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }
    bool needStructureHint() const override { return true; }
    void setStructureHint(const ColumnsDescription & structure_hint_) override { structure_hint = structure_hint_; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    const char * getStorageTypeName() const override { return "Input"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String structure;
    ColumnsDescription structure_hint;
};

void TableFunctionInput::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();

    if (!function->arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments", getName());

    auto args = function->arguments->children;

    if (args.empty())
    {
        structure = "auto";
        return;
    }

    if (args.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' requires exactly 1 argument: structure", getName());

    structure = checkAndGetLiteralArgument<String>(evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context), "structure");
}

ColumnsDescription TableFunctionInput::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (structure == "auto")
    {
        if (structure_hint.empty())
            throw Exception(
                ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                "Table function '{}' was used without structure argument but structure could not be determined automatically. Please, "
                "provide structure manually",
                getName());
        return structure_hint;
    }
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionInput::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    auto storage = std::make_shared<StorageInput>(StorageID(getDatabaseName(), table_name), getActualTableStructure(context, is_insert_query));
    storage->startup();
    return storage;
}

}

void registerTableFunctionInput(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionInput>();
}

}
