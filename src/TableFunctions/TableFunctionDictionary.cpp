#include <TableFunctions/TableFunctionDictionary.h>

#include <Parsers/ASTLiteral.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Storages/StorageDictionary.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void TableFunctionDictionary::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    // Parse args
    ASTList & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function ({}) must have arguments.", quoteString(getName()));

    ASTList & args = args_func.front()->children;

    if (args.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function ({}) requires 1 arguments", quoteString(getName()));

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    dictionary_name = checkAndGetLiteralArgument<String>(args.front(), "dictionary_name");
}

ColumnsDescription TableFunctionDictionary::getActualTableStructure(ContextPtr context) const
{
    const ExternalDictionariesLoader & external_loader = context->getExternalDictionariesLoader();
    auto dictionary_structure = external_loader.getDictionaryStructure(dictionary_name, context);
    auto result = ColumnsDescription(StorageDictionary::getNamesAndTypes(dictionary_structure));

    return result;
}

StoragePtr TableFunctionDictionary::executeImpl(
    const ASTPtr &, ContextPtr context, const std::string & table_name, ColumnsDescription) const
{
    StorageID dict_id(getDatabaseName(), table_name);
    auto dictionary_table_structure = getActualTableStructure(context);

    auto result = std::make_shared<StorageDictionary>(
        dict_id, dictionary_name, std::move(dictionary_table_structure), String{}, StorageDictionary::Location::Custom, context);

    return result;
}

void registerTableFunctionDictionary(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionDictionary>();
}

}
