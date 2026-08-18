#include <TableFunctions/TableFunctionDictionary.h>

#include <Parsers/ASTLiteral.h>

#include <Access/Common/AccessFlags.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>

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
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function ({}) must have arguments.", quoteString(getName()));

    ASTs & args = args_func.at(0)->children;

    if (args.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function ({}) requires 1 arguments", quoteString(getName()));

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    dictionary_name = checkAndGetLiteralArgument<String>(args[0], "dictionary_name");
}

ColumnsDescription TableFunctionDictionary::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    const ExternalDictionariesLoader & external_loader = context->getExternalDictionariesLoader();
    std::string resolved_name = external_loader.resolveDictionaryName(dictionary_name, context->getCurrentDatabase());
    auto load_result = external_loader.load(resolved_name);
    if (load_result)
    {
        /// for regexp tree dictionary, the table structure will be different with dictionary structure. it is:
        /// - id. identifier of the tree node
        /// - parent_id.
        /// - regexp. the regular expression
        /// - keys. the names of attributions of dictionary structure
        /// - values. the values of each attribution
        const auto dictionary = std::static_pointer_cast<const IDictionary>(load_result);
        if (dictionary->getTypeName() == "RegExpTree")
        {
            return ColumnsDescription(NamesAndTypesList({
                {"id", std::make_shared<DataTypeUInt64>()},
                {"parent_id", std::make_shared<DataTypeUInt64>()},
                {"regexp", std::make_shared<DataTypeString>()},
                {"keys", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
                {"values", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())}
            }));
        }
    }

    /// otherwise, we get table structure by dictionary structure.
    auto dictionary_structure = external_loader.getDictionaryStructure(dictionary_name, context);
    return ColumnsDescription(StorageDictionary::getNamesAndTypes(dictionary_structure, false));
}

StoragePtr TableFunctionDictionary::executeImpl(
    const ASTPtr &, ContextPtr context, const std::string & table_name, ColumnsDescription, bool is_insert_query) const
{
    StorageID dict_id(getDatabaseName(), table_name);
    auto dictionary_table_structure = getActualTableStructure(context, is_insert_query);

    auto result = std::make_shared<StorageDictionary>(
        dict_id, dictionary_name, std::move(dictionary_table_structure), String{}, StorageDictionary::Location::Custom, context);

    return result;
}


void registerTableFunctionDictionary(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionDictionary>();
}

}
