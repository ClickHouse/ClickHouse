#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionInput.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Storages/StorageInput.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StoragePtr TableFunctionInput::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    const auto * function = ast_function->as<ASTFunction>();

    if (!function->arguments)
        throw Exception("Table function '" + getName() + "' must have arguments", ErrorCodes::LOGICAL_ERROR);

    auto args = function->arguments->children;

    if (args.size() != 1)
        throw Exception("Table function '" + getName() + "' requires exactly 1 argument: structure",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context);
    String structure = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context)->as<ASTLiteral &>().value.safeGet<String>();

    // Create sample block
    Strings structure_vals;
    boost::split(structure_vals, structure, boost::algorithm::is_any_of(" ,"), boost::algorithm::token_compress_on);

    if (structure_vals.size() % 2 != 0)
        throw Exception("Odd number of elements in section structure: must be a list of name type pairs", ErrorCodes::LOGICAL_ERROR);

    Block sample_block;
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    for (size_t i = 0, size = structure_vals.size(); i < size; i += 2)
    {
        ColumnWithTypeAndName column;
        column.name = structure_vals[i];
        column.type = data_type_factory.get(structure_vals[i + 1]);
        column.column = column.type->createColumn();
        sample_block.insert(std::move(column));
    }

    StoragePtr storage = StorageInput::create(table_name, ColumnsDescription{sample_block.getNamesAndTypesList()});

    storage->startup();

    return storage;
}

void registerTableFunctionInput(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionInput>();
}

}
