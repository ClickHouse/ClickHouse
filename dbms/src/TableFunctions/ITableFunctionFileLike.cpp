#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/ITableFunctionFileLike.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Storages/StorageFile.h>
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

StoragePtr ITableFunctionFileLike::executeImpl(const ASTPtr & ast_function, const Context & context) const
{
    // Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.at(0)->children;

    if (args.size() != 3)
        throw Exception("Table function '" + getName() + "' requires exactly 3 arguments: filename, format and structure.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (size_t i = 0; i < 3; ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    std::string filename = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    std::string format = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    std::string structure = args[2]->as<ASTLiteral &>().value.safeGet<String>();

    // Create sample block
    std::vector<std::string> structure_vals;
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

    // Create table
    StoragePtr storage = getStorage(filename, format, sample_block, const_cast<Context &>(context));

    storage->startup();

    return storage;
}

}
