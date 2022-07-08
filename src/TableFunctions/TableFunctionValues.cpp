#include <Common/typeid_cast.h>
#include <Common/Exception.h>

#include <Core/Block.h>
#include <Storages/StorageValues.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/getLeastSupertype.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>

#include <TableFunctions/TableFunctionValues.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>

#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

static void parseAndInsertValues(MutableColumns & res_columns, const ASTs & args, const Block & sample_block, size_t start, ContextPtr context)
{
    if (res_columns.size() == 1) /// Parsing arguments as Fields
    {
        for (size_t i = start; i < args.size(); ++i)
        {
            const auto & [value_field, value_type_ptr] = evaluateConstantExpression(args[i], context);

            Field value = convertFieldToTypeOrThrow(value_field, *sample_block.getByPosition(0).type, value_type_ptr.get());
            res_columns[0]->insert(value);
        }
    }
    else /// Parsing arguments as Tuples
    {
        for (size_t i = start; i < args.size(); ++i)
        {
            const auto & [value_field, value_type_ptr] = evaluateConstantExpression(args[i], context);

            const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(value_type_ptr.get());
            if (!type_tuple)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Table function VALUES requires all but first argument (rows specification) to be either tuples or single values");

            const Tuple & value_tuple = value_field.safeGet<Tuple>();

            if (value_tuple.size() != sample_block.columns())
                throw Exception("Values size should match with number of columns", ErrorCodes::BAD_ARGUMENTS);

            const DataTypes & value_types_tuple = type_tuple->getElements();
            for (size_t j = 0; j < value_tuple.size(); ++j)
            {
                Field value = convertFieldToTypeOrThrow(value_tuple[j], *sample_block.getByPosition(j).type, value_types_tuple[j].get());
                res_columns[j]->insert(value);
            }
        }
    }
}

DataTypes TableFunctionValues::getTypesFromArgument(const ASTPtr & arg, ContextPtr context)
{
    const auto & [value_field, value_type_ptr] = evaluateConstantExpression(arg, context);
    DataTypes types;
    if (const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(value_type_ptr.get()))
        return type_tuple->getElements();

    return {value_type_ptr};
}

void TableFunctionValues::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.at(0)->children;

    if (args.empty())
        throw Exception("Table function '" + getName() + "' requires at least 1 argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto & literal = args[0]->as<const ASTLiteral>();
    String value;
    if (args.size() > 1 && literal && literal->value.tryGet(value) && tryParseColumnsListFromString(value, structure, context))
    {
        has_structure_in_arguments = true;
        return;
    }

    has_structure_in_arguments = false;
    DataTypes data_types = getTypesFromArgument(args[0], context);
    for (size_t i = 1; i < args.size(); ++i)
    {
        auto arg_types = getTypesFromArgument(args[i], context);
        if (data_types.size() != arg_types.size())
            throw Exception(
                ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                "Cannot determine common structure for {} function arguments: the amount of columns is differ for different arguments",
                getName());
        for (size_t j = 0; j != arg_types.size(); ++j)
            data_types[j] = getLeastSupertype(DataTypes{data_types[j], arg_types[j]});
    }

    NamesAndTypesList names_and_types;
    for (size_t i = 0; i != data_types.size(); ++i)
        names_and_types.emplace_back("c" + std::to_string(i + 1), data_types[i]);
    structure = ColumnsDescription(names_and_types);
}

ColumnsDescription TableFunctionValues::getActualTableStructure(ContextPtr /*context*/) const
{
    return structure;
}

StoragePtr TableFunctionValues::executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);

    Block sample_block;
    for (const auto & name_type : columns.getOrdinary())
        sample_block.insert({ name_type.type->createColumn(), name_type.type, name_type.name });

    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    ASTs & args = ast_function->children.at(0)->children;

    /// Parsing other arguments as values and inserting them into columns
    parseAndInsertValues(res_columns, args, sample_block, has_structure_in_arguments ? 1 : 0, context);

    Block res_block = sample_block.cloneWithColumns(std::move(res_columns));

    auto res = std::make_shared<StorageValues>(StorageID(getDatabaseName(), table_name), columns, res_block);
    res->startup();
    return res;
}

void registerTableFunctionValues(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionValues>(TableFunctionFactory::CaseInsensitive);
}

}
