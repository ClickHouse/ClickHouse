#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFile.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Storages/StorageMemory.h>
#include <Interpreters/evaluateConstantExpression.h>


namespace DB
{

    namespace ErrorCodes
    {
        extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    }


    StoragePtr TableFunctionFile::executeImpl(const ASTPtr & ast_function, const Context & context) const
    {
        ASTs & args_func = typeid_cast<ASTFunction &>(*ast_function).children;

        if (args_func.size() != 3)
            throw Exception("Table function 'file' requires exactly three arguments: path, format and structure.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

        if (args.size() != 3)
            throw Exception("Table function 'file' requires exactly three arguments: path, format and structure.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < 3; ++i)
            args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);


//        UInt64 limit = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<UInt64>();
//
//        auto res = StorageSystemNumbers::create(getName(), false, limit);
//        res->startup();

//        return res;
    }


    void registerTableFunctionFile(TableFunctionFactory & factory)
    {
        factory.registerFunction<TableFunctionFile>();
    }

}
