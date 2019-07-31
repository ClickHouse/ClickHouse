#include <Parsers/ASTExpressionList.h>
#include <Parsers/ParserCreateQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

void parseColumnsListFromString(const std::string & structure, Block & sample_block, const Context & context)
{
    Expected expected;

    Tokens tokens(structure.c_str(), structure.c_str() + structure.size());
    TokenIterator token_iterator(tokens);

    ParserColumnDeclarationList parser;
    ASTPtr columns_list_raw;

    if (!parser.parse(token_iterator, columns_list_raw, expected))
        throw Exception("Cannot parse columns declaration list.", ErrorCodes::SYNTAX_ERROR);

    auto * columns_list = dynamic_cast<ASTExpressionList *>(columns_list_raw.get());
    if (!columns_list)
        throw Exception("Could not cast AST to ASTExpressionList", ErrorCodes::LOGICAL_ERROR);

    ColumnsDescription columns_desc = InterpreterCreateQuery::getColumnsDescription(*columns_list, context);

    for (const auto & [name, type]: columns_desc.getAllPhysical())
    {
        ColumnWithTypeAndName column;
        column.name = name;
        column.type = type;
        column.column = type->createColumn();
        sample_block.insert(std::move(column));
    }
}

}