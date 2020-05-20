#include <Parsers/ASTExpressionList.h>
#include <Parsers/ParserCreateQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/Context.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SYNTAX_ERROR;
}

ColumnsDescription parseColumnsListFromString(const std::string & structure, const Context & context)
{
    Expected expected;

    Tokens tokens(structure.c_str(), structure.c_str() + structure.size());
    IParser::Pos token_iterator(tokens, context.getSettingsRef().max_parser_depth);

    ParserColumnDeclarationList parser;
    ASTPtr columns_list_raw;

    if (!parser.parse(token_iterator, columns_list_raw, expected))
        throw Exception("Cannot parse columns declaration list.", ErrorCodes::SYNTAX_ERROR);

    auto * columns_list = dynamic_cast<ASTExpressionList *>(columns_list_raw.get());
    if (!columns_list)
        throw Exception("Could not cast AST to ASTExpressionList", ErrorCodes::LOGICAL_ERROR);

    return InterpreterCreateQuery::getColumnsDescription(*columns_list, context, !context.getSettingsRef().allow_suspicious_codecs);
}

}
