#include <Parsers/ASTExpressionList.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/Context.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ColumnsDescription parseColumnsListFromString(const std::string & structure, const Context & context)
{
    ParserColumnDeclarationList parser;
    const Settings & settings = context.getSettingsRef();

    ASTPtr columns_list_raw = parseQuery(parser, structure, "columns declaration list", settings.max_query_size, settings.max_parser_depth);

    auto * columns_list = dynamic_cast<ASTExpressionList *>(columns_list_raw.get());
    if (!columns_list)
        throw Exception("Could not cast AST to ASTExpressionList", ErrorCodes::LOGICAL_ERROR);

    return InterpreterCreateQuery::getColumnsDescription(*columns_list, context, !settings.allow_suspicious_codecs);
}

}
