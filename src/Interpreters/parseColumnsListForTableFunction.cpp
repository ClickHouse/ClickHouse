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

ColumnsDescription parseColumnsListFromString(const std::string & structure, ContextPtr context)
{
    ParserColumnDeclarationList parser(true, true);
    const Settings & settings = context->getSettingsRef();

    ASTPtr columns_list_raw = parseQuery(parser, structure, "columns declaration list", settings.max_query_size, settings.max_parser_depth);

    auto * columns_list = dynamic_cast<ASTExpressionList *>(columns_list_raw.get());
    if (!columns_list)
        throw Exception("Could not cast AST to ASTExpressionList", ErrorCodes::LOGICAL_ERROR);

    return InterpreterCreateQuery::getColumnsDescription(*columns_list, context, false);
}

bool tryParseColumnsListFromString(const std::string & structure, ColumnsDescription & columns, ContextPtr context)
{
    ParserColumnDeclarationList parser(true, true);
    const Settings & settings = context->getSettingsRef();

    String error;
    const char * start = structure.data();
    const char * end = structure.data() + structure.size();
    ASTPtr columns_list_raw = tryParseQuery(parser, start, end, error, false, "columns declaration list", false, settings.max_query_size, settings.max_parser_depth);
    if (!columns_list_raw)
        return false;

    auto * columns_list = dynamic_cast<ASTExpressionList *>(columns_list_raw.get());
    if (!columns_list)
        return false;

    try
    {
        columns = InterpreterCreateQuery::getColumnsDescription(*columns_list, context, false);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

}
