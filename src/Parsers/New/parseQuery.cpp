#include <Parsers/New/parseQuery.h>

#include <Parsers/New/AST/Query.h>
#include <Parsers/New/ClickHouseLexer.h>
#include <Parsers/New/ClickHouseParser.h>
#include <Parsers/New/LexerErrorListener.h>
#include <Parsers/New/ParseTreeVisitor.h>
#include <Parsers/New/ParserErrorListener.h>

#include <ANTLRInputStream.h>
#include <CommonTokenStream.h>


namespace DB
{

using namespace antlr4;
using namespace AST;

ASTPtr parseQuery(const char * begin, const char * end, size_t, size_t)
{
    // TODO: do not ignore |max_parser_depth|.

    String query(begin, end); // FIXME: implement zero-copy ANTLRInputStream which checks for |max_query_size| internally.

    ANTLRInputStream input(query);
    ClickHouseLexer lexer(&input);
    CommonTokenStream tokens(&lexer);
    ClickHouseParser parser(&tokens);
    LexerErrorListener lexer_error_listener;
    ParserErrorListener parser_error_listener;

    lexer.removeErrorListeners();
    parser.removeErrorListeners();
    lexer.addErrorListener(&lexer_error_listener);
    parser.addErrorListener(&parser_error_listener);

    ParseTreeVisitor visitor;

    PtrTo<Query> new_ast = visitor.visit(parser.queryStmt());

    // new_ast->dump();

    return new_ast->convertToOld();
}

}
