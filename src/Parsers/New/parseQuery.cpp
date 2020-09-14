#include <Parsers/New/parseQuery.h>

#include <Parsers/New/AST/Query.h>

#include <Parsers/New/ClickHouseLexer.h>
#include <Parsers/New/ClickHouseParser.h>
#include <Parsers/New/LexerErrorListener.h>
#include <Parsers/New/ParserErrorListener.h>
#include <Parsers/New/ParseTreeVisitor.h>

#include <ANTLRInputStream.h>
#include <CommonTokenStream.h>


namespace DB
{

ASTPtr parseQuery(const std::string & query)
{
    using namespace antlr4;

    ANTLRInputStream input(query);
    ClickHouseLexer lexer(&input);
    CommonTokenStream tokens(&lexer);
    ClickHouseParser parser(&tokens);
    LexerErrorListener lexer_error_listener;
    ParserErrorListener parser_error_listener;

    lexer.removeErrorListeners();
    lexer.addErrorListener(&lexer_error_listener);

    parser.removeErrorListeners();
    parser.addErrorListener(&parser_error_listener);

    ParseTreeVisitor visitor;

    AST::PtrTo<AST::QueryList> new_ast = visitor.visit(parser.input());

    new_ast->dump();

    return new_ast->convertToOld();
}

}
