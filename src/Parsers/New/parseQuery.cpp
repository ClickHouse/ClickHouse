#include <strstream>

#include <Parsers/New/parseQuery.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/New/AST/InsertQuery.h>
#include <Parsers/New/ClickHouseLexer.h>
#include <Parsers/New/ClickHouseParser.h>
#include <Parsers/New/LexerErrorListener.h>
#include <Parsers/New/ParseTreeVisitor.h>
#include <Parsers/New/ParserErrorListener.h>

#include <CommonTokenStream.h>


namespace DB
{

using namespace antlr4;
using namespace AST;

// For testing only
PtrTo<Query> parseQuery(const String & query)
{
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

    return visitor.visit(parser.queryStmt());
}

ASTPtr parseQuery(const char * begin, const char * end, size_t, size_t)
{
    // TODO: do not ignore |max_parser_depth|.

    size_t size = end - begin;
    std::strstreambuf buffer(begin, size);
    std::wbuffer_convert<std::codecvt_utf8<wchar_t>> converter(&buffer);
    std::wistream stream(&converter);

    UnbufferedCharStream input(stream, size);
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
    auto old_ast = new_ast->convertToOld();

    if (const auto * insert = new_ast->as<InsertQuery>())
    {
        auto * old_insert = old_ast->as<ASTInsertQuery>();

        old_insert->end = end;
        if (insert->hasData())
        {
            old_insert->data = begin + insert->getDataOffset();

            // Data starts after the first newline, if there is one, or after all the whitespace characters, otherwise.
            auto & data = old_insert->data;
            while (data < end && (*data == ' ' || *data == '\t' || *data == '\f')) ++data;
            if (data < end && *data == '\r') ++data;
            if (data < end && *data == '\n') ++data;
        }

        old_insert->data = (old_insert->data != end) ? old_insert->data : nullptr;
    }

    return old_ast;
}

}
