#include <Analyzers/TranslatePositionalArguments.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/formatAST.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>


/// Parses query from stdin and print same query with translated positional arguments.

int main(int, char **)
try
{
    using namespace DB;

    ReadBufferFromFileDescriptor in(STDIN_FILENO);

    String query;
    readStringUntilEOF(query, in);

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "query");

    TranslatePositionalArguments translator;
    translator.process(ast);

    formatAST(*ast, std::cout, false);
    std::cout << "\n";
    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
    return 1;
}
