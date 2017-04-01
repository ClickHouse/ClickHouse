#include <DB/Analyzers/AnalyzeLambdas.h>
#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/Common/Exception.h>
#include <DB/Interpreters/Context.h>


/// Parses query from stdin and print found higher order functions and query with rewritten names of lambda parameters.

int main(int argc, char ** argv)
try
{
    using namespace DB;

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    String query;
    readStringUntilEOF(query, in);

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "query");

    AnalyzeLambdas analyzer;
    analyzer.process(ast);

    analyzer.dump(out);
    out.next();

    std::cout << "\n";
    formatAST(*ast, std::cout, 0, false, true);
    std::cout << "\n";

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
    return 1;
}
