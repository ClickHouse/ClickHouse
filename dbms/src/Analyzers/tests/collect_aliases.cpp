#include <DB/Analyzers/CollectAliases.h>
#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/Common/Exception.h>


/// Parses query from stdin and print found aliases.

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

    CollectAliases collector;
    collector.process(ast);
    collector.dump(out);

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
    return 1;
}
