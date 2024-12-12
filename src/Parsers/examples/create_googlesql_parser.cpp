#include <iostream>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <IO/WriteBufferFromOStream.h>
#include "Parsers/GoogleSQL/ParserGoogleSQLQuery.h"


int main(int, char **)
{
    using namespace DB;

    std::string input = "FROM orders";
    GoogleSQL::ParserGoogleSQLQuery parser;

    try
    {
        ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0, 0);
        WriteBufferFromFileDescriptor out(STDOUT_FILENO);
        formatAST(*ast, out);
        out.finalize();
    }
    catch (const DB::Exception & e)
    {
        std::cout << "ClickHouse Exception: " << e.message() << "\n";
    }

    return 0;
}
