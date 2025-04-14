#include <iostream>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <IO/WriteBufferFromOStream.h>


int main(int, char **)
{
    using namespace DB;

    std::string input = "CREATE TABLE hits (URL String, UserAgentMinor2 FixedString(2), EventTime DateTime) ENGINE = Log";
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0, 0);

    WriteBufferFromOStream out(std::cerr, 4096);
    formatAST(*ast, out);
    std::cerr << std::endl;

    return 0;
}
