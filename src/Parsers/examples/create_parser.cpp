#include <iostream>

#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <IO/WriteBufferFromOStream.h>


int main(int, char **)
{
    using namespace DB;

    std::string input = "CREATE TABLE hits (URL String, UserAgentMinor2 FixedString(2), EventTime DateTime) ENGINE = Log";
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0, 0);

    std::cerr << ast->formatWithSecretsOneLine() << std::endl;
    return 0;
}
