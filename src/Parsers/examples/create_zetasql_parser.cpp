#include <iostream>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <IO/WriteBufferFromOStream.h>
#include <Parsers/ZetaSQL/ParserZetaSQLQuery.h>


int main(int, char **)
{
    using namespace DB;

    std::string input = "FROM system.users |> aggregate count(*) as c group by auth_type";
    ZetaSQL::ParserZetaSQLQuery parser;

    try
    {
        ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 1000000, 10000000, 10000000);
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
