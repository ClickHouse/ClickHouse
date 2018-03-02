#include <iostream>

#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>


int main(int, char **)
try
{
    using namespace DB;

    std::string input =
        " SELECT 18446744073709551615, f(1), '\\\\', [a, b, c], (a, b, c), 1 + 2 * -3, a = b OR c > d.1 + 2 * -g[0] AND NOT e < f * (x + y)"
        " FROM default.hits"
        " WHERE CounterID = 101500 AND UniqID % 3 = 0"
        " GROUP BY UniqID"
        " HAVING SUM(Refresh) > 100"
        " ORDER BY Visits, PageViews"
        " LIMIT 1000, 10"
        " INTO OUTFILE 'test.out'"
        " FORMAT TabSeparated";

    ParserQueryWithOutput parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "");

    std::cout << "Success." << std::endl;
    formatAST(*ast, std::cerr);
    std::cout << std::endl;

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
    return 1;
}
