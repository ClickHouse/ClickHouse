#include <Parsers/queryToString.h>
#include <Parsers/formatAST.h>
#include <sstream>

namespace DB
{
    String queryToString(const ASTPtr & query)
    {
        std::ostringstream out;
        formatAST(*query, out, false, true);

        return out.str();
    }
}
