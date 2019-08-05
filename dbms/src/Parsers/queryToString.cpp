#include <Parsers/queryToString.h>
#include <Parsers/formatAST.h>
#include <sstream>

namespace DB
{
    String queryToString(const ASTPtr & query, bool mask_password)
    {
        return queryToString(*query, mask_password);
    }

    String queryToString(const IAST & query, bool mask_password)
    {
        std::ostringstream out;
        formatAST(query, out, false, true, mask_password);
        return out.str();
    }
}
