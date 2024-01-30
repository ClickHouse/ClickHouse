#include <Parsers/queryToString.h>
#include <Parsers/formatAST.h>

namespace DB
{
    String queryToStringNullable(const ASTPtr & query)
    {
        return query ? queryToString(query) : "";
    }

    String queryToString(const ASTPtr & query)
    {
        return queryToString(*query);
    }

    String queryToString(const IAST & query)
    {
        return serializeAST(query);
    }
}
