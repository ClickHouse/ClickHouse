#include <Parsers/queryToString.h>
#include <Parsers/formatAST.h>

namespace DB
{
    String queryToStringWithEmptyTupleNormalization(const ASTPtr & query)
    {
        auto query_string = queryToStringNullable(query);

        return query_string == "tuple()" ? "" : query_string;
    }

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
