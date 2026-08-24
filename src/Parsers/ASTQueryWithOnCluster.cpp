#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

std::string ASTQueryWithOnCluster::getRewrittenQueryWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const
{
    return getRewrittenASTWithoutOnCluster(params)->formatWithSecretsOneLine();
}


bool ASTQueryWithOnCluster::parse(Pos & pos, std::string & cluster_str, Expected & expected)
{
    if (!ParserKeyword(Keyword::CLUSTER).ignore(pos, expected))
        return false;

    return parseIdentifierOrStringLiteral(pos, expected, cluster_str);
}


void ASTQueryWithOnCluster::formatOnCluster(WriteBuffer & ostr, const IAST::FormatSettings &) const
{
    if (!cluster.empty())
    {
        ostr << " ON CLUSTER " << backQuoteIfNeed(cluster);
    }
}


}

namespace DB
{

void registerStatementOnCluster(StatementFactory & factory)
{
    factory.registerStatement("ON CLUSTER",
    {
        .description = R"(
For statements that support this clause, `ON CLUSTER` executes the query on all the servers of a cluster instead of only
on the server which received it. The query is put into the distributed DDL queue (see `system.distributed_ddl_queue`)
and is executed by every server of the cluster.

**Examples**

**Create a table on every server of a cluster**

```sql title="Query"
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits);
```
)",
        .syntax = R"(
<query> ... ON CLUSTER cluster ...
)",
        .related = {"CREATE", "DROP", "ALTER", "RENAME", "SYSTEM"},
    });
}

}
