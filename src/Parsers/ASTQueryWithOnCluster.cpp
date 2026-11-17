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
        .description = R"DOCS_MD(
By default, the `CREATE`, `DROP`, `ALTER`, and `RENAME` queries affect only the current server where they are executed. In a cluster setup, it is possible to run such queries in a distributed manner with the `ON CLUSTER` clause.

For example, the following query creates the `all_hits` `Distributed` table on each host in `cluster`:

```sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

In order to run these queries correctly, each host must have the same cluster definition (to simplify syncing configs, you can use substitutions from ZooKeeper). They must also connect to the ZooKeeper servers.

The local version of the query will eventually be executed on each host in the cluster, even if some hosts are currently not available.

<Warning>
The order for executing queries within a single host is guaranteed.
</Warning>
)DOCS_MD",
        .syntax = R"(
<query> ... ON CLUSTER cluster ...
)",
        .related = {"CREATE", "DROP", "ALTER", "RENAME", "SYSTEM"},
    });
}

}
