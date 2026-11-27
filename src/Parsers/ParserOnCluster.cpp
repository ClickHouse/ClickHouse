#include <Parsers/ParserOnCluster.h>

namespace DB
{

bool ParserOnCluster::parseImpl(Pos &, ASTPtr &, Expected &)
{
    return false;
}

std::map<String, Documentation> ParserOnCluster::getDocumentation() const
{
    std::map<String, Documentation> documentation;

    documentation["ON CLUSTER"] =
    {
        .description = R"DOCS_MD(
By default, the `CREATE`, `DROP`, `ALTER`, and `RENAME` queries affect only the current server where they are executed. In a cluster setup, it is possible to run such queries in a distributed manner with the `ON CLUSTER` clause.

For example, the following query creates the `all_hits` `Distributed` table on each host in `cluster`:

```sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

In order to run these queries correctly, each host must have the same cluster definition (to simplify syncing configs, you can use substitutions from ZooKeeper). They must also connect to the ZooKeeper servers.

In general, the local version of the query will eventually be executed on each host in the cluster, even if some hosts are currently not available. However, these queries are stored in a queue, and the time an item remains in the queue is limited by [several settings](/reference/settings/server-settings/settings/distributed). As a result, if a host is unavailable for long enough, it may not execute the query when it becomes available again.

<Warning>
The order for executing queries within a single host is guaranteed as long as [distributed_ddl.pool_size](/reference/settings/server-settings/settings/distributed#distributed_ddl.pool_size) is set to 1.
</Warning>
)DOCS_MD",
        .syntax = R"(
<query> ... ON CLUSTER cluster ...
)",
        .related = {"CREATE", "DROP", "ALTER", "RENAME", "SYSTEM"},
    };

    return documentation;
}

}
