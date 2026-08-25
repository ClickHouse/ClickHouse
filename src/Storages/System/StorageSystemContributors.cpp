#include <Storages/System/StorageSystemContributors.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Columns/IColumn.h>
#include <Common/thread_local_rng.h>
#include <DataTypes/DataTypeString.h>

#include <algorithm>


extern const char * auto_contributors[];

namespace DB
{
ColumnsDescription StorageSystemContributors::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Contributor (author) name from git log."},
    };
}

void StorageSystemContributors::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    std::vector<const char *> contributors;
    for (auto * it = auto_contributors; *it; ++it)
        contributors.emplace_back(*it);

    std::shuffle(contributors.begin(), contributors.end(), thread_local_rng);

    for (auto & it : contributors)
        res_columns[0]->insert(String(it));
}
}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemContributors) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "contributors",
    .description = R"DOCS_MD(
Contains information about contributors. The order is random at query execution time.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT * FROM system.contributors LIMIT 10
```

```text
┌─name─────────────┐
│ Olga Khvostikova │
│ Max Vetrov       │
│ LiuYangkuan      │
│ svladykin        │
│ zamulla          │
│ Šimon Podlipský  │
│ BayoNet          │
│ Ilya Khomutov    │
│ Amy Krishnevsky  │
│ Loud_Scream      │
└──────────────────┘
```

To find out yourself in the table, use a query:

```sql
SELECT * FROM system.contributors WHERE name = 'Olga Khvostikova'
```

```text
┌─name─────────────┐
│ Olga Khvostikova │
└──────────────────┘
```
)DOCS_MD")

}
