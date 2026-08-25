#include "config.h"
#include <Common/SystemTableDocumentation.h>

#if USE_LIBSTEMMER

#include <Storages/System/StorageSystemStemmers.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>

#include <libstemmer.h>


namespace DB
{

ColumnsDescription StorageSystemStemmers::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Identifier of the Snowball stemmer (language/algorithm)"}
    };
}

StorageSystemStemmers::StorageSystemStemmers(const StorageID & table_id)
    : IStorageSystemOneBlock(table_id, getColumnsDescription())
{
}

void StorageSystemStemmers::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (const char ** language = sb_stemmer_list(); *language != nullptr; ++language)
        res_columns[0]->insert(String(*language));
}

}

#endif /// USE_LIBSTEMMER

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "stemmers",
    .description = R"DOCS_MD(
Shows all available stemmers.
These can be used in the function [stem](/reference/functions/regular-functions/nlp-functions).

<Info>
**Availability**

`system.stemmers` is present only in ClickHouse builds compiled with the `libstemmer` dependency (`USE_LIBSTEMMER`). On builds without it, the table does not exist and queries against it will fail with `UNKNOWN_TABLE`. You can check whether your build has it enabled with:

```sql
SELECT value FROM system.build_options WHERE name = 'USE_LIBSTEMMER';
```
</Info>
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT * FROM system.stemmers;
```

```text
 ┌─name───────┐
 │ arabic     │
 │ armenian   │
 │ basque     │
 │ catalan    │
 │ danish     │
 │ dutch      │
 │ english    │
 │ [...]      │
 └────────────┘
```
)DOCS_MD")

}
