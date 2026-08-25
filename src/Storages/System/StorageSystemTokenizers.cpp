#include <Storages/System/StorageSystemTokenizers.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/TokenizerFactory.h>


namespace DB
{

ColumnsDescription StorageSystemTokenizers::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the tokenizer"}
    };
}

StorageSystemTokenizers::StorageSystemTokenizers(const StorageID & table_id)
    : IStorageSystemOneBlock(table_id, getColumnsDescription())
{
}

void StorageSystemTokenizers::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto & tokenizer_factory = TokenizerFactory::instance();
    const auto & tokenizers = tokenizer_factory.getAllTokenizers();

    for (const auto & tokenizer : tokenizers)
        res_columns[0]->insert(tokenizer.first);
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemTokenizers) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "tokenizers",
    .description = R"DOCS_MD(
Shows all available tokenizers.
These can be used in functions [tokens](/reference/functions/regular-functions/splitting-merging-functions#tokens), [hasAllTokens](/reference/functions/regular-functions/string-search-functions#hasAllTokens), [hasAnyTokens](/reference/functions/regular-functions/string-search-functions#hasAnyTokens), and the [text index](/reference/engines/table-engines/mergetree-family/textindexes).
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT * FROM system.tokenizers;
```

```text
┌─name────────────┐
│ ngrams          │
│ splitByNonAlpha │
│ sparseGrams     │
│ tokenbf_v1      │
│ ngrambf_v1      │
│ array           │
│ splitByString   │
│ sparse_grams    │
│ asciiCJK        │
│ icu             │
│ japanese        │
└─────────────────┘
```
)DOCS_MD")

}
