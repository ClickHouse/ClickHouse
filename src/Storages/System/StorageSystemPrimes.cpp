#include <Storages/System/StorageSystemPrimes.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromSystemPrimesStep.h>

namespace DB
{

StorageSystemPrimes::StorageSystemPrimes(
    const StorageID & table_id, const std::string & column_name_, std::optional<UInt64> limit_, UInt64 offset_, UInt64 step_)
    : StorageWithCommonVirtualColumns(table_id)
    , limit(limit_)
    , offset(offset_)
    , column_name(column_name_)
    , step(step_)
{
    StorageInMemoryMetadata storage_metadata;
    /// This column doesn't have a comment, because otherwise it will be added to all the tables which were created via
    /// CREATE TABLE test as primes(5)
    storage_metadata.setColumns(ColumnsDescription({{column_name_, std::make_shared<DataTypeUInt64>()}}));
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageSystemPrimes::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

void StorageSystemPrimes::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    query_plan.addStep(
        std::make_unique<ReadFromSystemPrimesStep>(
            column_names, query_info, storage_snapshot, context, shared_from_this(), max_block_size));
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemPrimes) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "primes",
    .description = R"DOCS_MD(
This table contains a single UInt64 column named `prime` that contains prime numbers in ascending order, starting from 2.

You can use this table for tests, or if you need to do a brute force search over prime numbers.

Reads from this table are not parallelized.

This is similar to the [`primes`](/reference/functions/table-functions/primes) table function.

You can also limit the output by predicates.
)DOCS_MD",
    .examples = R"DOCS_MD(
The first 10 primes.
```sql
SELECT * FROM system.primes LIMIT 10;
```

```response
  ┌─prime─┐
  │     2 │
  │     3 │
  │     5 │
  │     7 │
  │    11 │
  │    13 │
  │    17 │
  │    19 │
  │    23 │
  │    29 │
  └───────┘
```

The first prime greater than 1e15.
```sql
SELECT prime FROM system.primes WHERE prime > 1e15 LIMIT 1;
```

```response
  ┌────────────prime─┐
  │ 1000000000000037 │ -- 1.00 quadrillion
  └──────────────────┘
```
)DOCS_MD")

}
