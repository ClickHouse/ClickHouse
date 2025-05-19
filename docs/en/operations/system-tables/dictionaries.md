---
description: 'System table containing information about dictionaries'
keywords: ['system table', 'dictionaries']
slug: /operations/system-tables/dictionaries
title: 'system.dictionaries'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

<SystemTableCloud/>

Contains information about [dictionaries](../../sql-reference/dictionaries/index.md).

Columns:

- `database` ([String](../../sql-reference/data-types/string.md)) — Name of the database containing the dictionary created by DDL query. Empty string for other dictionaries.
- `name` ([String](../../sql-reference/data-types/string.md)) — [Dictionary name](../../sql-reference/dictionaries/index.md).
- `uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — Dictionary UUID.
- `status` ([Enum8](../../sql-reference/data-types/enum.md)) — Dictionary status. Possible values:
    - `NOT_LOADED` — Dictionary was not loaded because it was not used.
    - `LOADED` — Dictionary loaded successfully.
    - `FAILED` — Unable to load the dictionary as a result of an error.
    - `LOADING` — Dictionary is loading now.
    - `LOADED_AND_RELOADING` — Dictionary is loaded successfully, and is being reloaded right now (frequent reasons: [SYSTEM RELOAD DICTIONARY](/sql-reference/statements/system#reload-dictionaries) query, timeout, dictionary config has changed).
    - `FAILED_AND_RELOADING` — Could not load the dictionary as a result of an error and is loading now.
- `origin` ([String](../../sql-reference/data-types/string.md)) — Path to the configuration file that describes the dictionary.
- `type` ([String](../../sql-reference/data-types/string.md)) — Type of dictionary allocation. [Storing Dictionaries in Memory](/sql-reference/dictionaries#storing-dictionaries-in-memory).
- `key.names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Array of [key names](/operations/system-tables/dictionaries) provided by the dictionary.
- `key.types` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Corresponding array of [key types](/sql-reference/dictionaries#dictionary-key-and-fields) provided by the dictionary.
- `attribute.names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Array of [attribute names](/sql-reference/dictionaries#dictionary-key-and-fields) provided by the dictionary.
- `attribute.types` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Corresponding array of [attribute types](/sql-reference/dictionaries#dictionary-key-and-fields) provided by the dictionary.
- `bytes_allocated` ([UInt64](/sql-reference/data-types/int-uint#integer-ranges)) — Amount of RAM allocated for the dictionary.
- `query_count` ([UInt64](/sql-reference/data-types/int-uint#integer-ranges)) — Number of queries since the dictionary was loaded or since the last successful reboot.
- `hit_rate` ([Float64](../../sql-reference/data-types/float.md)) — For cache dictionaries, the percentage of uses for which the value was in the cache.
- `found_rate` ([Float64](../../sql-reference/data-types/float.md)) — The percentage of uses for which the value was found.
- `element_count` ([UInt64](/sql-reference/data-types/int-uint#integer-ranges)) — Number of items stored in the dictionary.
- `load_factor` ([Float64](../../sql-reference/data-types/float.md)) — Percentage filled in the dictionary (for a hashed dictionary, the percentage filled in the hash table).
- `source` ([String](../../sql-reference/data-types/string.md)) — Text describing the [data source](../../sql-reference/dictionaries/index.md#dictionary-sources) for the dictionary.
- `lifetime_min` ([UInt64](/sql-reference/data-types/int-uint#integer-ranges)) — Minimum [lifetime](/sql-reference/dictionaries#refreshing-dictionary-data-using-lifetime) of the dictionary in memory, after which ClickHouse tries to reload the dictionary (if `invalidate_query` is set, then only if it has changed). Set in seconds.
- `lifetime_max` ([UInt64](/sql-reference/data-types/int-uint#integer-ranges)) — Maximum [lifetime](/sql-reference/dictionaries#refreshing-dictionary-data-using-lifetime) of the dictionary in memory, after which ClickHouse tries to reload the dictionary (if `invalidate_query` is set, then only if it has changed). Set in seconds.
- `loading_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Start time for loading the dictionary.
- `last_successful_update_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — End time for loading or updating the dictionary. Helps to monitor some troubles with dictionary sources and investigate the causes.
- `loading_duration` ([Float32](../../sql-reference/data-types/float.md)) — Duration of a dictionary loading.
- `last_exception` ([String](../../sql-reference/data-types/string.md)) — Text of the error that occurs when creating or reloading the dictionary if the dictionary couldn't be created.
- `comment` ([String](../../sql-reference/data-types/string.md)) — Text of the comment to dictionary.

**Example**

Configure the dictionary:

```sql
CREATE DICTIONARY dictionary_with_comment
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'source_table'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000)
COMMENT 'The temporary dictionary';
```

Make sure that the dictionary is loaded.

```sql
SELECT * FROM system.dictionaries LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
database:                    default
name:                        dictionary_with_comment
uuid:                        4654d460-0d03-433a-8654-d4600d03d33a
status:                      NOT_LOADED
origin:                      4654d460-0d03-433a-8654-d4600d03d33a
type:
key.names:                   ['id']
key.types:                   ['UInt64']
attribute.names:             ['value']
attribute.types:             ['String']
bytes_allocated:             0
query_count:                 0
hit_rate:                    0
found_rate:                  0
element_count:               0
load_factor:                 0
source:
lifetime_min:                0
lifetime_max:                0
loading_start_time:          1970-01-01 00:00:00
last_successful_update_time: 1970-01-01 00:00:00
loading_duration:            0
last_exception:
comment:                     The temporary dictionary
```
