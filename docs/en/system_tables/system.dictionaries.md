# system.dictionaries

Contains information about external dictionaries.

Columns:

- `name String` – Dictionary name.
- `type String` – Dictionary type: Flat, Hashed, Cache.
- `origin String` – Path to the config file where the dictionary is described.
- `attribute.names Array(String)` – Array of attribute names provided by the dictionary.
- `attribute.types Array(String)` – Corresponding array of attribute types provided by the dictionary.
- `has_hierarchy UInt8` – Whether the dictionary is hierarchical.
- `bytes_allocated UInt64` – The amount of RAM used by the dictionary.
- `hit_rate Float64` – For cache dictionaries, the percent of usage for which the value  was in the cache.
- `element_count UInt64` – The number of items stored in the dictionary.
- `load_factor Float64` – The filled percentage of the dictionary (for a hashed dictionary, it is the filled percentage of the hash table).
- `creation_time DateTime` – Time spent for the creation or last successful reload of the dictionary.
- `last_exception String` – Text of an error that occurred when creating or reloading the dictionary, if the dictionary couldn't be created.
- `source String` – Text describing the data source for the dictionary.

Note that the amount of memory used by the dictionary is not proportional to the number of items stored in it. So for flat and cached dictionaries, all the memory cells are pre-assigned, regardless of how full the dictionary actually is.
