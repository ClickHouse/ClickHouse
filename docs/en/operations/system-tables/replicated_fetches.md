# replicated_fetches {#system_tables-replicated_fetches}

Contains information about currently running background fetches.

Columns:

-   `database` ([String](../../sql-reference/data-types/string.md)) — Name of the database.

-   `table` ([String](../../sql-reference/data-types/string.md)) — Name of the table.

-   `elapsed` ([Float64](../../sql-reference/data-types/float.md)) — The time elapsed (in seconds) since showing currently running background fetches started.

-   `progress` ([Float64](../../sql-reference/data-types/float.md)) — The percentage of completed work from 0 to 1.

-   `result_part_name` ([String](../../sql-reference/data-types/string.md)) — The name of the part that will be formed as the result of showing currently running background fetches.

-   `result_part_path` ([String](../../sql-reference/data-types/string.md)) — Absolute path to the part that will be formed as the result of showing currently running background fetches.

-   `partition_id` ([String](../../sql-reference/data-types/string.md)) — ID of the partition.

-   `total_size_bytes_compressed` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The total size (in bytes) of the compressed data in the result part.

-   `bytes_read_compressed` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of compressed bytes read from the result part.

-   `source_replica_path` ([String](../../sql-reference/data-types/string.md)) — Absolute path to the source replica.

-   `source_replica_hostname` ([String](../../sql-reference/data-types/string.md)) — Hostname of the source replica.

-   `source_replica_port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — Port number of the source replica.

-   `interserver_scheme` ([String](../../sql-reference/data-types/string.md)) — Name of the interserver scheme.

-   `URI` ([String](../../sql-reference/data-types/string.md)) — Uniform resource identifier.

-   `to_detached` ([UInt8](../../sql-reference/data-types/int-uint.md)) — The flag indicates whether the currently running background fetch is being performed using the `TO DETACHED` expression.

-   `thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Thread identifier.

**Example**

``` sql
SELECT * FROM system.replicated_fetches LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
database:                    default
table:                       t
elapsed:                     7.243039876
progress:                    0.41832135995612835
result_part_name:            all_0_0_0
result_part_path:            /var/lib/clickhouse/store/700/70080a04-b2de-4adf-9fa5-9ea210e81766/all_0_0_0/
partition_id:                all
total_size_bytes_compressed: 1052783726
bytes_read_compressed:       440401920
source_replica_path:         /clickhouse/test/t/replicas/1
source_replica_hostname:     node1
source_replica_port:         9009
interserver_scheme:          http
URI:                         http://node1:9009/?endpoint=DataPartsExchange%3A%2Fclickhouse%2Ftest%2Ft%2Freplicas%2F1&part=all_0_0_0&client_protocol_version=4&compress=false
to_detached:                 0
thread_id:                   54
```

**See Also**

-   [Managing ReplicatedMergeTree Tables](../../sql-reference/statements/system/#query-language-system-replicated)

[Original article](https://clickhouse.com/docs/en/operations/system_tables/replicated_fetches) <!--hide-->
