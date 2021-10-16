# system.licenses {#system-tables_system.licenses}

Сontains licenses of third-party libraries that are located in the [contrib](https://github.com/ClickHouse/ClickHouse/tree/master/contrib) directory of ClickHouse sources. 

Columns:

- `library_name` ([String](../../sql-reference/data-types/string.md)) — Name of the library, which is license connected with.
- `license_type` ([String](../../sql-reference/data-types/string.md)) — License type — e.g. Apache, MIT. 
- `license_path` ([String](../../sql-reference/data-types/string.md)) — Path to the file with the license text.
- `license_text` ([String](../../sql-reference/data-types/string.md)) — License text.

**Example**

``` sql
SELECT library_name, license_type, license_path FROM system.licenses LIMIT 15
```

``` text
┌─library_name───────┬─license_type─┬─license_path────────────────────────┐
│ FastMemcpy         │ MIT          │ /contrib/FastMemcpy/LICENSE         │
│ arrow              │ Apache       │ /contrib/arrow/LICENSE.txt          │
│ avro               │ Apache       │ /contrib/avro/LICENSE.txt           │
│ aws-c-common       │ Apache       │ /contrib/aws-c-common/LICENSE       │
│ aws-c-event-stream │ Apache       │ /contrib/aws-c-event-stream/LICENSE │
│ aws-checksums      │ Apache       │ /contrib/aws-checksums/LICENSE      │
│ aws                │ Apache       │ /contrib/aws/LICENSE.txt            │
│ base64             │ BSD 2-clause │ /contrib/base64/LICENSE             │
│ boost              │ Boost        │ /contrib/boost/LICENSE_1_0.txt      │
│ brotli             │ MIT          │ /contrib/brotli/LICENSE             │
│ capnproto          │ MIT          │ /contrib/capnproto/LICENSE          │
│ cassandra          │ Apache       │ /contrib/cassandra/LICENSE.txt      │
│ cctz               │ Apache       │ /contrib/cctz/LICENSE.txt           │
│ cityhash102        │ MIT          │ /contrib/cityhash102/COPYING        │
│ cppkafka           │ BSD 2-clause │ /contrib/cppkafka/LICENSE           │
└────────────────────┴──────────────┴─────────────────────────────────────┘

```

[Original article](https://clickhouse.tech/docs/en/operations/system-tables/licenses) <!--hide-->
