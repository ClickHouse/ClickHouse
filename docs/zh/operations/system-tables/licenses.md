# system.licenses {#system-tables_system.licenses}

包含位于 ClickHouse 源的 [contrib](https://github.com/ClickHouse/ClickHouse/tree/master/contrib) 目录中的第三方库的许可证.

列信息:

- `library_name` ([String](../../sql-reference/data-types/string.md)) — 库的名称, 它是与之连接的许可证.
- `license_type` ([String](../../sql-reference/data-types/string.md)) — 许可类型-例如Apache, MIT.
- `license_path` ([String](../../sql-reference/data-types/string.md)) — 带有许可文本的文件的路径.
- `license_text` ([String](../../sql-reference/data-types/string.md)) — 许可协议文本.

**示例**

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

[原始文章](https://clickhouse.com/docs/en/operations/system-tables/licenses) <!--hide-->
