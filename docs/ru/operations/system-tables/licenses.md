# system.licenses {#system-tables_system.licenses}

Содержит информацию о лицензиях сторонних библиотек, которые находятся в директории [contrib](https://github.com/ClickHouse/ClickHouse/tree/master/contrib) исходных кодов ClickHouse.

Столбцы:

- `library_name` ([String](../../sql-reference/data-types/string.md)) — Название библиотеки, к которой относится лицензия.
- `license_type` ([String](../../sql-reference/data-types/string.md)) — Тип лицензии, например, Apache, MIT.
- `license_path` ([String](../../sql-reference/data-types/string.md)) — Путь к файлу с текстом лицензии.
- `license_text` ([String](../../sql-reference/data-types/string.md)) — Текст лицензии.

**Пример**

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

