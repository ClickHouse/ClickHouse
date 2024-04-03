---
slug: /en/operations/system-tables/licenses
---
# licenses

Contains licenses of third-party libraries that are located in the [contrib](https://github.com/ClickHouse/ClickHouse/tree/master/contrib) directory of ClickHouse sources.

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
│ aws-c-common       │ Apache       │ /contrib/aws-c-common/LICENSE       │
│ base64             │ BSD 2-clause │ /contrib/aklomp-base64/LICENSE      │
│ brotli             │ MIT          │ /contrib/brotli/LICENSE             │
│ [...]              │ [...]        │ [...]                               │
└────────────────────┴──────────────┴─────────────────────────────────────┘

```
