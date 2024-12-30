---
slug: /ja/operations/system-tables/licenses
---
# licenses

ClickHouseソースの[contrib](https://github.com/ClickHouse/ClickHouse/tree/master/contrib)ディレクトリにあるサードパーティライブラリのライセンスを含みます。

カラム:

- `library_name` ([String](../../sql-reference/data-types/string.md)) — ライセンスが関連しているライブラリの名前。
- `license_type` ([String](../../sql-reference/data-types/string.md)) — ライセンスタイプ — 例えば、Apache、MITなど。
- `license_path` ([String](../../sql-reference/data-types/string.md)) — ライセンステキストが含まれているファイルへのパス。
- `license_text` ([String](../../sql-reference/data-types/string.md)) — ライセンステキスト。

**例**

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
