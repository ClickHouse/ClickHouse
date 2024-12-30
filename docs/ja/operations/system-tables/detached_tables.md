---
slug: /ja/operations/system-tables/detached_tables
---
# detached_tables

各分離されたテーブルの情報を含みます。

カラム:

- `database` ([String](../../sql-reference/data-types/string.md)) — テーブルが所属するデータベースの名前。

- `table` ([String](../../sql-reference/data-types/string.md)) — テーブル名。

- `uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — テーブルのUUID（Atomicデータベース）。

- `metadata_path` ([String](../../sql-reference/data-types/string.md)) - ファイルシステム内のテーブルメタデータへのパス。

- `is_permanently` ([UInt8](../../sql-reference/data-types/int-uint.md)) - テーブルがPERMANENTLYで分離されたことを示すフラグ。


**例**

```sql
SELECT * FROM system.detached_tables FORMAT Vertical;
```

```text
Row 1:
──────
database:                   base
table:                      t1
uuid:                       81b1c20a-b7c6-4116-a2ce-7583fb6b6736
metadata_path:              /var/lib/clickhouse/store/461/461cf698-fd0b-406d-8c01-5d8fd5748a91/t1.sql
is_permanently:             1
```
