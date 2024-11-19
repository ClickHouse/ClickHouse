---
slug: /ja/sql-reference/table-functions/merge
sidebar_position: 130
sidebar_label: merge
---

# merge

一時的な[Merge](../../engines/table-engines/special/merge.md)テーブルを作成します。テーブルの構造は、正規表現に一致する最初のテーブルから取得されます。

**構文**

```sql
merge(['db_name',] 'tables_regexp')
```
**引数**

- `db_name` — 以下のような値が可能です（オプション、デフォルトは`currentDatabase()`）:
    - データベース名、
    - データベース名を文字列として返す定数式、例えば`currentDatabase()`、
    - `REGEXP(expression)`、ここで`expression`はDB名に一致させるための正規表現です。

- `tables_regexp` — 指定されたDBまたはDB群でテーブル名と一致させるための正規表現。

**関連項目**

- [Merge](../../engines/table-engines/special/merge.md)テーブルエンジン
