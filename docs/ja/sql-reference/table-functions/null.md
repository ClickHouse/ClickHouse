---
slug: /ja/sql-reference/table-functions/null
sidebar_position: 140
sidebar_label: null 関数
title: 'null'
---

指定された構造の一時テーブルを [Null](../../engines/table-engines/special/null.md) テーブルエンジンで作成します。`Null` エンジンの特性により、テーブルデータは無視され、クエリの実行後すぐにテーブル自体が削除されます。この関数は、テストやデモンストレーションを行う際の利便性のために使用されます。

**構文**

``` sql
null('structure')
```

**パラメータ**

- `structure` — カラムとカラムタイプのリスト。[String](../../sql-reference/data-types/string.md)。

**返される値**

指定された構造を持つ一時的な `Null` エンジンテーブル。

**例**

`null` 関数を使用したクエリ:

``` sql
INSERT INTO function null('x UInt64') SELECT * FROM numbers_mt(1000000000);
```
は次の三つのクエリを置き換えることができます:

```sql
CREATE TABLE t (x UInt64) ENGINE = Null;
INSERT INTO t SELECT * FROM numbers_mt(1000000000);
DROP TABLE IF EXISTS t;
```

関連項目:

- [Null テーブルエンジン](../../engines/table-engines/special/null.md)

