---
description: '指定した構造の一時テーブルを Null テーブルエンジンで作成します。この関数は、テスト作成やデモンストレーションのために使われます。'
sidebar_label: 'null 関数'
sidebar_position: 140
slug: /sql-reference/table-functions/null
title: 'null'
doc_type: 'reference'
---

指定した構造の一時テーブルを [Null](../../engines/table-engines/special/null.md) テーブルエンジンで作成します。`Null` エンジンの特性上、テーブルデータは無視され、テーブル自体もクエリ実行直後にただちにドロップされます。この関数は、テスト作成やデモンストレーションのために使われます。

<div id="syntax">
  ## 構文
</div>

```sql
null('structure')
```

<div id="argument">
  ## 引数
</div>

* `structure` — カラムとカラム型のリスト。[String](../../sql-reference/data-types/string.md)。

<div id="returned_value">
  ## 戻り値
</div>

指定された構造を持つ一時的な `Null` エンジンのテーブル。

<div id="example">
  ## 例
</div>

`null` 関数を使用したクエリ:

```sql
INSERT INTO function null('x UInt64') SELECT * FROM numbers_mt(1000000000);
```

3つのクエリを置き換えることができます:

```sql
CREATE TABLE t (x UInt64) ENGINE = Null;
INSERT INTO t SELECT * FROM numbers_mt(1000000000);
DROP TABLE IF EXISTS t;
```

<div id="related">
  ## 関連
</div>

* [Null テーブルエンジン](../../engines/table-engines/special/null.md)