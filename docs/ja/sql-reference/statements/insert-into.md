---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: INSERT INTO
---

## INSERT {#insert}

データの追加。

基本的なクエリ形式:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

クエリでは、挿入する列のリストを指定できます `[(c1, c2, c3)]`. この場合、残りの列は:

-   から計算される値 `DEFAULT` テーブル定義で指定された式。
-   ゼロと空の文字列 `DEFAULT` 式は定義されていません。

もし [strict\_insert\_defaults=1](../../operations/settings/settings.md),を持たない列 `DEFAULT` 定義され記載されていることを返します。

データは、任意の場所でINSERTに渡すことができます [形式](../../interfaces/formats.md#formats) ClickHouseがサポートしています。 この形式は、クエリで明示的に指定する必要があります:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

For example, the following query format is identical to the basic version of INSERT … VALUES:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouseは、データの前にスペースと改行がある場合をすべて削除します。 クエリを作成するときは、クエリ演算子の後に新しい行にデータを置くことをお勧めします（データがスペースで始まる場合は重要です）。

例:

``` sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

挿入することができます。データから別のクエリのコマンドラインクライアント、HTTPインターフェース。 詳細については “[インター](../../interfaces/index.md#interfaces)”.

### 制約 {#constraints}

テーブルが [制約](create.md#constraints), their expressions will be checked for each row of inserted data. If any of those constraints is not satisfied — server will raise an exception containing constraint name and expression, the query will be stopped.

### の結果を挿入する `SELECT` {#insert_query_insert-select}

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

列は、SELECT句内の位置に従ってマップされます。 ただし、SELECT式とINSERTテーブルの名前は異なる場合があります。 必要に応じて、型鋳造が行われる。

値以外のデータ形式では、次のような式に値を設定できません `now()`, `1 + 2`、というように。 Values形式では式の使用は制限されますが、この場合は非効率的なコードが実行に使用されるため、これは推奨されません。

その他のクエリをデータ部品に対応していない: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
ただし、古いデータを削除するには `ALTER TABLE ... DROP PARTITION`.

`FORMAT` 次の場合、クエリの最後に句を指定する必要があります `SELECT` 句は、表関数を含みます [入力()](../table-functions/input.md).

### パフォーマン {#performance-considerations}

`INSERT` 入力データを主キーでソートし、パーティションキーでパーティションに分割します。 た場合のデータを挿入し複数の仕切りは一度で大幅に低減できることの `INSERT` クエリ。 これを避けるには:

-   一度に100,000行など、かなり大きなバッチでデータを追加します。
-   グループによるデータのパーティション鍵のアップロード前にでClickHouse.

パフォーマンスが低下しない場合:

-   データはリアルタイムで追加されます。
-   アップロードしたデータとは、通常はソートされました。

[元の記事](https://clickhouse.tech/docs/en/query_language/insert_into/) <!--hide-->
