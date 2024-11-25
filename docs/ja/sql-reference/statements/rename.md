---
slug: /ja/sql-reference/statements/rename
sidebar_position: 48
sidebar_label: RENAME
---

# RENAME ステートメント

データベース、テーブル、または Dictionary の名前を変更します。複数のエンティティを単一のクエリでリネームできます。複数のエンティティを伴う `RENAME` クエリは非原子的操作であることに注意してください。エンティティの名前を原子的に交換するには、[EXCHANGE](./exchange.md) ステートメントを使用してください。

**構文**

```sql
RENAME [DATABASE|TABLE|DICTIONARY] name TO new_name [,...] [ON CLUSTER cluster]
```

## RENAME DATABASE

データベースの名前を変更します。

**構文**

```sql
RENAME DATABASE atomic_database1 TO atomic_database2 [,...] [ON CLUSTER cluster]
```

## RENAME TABLE

一つまたは複数のテーブルの名前を変更します。

テーブルのリネームは軽量な操作です。`TO` の後に異なるデータベースを指定すると、テーブルはそのデータベースに移動します。ただし、データベースのディレクトリは同じファイルシステムに存在する必要があります。そうでない場合、エラーが返されます。 もし一つのクエリで複数のテーブルをリネームすると、その操作は原子的ではありません。部分的に実行されることがあり、他のセッションで `Table ... does not exist ...` エラーが発生する可能性があります。

**構文**

``` sql
RENAME TABLE [db1.]name1 TO [db2.]name2 [,...] [ON CLUSTER cluster]
```

**例**

```sql
RENAME TABLE table_A TO table_A_bak, table_B TO table_B_bak;
```

そして、よりシンプルな SQL を使うこともできます：  
```sql
RENAME table_A TO table_A_bak, table_B TO table_B_bak;
```

## RENAME DICTIONARY

一つまたは複数の Dictionary の名前を変更します。このクエリは、Dictionary をデータベース間で移動するために使用できます。

**構文**

```sql
RENAME DICTIONARY [db0.]dict_A TO [db1.]dict_B [,...] [ON CLUSTER cluster]
```

**関連項目**

- [Dictionaries](../../sql-reference/dictionaries/index.md)
