---
description: 'RENAME ステートメントのドキュメント'
sidebar_label: 'RENAME'
sidebar_position: 48
slug: /sql-reference/statements/rename
title: 'RENAME ステートメント'
doc_type: 'reference'
---

データベース、テーブル、またはDictionaryの名前を変更します。複数のエンティティを1回のクエリでリネームできます。
複数のエンティティを含む `RENAME` クエリは非アトミックな操作であることに注意してください。エンティティの名前をアトミックに入れ替えるには、[EXCHANGE](./exchange.md) ステートメントを使用してください。

**構文**

```sql
RENAME [DATABASE|TABLE|DICTIONARY] name TO new_name [,...] [ON CLUSTER cluster]
```

<div id="rename-database">
  ## RENAME DATABASE
</div>

データベース名を変更します。

**構文**

```sql
RENAME DATABASE atomic_database1 TO atomic_database2 [,...] [ON CLUSTER cluster]
```

<div id="rename-table">
  ## RENAME TABLE
</div>

1 つ以上のテーブルの名前を変更します。

テーブルのリネームは軽量な操作です。`TO` の後に別のデータベースを指定した場合、テーブルはそのデータベースへ移動されます。ただし、データベースのディレクトリは同一のファイルシステム上に存在している必要があります。そうでない場合は、エラーが返されます。
1 つのクエリで複数のテーブルをリネームする場合、この操作はアトミックではありません。部分的に実行される可能性があり、他のセッションのクエリで `Table ... does not exist ...` エラーが発生することがあります。

**構文**

```sql
RENAME TABLE [db1.]name1 TO [db2.]name2 [,...] [ON CLUSTER cluster]
```

**例**

```sql
RENAME TABLE table_A TO table_A_bak, table_B TO table_B_bak;
```

また、よりシンプルなSQLを使用することもできます:

```sql
RENAME table_A TO table_A_bak, table_B TO table_B_bak;
```

<div id="rename-dictionary">
  ## RENAME DICTIONARY
</div>

1 つまたは複数のDictionaryの名前を変更します。このクエリを使用すると、Dictionaryをデータベース間で移動できます。

**構文**

```sql
RENAME DICTIONARY [db0.]dict_A TO [db1.]dict_B [,...] [ON CLUSTER cluster]
```

**関連項目**

* [Dictionaries](./create/dictionary/overview.md)