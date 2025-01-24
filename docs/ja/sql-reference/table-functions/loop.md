# loop

**構文**

``` sql
SELECT ... FROM loop(database, table);
SELECT ... FROM loop(database.table);
SELECT ... FROM loop(table);
SELECT ... FROM loop(other_table_function(...));
```

**パラメータ**

- `database` — データベース名。
- `table` — テーブル名。
- `other_table_function(...)` — 他のテーブル関数。  
  例: `SELECT * FROM loop(numbers(10));`  
  `other_table_function(...)` ここでは `numbers(10)`。

**返される値**

クエリの結果を返すための無限ループ。

**例**

ClickHouseからデータを選択する:

``` sql
SELECT * FROM loop(test_database, test_table);
SELECT * FROM loop(test_database.test_table);
SELECT * FROM loop(test_table);
```

または他のテーブル関数を使用する場合:

``` sql
SELECT * FROM loop(numbers(3)) LIMIT 7;
   ┌─number─┐
1. │      0 │
2. │      1 │
3. │      2 │
   └────────┘
   ┌─number─┐
4. │      0 │
5. │      1 │
6. │      2 │
   └────────┘
   ┌─number─┐
7. │      0 │
   └────────┘
``` 
``` sql
SELECT * FROM loop(mysql('localhost:3306', 'test', 'test', 'user', 'password'));
...
```
