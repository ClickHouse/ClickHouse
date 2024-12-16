## 管理者権限のテスト

ユーザー `default` からログアウトし、ユーザー `clickhouse_admin` としてログインし直してください。

これらすべてが成功するはずです：

```sql
SHOW GRANTS FOR clickhouse_admin;
```

```sql
CREATE DATABASE db1
```

```sql
CREATE TABLE db1.table1 (id UInt64, column1 String) ENGINE = MergeTree() ORDER BY id;
```

```sql
INSERT INTO db1.table1 (id, column1) VALUES (1, 'abc');
```

```sql
SELECT * FROM db1.table1;
```

```sql
DROP TABLE db1.table1;
```

```sql
DROP DATABASE db1;
```

## 非管理者ユーザー

ユーザーは必要な権限を持ち、全員が管理者であるべきではありません。このドキュメントの残りの部分では、例のシナリオと必要な役割を提供します。

### 準備

例で使用されるテーブルとユーザーを作成します。

#### サンプルデータベース、テーブル、および行の作成

1. テストデータベースを作成

   ```sql
   CREATE DATABASE db1;
   ```

2. テーブルを作成

   ```sql
   CREATE TABLE db1.table1 (
       id UInt64,
       column1 String,
       column2 String
   )
   ENGINE MergeTree
   ORDER BY id;
   ```

3. サンプル行でテーブルを埋める

   ```sql
   INSERT INTO db1.table1
       (id, column1, column2)
   VALUES
       (1, 'A', 'abc'),
       (2, 'A', 'def'),
       (3, 'B', 'abc'),
       (4, 'B', 'def');
   ```

4. テーブルを確認する：

   ```sql
   SELECT *
   FROM db1.table1
   ```

   ```response
   Query id: 475015cc-6f51-4b20-bda2-3c9c41404e49

   ┌─id─┬─column1─┬─column2─┐
   │  1 │ A       │ abc     │
   │  2 │ A       │ def     │
   │  3 │ B       │ abc     │
   │  4 │ B       │ def     │
   └────┴─────────┴─────────┘
   ```

5. 特定のカラムへのアクセスを制限することを示すために使用される通常のユーザーを作成：

   ```sql
   CREATE USER column_user IDENTIFIED BY 'password';
   ```

6. 特定の値を持つ行へのアクセスを制限することを示すために使用される通常のユーザーを作成：
   ```sql
   CREATE USER row_user IDENTIFIED BY 'password';
   ```

#### 役割の作成

この例を使って：

- カラムや行、異なる権限のための役割を作成します
- 役割に権限を付与します
- ユーザーを各役割に割り当てます

役割は、各ユーザーを個別に管理する代わりに、特定の権限を持つユーザーのグループを定義するために使用されます。

1.  `db1` データベースおよび `table1` において、`column1` のみを閲覧できるユーザーの役割を作成：

    ```sql
    CREATE ROLE column1_users;
    ```

2.  `column1` のみが閲覧可能な権限を設定

    ```sql
    GRANT SELECT(id, column1) ON db1.table1 TO column1_users;
    ```

3.  `column_user` ユーザーを `column1_users` 役割に追加

    ```sql
    GRANT column1_users TO column_user;
    ```

4.  `column1` に `A` を含む行のみを閲覧できるユーザーの役割を作成

    ```sql
    CREATE ROLE A_rows_users;
    ```

5.  `row_user` を `A_rows_users` 役割に追加

    ```sql
    GRANT A_rows_users TO row_user;
    ```

6.  `column1` が `A` の値を持つ行のみを閲覧可能とするポリシーを作成

    ```sql
    CREATE ROW POLICY A_row_filter ON db1.table1 FOR SELECT USING column1 = 'A' TO A_rows_users;
    ```

7.  データベースとテーブルへの権限を設定

    ```sql
    GRANT SELECT(id, column1, column2) ON db1.table1 TO A_rows_users;
    ```

8.  他の役割に対してもすべての行にアクセスできるように明示的な権限を付与

    ```sql
    CREATE ROW POLICY allow_other_users_filter 
    ON db1.table1 FOR SELECT USING 1 TO clickhouse_admin, column1_users;
    ```

    :::note
    テーブルにポリシーをアタッチすると、システムはそのポリシーを適用し、定義されたユーザーと役割のみがそのテーブルでの操作を行うことができます。その他のユーザーは操作を拒否されます。制限された行ポリシーが他のユーザーに適用されないようにするため、他のユーザーと役割が通常または他のタイプのアクセスを持つことを許可する別のポリシーを定義する必要があります。
    :::

## 検証

### カラム制限ユーザーでの役割の権限テスト

1. `clickhouse_admin` ユーザーでClickHouseクライアントにログイン

   ```
   clickhouse-client --user clickhouse_admin --password password
   ```

2. 管理者ユーザーを使用して、データベース、テーブル、およびすべての行のアクセスを確認。

   ```sql
   SELECT *
   FROM db1.table1
   ```

   ```response
   Query id: f5e906ea-10c6-45b0-b649-36334902d31d

   ┌─id─┬─column1─┬─column2─┐
   │  1 │ A       │ abc     │
   │  2 │ A       │ def     │
   │  3 │ B       │ abc     │
   │  4 │ B       │ def     │
   └────┴─────────┴─────────┘
   ```

3. `column_user` ユーザーでClickHouseクライアントにログイン

   ```
   clickhouse-client --user column_user --password password
   ```

4. すべてのカラムを使用した `SELECT`

   ```sql
   SELECT *
   FROM db1.table1
   ```

   ```response
   Query id: 5576f4eb-7450-435c-a2d6-d6b49b7c4a23

   0 rows in set. Elapsed: 0.006 sec.

   Received exception from server (version 22.3.2):
   Code: 497. DB::Exception: Received from localhost:9000. 
   DB::Exception: column_user: Not enough privileges. 
   To execute this query it's necessary to have grant 
   SELECT(id, column1, column2) ON db1.table1. (ACCESS_DENIED)
   ```

   :::note
   すべてのカラムが指定されたためアクセスが拒否されました。ユーザーは `id` と `column1` のみへのアクセス権を持っています
   :::

5. 指定されたカラムのみを用いた `SELECT` クエリを確認：

   ```sql
   SELECT
       id,
       column1
   FROM db1.table1
   ```

   ```response
   Query id: cef9a083-d5ce-42ff-9678-f08dc60d4bb9

   ┌─id─┬─column1─┐
   │  1 │ A       │
   │  2 │ A       │
   │  3 │ B       │
   │  4 │ B       │
   └────┴─────────┘
   ```

### 行制限ユーザーでの役割の権限テスト

1. `row_user` でClickHouseクライアントにログイン

   ```
   clickhouse-client --user row_user --password password
   ```

2. 利用可能な行を表示

   ```sql
   SELECT *
   FROM db1.table1
   ```

   ```response
   Query id: a79a113c-1eca-4c3f-be6e-d034f9a220fb

   ┌─id─┬─column1─┬─column2─┐
   │  1 │ A       │ abc     │
   │  2 │ A       │ def     │
   └────┴─────────┴─────────┘
   ```

   :::note
   上記の2行のみが返されることを確認し、`column1` に `B` の値を持つ行は除外されるべきです。
   :::

## ユーザーと役割の変更

ユーザーは必要な権限の組み合わせに対して複数の役割を割り当てることができます。複数の役割を使用する場合、システムは役割を組み合わせて権限を決定し、その結果、役割の権限が累積されます。

例えば、1つの `role1` が `column1` のみの選択を許可し、`role2` が `column1` と `column2` の選択を許可する場合、ユーザーは両方のカラムにアクセスできます。

1. 管理者アカウントを使用して、デフォルトの役割で行とカラムの両方を制限する新しいユーザーを作成

   ```sql
   CREATE USER row_and_column_user IDENTIFIED BY 'password' DEFAULT ROLE A_rows_users;
   ```

2. `A_rows_users` 役割に対する以前の権限を削除

   ```sql
   REVOKE SELECT(id, column1, column2) ON db1.table1 FROM A_rows_users;
   ```

3. `A_row_users` 役割に `column1` のみの選択を許可

   ```sql
   GRANT SELECT(id, column1) ON db1.table1 TO A_rows_users;
   ```

4. `row_and_column_user` でClickHouseクライアントにログイン

   ```
   clickhouse-client --user row_and_column_user --password password;
   ```

5. すべてのカラムでテスト：

   ```sql
   SELECT *
   FROM db1.table1
   ```

   ```response
   Query id: 8cdf0ff5-e711-4cbe-bd28-3c02e52e8bc4

   0 rows in set. Elapsed: 0.005 sec.

   Received exception from server (version 22.3.2):
   Code: 497. DB::Exception: Received from localhost:9000. 
   DB::Exception: row_and_column_user: Not enough privileges. 
   To execute this query it's necessary to have grant 
   SELECT(id, column1, column2) ON db1.table1. (ACCESS_DENIED)
   ```

6. 制限されたカラムでテスト：

   ```sql
   SELECT
       id,
       column1
   FROM db1.table1
   ```

   ```response
   Query id: 5e30b490-507a-49e9-9778-8159799a6ed0

   ┌─id─┬─column1─┐
   │  1 │ A       │
   │  2 │ A       │
   └────┴─────────┘
   ```

## トラブルシューティング

権限が交差または結合して予期しない結果を生む場合があります。次のコマンドを使用して管理者アカウントを使用して問題を絞り込むことができます。

### ユーザーの権限と役割のリスト

```sql
SHOW GRANTS FOR row_and_column_user
```

```response
Query id: 6a73a3fe-2659-4aca-95c5-d012c138097b

┌─GRANTS FOR row_and_column_user───────────────────────────┐
│ GRANT A_rows_users, column1_users TO row_and_column_user │
└──────────────────────────────────────────────────────────┘
```

### ClickHouse の役割のリスト

```sql
SHOW ROLES
```

```response
Query id: 1e21440a-18d9-4e75-8f0e-66ec9b36470a

┌─name────────────┐
│ A_rows_users    │
│ column1_users   │
└─────────────────┘
```

### ポリシーの表示

```sql
SHOW ROW POLICIES
```

```response
Query id: f2c636e9-f955-4d79-8e80-af40ea227ebc

┌─name───────────────────────────────────┐
│ A_row_filter ON db1.table1             │
│ allow_other_users_filter ON db1.table1 │
└────────────────────────────────────────┘
```

### ポリシーがどのように定義されているかと現在の権限を表示

```sql
SHOW CREATE ROW POLICY A_row_filter ON db1.table1
```

```response
Query id: 0d3b5846-95c7-4e62-9cdd-91d82b14b80b

┌─CREATE ROW POLICY A_row_filter ON db1.table1────────────────────────────────────────────────┐
│ CREATE ROW POLICY A_row_filter ON db1.table1 FOR SELECT USING column1 = 'A' TO A_rows_users │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

## ロール、ポリシー、およびユーザーを管理するためのコマンドの例

次のコマンドを使用して：

- 権限の削除
- ポリシーの削除
- ユーザーを役割から解除
- ユーザーと役割の削除
  <br />

:::tip
これらのコマンドは管理者ユーザーまたは `default` ユーザーとして実行してください
:::

### 役割からの権限を削除

```sql
REVOKE SELECT(column1, id) ON db1.table1 FROM A_rows_users;
```

### ポリシーを削除

```sql
DROP ROW POLICY A_row_filter ON db1.table1;
```

### ユーザーを役割から解除

```sql
REVOKE A_rows_users FROM row_user;
```

### 役割を削除

```sql
DROP ROLE A_rows_users;
```

### ユーザーを削除

```sql
DROP USER row_user;
```

## 要約

このドキュメントでは、SQLユーザーと役割の作成の基本を示し、ユーザーおよび役割の権限を設定および変更する手順を提供しました。それぞれの詳細情報については、ユーザーガイドおよびリファレンスドキュメントを参照してください。
