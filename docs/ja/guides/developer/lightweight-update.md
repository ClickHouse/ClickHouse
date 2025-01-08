---
slug: /ja/guides/developer/lightweight-update
sidebar_label: 論理更新
title:  論理更新
keywords: [lightweight update]
---

## 論理更新

:::note
論理更新はClickHouse Cloudでのみ利用可能です。
:::

論理更新が有効な場合、更新された行は即座に更新済みとしてマークされ、後続の`SELECT`クエリは自動的に変更された値を返します。論理更新が有効でない場合は、バックグラウンド処理でミューテーションが適用されるのを待つ必要があるかもしれません。

論理更新は、`MergeTree`ファミリーのテーブルに対してクエリレベルの設定`apply_mutations_on_fly`を有効にすることで使用できます。

```sql
SET apply_mutations_on_fly = 1;
```

## 例

テーブルを作成し、いくつかのミューテーションを実行してみましょう：
```sql
CREATE TABLE test_on_fly_mutations (id UInt64, v String) 
ENGINE = MergeTree ORDER BY id;

-- 論理更新が有効でない場合のデフォルトの動作を示すために
-- ミューテーションのバックグラウンドの具体化を無効化
SYSTEM STOP MERGES test_on_fly_mutations;
SET mutations_sync = 0;

-- 新しいテーブルにいくつかの行を挿入
INSERT INTO test_on_fly_mutations VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- 行の値を更新
ALTER TABLE test_on_fly_mutations UPDATE v = 'd' WHERE id = 1;
ALTER TABLE test_on_fly_mutations DELETE WHERE v = 'd';
ALTER TABLE test_on_fly_mutations UPDATE v = 'e' WHERE id = 2;
ALTER TABLE test_on_fly_mutations DELETE WHERE v = 'e';
```

`SELECT`クエリで更新の結果を確認してみましょう：
```sql
-- 論理更新を明示的に無効化
SET apply_mutations_on_fly = 0;

SELECT id, v FROM test_on_fly_mutations ORDER BY id;
```

新しいテーブルをクエリした際に、行の値がまだ更新されていないことに注意してください：

```
┌─id─┬─v─┐
│  1 │ a │
│  2 │ b │
│  3 │ c │
└────┴───┘
```

論理更新を有効にするとどうなるかを見てみましょう：

```sql
-- 論理更新を有効化
SET apply_mutations_on_fly = 1;

SELECT id, v FROM test_on_fly_mutations ORDER BY id;
```

この`SELECT`クエリは、ミューテーションが適用されるのを待たなくても、正しい結果を即座に返すようになります：

```
┌─id─┬─v─┐
│  3 │ c │
└────┴───┘
```

## パフォーマンスへの影響

論理更新を有効にすると、ミューテーションは即座に具体化されず、`SELECT`クエリの際にのみ適用されます。ただし、ミューテーションはバックグラウンドで非同期に具体化されていることに注意してください。これは重いプロセスです。

特定の時間間隔内で処理されるミューテーションの数を送信されたミューテーションの数が常に超える場合、適用されるべき具体化されていないミューテーションのキューが増え続けます。これにより、最終的に`SELECT`クエリのパフォーマンスの劣化を招く可能性があります。

具体化されていないミューテーションの無限成長を制限するために、`apply_mutations_on_fly`の設定を`number_of_mutations_to_throw`や`number_of_mutations_to_delay`といった他の`MergeTree`レベルの設定と併せて有効にすることをお勧めします。

## サブクエリと非決定的な関数のサポート

論理更新は、サブクエリと非決定的な関数に対して限定的なサポートを提供します。妥当なサイズ（設定`mutations_max_literal_size_to_replace`で制御される）の結果を持つスカラーサブクエリのみがサポートされています。また、定数の非決定的な関数（例:`now()`関数）のみがサポートされています。

これらの動作は次の設定によって制御されます：

- `mutations_execute_nondeterministic_on_initiator` - trueの場合、非決定的な関数はイニシエーターレプリカで実行され、`UPDATE`や`DELETE`クエリでリテラルとして置き換えられます。デフォルト値：`false`。
- `mutations_execute_subqueries_on_initiator` - trueの場合、スカラーサブクエリはイニシエーターレプリカで実行され、`UPDATE`や`DELETE`クエリでリテラルとして置き換えられます。デフォルト値：`false`。
- `mutations_max_literal_size_to_replace` - `UPDATE`や`DELETE`クエリで置き換えるリテラルのシリアライズされた最大サイズ（バイト単位）。デフォルト値：`16384`（16 KiB）。
