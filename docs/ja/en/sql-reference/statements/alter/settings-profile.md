---
description: 'SETTINGS PROFILE に関するドキュメント'
sidebar_label: 'SETTINGS PROFILE'
sidebar_position: 48
slug: /sql-reference/statements/alter/settings-profile
title: 'ALTER SETTINGS PROFILE'
doc_type: 'reference'
---

設定プロファイルを変更します。

構文:

```sql
ALTER SETTINGS PROFILE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...]
    [SET variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...] ]
    [DROP SETTINGS variable [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [DROP ALL SETTINGS]
    [DROP ALL PROFILES]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
```

`ON CLUSTER` 句を使用すると、クラスター内の設定プロファイルを変更できます。詳細は[分散 DDL](../../../sql-reference/distributed-ddl.md)を参照してください。

<div id="replacing-vs-modifying">
  ## 設定の置き換えと変更
</div>

`ALTER SETTINGS PROFILE` では、プロファイルの設定や親 (継承元) プロファイルを変更する方法として、2つの異なる方式がサポートされています。これらは挙動が大きく異なるため、適切なものを選ぶことが重要です。

<div id="replacing-form">
  ### 置換形式: 単独の `SETTINGS` / `INHERIT`
</div>

単独の `SETTINGS` 句 (`ADD`、`MODIFY`、`DROP` なし) を使うと、プロファイルの**設定リスト全体とすべての親プロファイル**が、ここで列挙した内容だけに完全に置き換えられます。以前存在していても列挙されていないものは、警告なしでそのまま削除されます。

```sql
CREATE SETTINGS PROFILE OR REPLACE p
    SETTINGS max_execution_time = 10, enable_lazy_columns_replication = 1;

ALTER SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360;

SHOW CREATE SETTINGS PROFILE p;
-- → CREATE SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360
-- max_execution_time and enable_lazy_columns_replication are gone.
```

:::warning
`SETTINGS` の素の形式は完全な置き換えであるため、設定済みのベースプロファイルに対してこれを使って「1 つの設定だけを上書き」すると、そのプロファイル上のほかのすべての設定 (およびすべての親プロファイル) が削除されます。ほかの設定を保持したまま 1 つだけ変更したい場合は、以下で説明するインクリメンタルな `MODIFY`/`ADD`/`DROP` 形式を使用してください。
:::

これは [`CREATE SETTINGS PROFILE`](../create/settings-profile.md) における `SETTINGS` と同じ挙動で、この句が完全な設定リストを定義します。

<div id="incremental-form">
  ### インクリメンタル形式: `ADD` / `MODIFY` / `DROP`
</div>

`ADD`、`MODIFY`、`DROP` キーワードを使うと、プロファイル内のそれ以外の内容はそのままに、個々のエントリだけを変更できます。

* `ADD SETTINGS variable = value [constraints]` — まだ存在しない設定を追加します。
* `MODIFY SETTINGS variable = value [constraints]` — 1 つの設定のエントリを置き換えます。エントリ全体 (値と制約) が上書きされるため、維持したい場合は `MIN` / `MAX` / `READONLY` / なども再指定してください。
* `DROP SETTINGS variable [,...]` — 指定した設定を削除します。
* `ADD PROFILES 'profile_name' [,...]` / `DROP PROFILES 'profile_name' [,...]` — 親 (継承元) プロファイルを追加または削除します。
* `DROP ALL SETTINGS` / `DROP ALL PROFILES` — すべての設定、またはすべての親プロファイルを削除します。

これらの句の一部は 1 つのステートメント内で組み合わせることができ、たとえば `DROP SETTINGS a ADD SETTINGS b = 1` のように記述できます。

`SET variable = value` は `MODIFY SETTINGS variable = value` の別名です。これは、`SET` のほうが自然に感じられることと、インクリメンタルな変更を意図している場合に、置き換え用の `SETTINGS` 句を入力してしまうのがよくあるミスであるため用意されています。

<div id="examples">
  ## 例
</div>

値が設定済みのプロファイルの他の設定はそのままに、1 つの設定だけを上書きします:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 16106127360;
```

新しい制約付き設定を追加し、別の設定を削除します。

```sql
ALTER SETTINGS PROFILE my_profile
    DROP SETTINGS readonly
    ADD SETTINGS max_threads = 8 MIN 4 MAX 16 WRITABLE;
```

親プロファイルをインクリメンタルに管理する：

```sql
ALTER SETTINGS PROFILE my_profile ADD PROFILES p1;
ALTER SETTINGS PROFILE my_profile DROP PROFILES p1;
```

必ず [`SHOW CREATE SETTINGS PROFILE`](../show.md) で結果を確認してください。

```sql
SHOW CREATE SETTINGS PROFILE my_profile;
```

<div id="incremental-vs-full-replacement">
  ## インクリメンタル vs 完全置換
</div>

:::warning
`SETTINGS` 句を単独で使用すると、新しい設定を適用する前に、プロファイルから**既存の設定と継承元 (親) プロファイルがすべて削除されます**。
:::

他の設定はそのままに 1 つの設定だけを変更するには、`ADD SETTINGS` または `MODIFY SETTINGS` を使用します (以下の例を参照) 。

<div id="add-vs-modify">
  ## ADD と MODIFY
</div>

`ADD SETTINGS` と `MODIFY SETTINGS` はどちらもプロファイル内の他の設定を保持しますが、*同じ* 設定に対する既存エントリの扱いが異なります。

* `ADD SETTINGS variable = value ...` は、まず `variable` の既存エントリを削除し、その後で新しいエントリを挿入します。つまり、その設定の**値とすべての制約をまとめて置き換えます**。`variable` に対して以前定義されていた `MIN`、`MAX`、または書き込み可否 (`READONLY`/`WRITABLE`/`CONST`/`CHANGEABLE_IN_READONLY`) のうち、再指定しなかったものは破棄されます。
* `MODIFY SETTINGS variable = value ...` は**フィールドごとにマージ**します。実際に指定したフィールド (値、`MIN`、`MAX`、または書き込み可否) だけを上書きし、その設定の他のフィールドはそのまま維持します。

:::tip
要するに、設定の一部だけを調整したい場合 (たとえば既存の `MAX` は維持したまま値だけを変更する場合) は `MODIFY SETTINGS` を使用し、設定を最初から定義し直したい場合は `ADD SETTINGS` を使用してください。
:::

<div id="examples">
  ## 例
</div>

以下の例で使用するプロファイルを作成します。

```sql
CREATE SETTINGS PROFILE OR REPLACE p SETTINGS max_execution_time = 60;
```

<div id="example-modify-settings">
  ### MODIFY SETTINGS
</div>

他の設定はそのままに、個別の設定を追加または変更します:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000;
SHOW CREATE SETTINGS PROFILE p;
-- CREATE SETTINGS PROFILE p SETTINGS
--     max_execution_time = 60,
--     max_memory_usage = 20000000000
```

`MODIFY` はフィールド単位でマージされるため、設定の値だけを変更しても、既存の制約はそのまま維持されます:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000 MAX 30000000000;
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 25000000000;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_memory_usage = 25000000000 MAX 30000000000  -- the MAX constraint is preserved
```

<div id="example-add-settings">
  ### ADD SETTINGS
</div>

設定を追加します (ほかの設定はそのまま維持されます) 。すでに存在する場合は、完全に上書きされます。

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 8 MAX 16 READONLY;
```

`MODIFY` とは異なり、値だけを指定して `ADD` を再実行すると、その設定に以前定義されていた制約は削除されます。

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 4;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_threads = 4   -- the MAX and READONLY constraints are gone
```

<div id="example-drop-settings">
  ### DROP SETTINGS
</div>

名前付き設定を1つ以上削除します:

```sql
ALTER SETTINGS PROFILE p DROP SETTINGS max_threads;
```

すべての設定を一度に削除するには：

```sql
ALTER SETTINGS PROFILE p DROP ALL SETTINGS;
```

<div id="example-profiles">
  ### 継承元プロファイルの操作
</div>

プロファイル自体の設定に影響を与えることなく、親 (継承元) プロファイルを追加または削除できます：

```sql
ALTER SETTINGS PROFILE p ADD PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP ALL PROFILES;
```