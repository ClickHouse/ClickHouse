---
description: 'SETステートメントの説明'
sidebar_label: 'SET'
sidebar_position: 50
slug: /sql-reference/statements/set
title: 'SETステートメント'
doc_type: 'reference'
---

```sql
SET param = value
```

現在のセッションの [設定](/ja/operations/settings/overview) `param` に `value` を割り当てます。この方法では [サーバー設定](../../operations/server-configuration-parameters/settings.md) は変更できません。

また、指定した設定プロファイルのすべての値を 1 つのクエリで設定することもできます。

```sql
SET profile = 'profile-name-from-the-settings-file'
```

true に設定するブール値の設定では、値の指定を省略した短縮構文を使用できます。設定名のみを指定すると、自動的に `1` (true) が設定されます。

```sql
-- These are equivalent:
SET force_index_by_date = 1
SET force_index_by_date
```

<div id="set-time-zone">
  ## SET TIME ZONE
</div>

```sql
SET TIME ZONE [=] 'timezone'
```

セッションのタイムゾーンを設定します。これは `SET session_timezone = 'timezone'` のエイリアスで、PostgreSQL やその他の SQL データベースとの互換性のために用意されています。

多くの SQL クライアント、ORM、JDBC ドライバーは、接続時に自動的に `SET TIME ZONE` を実行します。この構文により、そのようなツールをカスタムの回避策なしで ClickHouse でも利用できます。

```sql
SET TIME ZONE 'UTC';
SET TIME ZONE 'Europe/Amsterdam';
SET TIME ZONE 'America/New_York';

-- Verify the current session time zone
SELECT getSetting('session_timezone');
```

timezone の値には、[IANA Time Zone Database](https://www.iana.org/time-zones) に記載されている有効な名前を指定する必要があります。無効な timezone 名を指定すると、エラーが発生します。

`session_timezone` 設定の詳細については、[session&#95;timezone](/ja/operations/settings/settings#session_timezone) を参照してください。

<div id="setting-query-parameters">
  ## クエリパラメータの設定
</div>

`SET` ステートメントは、パラメータ名の先頭に `param_` を付けることで、クエリパラメータの定義にも使用できます。
クエリパラメータを使用すると、実行時に実際の値へ置き換えられるプレースホルダーを含む汎用的なクエリを記述できます。

```sql
SET param_name = value
```

クエリ内でクエリパラメータを使用するには、`{name: datatype}` 構文で参照します：

```sql
SET param_id = 42;
SET param_name = 'John';

SELECT * FROM users
WHERE id = {id: UInt32}
AND name = {name: String};
```

同じクエリを異なる値で複数回実行する必要がある場合、クエリパラメータは特に便利です。

`Identifier` 型での使用を含むクエリパラメータの詳細については、[クエリパラメータの定義と使用](../../sql-reference/syntax.md#defining-and-using-query-parameters)を参照してください。

詳細については、[Settings](../../operations/settings/settings.md)を参照してください。