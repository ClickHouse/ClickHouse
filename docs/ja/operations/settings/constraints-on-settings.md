---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 62
toc_title: "\u8A2D\u5B9A\u306E\u5236\u7D04"
---

# 設定の制約 {#constraints-on-settings}

設定に関する制約は、以下で定義することができます。 `profiles` のセクション `user.xml` 設定ファイルとユーザーが設定の一部を変更することを禁止します。 `SET` クエリ。
制約は次のように定義されます:

``` xml
<profiles>
  <user_name>
    <constraints>
      <setting_name_1>
        <min>lower_boundary</min>
      </setting_name_1>
      <setting_name_2>
        <max>upper_boundary</max>
      </setting_name_2>
      <setting_name_3>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
      </setting_name_3>
      <setting_name_4>
        <readonly/>
      </setting_name_4>
    </constraints>
  </user_name>
</profiles>
```

ユーザーが制約に違反しようとすると、例外がスローされ、設定は変更されません。
サポートされている制約は以下の通りです: `min`, `max`, `readonly`. その `min` と `max` 制約は、数値設定の上限と下限を指定し、組み合わせて使用できます。 その `readonly` 制約を指定すると、ユーザーは変更できませんので、対応する設定です。

**例:** さあ `users.xml` 行を含む:

``` xml
<profiles>
  <default>
    <max_memory_usage>10000000000</max_memory_usage>
    <force_index_by_date>0</force_index_by_date>
    ...
    <constraints>
      <max_memory_usage>
        <min>5000000000</min>
        <max>20000000000</max>
      </max_memory_usage>
      <force_index_by_date>
        <readonly/>
      </force_index_by_date>
    </constraints>
  </default>
</profiles>
```

以下のクエリすべての例外をスロー:

``` sql
SET max_memory_usage=20000000001;
SET max_memory_usage=4999999999;
SET force_index_by_date=1;
```

``` text
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be greater than 20000000000.
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be less than 5000000000.
Code: 452, e.displayText() = DB::Exception: Setting force_index_by_date should not be changed.
```

**注:** その `default` プロファイルには特別な処理があります。 `default` プロのデフォルトの制約、その制限するすべてのユーザーまでの彼らのメソッドを明示的にこれらのユーザー

[元の記事](https://clickhouse.com/docs/en/operations/settings/constraints_on_settings/) <!--hide-->
