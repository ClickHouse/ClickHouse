---
slug: /ja/operations/settings/constraints-on-settings
sidebar_position: 62
sidebar_label: 設定の制約
---

# 設定の制約

設定の制約は、`user.xml`構成ファイルの`profiles`セクションで定義でき、ユーザーが`SET`クエリで一部の設定を変更することを禁止します。
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
      <setting_name_5>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
        <changeable_in_readonly/>
      </setting_name_5>
    </constraints>
  </user_name>
</profiles>
```

ユーザーが制約を違反しようとする場合、例外が投げられ、設定は変更されません。
サポートされている制約タイプには、`min`、`max`、`readonly`（別名`const`）および`changeable_in_readonly`があります。`min`と`max`制約は数値設定に対して上下限を指定し、組み合わせて使用することができます。`readonly`または`const`制約は、ユーザーがその設定を全く変更できないことを指定します。`changeable_in_readonly`制約タイプは、`readonly`設定が1に設定されている場合でも、`min`/`max`範囲内で設定を変更することを許可します。それ以外の場合、設定は`readonly=1`モードで変更できません。`changeable_in_readonly`は、`settings_constraints_replace_previous`が有効になっている場合にのみサポートされます:
``` xml
<access_control_improvements>
  <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
</access_control_improvements>
```

複数のプロファイルがユーザーに対してアクティブになっている場合、制約はマージされます。マージプロセスは`settings_constraints_replace_previous`に依存します:
- **true**（推奨）: 同じ設定に対する制約はマージ中に置き換えられ、最後の制約が使用され、前のすべては無視され、新しい制約に設定されていないフィールドも含まれます。
- **false**（デフォルト）: 同じ設定に対する制約は、設定されていない制約タイプが前のプロファイルから取得され、設定された制約タイプが新しいプロファイルの値に置き換えられる方法でマージされます。

読み取り専用モードは`readonly`設定によって有効化されます（`readonly`制約タイプと混同しないでください）:
- `readonly=0`: 読み取り専用の制限はありません。
- `readonly=1`: 読み取りクエリのみ許可され、`changeable_in_readonly`が設定されていない限り設定は変更できません。
- `readonly=2`: 読み取りクエリのみ許可されますが、設定は変更可能です。ただし、`readonly`設定自体は変更できません。

**例:** `users.xml`に次の行が含まれているとします:

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

次のクエリはすべて例外を投げます:

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

**注:** `default`プロファイルは特別な処理がされます: `default`プロファイルに定義されたすべての制約はデフォルト制約となり、これらのユーザーに対して明示的に上書きされるまで、すべてのユーザーを制限します。

## MergeTree設定の制約
[MergeTree設定](merge-tree-settings.md)に制約を設定することができます。これらの制約は、MergeTreeエンジンを使用してテーブルを作成する際や、そのストレージ設定を変更する際に適用されます。MergeTree設定の名前は、`<constraints>`セクションで参照する際に`merge_tree_`プレフィックスを追加する必要があります。

**例:** 明示的に指定された`storage_policy`を持つ新しいテーブルの作成を禁止します。

``` xml
<profiles>
  <default>
    <constraints>
      <merge_tree_storage_policy>
        <const/>
      </merge_tree_storage_policy>
    </constraints>
  </default>
</profiles>
```
