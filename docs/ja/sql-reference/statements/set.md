---
slug: /ja/sql-reference/statements/set
sidebar_position: 50
sidebar_label: SET
---

# SET ステートメント

``` sql
SET param = value
```

現在のセッションの`param` [設定](../../operations/settings/index.md)に`value`を割り当てます。この方法では[サーバー設定](../../operations/server-configuration-parameters/settings.md)を変更することはできません。

また、指定された設定プロファイルからすべての値を単一のクエリで設定することもできます。

``` sql
SET profile = 'profile-name-from-the-settings-file'
```

詳細については、[設定](../../operations/settings/settings.md)を参照してください。
