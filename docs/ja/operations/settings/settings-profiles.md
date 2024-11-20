---
slug: /ja/operations/settings/settings-profiles
sidebar_position: 61
sidebar_label:  設定プロファイル
---

# 設定プロファイル

設定プロファイルは、同じ名前でグループ化された設定のコレクションです。

:::note
ClickHouseは、設定プロファイルを管理するための[SQL駆動のワークフロー](../../guides/sre/user-management/index.md#access-control)もサポートしています。これの使用をお勧めします。
:::

プロファイルには任意の名前を付けることができます。異なるユーザーに同じプロファイルを指定することも可能です。設定プロファイルで最も重要なことは、`readonly=1`と記述して読み取り専用アクセスを確保することです。

設定プロファイルは互いに継承できます。継承を使用するには、プロファイル内にリストされた他の設定の前に、1つまたは複数の`profile`設定を示します。同じ設定が異なるプロファイルで定義されている場合、最後に定義されたものが使用されます。

プロファイル内のすべての設定を適用するには、`profile`設定をセットします。

例：

`web`プロファイルをインストールします。

``` sql
SET profile = 'web'
```

設定プロファイルはユーザー設定ファイルで宣言されます。通常、これは`users.xml`です。

例：

``` xml
<!-- 設定プロファイル -->
<profiles>
    <!-- デフォルト設定 -->
    <default>
        <!-- 単一のクエリを実行する際の最大スレッド数 -->
        <max_threads>8</max_threads>
    </default>

    <!-- ユーザーインターフェイスからのクエリ用設定 -->
    <web>
        <max_rows_to_read>1000000000</max_rows_to_read>
        <max_bytes_to_read>100000000000</max_bytes_to_read>

        <max_rows_to_group_by>1000000</max_rows_to_group_by>
        <group_by_overflow_mode>any</group_by_overflow_mode>

        <max_rows_to_sort>1000000</max_rows_to_sort>
        <max_bytes_to_sort>1000000000</max_bytes_to_sort>

        <max_result_rows>100000</max_result_rows>
        <max_result_bytes>100000000</max_result_bytes>
        <result_overflow_mode>break</result_overflow_mode>

        <max_execution_time>600</max_execution_time>
        <min_execution_speed>1000000</min_execution_speed>
        <timeout_before_checking_execution_speed>15</timeout_before_checking_execution_speed>

        <max_columns_to_read>25</max_columns_to_read>
        <max_temporary_columns>100</max_temporary_columns>
        <max_temporary_non_const_columns>50</max_temporary_non_const_columns>

        <max_subquery_depth>2</max_subquery_depth>
        <max_pipeline_depth>25</max_pipeline_depth>
        <max_ast_depth>50</max_ast_depth>
        <max_ast_elements>100</max_ast_elements>

        <max_sessions_for_user>4</max_sessions_for_user>

        <readonly>1</readonly>
    </web>
</profiles>
```

この例では、2つのプロファイル: `default`と`web`を指定しています。

`default`プロファイルには特別な目的があります。それは、常に存在し、サーバーの開始時に適用されることです。言い換えれば、`default`プロファイルにはデフォルトの設定が含まれています。

`web`プロファイルは、`SET`クエリを使用するか、HTTPクエリにURLパラメータを使用して設定できる通常のプロファイルです。
