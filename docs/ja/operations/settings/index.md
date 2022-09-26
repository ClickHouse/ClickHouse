---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "\u8A2D\u5B9A"
toc_priority: 55
toc_title: "\u306F\u3058\u3081\u306B"
---

# 設定 {#session-settings-intro}

複数あるというものですすべての設定は、このセクションで説明する文書

設定はレイヤーで構成されるため、後続の各レイヤーは以前の設定を再定義します。

優先順位の順に設定する方法:

-   の設定 `users.xml` サーバー構成ファイル。

    要素に設定 `<profiles>`.

-   セッション設定。

    送信 `SET setting=value` 対話モードでClickHouseコンソールクライアントから。
    同様に、HttpプロトコルでClickHouseセッションを使用できます。 これを行うには、以下を指定する必要があります `session_id` HTTPパラメータ。

-   クエリ設定。

    -   ClickHouse consoleクライアントを非対話モードで起動するときは、startupパラメータを設定します `--setting=value`.
    -   HTTP APIを使用する場合は、CGIパラメータを渡します (`URL?setting_1=value&setting_2=value...`).

サーバー設定ファイルでのみ行うことができる設定は、このセクションでは説明しません。

[元の記事](https://clickhouse.com/docs/en/operations/settings/) <!--hide-->
