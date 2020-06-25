---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_folder_title: Settings
toc_priority: 55
toc_title: "\u5C0E\u5165"
---

# 設定 {#settings}

以下に説明するすべての設定を行うには、複数の方法があります。
設定はレイヤーで構成されるので、後続の各レイヤーは以前の設定を再定義します。

優先順位の順に設定を構成する方法:

-   の設定 `users.xml` サーバー構成ファイル。

    要素内に設定する `<profiles>`.

-   セッションの設定。

    送信 `SET setting=value` 対話モードでのClickHouseコンソールクライアントから。
    同様に、httpプロトコルでclickhouseセッションを使用できます。 これを行うには、以下を指定する必要があります。 `session_id` HTTPパラメータ。

-   クエリの設定。

    -   を開始する場合にclickhouseコンソールがクライアントを非インタラクティブモードの設定を起動パラメータ `--setting=value`.
    -   HTTP APIを使用する場合は、CGIパラメーターを渡します (`URL?setting_1=value&setting_2=value...`).

このセクションでは、server configファイルでのみ行うことができる設定については説明しません。

[元の記事](https://clickhouse.tech/docs/en/operations/settings/) <!--hide-->
