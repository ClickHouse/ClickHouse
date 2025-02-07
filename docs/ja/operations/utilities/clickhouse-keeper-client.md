---
slug: /ja/operations/utilities/clickhouse-keeper-client
sidebar_label: clickhouse-keeper-client
---

# clickhouse-keeper-client

ネイティブプロトコルを通じてclickhouse-keeperとやりとりするためのクライアントアプリケーションです。

## キー {#clickhouse-keeper-client}

-   `-q QUERY`, `--query=QUERY` — 実行するクエリ。 このパラメータが渡されない場合、`clickhouse-keeper-client`はインタラクティブモードで開始されます。
-   `-h HOST`, `--host=HOST` — サーバーホスト。デフォルト値：`localhost`。
-   `-p N`, `--port=N` — サーバーポート。デフォルト値：9181。
-   `-c FILE_PATH`, `--config-file=FILE_PATH` — 接続文字列を取得するための設定ファイルのパスを設定します。デフォルト値：`config.xml`。
-   `--connection-timeout=TIMEOUT` — 接続タイムアウトを秒単位で設定します。デフォルト値：10秒。
-   `--session-timeout=TIMEOUT` — セッションタイムアウトを秒単位で設定します。デフォルト値：10秒。
-   `--operation-timeout=TIMEOUT` — 操作タイムアウトを秒単位で設定します。デフォルト値：10秒。
-   `--history-file=FILE_PATH` — 履歴ファイルのパスを設定します。デフォルト値：`~/.keeper-client-history`。
-   `--log-level=LEVEL` — ログレベルを設定します。デフォルト値：`information`。
-   `--no-confirmation` — 設定すると、いくつかのコマンドで確認を必要としません。インタラクティブではデフォルト値`false`、クエリでは`true`です。
-   `--help` — ヘルプメッセージを表示します。

## 例 {#clickhouse-keeper-client-example}

```bash
./clickhouse-keeper-client -h localhost -p 9181 --connection-timeout 30 --session-timeout 30 --operation-timeout 30
Connected to ZooKeeper at [::1]:9181 with session_id 137
/ :) ls
keeper foo bar
/ :) cd 'keeper'
/keeper :) ls
api_version
/keeper :) cd 'api_version'
/keeper/api_version :) ls

/keeper/api_version :) cd 'xyz'
Path /keeper/api_version/xyz does not exist
/keeper/api_version :) cd ../../
/ :) ls
keeper foo bar
/ :) get 'keeper/api_version'
2
```

## コマンド {#clickhouse-keeper-client-commands}

-   `ls '[path]'` -- 指定されたパスのノードを一覧表示します（デフォルト: 作業ディレクトリ）。
-   `cd '[path]'` -- 作業パスを変更します（デフォルト `.`）。
-   `cp '<src>' '<dest>'`  -- 'src'ノードを'dest'パスにコピーします。
-   `mv '<src>' '<dest>'`  -- 'src'ノードを'dest'パスに移動します。
-   `exists '<path>'` -- ノードが存在する場合は`1`を返し、存在しない場合は`0`を返します。
-   `set '<path>' <value> [version]` -- ノードの値を更新します。バージョンが一致する場合のみ更新されます（デフォルト: -1）。
-   `create '<path>' <value> [mode]` -- 設定された値で新しいノードを作成します。
-   `touch '<path>'` -- ノードを空の文字列として作成します。ノードが既に存在する場合は例外をスローしません。
-   `get '<path>'` -- ノードの値を返します。
-   `rm '<path>' [version]` -- バージョンが一致する場合のみノードを削除します（デフォルト: -1）。
-   `rmr '<path>' [limit]` -- サブツリーのサイズがリミットよりも小さい場合にパスを再帰的に削除します。確認が必要です（デフォルトのリミット = 100）。
-   `flwc <command>` -- 4文字コマンドを実行します。
-   `help` -- このメッセージをプリントします。
-   `get_direct_children_number '[path]'` -- 特定のパスの直下の子ノードの数を取得します。
-   `get_all_children_number '[path]'` -- 特定のパスのすべての子ノード数を取得します。
-   `get_stat '[path]'` -- ノードのステータスを返します（デフォルト `.`）。
-   `find_super_nodes <threshold> '[path]'` -- 指定されたパスで子ノードの数がしきい値を超えるノードを探します（デフォルト `.`）。
-   `delete_stale_backups` -- 非アクティブなバックアップに使用されたClickHouseノードを削除します。
-   `find_big_family [path] [n]` -- サブツリーで最大のファミリーを持つトップnノードを返します（デフォルトパス = `.`、n = 10）。
-   `sync '<path>'` -- プロセス間およびリーダー間でノードを同期します。
-   `reconfig <add|remove|set> "<arg>" [version]` -- Keeperクラスタを再構成します。詳しくは https://clickhouse.com/docs/ja/guides/sre/keeper/clickhouse-keeper#reconfiguration を参照してください。
