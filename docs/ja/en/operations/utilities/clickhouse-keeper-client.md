---
description: 'ClickHouse Keeper クライアントユーティリティのドキュメント'
sidebar_label: 'clickhouse-keeper-client'
slug: /operations/utilities/clickhouse-keeper-client
title: 'clickhouse-keeper-client ユーティリティ'
doc_type: 'reference'
---

clickhouse-keeper とネイティブプロトコルでやり取りするためのクライアントアプリケーションです。

<div id="clickhouse-keeper-client">
  ## オプション
</div>

* `-q QUERY`, `--query=QUERY` — 実行するクエリ。このパラメータを指定しない場合、`clickhouse-keeper-client` は対話型モードで起動します。
* `-h HOST`, `--host=HOST` — サーバーのホスト。デフォルト値: `localhost`。
* `-p N`, `--port=N` — サーバーのポート。デフォルト値: 9181
* `-c FILE_PATH`, `--config-file=FILE_PATH` — 接続文字列の取得元となる設定ファイルのパスを設定します。デフォルト値: `config.xml`。
* `--password=PASSWORD` — 認証に使用するパスワード。`CLICKHOUSE_KEEPER_PASSWORD` 環境変数、または XML 設定ファイル内の `<zookeeper><password>` でも設定できます。
* `--identity=IDENTITY` — `digest` 認証スキームで使用する アイデンティティ。`CLICKHOUSE_KEEPER_IDENTITY` 環境変数、または XML 設定ファイル内の `<zookeeper><identity>` でも設定できます。
* `--connection-timeout=TIMEOUT` — 接続タイムアウトを秒単位で設定します。デフォルト値: 10s。
* `--session-timeout=TIMEOUT` — セッションタイムアウトを秒単位で設定します。デフォルト値: 10s。
* `--operation-timeout=TIMEOUT` — 操作タイムアウトを秒単位で設定します。デフォルト値: 10s。
* `--history-file=FILE_PATH` — 履歴ファイルのパスを設定します。デフォルト値: `~/.keeper-client-history`。
* `--log-level=LEVEL` — ログレベルを設定します。デフォルト値: `information`。
* `--no-confirmation` — 指定すると、一部のコマンドで確認を求めなくなります。デフォルト値は、対話型では `false`、クエリでは `true` です
* `--help` — ヘルプメッセージを表示します。

<div id="clickhouse-keeper-client-env">
  ## 環境変数
</div>

* `CLICKHOUSE_KEEPER_PASSWORD` — コマンドラインで `--password` が指定されていない場合、デフォルトのパスワードとして使用されます。
* `CLICKHOUSE_KEEPER_IDENTITY` — コマンドラインで `--identity` が指定されていない場合、デフォルトのアイデンティティとして使用されます。

<div id="clickhouse-keeper-client-auth">
  ## 認証
</div>

認証が必要な Keeper サーバーに接続する場合、パスワードは次の優先順位で決定されます (最初に一致したものが使用されます) 。

1. `--password` コマンドライン引数
2. `CLICKHOUSE_KEEPER_PASSWORD` 環境変数
3. `--config-file` で指定した XML 設定ファイル内の `<zookeeper><password>`

`--identity` / `CLICKHOUSE_KEEPER_IDENTITY` / `<zookeeper><identity>` にも、同じ優先順位が適用されます。

認証設定を含む XML 設定ファイルの例:

```xml
<clickhouse>
    <zookeeper>
        <password>secret</password>
        <node index="1">
            <host>localhost</host>
            <port>9181</port>
        </node>
    </zookeeper>
</clickhouse>
```

<div id="clickhouse-keeper-client-example">
  ## 例
</div>

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

<div id="clickhouse-keeper-client-commands">
  ## コマンド
</div>

* `ls '[path]' [watch_id]` -- 指定したパスのノードを一覧表示します (デフォルト: cwd) 。必要に応じて、`watch_id` で識別される children watch を設定します
* `cd '[path]'` -- 作業パスを変更します (デフォルト `.`) 
* `cp '<src>' '<dest>'`  -- &#39;src&#39; ノードを &#39;dest&#39; パスにコピーします
* `cpr '<src>' '<dest>'`  -- &#39;src&#39; ノードのサブツリーを &#39;dest&#39; パスにコピーします
* `mv '<src>' '<dest>'`  -- &#39;src&#39; ノードを &#39;dest&#39; パスへ移動します
* `mvr '<src>' '<dest>'`  -- &#39;src&#39; ノードのサブツリーを &#39;dest&#39; パスへ移動します
* `exists '<path>' [watch_id]` -- ノードが存在する場合は `1`、存在しない場合は `0` を返します。必要に応じて、`watch_id` で識別される watch を設定します
* `set '<path>' <value> [version]` -- ノードの値を更新します。version が一致する場合にのみ更新されます (デフォルト: -1) 
* `create '<path>' <value> [mode]` -- 指定した値で新しいノードを作成します
* `touch '<path>'` -- 値を空文字列として新しいノードを作成します。ノードがすでに存在する場合でも例外を送出しません
* `get '<path>' [watch_id]` -- ノードの値を返します。必要に応じて、`watch_id` で識別される data watch を設定します
* `watch <watch_id> [timeout_seconds]` -- `watch_id` で識別される watch イベントを待機し、イベントタイプとパスを出力します。`timeout_seconds` が指定されている場合は、指定した timeout 後に error を返します
* `rm '<path>' [version]` -- version が一致する場合にのみノードを削除します (デフォルト: -1) 
* `rmr '<path>' [limit]` -- サブツリーのサイズが上限未満の場合、パスを再帰的に削除します。確認が必要です (デフォルトの上限 = 100) 
* `flwc <command>` -- four-letter-word コマンドを実行します
* `help` -- このメッセージを表示します
* `get_direct_children_number '[path]'` -- 特定のパス配下にある直接の子ノード数を取得します
* `get_all_children_number '[path]'` -- 特定のパス配下にあるすべての子ノード数を取得します
* `get_stat '[path]'` -- ノードの stat を返します (デフォルト `.`) 
* `find_super_nodes <threshold> '[path]'` -- 指定したパスについて、子ノード数がしきい値を超えるノードを見つけます (デフォルト `.`) 
* `delete_stale_backups` -- 現在は非アクティブなバックアップ用の ClickHouse ノードを削除します
* `find_big_family [path] [n]` -- サブツリー内で family が最大の上位 n 個のノードを返します (デフォルトのパス = `.`、n = 10) 
* `sync '<path>'` -- プロセス間およびリーダーとの間でノードを同期します
* `reconfig <add|remove|set> "<arg>" [version]` -- Keeper クラスターを再構成します。/docs/en/guides/sre/keeper/clickhouse-keeper#reconfiguration を参照してください