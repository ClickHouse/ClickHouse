---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 47
toc_title: "\u30AF\u30EA\u30C3\u30AF\u30CF\u30A6\u30B9\u306E\u66F4\u65B0"
---

# クリックハウスの更新 {#clickhouse-update}

ClickHouseがdebパッケージからインストールさ:

``` bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

推奨されるdebパッケージ以外のものを使用してClickHouseをインストールした場合は、適切な更新方法を使用します。

ClickHouseは分散updateをサポートしていません。 操作は、個別のサーバーごとに連続して実行する必要があります。 クラスター上のすべてのサーバーを同時に更新しないでください。
