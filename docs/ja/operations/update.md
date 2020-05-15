---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 47
toc_title: "\u30AF\u30EA\u30C3\u30AF\u30CF\u30A6\u30B9\u66F4\u65B0"
---

# クリックハウス更新 {#clickhouse-update}

まclickhouse設置されたからdebパッケージ、以下のコマンドを実行し、サーバー:

``` bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

推奨されるdebパッケージ以外のものを使用してclickhouseをインストールした場合は、適切な更新方法を使用します。

ClickHouseは分散updateをサポートしていません。 この操作は、個別のサーバーごとに連続して実行する必要があります。 クラスター上のすべてのサーバーを同時に更新しないでください。
