---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 40
toc_title: "\u30EA\u30E2\u30FC\u30C8"
---

# リモート,remoteSecure {#remote-remotesecure}

へ自由にアクセスできるリモートサーバーを作成することなく `Distributed` テーブル。

署名:

``` sql
remote('addresses_expr', db, table[, 'user'[, 'password']])
remote('addresses_expr', db.table[, 'user'[, 'password']])
```

`addresses_expr` – An expression that generates addresses of remote servers. This may be just one server address. The server address is `host:port`、またはちょうど `host`. ホストは、サーバー名またはIPv4またはIPv6アドレスとして指定できます。 IPv6アドレスは角かっこで指定します。 ポートは、リモートサーバー上のTCPポートです。 ポートが省略された場合は、以下を使用します `tcp_port` サーバの設定ファイルから(デフォルトでは9000)

!!! important "重要"
    このポートはipv6アドレスに必要です。

例:

``` text
example01-01-1
example01-01-1:9000
localhost
127.0.0.1
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

複数のアドレスをコンマ区切りにできます。 この場合、clickhouseは分散処理を使用するため、指定されたすべてのアドレス（異なるデータを持つシャードなど）にクエリを送信します。

例えば:

``` text
example01-01-1,example01-02-1
```

式の一部は、中括弧で指定できます。 前の例は次のように書くことができます:

``` text
example01-0{1,2}-1
```

中括弧は、二つのドット（負でない整数）で区切られた数の範囲を含めることができます。 この場合、範囲はシャードアドレスを生成する値のセットに拡張されます。 最初の数値がゼロから始まる場合、値は同じゼロ整列で形成されます。 前の例は次のように書くことができます:

``` text
example01-{01..02}-1
```

中括弧の複数のペアがある場合は、対応するセットの直接積が生成されます。

中括弧の中のアドレスとアドレスの一部は、パイプ記号（\|）で区切ることができます。 この場合、対応するアドレスのセットはレプリカとして解釈され、クエリは最初の正常なレプリカに送信されます。 ただし、レプリカは、現在設定されている順序で反復されます。 [load\_balancing](../../operations/settings/settings.md) 設定。

例えば:

``` text
example01-{01..02}-{1|2}
```

この例では、指定さつする資料をそれぞれ二つのレプリカ.

生成されるアドレスの数は定数によって制限されます。 今これは1000アドレスです。

を使用して `remote` テーブル関数が作成するよりも最適ではない `Distributed` この場合、サーバー接続はすべての要求に対して再確立されるためです。 さらに、ホスト名が設定されている場合、名前は解決され、さまざまなレプリカを操作するときにエラーはカウントされません。 多数のクエリを処理する場合は、常に `Distributed` テーブルは時間に先んじて、使用しないし `remote` テーブル機能。

その `remote` テーブル関数は、次の場合に便利です:

-   アクセスの特定のサーバーのためのデータとの比較、デバッグ、テスト実施をしておりました。
-   研究目的のための様々なclickhouseクラスタ間のクエリ。
-   手動で行われる頻度の低い分散要求。
-   サーバーのセットが毎回再定義される分散要求。

ユーザーが指定されていない場合, `default` 使用される。
パスワードを指定しない場合は、空のパスワードが使用されます。

`remoteSecure` -と同じ `remote` but with secured connection. Default port — [tcp\_port\_secure](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure) 設定または9440から。

[元の記事](https://clickhouse.tech/docs/en/query_language/table_functions/remote/) <!--hide-->
