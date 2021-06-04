---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: "\u30EA\u30E2\u30FC\u30C8"
---

# リモート、remoteSecure {#remote-remotesecure}

へ自由にアクセスできるリモートサーバーを作成することなく `Distributed` テーブル。

署名:

``` sql
remote('addresses_expr', db, table[, 'user'[, 'password']])
remote('addresses_expr', db.table[, 'user'[, 'password']])
remoteSecure('addresses_expr', db, table[, 'user'[, 'password']])
remoteSecure('addresses_expr', db.table[, 'user'[, 'password']])
```

`addresses_expr` – An expression that generates addresses of remote servers. This may be just one server address. The server address is `host:port`、または単に `host`. ホストは、サーバー名またはIPv4またはIPv6アドレスとして指定できます。 IPv6アドレスは角かっこで指定します。 ポートは、リモートサーバー上のTCPポートです。 ポートが省略されると、次のようになります `tcp_port` サーバーの設定ファイルから(デフォルトでは9000)。

!!! important "重要"
    ポートはIPv6アドレスに必要です。

例:

``` text
example01-01-1
example01-01-1:9000
localhost
127.0.0.1
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

複数のアドレスはコンマ区切りできます。 この場合、ClickHouseは分散処理を使用するため、指定されたすべてのアドレス（異なるデータを持つシャードなど）にクエリを送信します。

例:

``` text
example01-01-1,example01-02-1
```

式の一部は中括弧で指定できます。 前の例は次のように記述できます:

``` text
example01-0{1,2}-1
```

中括弧には、二つのドット（非負の整数）で区切られた数値の範囲を含めることができます。 この場合、範囲はシャードアドレスを生成する値のセットに拡張されます。 最初の数値がゼロで始まる場合、値は同じゼロ配置で形成されます。 前の例は次のように記述できます:

``` text
example01-{01..02}-1
```

中括弧のペアが複数ある場合、対応する集合の直接積を生成します。

中括弧の中の住所と住所の一部は、パイプ記号(\|)で区切ることができます。 この場合、対応するアドレスのセットはレプリカとして解釈され、クエリは最初の正常なレプリカに送信されます。 ただし、レプリカは、現在設定されている順序で反復処理されます。 [load_balancing](../../operations/settings/settings.md) 設定。

例:

``` text
example01-{01..02}-{1|2}
```

この例では、レプリカが二つあるシャードを指定します。

生成されるアドレスの数は定数によって制限されます。 今これは1000アドレスです。

を使用して `remote` テーブル関数は、 `Distributed` この場合、サーバー接続は要求ごとに再確立されるからです。 さらに、ホスト名が設定されている場合、名前は解決され、さまざまなレプリカで作業するときにエラーはカウントされません。 多数のクエリを処理する場合は、常に `Distributed` を使用しないでください。 `remote` テーブル関数。

その `remote` 表関数は、次の場合に役立ちます:

-   アクセスの特定のサーバーのためのデータとの比較、デバッグ、テスト実施をしておりました。
-   研究目的のための様々なClickHouseクラスター間のクエリ。
-   手動で行われる頻度の低い分散要求。
-   サーバーのセットが毎回定義される分散要求。

ユーザーが指定されていない場合, `default` が使用される。
パスワードを指定しない場合は、空のパスワードが使用されます。

`remoteSecure` -同じように `remote` but with secured connection. Default port — [tcp_port_secure](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure) 設定または9440から。

[元の記事](https://clickhouse.tech/docs/en/query_language/table_functions/remote/) <!--hide-->
