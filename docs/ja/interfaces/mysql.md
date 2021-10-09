---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 20
toc_title: "MySQL\u30A4\u30F3\u30BF"
---

# MySQLインタ {#mysql-interface}

ClickHouseはMySQL wire protocolをサポートしています。 で有効にすることができる [mysql_port](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-mysql_port) 設定ファイルでの設定:

``` xml
<mysql_port>9004</mysql_port>
```

コマンドラインツールを使用した接続例 `mysql`:

``` bash
$ mysql --protocol tcp -u default -P 9004
```

接続が成功した場合の出力:

``` text
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 4
Server version: 20.2.1.1-ClickHouse

Copyright (c) 2000, 2019, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql>
```

との互換性を維持するため、すべてのMySQLのお客様におすすめで指定ユーザのパスワード [ダブルSHA1](../operations/settings/settings-users.md#password_double_sha1_hex) 設定ファイル。
場合は、ユーザのパスワードが指定 [SHA256](../operations/settings/settings-users.md#password_sha256_hex) 一部のクライアントは認証できません（mysqljsおよび古いバージョンのコマンドラインツールmysql）。

制限:

-   作成問合せには対応していない

-   一部のデータ型は文字列として送信されます

[元の記事](https://clickhouse.com/docs/en/interfaces/mysql/) <!--hide-->
