---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 76
toc_title: "\u30BB\u30AD\u30E5\u30EA\u30C6\u30A3\u306E\u5909\u66F4\u5C65\u6B74"
---

## ClickHouseリリース19.14.3.3で修正、2019-09-10 {#fixed-in-clickhouse-release-19-14-3-3-2019-09-10}

### CVE-2019-15024 {#cve-2019-15024}

Аn attacker that has write access to ZooKeeper and who ican run a custom server available from the network where ClickHouse runs, can create a custom-built malicious server that will act as a ClickHouse replica and register it in ZooKeeper. When another replica will fetch data part from the malicious replica, it can force clickhouse-server to write to arbitrary path on filesystem.

クレジット：yandex情報セキュリティチームのeldar zaitov

### CVE-2019-16535 {#cve-2019-16535}

Аn OOB read, OOB write and integer underflow in decompression algorithms can be used to achieve RCE or DoS via native protocol.

クレジット：yandex情報セキュリティチームのeldar zaitov

### CVE-2019-16536 {#cve-2019-16536}

スタックオーバーフローへのdosによっても実行させることができ、悪意のある認証クライアント

クレジット：yandex情報セキュリティチームのeldar zaitov

## クリックハウスリリース19.13.6.1で修正、2019-09-20 {#fixed-in-clickhouse-release-19-13-6-1-2019-09-20}

### CVE-2019-18657 {#cve-2019-18657}

テーブル機能 `url` この脆弱性により、攻撃者が要求に任意のHTTPヘッダーを挿入することができました。

クレジット: [Nikita Tikhomirov](https://github.com/NSTikhomirov)

## クリックハウスリリース18.12.13で修正、2018-09-10 {#fixed-in-clickhouse-release-18-12-13-2018-09-10}

### CVE-2018-14672 {#cve-2018-14672}

機能搭載catboostモデルの可路のフォーカストラバーサル読書任意のファイルをエラーメッセージが返されます。

クレジット：yandexの情報セキュリティチームのandrey krasichkov

## クリックハウスリリース18.10.3、2018-08-13で修正 {#fixed-in-clickhouse-release-18-10-3-2018-08-13}

### CVE-2018-14671 {#cve-2018-14671}

unixODBC許容荷重任意の共有オブジェクトからのファイルシステムにおけるリモートでコードが実行の脆弱性が存在します。

クレジット：yandexの情報セキュリティチームのandrey krasichkovとevgeny sidorov

## クリックハウスリリース1.1.54388で修正、2018-06-28 {#fixed-in-clickhouse-release-1-1-54388-2018-06-28}

### CVE-2018-14668 {#cve-2018-14668}

“remote” テーブル関数は、任意のシンボルを “user”, “password” と “default\_database” クロスプロトコル要求偽造攻撃につながったフィールド。

クレジット：yandexの情報セキュリティチームのandrey krasichkov

## クリックハウスリリース1.1.54390、2018-07-06で修正 {#fixed-in-clickhouse-release-1-1-54390-2018-07-06}

### CVE-2018-14669 {#cve-2018-14669}

ClickHouse MySQLクライアントは “LOAD DATA LOCAL INFILE” 接続されたClickHouseサーバから任意のファイルを読み取る悪意のあるMySQLデータベー

クレジット：yandexの情報セキュリティチームのandrey krasichkovとevgeny sidorov

## クリックハウスリリース1.1.54131、2017-01-10で修正されました {#fixed-in-clickhouse-release-1-1-54131-2017-01-10}

### CVE-2018-14670 {#cve-2018-14670}

Debパッケージの不適切な構成は、データベースの不正使用につながる可能性があります。

クレジット:英国の国家サイバーセキュリティセンター(ncsc)

{## [元の記事](https://clickhouse.tech/docs/en/security_changelog/) ##}
