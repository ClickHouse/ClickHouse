---
sidebar_label: Java
sidebar_position: 1
keywords: [clickhouse, java, jdbc, client, integrate, r2dbc]
description: Java から ClickHouse への接続オプション
slug: /ja/integrations/java
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeBlock from '@theme/CodeBlock';

# Java クライアント概要

- [Client-V2](./client-v2.md)
- [Client-V1 (旧版)](./client-v1.md)
- [JDBC ドライバー](./jdbc-driver.md)
- [R2DBC ドライバー](./r2dbc.md)

## ClickHouse クライアント

Java クライアントは、ClickHouse サーバーとのネットワーク通信の詳細を抽象化する独自の API を実装するライブラリです。現在、HTTP インターフェースのみサポートされています。このライブラリは、様々な ClickHouse フォーマットおよび関連する機能を扱うためのユーティリティを提供します。

Java クライアントは 2015 年に開発されました。そのコードベースは非常に維持しにくく、API は混乱し、これ以上最適化するのが難しくなりました。そのため、2024 年に新しいコンポーネント `client-v2` へのリファクタリングを行いました。これには、明確な API、より軽量なコードベース、パフォーマンスの向上、より良い ClickHouse フォーマットサポート（主に RowBinary と Native）が含まれます。JDBC は近い将来このクライアントを使用する予定です。

### サポートされているデータタイプ

|**データタイプ**       |**Client V2 サポート**|**Client V1 サポート**|
|-----------------------|----------------------|----------------------|
|Int8                   |✔                     |✔                     |
|Int16                  |✔                     |✔                     |
|Int32                  |✔                     |✔                     |
|Int64                  |✔                     |✔                     |
|Int128                 |✔                     |✔                     |
|Int256                 |✔                     |✔                     |
|UInt8                  |✔                     |✔                     |
|UInt16                 |✔                     |✔                     |
|UInt32                 |✔                     |✔                     |
|UInt64                 |✔                     |✔                     |
|UInt128                |✔                     |✔                     |
|UInt256                |✔                     |✔                     |
|Float32                |✔                     |✔                     |
|Float64                |✔                     |✔                     |
|Decimal                |✔                     |✔                     |
|Decimal32              |✔                     |✔                     |
|Decimal64              |✔                     |✔                     |
|Decimal128             |✔                     |✔                     |
|Decimal256             |✔                     |✔                     |
|Bool                   |✔                     |✔                     |
|String                 |✔                     |✔                     |
|FixedString            |✔                     |✔                     |
|Nullable               |✔                     |✔                     |
|Date                   |✔                     |✔                     |
|Date32                 |✔                     |✔                     |
|DateTime               |✔                     |✔                     |
|DateTime32             |✔                     |✔                     |
|DateTime64             |✔                     |✔                     |
|Interval               |✗                     |✗                     |
|Enum                   |✔                     |✔                     |
|Enum8                  |✔                     |✔                     |
|Enum16                 |✔                     |✔                     |
|Array                  |✔                     |✔                     |
|Map                    |✔                     |✔                     |
|Nested                 |✔                     |✔                     |
|Tuple                  |✔                     |✔                     |
|UUID                   |✔                     |✔                     |
|IPv4                   |✔                     |✔                     |
|IPv6                   |✔                     |✔                     |
|Object                 |✗                     |✔                     |
|Point                  |✔                     |✔                     |
|Nothing                |✔                     |✔                     |
|MultiPolygon           |✔                     |✔                     |
|Ring                   |✔                     |✔                     |
|Polygon                |✔                     |✔                     |
|SimpleAggregateFunction|✔                     |✔                     |
|AggregateFunction      |✗                     |✔                     |

[ClickHouse データタイプ](/docs/ja/sql-reference/data-types)

:::note
- AggregatedFunction - :warning: `SELECT * FROM table ...` はサポートされていません
- Decimal - 一貫性のために 21.9+ で `SET output_format_decimal_trailing_zeros=1` を使用
- Enum - 文字列および整数の両方として扱うことができます
- UInt64 - client-v1 では `long` にマッピングされます
:::

### 機能

クライアントの機能の表:

| 名称                                         | Client V2 | Client V1 | コメント |
|----------------------------------------------|:---------:|:---------:|:---------:|
| Http 接続                                    |✔        |✔       | |
| Http 圧縮（LZ4）                              |✔        |✔       | |
| サーバー応答の圧縮 - LZ4                      |✔        |✔       | | 
| クライアント要求の圧縮 - LZ4                  |✔        |✔       | |
| HTTPs                                        |✔        |✔       | |
| クライアント SSL 証明書 (mTLS)                |✔        |✔       | |
| Http プロキシ                                 |✔        |✔       | |
| POJO シリアライズ・デシリアライズ             |✔        |✗       | |
| コネクションプール                            |✔        |✔       | Apache HTTP クライアントを使用する場合 |
| 名前付きパラメータ                            |✔        |✔       | |
| 失敗時のリトライ                              |✔        |✔       | |
| フェイルオーバー                              |✗        |✔       | |
| ロードバランシング                            |✗        |✔       | |
| サーバー自動検出                              |✗        |✔       | |
| ログコメント                                  |✗        |✔       | |
| セッションロール                              |✗        |✔       | V2 で導入予定 |
| SSL クライアント認証                           |✗        |✔       | V2 で導入予定 |
| セッションタイムゾーン                         |✔        |✔       | |

JDBC ドライバーは、基盤となるクライアント実装と同様の機能を継承します。他の JDBC の機能はその [ページ](/docs/ja/integrations/java/jdbc-driver#features) に記載されています。

### 互換性

- このリポジトリ内のすべてのプロジェクトは、すべての [アクティブな LTS バージョン](https://github.com/ClickHouse/ClickHouse/pulls?q=is%3Aopen+is%3Apr+label%3Arelease) の ClickHouse でテストされています。
- [サポートポリシー](https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md#security-change-log-and-support)
- セキュリティ修正や新たな改善を逃さないよう、クライアントを継続的にアップグレードすることをお勧めします
- v2 API への移行で問題がある場合は、[問題を作成](https://github.com/ClickHouse/clickhouse-java/issues/new?assignees=&labels=v2-feedback&projects=&template=v2-feedback.md&title=) していただければ対応します！
