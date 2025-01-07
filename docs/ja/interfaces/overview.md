---
slug: /ja/interfaces/overview
sidebar_label: Overview
sidebar_position: 1
keywords: [clickhouse, network, interfaces, http, tcp, grpc, command-line, client, jdbc, odbc, driver]
description: ClickHouse は 3つのネットワークインターフェースを提供します
---

# ドライバーとインターフェース

ClickHouse は3つのネットワークインターフェースを提供します（追加のセキュリティのためにTLSでラッピングすることができます）:

- [HTTP](http.md)、これは文書化されており直接使用するのが簡単です。
- オーバーヘッドが少ない[ネイティブTCP](../interfaces/tcp.md)。
- [gRPC](grpc.md)。

ほとんどのケースでは、これらに直接対話するのではなく、適切なツールやライブラリを使用することが推奨されます。ClickHouse が公式にサポートしているものは以下の通りです：

- [コマンドラインクライアント](../interfaces/cli.md)
- [JDBCドライバー](../interfaces/jdbc.md)
- [ODBCドライバー](../interfaces/odbc.md)
- [C++クライアントライブラリ](../interfaces/cpp.md)

ClickHouseサーバーはパワーユーザー向けの埋め込みビジュアルインターフェースを提供しています：

- Play UI: ブラウザで`/play`を開く;
- 高度なダッシュボード: ブラウザで`/dashboard`を開く;
- ClickHouseエンジニア向けのバイナリシンボルビューアー: ブラウザで`/binary`を開く;

ClickHouseと連携するためのサードパーティ製ライブラリも多数あります：

- [クライアントライブラリ](../interfaces/third-party/client-libraries.md)
- [統合](../interfaces/third-party/integrations.md)
- [ビジュアルインターフェース](../interfaces/third-party/gui.md)
