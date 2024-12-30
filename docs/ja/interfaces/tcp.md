---
slug: /ja/interfaces/tcp
sidebar_position: 18
sidebar_label: ネイティブ インターフェース (TCP)
---

# ネイティブ インターフェース (TCP)

ネイティブプロトコルは、[コマンドラインクライアント](../interfaces/cli.md)で使用され、分散クエリ処理中のサーバー間通信や他のC++プログラムでも使用されます。残念ながら、ネイティブClickHouseプロトコルにはまだ正式な仕様がありませんが、ClickHouseのソースコード（[こちら](https://github.com/ClickHouse/ClickHouse/tree/master/src/Client)あたりから）をリバースエンジニアリングすることや、TCPトラフィックを傍受して分析することで理解できます。
