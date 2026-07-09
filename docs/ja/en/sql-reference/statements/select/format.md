---
description: 'FORMAT 句のドキュメント'
sidebar_label: 'FORMAT'
slug: /sql-reference/statements/select/format
title: 'FORMAT 句'
doc_type: 'reference'
---

ClickHouse は、[シリアライゼーション フォーマット](../../../interfaces/formats.md)を幅広くサポートしており、クエリ結果の出力などに利用できます。`SELECT` の出力フォーマットを選択する方法はいくつかありますが、その 1 つはクエリの末尾に `FORMAT format` を指定して、結果データを特定のフォーマットで取得する方法です。

特定のフォーマットは、利便性の向上、他のシステムとのインテグレーション、またはパフォーマンス向上のために使用できます。

<div id="default-format">
  ## デフォルトフォーマット
</div>

`FORMAT` 句を省略した場合は、デフォルトのフォーマットが使用されます。これは、設定と ClickHouse server へのアクセスに使用されるインターフェイスの両方に依存します。[HTTP インターフェイス](/ja/interfaces/http) と、バッチモードの [コマンドラインクライアント](../../../interfaces/client.md) では、デフォルトのフォーマットは `TabSeparated` です。対話型モードのコマンドラインクライアントでは、デフォルトのフォーマットは `PrettyCompact` です (コンパクトで人が読みやすいテーブルを出力します) 。

<div id="implementation-details">
  ## 実装の詳細
</div>

コマンドラインクライアントを使用する場合、データは常に内部の効率的なフォーマット (`Native`) でネットワーク経由でやり取りされます。クライアントはクエリの `FORMAT` 句を独自に解釈し、データのフォーマットも自身で行います (そのため、ネットワークやサーバーに余分な負荷がかかりません) 。