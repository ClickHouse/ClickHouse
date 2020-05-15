---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 44
toc_title: "\u8981\u4EF6"
---

# 要件 {#requirements}

## CPU {#cpu}

Prebuilt debパッケージからインストールする場合は、X86\_64アーキテクチャを持つCPUを使用し、SSE4.2命令をサポートします。 Sse4.2をサポートしていない、またはAArch64またはPowerPC64LEアーキテクチャを持つプロセッサでClickHouseを実行するには、ソースからClickHouseをビルドする必要があり

ClickHouseは、並列処理を実装し、利用可能なすべてのハードウェアリソースを使用します。 プロセッサを選択するときは、コア数が多い構成では、コア数が少ない構成よりもクロックレートが低く、クロックレートが高い構成では、クリックハウ 例えば、16MHzの2600コアは、8MHzの3600コアよりも好ましい。

の使用 **ターボブースト** と **ハイパースレッド** 技術が推奨されます。 典型的な負荷でパフォーマンスが大幅に向上します。

## RAM {#ram}

些細なクエリを実行するには、最小4gbのramを使用することをお勧めします。 のclickhouseサーバーへアクセスできる走りはるかに小さなramが要求されるメモリ処理ます。

必要なramの容量は次のとおりです:

-   クエリの複雑さ。
-   クエリで処理されるデータの量。

計算に必要な量のram、推定値のサイズを一時的にデータのための [GROUP BY](../sql-reference/statements/select.md#select-group-by-clause), [DISTINCT](../sql-reference/statements/select.md#select-distinct), [JOIN](../sql-reference/statements/select.md#select-join) そしてあなたが使用する他の操作。

ClickHouseは、一時的なデータに外部メモリを使用できます。 見る [外部メモリによるグループ化](../sql-reference/statements/select.md#select-group-by-in-external-memory) 詳細については。

## Swapファイル {#swap-file}

運用環境用のスワップファイルを無効にします。

## 格納サブシステム {#storage-subsystem}

ClickHouseをインストールするには2GBの空きディスク容量が必要です。

データに必要なストレージ容量は、個別に計算する必要があります。 評価には:

-   データ量の推定。

    データのサンプルを取得し、そこから行の平均サイズを取得できます。 次に、値に格納する予定の行の数を掛けます。

-   データ圧縮係数。

    データ圧縮係数を推定するには、データのサンプルをclickhouseにロードし、データの実際のサイズと格納されているテーブルのサイズを比較します。 たとえば、通常、クリックストリームデータは6-10倍圧縮されます。

保存するデータの最終ボリュームを計算するには、推定データボリュームに圧縮係数を適用します。 複数のレプリカにデータを格納する場合は、推定ボリュームにレプリカの数を掛けます。

## ネットワーク {#network}

可能であれば、10g以上のネットワークを使用してください。

ネットワーク帯域幅は、大量の中間データを使用して分散クエリを処理する場合に重要です。 また、ネットワーク速度に影響する複製プロセス。

## ソフト {#software}

ClickHouseが開発されたLinuxの家族システムです。 推奨されるLinuxの配布はUbuntuです。 その `tzdata` パッケージを設置する必要がある。

ClickHouse働きかけることができ、その他業務システム。 の詳細を参照してください [はじめに](../getting-started/index.md) ドキュメントのセクション。
