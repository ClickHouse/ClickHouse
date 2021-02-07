---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: "\u8981\u4EF6"
---

# 要件 {#requirements}

## CPU {#cpu}

ビルド済みのdebパッケージからインストールするには、X86_64アーキテクチャのCPUを使用し、SSE4.2命令をサポートします。 走ClickHouseプロセッサをサポートしていないSSE4.2てAArch64はPowerPC64LE建築、協力して進めることが必要でありClickHouseから。

ClickHouseを実装した並列データの処理-利用のすべてのハードウェア資料を備えています。 プロセッサを選択するときは、ClickHouseが多数のコアを持つ構成でより効率的に動作するが、コアが少なくクロックレートが高い構成ではatよりも低いク たとえば、16 2600MHzのコアは、8 3600MHzのコアよりも好ましいです。

使用することをお勧めします **ターボブースト** と **ハイパースレッド** テクノロジー 大幅なパフォーマンス向上を図は、典型的な負荷も大きくなっていました。

## RAM {#ram}

些細でないクエリを実行するには、最小4GBのRAMを使用することをお勧めします。 ClickHouseサーバーは、はるかに少ない量のRAMで実行できますが、クエリを処理するためにメモリが必要です。

必要なRAM容量は次のとおりです:

-   クエリの複雑さ。
-   クエリで処理されるデータの量。

RAMの必要量を計算するには、次のような一時データのサイズを推定する必要があります [GROUP BY](../sql-reference/statements/select/group-by.md#select-group-by-clause), [DISTINCT](../sql-reference/statements/select/distinct.md#select-distinct), [JOIN](../sql-reference/statements/select/join.md#select-join) そしてあなたが使用する他の操作。

ClickHouseは一時データに外部メモリを使用できます。 見る [外部メモリのGROUP BY](../sql-reference/statements/select/group-by.md#select-group-by-in-external-memory) 詳細については.

## Swapファイル {#swap-file}

運用環境のスワップファイルを無効にします。

## Storageサブシステム {#storage-subsystem}

ClickHouseをインストールするには、2GBの空きディスク領域が必要です。

データに必要なストレージ容量は、別々に計算する必要があります。 評価には:

-   データ量の推定。

    データのサンプルを取得し、そこから行の平均サイズを取得できます。 次に、値に格納する予定の行数を掛けます。

-   データ圧縮係数。

    データ圧縮係数を推定するには、データのサンプルをClickHouseに読み込み、データの実際のサイズと格納されているテーブルのサイズを比較します。 たとえば、clickstreamデータは通常6-10回圧縮されます。

格納するデータの最終ボリュームを計算するには、推定データボリュームに圧縮係数を適用します。 複数のレプリカにデータを格納する場合は、推定ボリュームにレプリカの数を掛けます。

## ネット {#network}

可能であれば、10G以上のネットワークを使用してください。

大量の中間データを含む分散クエリを処理するには、ネットワーク帯域幅が重要です。 また、ネットワーク速度に影響する複製プロセス。

## ソフト {#software}

ClickHouseの開発を中心に、Linuxの家族システムです。 推奨されるLinuxディストリビュ その `tzdata` パッケ

ClickHouse働きかけることができ、その他業務システム。 の詳細を参照してください [はじめに](../getting-started/index.md) ドキュメントのセクション。
