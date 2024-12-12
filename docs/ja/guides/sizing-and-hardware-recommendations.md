---
slug: /ja/guides/sizing-and-hardware-recommendations
sidebar_label: サイズおよびハードウェアの推奨事項
sidebar_position: 4
---

# サイズおよびハードウェアの推奨事項

このガイドでは、オープンソースユーザー向けのハードウェア、コンピュート、メモリ、およびディスク構成に関する一般的な推奨事項について説明します。セットアップを簡略化したい場合は、[ClickHouse Cloud](https://clickhouse.com/cloud) の使用をお勧めします。これは、インフラ管理に関連するコストを最小限に抑えつつ、ワークロードに自動的に対応し、スケールします。

ClickHouse クラスターの構成は、お客様のアプリケーションのユースケースやワークロードパターンによって大きく異なります。アーキテクチャを計画する際には、以下の要因を考慮する必要があります。

- 同時実行性（リクエスト数/秒）
- スループット（処理される行数/秒）
- データ量
- データ保持ポリシー
- ハードウェアコスト
- メンテナンスコスト

## ディスク

ClickHouse で使用するディスクの種類は、データ量、レイテンシー、スループットの要件によって異なります。

### パフォーマンスを最適化する

パフォーマンスを最大化するために、[AWS のプロビジョンドIOPS SSDボリューム](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/provisioned-iops.html) またはクラウドプロバイダーの同等の提供を直接接続することをお勧めします。これはIOを最適化します。

### ストレージコストを最適化する

コストを抑えるために、[汎用SSD EBSボリューム](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/general-purpose.html)を使用できます。また、[ホット/ウォーム/コールドアーキテクチャ](/ja/guides/developer/ttl#implementing-a-hotwarmcold-architecture)を使用してSSDとHDDを組み合わせた階層型ストレージを実装することもできます。また、ストレージを分離するために[AWS S3](https://aws.amazon.com/s3/)を使用することも可能です。コンピュートとストレージの分離でオープンソースのClickHouseを使用するためのガイドは[こちら](/ja/guides/separation-storage-compute)をご覧ください。ClickHouse Cloud ではデフォルトでコンピュートとストレージの分離が利用可能です。

## CPU

### どの CPU を使用すべきか？

使用する CPU の種類は、使用パターンに依存します。ただし、一般的に、多くの頻繁な同時実行クエリを処理し、より多くのデータを処理する、または計算集約的な UDF を使用するアプリケーションは、より多くの CPU コアを必要とします。

**低レイテンシまたは顧客向けアプリケーション**

顧客向けワークロードのように10ミリ秒単位の低レイテンシ要件に対しては、AWS の [i3 ライン](https://aws.amazon.com/ec2/instance-types/i3/) または [i4i ライン](https://aws.amazon.com/ec2/instance-types/i4i/) またはクラウドプロバイダーの同等のIO最適化された提供をお勧めします。

**高同時実行性アプリケーション**

同時実行性を最適化する必要があるワークロード（1秒あたり100以上のクエリ）に対しては、AWS の[計算最適化されたCシリーズ](https://aws.amazon.com/ec2/instance-types/#Compute_Optimized) またはクラウドプロバイダーの同等の提供をお勧めします。

**データウェアハウジングユースケース**

データウェアハウジングワークロードやアドホック分析クエリに対しては、AWS の [Rタイプシリーズ](https://aws.amazon.com/ec2/instance-types/#Memory_Optimized) 又はクラウドプロバイダーのメモリ最適化された提供をお勧めします。

---

### CPU 利用率はどの程度にすべきか？

ClickHouse に標準的な CPU 利用率の目標はありません。[iostat](https://linux.die.net/man/1/iostat) などのツールを利用して平均の CPU 使用率を測定し、予期しないトラフィックの急増を管理できるようにサーバーのサイズを調整します。ただし、アドホッククエリを伴う分析またはデータウェアハウジングユースケースの場合、10-20% の CPU 利用率を目標とすべきです。

### 何 CPU コア使うべきか？

使用する CPU 数はワークロードに依存します。しかし、一般的に、CPU の種類に基づいて以下のメモリ対 CPU コア比を推奨します。

- **[Mタイプ](https://aws.amazon.com/ec2/instance-types/)（一般的な利用ケース）：** メモリ対 CPU コア比 4:1
- **[Rタイプ](https://aws.amazon.com/ec2/instance-types/#Memory_Optimized)（データウェアハウジングユースケース）：** メモリ対 CPU コア比 8:1
- **[Cタイプ](https://aws.amazon.com/ec2/instance-types/#Compute_Optimized)（計算最適化ユースケース）：** メモリ対 CPU コア比 2:1

具体例として、Mタイプの CPU を使用する場合は、25 CPU コアあたり 100GB のメモリをプロビジョニングすることをお勧めします。適切なメモリ量を決定するには、メモリ使用量をプロファイリングする必要があります。[メモリ問題のデバッグに関するガイド](https://clickhouse.com/docs/ja/guides/developer/debugging-memory-issues)を読むか、[組み込みのオブザーバビリティダッシュボード](https://clickhouse.com/docs/ja/operations/monitoring)を使用して ClickHouse を監視できます。

## メモリ

CPU の選択と同様に、ストレージ対メモリの比率やメモリ対 CPU の比率の選択はユースケースに依存します。ただし、一般的にはメモリが多いほどクエリは高速になります。価格に敏感なユースケースの場合、低メモリ量でも動作しますが、設定を有効にすることができます（[max_bytes_before_external_group_by](/ja/operations/settings/query-complexity#settings-max_bytes_before_external_group_by) や [max_bytes_before_external_sort](/ja/operations/settings/query-complexity#settings-max_bytes_before_external_sort)）。これによりデータをディスクにスピルすることが許可されますが、クエリパフォーマンスに大きな影響を与える可能性があります。

### メモリ対ストレージ比はどの程度にすべきか？

低データ量の場合、1:1 のメモリ対ストレージ比が許容されますが、合計メモリは8GB未満にしてはいけません。

長期間のデータ保持や高データ量のユースケースの場合、1:100 から 1:130 のメモリ対ストレージ比を推奨します。例えば、10TB のデータを保存している場合、レプリカあたり 100GB の RAM が良い例です。

顧客向けワークロードのように頻繁にアクセスされるユースケースには、より多くのメモリを使用し、1:30 から 1:50 のメモリ対ストレージ比を推奨します。

## レプリカ

各シャードに少なくとも3つのレプリカを持つことを推奨します（または [Amazon EBS](https://aws.amazon.com/ebs/) を使用した 2 つのレプリカ）。さらに、追加のレプリカを追加する前にすべてのレプリカを垂直スケーリングすることを提案します（水平スケーリング）。

ClickHouse は自動的にシャーディングを行わないため、データセットの再シャーディングには多大なコンピューティングリソースが必要になります。したがって、将来的にデータを再シャーディングする必要を避けるため、可能な限り大きなサーバーを使用することを一般的に推奨します。

[ClickHouse Cloud](https://clickhouse.com/cloud) を使用することを検討してください。これは自動的にスケールし、ユースケースに合わせてレプリカの数を簡単に制御できます。

## 大規模ワークロードの例示的な構成

ClickHouse の構成は、特定のアプリケーションの要件に大きく依存します。コストとパフォーマンスを最適化するための支援を希望される場合は、[営業までお問い合わせください](https://clickhouse.com/company/contact?loc=docs-sizing-and-hardware-recommendations)。

ガイダンス（推奨ではありません）を提供するために、以下は本番環境での ClickHouse ユーザーの例示的な構成です。

### Fortune 500 B2B SaaS

<table>
    <tr>
        <td col="2"><strong><em>ストレージ</em></strong></td>
    </tr>
    <tr>
        <td><strong>月間新規データ量</strong></td>
        <td>30TB</td>
    </tr>
    <tr>
        <td><strong>総ストレージ（圧縮）</strong></td>
        <td>540TB</td>
    </tr>
    <tr>
        <td><strong>データ保持</strong></td>
        <td>18ヶ月</td>
    </tr>
    <tr>
        <td><strong>ノードあたりのディスク</strong></td>
        <td>25TB</td>
    </tr>
    <tr>
        <td col="2"><strong><em>CPU</em></strong></td>
    </tr>
    <tr>
        <td><strong>同時実行性</strong></td>
        <td>200以上の同時クエリ</td>
    </tr>
    <tr>
        <td><strong>レプリカ数（HAペアを含む）</strong></td>
        <td>44</td>
    </tr>
    <tr>
        <td><strong>ノードあたりの vCPU</strong></td>
        <td>62</td>
    </tr>
    <tr>
        <td><strong>総 vCPU</strong></td>
        <td>2700</td>
    </tr>
    <tr>
        <td col="2"><strong><em>メモリ</em></strong></td>
    </tr>
    <tr>
        <td><strong>総 RAM</strong></td>
        <td>11TB</td>
    </tr>
    <tr>
        <td><strong>レプリカあたりの RAM</strong></td>
        <td>256GB</td>
    </tr>
    <tr>
        <td><strong>RAM 対 vCPU 比率</strong></td>
        <td>4:1</td>
    </tr>
    <tr>
        <td><strong>RAM 対ディスク比率</strong></td>
        <td>1:50</td>
    </tr>
</table>

### Fortune 500 テレコムオペレーターのログユースケース

<table>
    <tr>
        <td col="2"><strong><em>ストレージ</em></strong></td>
    </tr>
    <tr>
        <td><strong>月間ログデータ量</strong></td>
        <td>4860TB</td>
    </tr>
    <tr>
        <td><strong>総ストレージ（圧縮）</strong></td>
        <td>608TB</td>
    </tr>
    <tr>
        <td><strong>データ保持</strong></td>
        <td>30日</td>
    </tr>
    <tr>
        <td><strong>ノードあたりのディスク</strong></td>
        <td>13TB</td>
    </tr>
    <tr>
        <td col="2"><strong><em>CPU</em></strong></td>
    </tr>
    <tr>
        <td><strong>レプリカ数（HAペアを含む）</strong></td>
        <td>38</td>
    </tr>
    <tr>
        <td><strong>ノードあたりの vCPU</strong></td>
        <td>42</td>
    </tr>
    <tr>
        <td><strong>総 vCPU</strong></td>
        <td>1600</td>
    </tr>
    <tr>
        <td col="2"><strong><em>メモリ</em></strong></td>
    </tr>
    <tr>
        <td><strong>総 RAM</strong></td>
        <td>10TB</td>
    </tr>
    <tr>
        <td><strong>レプリカあたりの RAM</strong></td>
        <td>256GB</td>
    </tr>
    <tr>
        <td><strong>RAM 対 vCPU 比率</strong></td>
        <td>6:1</td>
    </tr>
    <tr>
        <td><strong>RAM 対ディスク比率</strong></td>
        <td>1:60</td>
    </tr>
</table>

## さらなる読み物

以下は、オープンソースの ClickHouse を使用する企業のアーキテクチャに関するブログ記事です。

- [Cloudflare](https://blog.cloudflare.com/http-analytics-for-6m-requests-per-second-using-clickhouse/?utm_source=linkedin&utm_medium=social&utm_campaign=blog)
- [eBay](https://innovation.ebayinc.com/tech/engineering/ou-online-analytical-processing/)
- [GitLab](https://handbook.gitlab.com/handbook/engineering/development/ops/monitor/observability/#clickhouse-datastore)
- [Lyft](https://eng.lyft.com/druid-deprecation-and-clickhouse-adoption-at-lyft-120af37651fd)
- [MessageBird](https://clickhouse.com/blog/how-messagebird-uses-clickhouse-to-monitor-the-delivery-of-billions-of-messages)
- [Microsoft](https://clickhouse.com/blog/self-service-data-analytics-for-microsofts-biggest-web-properties)
- [Uber](https://www.uber.com/en-ES/blog/logging/)
- [Zomato](https://blog.zomato.com/building-a-cost-effective-logging-platform-using-clickhouse-for-petabyte-scale)
