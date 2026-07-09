---
description: 'アップグレードに関するドキュメント'
sidebar_title: 'セルフマネージドのアップグレード'
slug: /operations/update
title: 'セルフマネージドのアップグレード'
doc_type: 'guide'
---

<div id="clickhouse-upgrade-overview">
  ## ClickHouse アップグレードの概要
</div>

このドキュメントには、次の内容が含まれています。

* 一般的なガイドライン
* 推奨される手順
* システム上のバイナリをアップグレードする際の具体的な情報

<div id="general-guidelines">
  ## 一般的なガイドライン
</div>

以下の注意事項は、計画を立てる際の参考となり、後述する推奨事項の理由を理解するのに役立ちます。

<div id="upgrade-clickhouse-server-separately-from-clickhouse-keeper-or-zookeeper">
  ### ClickHouse server は ClickHouse Keeper または ZooKeeper とは別にアップグレードする
</div>

ClickHouse Keeper または Apache ZooKeeper に対するセキュリティ修正が必要な場合を除き、ClickHouse server のアップグレード時に Keeper までアップグレードする必要はありません。アップグレード中は Keeper の安定性を維持する必要があるため、Keeper のアップグレードを検討する前に、まず ClickHouse server のアップグレードを完了してください。

<div id="minor-version-upgrades-should-be-adopted-often">
  ### マイナーバージョンのアップグレードはこまめに行うべきです
</div>

新しいマイナーバージョンがリリースされたら、できるだけ早く最新のものへアップグレードすることを強く推奨します。マイナーリリースに互換性のない変更はありませんが、重要なバグ修正 (場合によってはセキュリティ修正も) が含まれています。

<div id="test-experimental-features-on-a-separate-clickhouse-server-running-the-target-version">
  ### 対象バージョンで稼働する別の ClickHouse server で実験的機能をテストする
</div>

実験的機能の互換性は、いつどのような形で失われてもおかしくありません。実験的機能を使用している場合は、変更履歴を確認し、対象バージョンをインストールした別の ClickHouse server を用意して、その環境で実験的機能の利用をテストすることを検討してください。

<div id="downgrades">
  ### ダウングレード
</div>

アップグレード後、新しいバージョンが依存している機能の一部と互換性がないことが判明した場合でも、新機能をまだ一切使用していなければ、比較的新しい (1年未満前の) バージョンにダウングレードできることがあります。新機能を使用してしまうと、ダウングレードはできなくなります。

<div id="multiple-clickhouse-server-versions-in-a-cluster">
  ### クラスター内で複数の ClickHouse server バージョンを使用する
</div>

当社では、1 年間の互換性期間 (2 つの LTS バージョンを含む) を維持するよう努めています。つまり、2 つのバージョンの差が 1 年未満である場合 (またはその間に LTS バージョンが 2 つ未満しかない場合) 、どの 2 つのバージョンでも同じクラスター内で一緒に動作できるはずです。ただし、分散クエリの速度低下や、ReplicatedMergeTree の一部のバックグラウンド操作で再試行可能なエラーが発生するなど、軽微な問題が起こる可能性があるため、クラスター内のすべてのメンバーはできるだけ早く同じバージョンにアップグレードすることを推奨します。

同じクラスター内で、リリース日の差が 1 年を超える異なるバージョンを実行することは、決して推奨しません。データ損失は想定していませんが、クラスターが使用不能になる可能性があります。バージョン差が 1 年を超える場合に想定される問題には、次のようなものがあります。

* クラスターが動作しない可能性がある
* 一部のクエリ、あるいはすべてのクエリが予測不能なエラーで失敗する可能性がある
* ログに予測不能なエラーや警告が表示される可能性がある
* ダウングレードできなくなる可能性がある

<div id="incremental-upgrades">
  ### 段階的アップグレード
</div>

現在のバージョンと対象バージョンの差が1年を超える場合は、次のいずれかの方法を推奨します。

* ダウンタイムを伴うアップグレード (すべてのサーバーを停止し、すべてのサーバーをアップグレードしてから、すべてのサーバーを起動する) 。
* または、中間バージョンを経由してアップグレードする (現在のバージョンより新しく、差が1年未満のバージョン) 。

<div id="recommended-plan">
  ## 推奨プラン
</div>

以下は、ClickHouse をダウンタイムなしでアップグレードする際の推奨手順です。

1. 設定変更はデフォルトの `/etc/clickhouse-server/config.xml` ファイルではなく、`/etc/clickhouse-server/config.d/` に配置されていることを確認してください。`/etc/clickhouse-server/config.xml` はアップグレード中に上書きされる可能性があるためです。
2. [changelog](/ja/whats-new/changelog/index.md) を確認し、互換性のない変更を把握してください (対象リリースから現在使用中のリリースまでさかのぼって確認します) 。
3. 互換性のない変更のうち、アップグレード前に実施できるものは事前に対応し、アップグレード後に必要となる変更については一覧を作成してください。
4. 各分片について、各分片内のほかのレプリカをアップグレードしている間も稼働を維持するため、1 つ以上のレプリカを特定してください。
5. アップグレード対象のレプリカごとに、1 台ずつ次を実施します。

* ClickHouse server を停止する
* server を対象バージョンにアップグレードする
* ClickHouse server を起動する
* システムが安定したことを示す Keeper メッセージが出るまで待つ
* 次のレプリカに進む

6. Keeper ログと ClickHouse ログにエラーがないか確認します

7. 手順 4 で特定したレプリカを新しいバージョンにアップグレードします。

8. 手順 1 〜 3 で作成した変更一覧を参照し、アップグレード後に必要な変更を実施してください。

:::note
レプリケート環境で複数のバージョンの ClickHouse が稼働している場合、このエラーメッセージは想定内です。すべてのレプリカが同じバージョンにアップグレードされると、これらは表示されなくなります。

```text
MergeFromLogEntryTask: Code: 40. DB::Exception: Checksums of parts don't match:
hash of uncompressed files doesn't match. (CHECKSUM_DOESNT_MATCH)  Data after merge is not
byte-identical to data on another replicas.
```

:::

<div id="clickhouse-server-binary-upgrade-process">
  ## ClickHouse server バイナリのアップグレード手順
</div>

ClickHouse を `deb` パッケージからインストールしている場合は、サーバー上で次のコマンドを実行します。

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

推奨されている`deb`パッケージ以外の方法でClickHouseをインストールした場合は、その方法に応じたアップデート手順を使用してください。

:::note
1つの分片に属するすべてのレプリカが同時にオフラインになることがなければ、複数のサーバーを同時にアップデートできます。
:::

古いバージョンのClickHouseを特定のバージョンにアップグレードする場合:

例:

`xx.yy.a.b`は現在の安定版バージョンです。最新の安定版バージョンは[こちら](https://github.com/ClickHouse/ClickHouse/releases)で確認できます

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-server=xx.yy.a.b clickhouse-client=xx.yy.a.b clickhouse-common-static=xx.yy.a.b
$ sudo service clickhouse-server restart
```