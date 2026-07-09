---
description: 'Logエンジンファミリーのドキュメント'
sidebar_label: 'Logエンジンファミリー'
sidebar_position: 20
slug: /engines/table-engines/log-family/
title: 'Logエンジンファミリー'
doc_type: 'guide'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="log-table-engine-family">
  # Log テーブルエンジンファミリー
</div>

<CloudNotSupportedBadge />

これらのエンジンは、多数の小さなテーブル (最大約 100 万行) へすばやく書き込み、後でテーブル全体をまとめて読み取る必要がある場合のために開発されました。

このファミリーのエンジン:

| Log エンジン                                                    |
| ----------------------------------------------------------- |
| [StripeLog](/ja/engines/table-engines/log-family/stripelog.md) |
| [Log](/ja/engines/table-engines/log-family/log.md)             |
| [TinyLog](/ja/engines/table-engines/log-family/tinylog.md)     |

`Log` ファミリーのテーブルエンジンは、[HDFS](/ja/engines/table-engines/integrations/hdfs) または [S3](/ja/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3) の分散ファイルシステムにデータを保存できます。

:::warning このエンジンはログデータ向けではありません。
名前に反して、*Log テーブルエンジンはログデータの保存を目的としたものではありません。すばやく書き込む必要がある少量のデータにのみ使用してください。
:::

<div id="common-properties">
  ## 共通の特性
</div>

エンジン:

* データをディスクに保存します。

* 書き込み時には、ファイルの末尾にデータを追記します。

* データへの同時実行アクセス向けのロックをサポートします。

  `INSERT` クエリの実行中はテーブルがロックされるため、データの読み取り用・書き込み用の他のクエリは、いずれもテーブルのロックが解除されるまで待機します。データ書き込みクエリが存在しない場合は、任意の数のデータ読み取りクエリを同時に実行できます。

* [ミューテーション](/ja/sql-reference/statements/alter#mutations)はサポートしていません。

* 索引はサポートしていません。

  つまり、データの範囲に対する `SELECT` クエリは効率的ではありません。

* データはアトミックに書き込まれません。

  たとえばサーバーの異常終了などにより書き込み処理が中断されると、テーブル内のデータが破損する可能性があります。

<div id="differences">
  ## 違い
</div>

`TinyLog` エンジンはこのファミリーの中で最もシンプルですが、機能は最も限定的で、効率も最も低くなります。`TinyLog` エンジンは、単一のクエリで複数スレッドによる並列データ読み取りをサポートしていません。また、各カラムを別々のファイルに格納するため、`Log` エンジンとほぼ同数のファイルディスクリプタを使用します。そのうえ、単一クエリでの並列読み取りをサポートするこのファミリーの他のエンジンと比べて、データの読み取り速度も低くなります。使用するのは単純なシナリオに限ってください。

`Log` エンジンと `StripeLog` エンジンは、並列データ読み取りをサポートしています。データ読み取り時には、ClickHouse は複数のスレッドを使用します。各スレッドは別々のデータブロックを処理します。`Log` エンジンは、テーブルの各カラムごとに個別のファイルを使用します。`StripeLog` はすべてのデータを 1 つのファイルに格納します。その結果、`StripeLog` エンジンのほうが使用するファイルディスクリプタは少なくなりますが、データ読み取り時の効率は `Log` エンジンのほうが高くなります。