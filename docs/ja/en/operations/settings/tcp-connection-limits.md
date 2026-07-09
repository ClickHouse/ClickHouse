---
description: 'TCP接続の制限。'
sidebar_label: 'TCP接続の制限'
slug: /operations/settings/tcp-connection-limits
title: 'TCP接続の制限'
doc_type: 'reference'
---

<div id="overview">
  ## 概要
</div>

ClickHouse の TCP 接続 (つまり、[コマンドラインクライアント](https://clickhouse.com/docs/interfaces/client) 経由の接続) は、一定数のクエリ実行後、または一定時間の経過後に自動的に切断されることがあります。
切断後、自動的に再接続されることはありません (コマンドラインクライアントで別のクエリを送信するなど、
別の要因でトリガーされた場合を除きます) 。

接続制限は、サーバー設定
`tcp_close_connection_after_queries_num` (クエリ数の制限) 
または `tcp_close_connection_after_queries_seconds` (時間の制限) を 0 より大きい値に設定することで有効になります。
両方の制限が有効な場合は、どちらか一方に先に達した時点で接続がクローズされます。

制限に達して切断されると、クライアントは
`TCP_CONNECTION_LIMIT_REACHED` 例外を受け取り、**切断の原因となったクエリは処理されません**。

<div id="query-limits">
  ## クエリ制限
</div>

`tcp_close_connection_after_queries_num` が N に設定されている場合、その接続では
成功したクエリを N 件まで実行できます。その後、N + 1 件目のクエリでクライアントは切断されます。

処理されたすべてのクエリがクエリ制限にカウントされます。したがって、コマンドラインクライアントで接続する場合、
自動的に最初の system warnings クエリが実行され、それも制限にカウントされることがあります。

TCP 接続がアイドル状態の場合 (つまり、一定時間クエリが処理されておらず、
その時間はセッション設定 `poll_interval` で指定されます) 、
それまでにカウントされたクエリ数は 0 にリセットされます。
つまり、アイドル状態が発生した場合、1 つの接続でのクエリ総数が
`tcp_close_connection_after_queries_num` を超える可能性があります。

<div id="duration-limits">
  ## 接続時間の制限
</div>

接続時間は、クライアントが接続した時点から計測されます。
`tcp_close_connection_after_queries_seconds` 秒が経過した後、最初のクエリ実行時にクライアントは切断されます。