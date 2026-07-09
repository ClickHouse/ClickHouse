---
description: 'ウェブターミナルのドキュメント。ブラウザ内で WebSocket 経由の対話型 `clickhouse-client` セッションを提供します'
sidebar_label: 'ウェブターミナル'
sidebar_position: 22
slug: /interfaces/web-terminal
title: 'ウェブターミナル'
doc_type: 'reference'
---

ウェブターミナルは、ブラウザ内で WebSocket 経由の対話型 `clickhouse-client` セッションを提供するインターフェイスです。任意の ClickHouse HTTP ポートの `/webterminal` パスで提供されます。

任意の ClickHouse HTTP ポートの `/webterminal` (たとえば `http://localhost:8123/webterminal`) にアクセスすると、ターミナルを開けます。

<div id="enabling-the-feature">
  ## 機能の有効化と無効化
</div>

`/webterminal` エンドポイントはデフォルトで有効になっており、`enable_webterminal` サーバー設定で制御されます。無効にするには、この設定を `false` にします。すると、`/webterminal` へのリクエストに対して HTTP ステータス `403 Forbidden` が返されます。

```xml
<clickhouse>
    <enable_webterminal>false</enable_webterminal>
</clickhouse>
```

:::note
`enable_webterminal` は、従来の `allow_experimental_webterminal` 設定に置き換わるものです。`enable_webterminal` が設定されていない場合は、後方互換性のため、古い名前も引き続き使用できます。
:::

<div id="authentication">
  ## 認証
</div>

ウェブターミナルでは、HTTPプロトコルと同じ `Session` およびアクセス制御のチェックを用いてユーザーを認証しますが、credentials は HTTP Upgrade リクエスト経由ではなく、確立済みの WebSocket connection 上でインバンドにやり取りされます。WebSocket のハンドシェイクが完了すると、ブラウザーは最初のメッセージを JSON として送信します。

```json
{"type": "auth", "user": "<user>", "password": "<password>"}
```

これにより、認証情報を URLクエリパラメータや、アップグレードリクエストに付与された `Authorization` ヘッダーに含めずに済みます。これらの場所に含めると、ブラウザーの履歴、サーバーのアクセスログ、リバースプロキシのログに残ってしまう可能性があります。アップグレードリクエストの URLパラメータ、HTTP Basic、`X-ClickHouse-User`/`X-ClickHouse-Key` ヘッダーは、`/webterminal` では意図的に**参照されません**。

認証情報が無効な場合、サーバーはコード `1008` で WebSocket を閉じ、ブラウザーUI は認証情報の再入力を求めます。

<div id="session">
  ## セッションの構成
</div>

認証が完了すると、サーバーは擬似端末上で `clickhouse-client` を実行し、その入出力を WebSocket 経由で中継します。このセッションでは、`clickhouse-client` の機能をフルに利用でき、以下が含まれます。

* 構文ハイライト。
* 自動補完。
* 複数行クエリ。
* コマンド履歴 (セッションの有効期間中はサーバー側に保存されます) 。

端末の描画には [xterm.js](https://xtermjs.org/) を使用しています。すべてのアセットは ClickHouse バイナリ自体から配信され、サードパーティ製 CDN は読み込まれません。

<div id="play-integration">
  ## `/play` とのインテグレーション
</div>

[`/play`](/ja/interfaces/http) の Web SQL UI には、ウェブターミナルがドッキング可能なパネルとして組み込まれています。サイドバーのターミナルアイコンで表示を切り替えるか、クエリエディタが空の状態で `~` キーを押してください。`/play` ページは読み込み時に `/webterminal` が利用可能かどうかを検出し、エンドポイント が利用できない場合 (たとえば `enable_webterminal` が `false` に設定されている場合) は、ターミナルの操作項目を非表示にします。

<div id="security">
  ## セキュリティに関する考慮事項
</div>

ウェブターミナルは、ClickHouse の HTTP エンドポイントで認証できるすべてのユーザーに対して、対話型のシェル風セッションを提供します。そのため、HTTP プロトコルに適用される注意事項は、ここでも同様に当てはまります。

* 信頼できない環境では、認証情報とセッショントラフィックを保護するため、必ず `/webterminal` を HTTPS 経由で提供してください。
* HTTP プロトコルへのアクセスを制限するのと同様に、ネットワークレベル (ファイアウォール、リバースプロキシ、または `listen_host` 設定) でもアクセスを制限してください。
* このエンドポイントは、クロスオリジンの WebSocket ハイジャックを防ぐため、`Origin` ヘッダーを `Host` と照合して検証します。TLS を外部で終端する場合は、それに合わせてリバースプロキシを設定してください。
* TLS を終端するリバースプロキシの背後では、ブラウザが `https` を使用していても、ClickHouse へのアップストリーム接続は平文の `http` になります。そのため、厳格な same-origin チェックでは正当な接続も拒否されます。このようなデプロイでは、WebSocket セッションを開くことを許可する完全なオリジンのカンマ区切りリストを `webterminal_allowed_origins` に設定してください。この設定が空でない場合、デフォルトの same-origin チェックの代わりに使用されます。例: `<webterminal_allowed_origins>https://example.com,https://app.example.com:8443</webterminal_allowed_origins>`。

このハンドラーは、RFC 6455 に準拠した WebSocket プロトコルの適合性も強制します。マスクされていないクライアントフレーム、予約済みオペコード、サイズ超過または断片化された制御フレーム、予約済みの RSV ビットは、プロトコルエラーの close code で拒否されます。

<div id="platform">
  ## プラットフォーム対応
</div>

このハンドラーは、ClickHouse がサポートするすべてのプラットフォームでコンパイルされます。埋め込み `clickhouse-client` ランナーで使われる擬似端末レイヤーは、移植性のある POSIX プリミティブ (`posix_openpt`/`grantpt`/`unlockpt`) をベースに実装されており、Linux 固有のパスではスレッドセーフな `ptsname_r` を使用します。ClickHouse のスタートページの `/webterminal` へのリンクと `/play` 内のリンクは、エンドポイント が利用できない場合 (たとえば `enable_webterminal` が `false` に設定されている場合) に自動的に非表示になります。