---
sidebar_position: 20
slug: /ja/integrations/sql-clients/clickhouse-client-local
sidebar_label: コマンドラインインターフェース (CLI)
---

# コマンドラインインターフェース (CLI)

`clickhouse client` はコマンドラインからClickHouseに接続するために使用されるクライアントアプリケーションです。`clickhouse local` はディスク上およびネットワーク越しにファイルをクエリするために使用されるクライアントアプリケーションです。ClickHouseのドキュメントの多くのガイドでは、`clickhouse local`を使用してファイル（CSV、TSV、Parquetなど）のスキーマを調べ、そのファイルをクエリし、データを操作してClickHouseへの挿入の準備をする方法を説明しています。`clickhouse local`でファイルをクエリし、その出力を`clickhouse client`にパイプで渡してClickHouseにデータをストリームすることもよくあります。このドキュメントの最後の「次のステップ」セクションでは、`clickhouse client` と `clickhouse local` の両方を使用した例のデータセットを紹介しています。

:::tip
もし既にローカルにClickHouseサーバーをインストールしている場合は、**clickhouse client** と **clickhouse local** がインストールされているかもしれません。コマンドラインで **clickhouse client** と **clickhouse local** を実行して確認してください。そうでない場合は、あなたのオペレーティングシステムに対するインストール手順を確認してください。
:::


## Microsoft Windowsの前提条件

Windows 10または11でWindows Subsystem for Linux (WSL) Version 2 (WSL 2) を使用すると、Ubuntu Linuxを実行でき、その上で`clickhouse client` および `clickhouse local`を実行することができます。

Microsoftの[WSLドキュメント](https://docs.microsoft.com/en-us/windows/wsl/install)に従ってWSLをインストールしてください。

#### WSL 2でシェルを開く:

ターミナルから `bash` コマンドを実行すると、WSLに入ります:

```bash
bash
```

## ClickHouseのダウンロード

```
curl https://clickhouse.com/ | sh
```

## `clickhouse client` の確認

```bash
./clickhouse client
```
:::note
`clickhouse client` はローカルのClickHouseサーバーインスタンスに接続しようとしますが、実行中でない場合はタイムアウトします。[`clickhouse-client`](/docs/ja/integrations/cli.mdx) ドキュメントを参照して例を見てください。
:::

## `clickhouse local` の確認

```bash
./clickhouse local
```

## 次のステップ
[`NYPD Complaint` データセット](/docs/ja/getting-started/example-datasets/nypd_complaint_data.md)で `clickhouse-client` と `clickhouse-local` の使用例を確認してください。

[`clickhouse-client`](/docs/ja/integrations/cli.mdx) ドキュメントを参照してください。

[`clickhouse-local`](/docs/ja/operations/utilities/clickhouse-local.md) ドキュメントを参照してください。

[ClickHouseのインストール](/docs/ja/getting-started/install.md) ドキュメントを参照してください。

## 関連コンテンツ

- ブログ: [`clickhouse-local`を使ったローカルファイルからのデータの抽出、変換、およびクエリ](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)
