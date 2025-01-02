---
slug: /ja/operations/utilities/static-files-disk-uploader
title: clickhouse-static-files-disk-uploader
keywords: [clickhouse-static-files-disk-uploader, ユーティリティ, ディスク, アップローダー]
---

# clickhouse-static-files-disk-uploader

指定された ClickHouse テーブル用のメタデータを含むデータディレクトリを出力します。このメタデータは、`web` ディスクによってバックアップされた読み取り専用データセットを含む ClickHouse テーブルを異なるサーバーで作成するために使用できます。

データの移行にはこのツールを使用しないでください。代わりに、[`BACKUP` と `RESTORE` コマンド](/docs/ja/operations/backup)を使用してください。

## 使用法

```
$ clickhouse static-files-disk-uploader [args]
```

## コマンド

|コマンド|説明|
|---|---|
|`-h`, `--help`|ヘルプ情報を表示|
|`--metadata-path [path]`|指定されたテーブルのメタデータを含むパス|
|`--test-mode`|`test` モードを有効にし、テーブルメタデータを指定された URL に PUT リクエストとして送信|
|`--link`|ファイルをコピーする代わりに symlink を作成|
|`--url [url]`|`test` モード用のウェブサーバー URL|
|`--output-dir [dir]`|`non-test` モードでファイルを出力するディレクトリ|

## 指定されたテーブルのメタデータパスを取得する

`clickhouse-static-files-disk-uploader` を使用する際には、目的のテーブルのメタデータパスを取得する必要があります。

1. 目的のテーブルとデータベースを指定して、次のクエリを実行します:

<br />

```sql
SELECT data_paths
  FROM system.tables
  WHERE name = 'mytable' AND database = 'default';
```

2. これにより、指定されたテーブルのデータディレクトリへのパスが返されます:

<br />

```
┌─data_paths────────────────────────────────────────────┐
│ ['./store/bcc/bccc1cfd-d43d-43cf-a5b6-1cda8178f1ee/'] │
└───────────────────────────────────────────────────────┘
```

## ローカルファイルシステムにテーブルメタデータディレクトリを出力する

ターゲット出力ディレクトリ `output` と指定されたメタデータパスを使用して、次のコマンドを実行します:

```
$ clickhouse static-files-disk-uploader --output-dir output --metadata-path ./store/bcc/bccc1cfd-d43d-43cf-a5b6-1cda8178f1ee/
```

成功した場合、次のメッセージが表示され、`output` ディレクトリに指定されたテーブルのメタデータが含まれているはずです:

```
Data path: "/Users/john/store/bcc/bccc1cfd-d43d-43cf-a5b6-1cda8178f1ee", destination path: "output"
```

## 外部 URL にテーブルメタデータディレクトリを出力する

このステップは、データディレクトリをローカルファイルシステムに出力する場合と似ていますが、 追加で `--test-mode` フラグを使用します。出力ディレクトリを指定する代わりに、`--url` フラグを介してターゲット URL を指定する必要があります。

`test` モードが有効になっている場合、テーブルメタデータディレクトリが指定された URL に PUT リクエストとしてアップロードされます。

```
$ clickhouse static-files-disk-uploader --test-mode --url http://nginx:80/test1 --metadata-path ./store/bcc/bccc1cfd-d43d-43cf-a5b6-1cda8178f1ee/
```

## テーブルメタデータディレクトリを使用して ClickHouse テーブルを作成する

テーブルメタデータディレクトリを手に入れたら、それを使用して異なるサーバーで ClickHouse テーブルを作成できます。

デモを示す [この GitHub リポジトリ](https://github.com/ClickHouse/web-tables-demo)をご覧ください。例では、`web` ディスクを使用してテーブルを作成し、異なるサーバー上のデータセットにテーブルをアタッチします。
