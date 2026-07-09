---
description: 'ファイルシステムにアクセスし、ファイルの一覧表示とそのメタデータおよび内容の取得を行います。'
sidebar_label: 'filesystem'
sidebar_position: 62
slug: /sql-reference/table-functions/filesystem
title: 'filesystem'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="filesystem-table-function">
  # filesystem テーブル関数
</div>

<CloudNotSupportedBadge />

ディレクトリを再帰的に走査し、ファイルのメタデータ (パス、サイズ、種類、権限、最終更新時刻) と、必要に応じてファイルの内容を含むテーブルを返します。

`clickhouse-server` モードでは、パスは [user&#95;files&#95;path](/ja/operations/server-configuration-parameters/settings.md#user_files_path) ディレクトリ内になければなりません。`user_files_path` 内にあるシンボリックリンクがその外部を指している場合でも、そのリンクはたどられますが、 (シンボリックリンク経由の) パスが `user_files_path` で始まるエントリのみが返されます。

`clickhouse-local` モードでは、パスに制限はありません。

<div id="syntax">
  ## 構文
</div>

```sql
filesystem([path])
```

<div id="arguments">
  ## 引数
</div>

| パラメータ  | 説明                                                                                                                                                  |
| ------ | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path` | 一覧表示するディレクトリです。絶対パス (サーバーモードでは `user_files_path` 内にある必要があります) または `user_files_path` からの相対パスを指定できます。空の場合、または省略した場合は、`user_files_path` がデフォルトで使用されます。 |

<div id="returned_columns">
  ## 返されるカラム
</div>

| Column              | Type                       | Description                                                                                                                             |
| ------------------- | -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `path`              | `String`                   | エントリを含むディレクトリ (ファイル名やディレクトリ名そのものは含みません) 。                                                                                               |
| `name`              | `String`                   | ファイル名またはディレクトリ名 (パスの最後の要素) 。                                                                                                            |
| `file`              | `String` (ALIAS of `name`) | `name` カラムの別名です。                                                                                                                        |
| `type`              | `Enum8`                    | ファイルタイプ: `'none'`, `'not_found'`, `'regular'`, `'directory'`, `'symlink'`, `'block'`, `'character'`, `'fifo'`, `'socket'`, `'unknown'`。 |
| `size`              | `Nullable(UInt64)`         | ファイルサイズ (バイト単位。通常ファイルの場合) 。通常ファイル以外 (ディレクトリ、シンボリックリンクなど) およびエラー時は `NULL` です。                                                            |
| `depth`             | `UInt16`                   | 再帰の深さ。クエリ対象のディレクトリ自体とその直下の子要素は `0`、1 階層下のエントリは `1`、以降も同様です。                                                                               |
| `modification_time` | `Nullable(DateTime64(6))`  | マイクロ秒精度の最終更新時刻。エラー時は `NULL` です。                                                                                                         |
| `is_symlink`        | `Bool`                     | エントリがシンボリックリンクかどうか。                                                                                                                     |
| `content`           | `Nullable(String)`         | ファイル内容 (通常ファイルの場合) 。通常ファイル以外 (ディレクトリ、シンボリックリンクなど) では `NULL` です。読み取りエラー時には例外が発生します。このカラムを読み取ると実際のファイル I/O が発生するため、不要であれば省略してください。        |
| `owner_read`        | `Bool`                     | 所有者に読み取り権限があります。                                                                                                                        |
| `owner_write`       | `Bool`                     | 所有者に書き込み権限があります。                                                                                                                        |
| `owner_exec`        | `Bool`                     | 所有者に実行権限があります。                                                                                                                          |
| `group_read`        | `Bool`                     | グループに読み取り権限があります。                                                                                                                       |
| `group_write`       | `Bool`                     | グループに書き込み権限があります。                                                                                                                       |
| `group_exec`        | `Bool`                     | グループに実行権限があります。                                                                                                                         |
| `others_read`       | `Bool`                     | その他のユーザーに読み取り権限があります。                                                                                                                   |
| `others_write`      | `Bool`                     | その他のユーザーに書き込み権限があります。                                                                                                                   |
| `others_exec`       | `Bool`                     | その他のユーザーに実行権限があります。                                                                                                                     |
| `set_gid`           | `Bool`                     | Set-GID ビット。                                                                                                                            |
| `set_uid`           | `Bool`                     | Set-UID ビット。                                                                                                                            |
| `sticky_bit`        | `Bool`                     | スティッキービット。                                                                                                                              |

実際にクエリで使用されるカラムだけが計算されるため、一部のカラムだけを選択するのは効率的です (特に `content` を省略する場合) 。

<div id="examples">
  ## 例
</div>

<div id="list-files">
  ### user_files 内のファイルを一覧表示する
</div>

```sql
SELECT name, type, size, depth
FROM filesystem()
ORDER BY name;
```

<div id="find-large-files">
  ### サイズの大きいファイルを探す
</div>

```sql
SELECT path, name, size
FROM filesystem()
WHERE type = 'regular' AND size > 1000000
ORDER BY size DESC;
```

<div id="read-contents">
  ### ファイルの内容を読む
</div>

```sql
SELECT name, content
FROM filesystem('my_directory')
WHERE name LIKE '%.csv';
```

<div id="list-immediate">
  ### 直下の子要素のみを一覧表示
</div>

```sql
SELECT name, type
FROM filesystem('my_directory')
WHERE depth = 0;
```