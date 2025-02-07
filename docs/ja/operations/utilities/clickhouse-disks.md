---
slug: /ja/operations/utilities/clickhouse-disks
sidebar_position: 59
sidebar_label: clickhouse-disks
---

# Clickhouse-disks

ClickHouse ディスクのためのファイルシステムのような操作を提供するユーティリティです。インタラクティブモードと非インタラクティブモードの両方で動作します。

## プログラム全体のオプション

* `--config-file, -C` -- ClickHouse の設定ファイルへのパス。デフォルトは `/etc/clickhouse-server/config.xml`。
* `--save-logs` -- 実行したコマンドの進行状況を `/var/log/clickhouse-server/clickhouse-disks.log` に記録。
* `--log-level` -- 記録する [イベントのタイプ](../server-configuration-parameters/settings#logger)。デフォルトは `none`。
* `--disk` -- `mkdir, move, read, write, remove` コマンドで使用するディスク。デフォルトは `default`。
* `--query, -q` -- インタラクティブモードを起動せずに実行できる単一のクエリ
* `--help, -h` -- 説明付きで全てのオプションとコマンドを表示

## デフォルトディスク
起動後、2つのディスクが初期化されます。最初のディスクは `local` で、clickhouse-disks ユーティリティが起動されたローカルファイルシステムを模倣することを意図しています。2つ目のディスクは `default` で、設定でパラメーター `clickhouse/path` として見つけることができるディレクトリにローカルファイルシステムにマウントされています（デフォルト値は `/var/lib/clickhouse`）。

## Clickhouse-disks の状態
追加された各ディスクについて、ユーティリティは通常のファイルシステムと同様に現在のディレクトリを保持します。ユーザーは現在のディレクトリを変更したり、ディスクを切り替えたりできます。

状態はプロンプト "`disk_name`:`path_name`" に反映されます

## コマンド

このドキュメンテーションファイルでは、すべての必須の位置引数は `<parameter>`、名前付き引数は `[--parameter value]` として示されています。すべての位置指定パラメータは対応する名前の名前付きパラメータとして言及できます。

* `cd (change-dir, change_dir) [--disk disk] <path>`
  ディスク `disk` の `path` にディレクトリを変更（デフォルト値は現在のディスク）。ディスクの切り替えは行われません。
* `copy (cp) [--disk-from disk_1] [--disk-to disk_2] <path-from> <path-to>`.
  `path-from` のデータをディスク `disk_1`（デフォルトでは現在のディスク（非インタラクティブモードではパラメータ `disk`））から再帰的に `path-to` のディスク `disk_2`（デフォルトでは現在のディスク（非インタラクティブモードではパラメータ `disk`））にコピー。
* `current_disk_with_path (current, current_disk, current_path)`
  現在の状態を以下の形式で表示：
    `Disk: "current_disk" Path: "current path on current disk"`
* `help [<command>]`
  コマンド `command` に関するヘルプメッセージを表示。`command` が指定されていない場合は、すべてのコマンドに関する情報を表示。
* `move (mv) <path-from> <path-to>`.
  現在のディスク内で `path-from` から `path-to` へファイルまたはディレクトリを移動。
* `remove (rm, delete) <path>`.
  現在のディスク上で `path` を再帰的に削除。
* `link (ln) <path-from> <path-to>`.
  現在のディスク上で `path-from` から `path-to` へのハードリンクを作成。
* `list (ls) [--recursive] <path>`
  現在のディスク上で `path` にあるファイルをリスト。デフォルトでは非再帰的。
* `list-disks (list_disks, ls-disks, ls_disks)`.
  ディスク名をリスト。
* `mkdir [--recursive] <path>` 現在のディスク上。
  ディレクトリを作成。デフォルトでは非再帰的。
* `read (r) <path-from> [--path-to path]`
  `path-from` からファイルを読み込み `path` に出力（指定されていない場合は `stdout` に出力）。
* `switch-disk [--path path] <disk>`
  パス `path`（指定されていない場合はディスク `disk` 上の以前のパスがデフォルト）でディスク `disk` に切り替え。
* `write (w) [--path-from path] <path-to>`.
  `path` からファイルを書き込み（`path` が指定されていない場合は `stdin` から読み込み、入力は Ctrl+D で終了）`path-to` に出力。
