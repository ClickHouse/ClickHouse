---
description: 'Clickhouse-disks のドキュメント'
sidebar_label: 'clickhouse-disks'
sidebar_position: 59
slug: /operations/utilities/clickhouse-disks
title: 'Clickhouse-disks'
doc_type: 'reference'
---

ClickHouse のディスクに対して、ファイルシステムのような操作を提供するユーティリティです。対話型モードと非対話型モードの両方に対応しています。

<div id="program-wide-options">
  ## プログラム全体のオプション
</div>

* `--config-file, -C` -- ClickHouse の config への path。デフォルト値は `/etc/clickhouse-server/config.xml` です。
* `--save-logs` -- 呼び出したコマンドの実行状況を `/var/log/clickhouse-server/clickhouse-disks.log` に記録します。
* `--log-level` -- ログに記録するイベントの[種類](../server-configuration-parameters/settings#logger)。デフォルト値は `none` です。
* `--disk` -- `mkdir, move, read, write, remove` コマンドで使用するディスク。デフォルト値は `default` です。
* `--query, -q` -- 対話型モードを起動せずに実行できる単一のクエリ
* `--help, -h` -- すべてのオプションとコマンドを説明付きで表示します

<div id="lazy-initialization">
  ## 遅延初期化
</div>

設定で利用可能なすべてのディスクは、遅延初期化されます。つまり、ディスクに対応するオブジェクトは、そのディスクが何らかのコマンドで使用されたときにのみ初期化されます。これは、ユーティリティの堅牢性を高めるとともに、設定には記述されていてもユーザーが使用しておらず、初期化時に失敗する可能性があるディスクにアクセスしないようにするためです。ただし、`clickhouse-disks` の起動時に初期化されるディスクが 1 つ必要です。このディスクは、コマンドラインの `--disk` パラメータで指定します (デフォルト値は `default` です) 。

<div id="default-disks">
  ## デフォルトディスク
</div>

起動後、設定には明示されていないものの、初期化に利用できるディスクが 2 つあります。

1. **`local` Disk**: このディスクは、`clickhouse-disks` ユーティリティを起動したローカルファイルシステムを模したものです。初期パスは `clickhouse-disks` を起動したディレクトリで、ファイルシステムのルートディレクトリにマウントされます。

2. **`default` Disk**: このディスクは、設定内の `clickhouse/path` パラメーターで指定されたディレクトリ (デフォルト値は `/var/lib/clickhouse`) でローカルファイルシステムにマウントされます。初期パスは `/` に設定されています。

<div id="clickhouse-disks-state">
  ## Clickhouse-disks の状態
</div>

追加された各ディスクごとに、このユーティリティは現在のディレクトリ (通常のファイルシステムと同様) を保持します。ユーザーは現在のディレクトリを変更したり、ディスクを切り替えたりできます。

状態はプロンプト &quot;`disk_name`:`path_name`&quot; に反映されます。

<div id="commands">
  ## コマンド
</div>

このドキュメントでは、必須の位置引数は `<parameter>`、名前付き引数は `[--parameter value]` と表記します。すべての位置パラメータは、対応する名前の名前付きパラメータとして記載される場合もあります。

* `cd (change-dir, change_dir) [--disk disk] <path>`
  ディスク `disk` 上のパス `path` にディレクトリを変更します (デフォルト値は現在のディスク) 。ディスクの切り替えは行われません。
* `copy (cp) [--disk-from disk_1] [--disk-to disk_2] <path-from> <path-to>`.
  ディスク `disk_1` 上の `path-from` からデータを再帰的にコピーし (デフォルト値は現在のディスク (非対話型モードではパラメータ `disk`) ) 、
  ディスク `disk_2` 上の `path-to` にコピーします (デフォルト値は現在のディスク (非対話型モードではパラメータ `disk`) ) 。
* `current_disk_with_path (current, current_disk, current_path)`
  現在の状態を次のフォーマットで出力します:
  `Disk: "current_disk" Path: "current path on current disk"`
* `du [--human-readable] [<path>]`
  現在のディスク上の `path` にあるファイルまたはディレクトリの合計サイズをバイト単位で出力します。ディレクトリの場合は、その中に含まれるすべてのファイルのサイズが再帰的に合計されます。`path` が指定されていない場合は、現在のディレクトリが使用されます。`--human-readable` (`-h`) を付けると、サイズは人間が読みやすいフォーマット (例: `1.23 GiB`) で出力されます。
* `help [<command>]`
  コマンド `command` のヘルプメッセージを出力します。`command` が指定されていない場合は、すべてのコマンドに関する情報を出力します。
* `move (mv) <path-from> <path-to>`.
  現在のディスク内で、ファイルまたはディレクトリを `path-from` から `path-to` に移動します。
* `remove (rm, delete) <path>`.
  現在のディスク上の `path` を再帰的に削除します。
* `link (ln) <path-from> <path-to>`.
  現在のディスク上で、`path-from` から `path-to` へのハードリンクを作成します。
* `list (ls) [--recursive] <path>`
  現在のディスク上の `path` にあるファイルを一覧表示します。デフォルトでは再帰的ではありません。
* `list-disks (list_disks, ls-disks, ls_disks)`.
  ディスク名を一覧表示します。
* `mkdir [--recursive] <path>` 現在のディスク上。
  ディレクトリを作成します。デフォルトでは再帰的ではありません。
* `read (r) <path-from> [--path-to path]`
  `path-from` からファイルを読み取り、`path` に出力します (指定されていない場合は `stdout`) 。
* `read-bitmap <path-from> [--values]`
  `path-from` にある delete-bitmap (`.rbm`) サイドカーを調べます。magic とバージョン、CRC の妥当性、カーディナリティ (削除された行数) 、および行範囲を出力します。`--values` を付けると、すべての set bits (削除された行の offsets) も昇順でダンプします。
* `switch-disk [--path path] <disk>`
  パス `path` でディスク `disk` に切り替えます (`path` が指定されていない場合、デフォルト値はディスク `disk` 上の直前のパスです) 。
* `write (w) [--path-from path] <path-to>`.
  `path` から `path-to` へファイルを書き込みます (`path` が指定されていない場合は `stdin`、入力は Ctrl+D で終了する必要があります) 。
* `wc <path> [--bytes] [--lines] [--words]`
  現在のディスク上の `path` にあるファイルのバイト数、行数、単語数を数えます (Unix の `wc` と同様) 。フラグを指定しない場合は、行数、単語数、バイト数の順ですべてのカウントが出力されます。特定のカウントを選択するには `--bytes` (`-c`)、`--lines` (`-l`)、`--words` (`-w`) を使用します。
* `sed <expression> <path>`
  現在のディスク上の `path` にあるファイルに対して、その場で `sed` の `expression` を適用します。ホストに `sed` がインストールされている必要があります。サポートされるのは、オプションなしの単一の `sed` expression のみです (例: `'s/foo/bar/g'`, `'/foo/d'`) 。複数の expression (`-e ... -e ...`) や、アドレスと組み合わせたオプション (例: `-n` と `4,10p` の組み合わせ) はサポートされません。
* `read-checksums <path>`
  現在のディスク上にある `MergeTree` data part の `checksums.txt` ファイルを読み取り、`name`、`file_size`、`file_hash`、`uncompressed_size`、`uncompressed_hash` の各カラムを持つタブ区切りの人間が読みやすいテーブルとして `stdout` に出力します。最後の 2 つのカラムは compressed ファイルに対してのみ存在します。