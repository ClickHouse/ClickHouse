---
description: 'ClickHouse-disks 文档'
sidebar_label: 'clickhouse-disks'
sidebar_position: 59
slug: /operations/utilities/clickhouse-disks
title: 'Clickhouse-disks'
doc_type: 'reference'
---

一个用于对 ClickHouse 磁盘执行类似文件系统操作的实用工具。既支持交互模式，也支持非交互模式。

<div id="program-wide-options">
  ## 程序级选项
</div>

* `--config-file, -C` -- ClickHouse 配置文件的路径，默认为 `/etc/clickhouse-server/config.xml`。
* `--save-logs` -- 将所调用命令的执行进度记录到 `/var/log/clickhouse-server/clickhouse-disks.log`。
* `--log-level` -- 要记录哪种[类型](../server-configuration-parameters/settings#logger)的事件，默认为 `none`。
* `--disk` -- `mkdir, move, read, write, remove` 命令要使用哪个磁盘。默认为 `default`。
* `--query, -q` -- 可在不启动交互模式的情况下执行的单条查询
* `--help, -h` -- 打印所有选项和命令及其说明

<div id="lazy-initialization">
  ## 惰性初始化
</div>

config 中所有可用的磁盘都会采用惰性初始化。这意味着，只有当某个命令实际使用到相应磁盘时，才会初始化该磁盘对应的对象。这样做是为了让该工具更稳健，并避免处理那些虽在 config 中定义、但用户并未使用且可能在初始化时失败的磁盘。不过，在 `clickhouse-disks` 启动时，仍需初始化一个磁盘。该磁盘通过命令行参数 `--disk` 指定 (默认值为 `default`) 。

<div id="default-disks">
  ## 默认磁盘
</div>

启动后，会有两个未在配置中指定、但可用于初始化的磁盘。

1. **`local` 磁盘**：该磁盘用于模拟启动 `clickhouse-disks` 工具时所在的本地文件系统。它的初始路径是启动 `clickhouse-disks` 时所在的目录，并挂载到文件系统的根目录。

2. **`default` 磁盘**：该磁盘挂载到本地文件系统中由配置里的 `clickhouse/path` 参数指定的目录 (默认值为 `/var/lib/clickhouse`) 。它的初始路径设置为 `/`。

<div id="clickhouse-disks-state">
  ## Clickhouse-disks 状态
</div>

对于已添加的每个 磁盘，该工具都会保存当前目录 (与普通文件系统中的概念相同) 。用户可以更改当前目录，并在不同 磁盘 之间切换。

状态会显示在提示符 &quot;`disk_name`:`path_name`&quot; 中

<div id="commands">
  ## 命令
</div>

在本文档中，所有必需的位置参数均记为 `<parameter>`，命名参数记为 `[--parameter value]`。所有位置参数也可以用具有对应名称的命名参数来表示。

* `cd (change-dir, change_dir) [--disk disk] <path>`
  将当前目录切换到磁盘 `disk` 上的路径 `path`。默认使用当前磁盘，不会切换磁盘。
* `copy (cp) [--disk-from disk_1] [--disk-to disk_2] <path-from> <path-to>`.
  将磁盘 `disk_1` 上 `path-from` 中的数据递归复制
  到磁盘 `disk_2` 上的 `path-to` (默认值为当前磁盘；在非交互模式下为参数 `disk`) 。
* `current_disk_with_path (current, current_disk, current_path)`
  按以下格式打印当前状态：
  `Disk: "current_disk" Path: "current path on current disk"`
* `du [--human-readable] [<path>]`
  打印当前磁盘上 `path` 处文件或目录的总字节大小。对于目录，会递归汇总其包含的所有文件大小。如果未指定 `path`，则使用当前目录。使用 `--human-readable` (`-h`) 时，会以人类可读的格式输出大小 (例如 `1.23 GiB`) 。
* `help [<command>]`
  打印命令 `command` 的帮助信息。如果未指定 `command`，则打印所有命令的信息。
* `move (mv) <path-from> <path-to>`.
  在当前磁盘内将文件或目录从 `path-from` 移动到 `path-to`。
* `remove (rm, delete) <path>`.
  在当前磁盘上递归删除 `path`。
* `link (ln) <path-from> <path-to>`.
  在当前磁盘上创建从 `path-from` 到 `path-to` 的硬链接。
* `list (ls) [--recursive] <path>`
  列出当前磁盘上 `path` 下的文件。默认不递归。
* `list-disks (list_disks, ls-disks, ls_disks)`.
  列出磁盘名称。
* `mkdir [--recursive] <path>` on a current disk.
  在当前磁盘上创建目录。默认不递归。
* `read (r) <path-from> [--path-to path]`
  将 `path-from` 的文件读取到 `path` (如果未提供，则输出到 `stdout`) 。
* `read-bitmap <path-from> [--values]`
  检查位于 `path-from` 的 delete-bitmap (`.rbm`) 伴生文件。会打印 magic 和 version、CRC 是否有效、cardinality (已删除行数) 以及行范围。使用 `--values` 时，还会按升序转储所有置位 (已删除行的 offsets) 。
* `switch-disk [--path path] <disk>`
  切换到磁盘 `disk` 上的路径 `path` (如果未指定 `path`，则默认使用磁盘 `disk` 上之前的路径) 。
* `write (w) [--path-from path] <path-to>`.
  将文件从 `path` 写入 `path-to` (如果未提供 `path`，则从 `stdin` 读取，输入必须以 Ctrl+D 结束) 。
* `wc <path> [--bytes] [--lines] [--words]`
  统计当前磁盘上 `path` 文件中的字节数、行数和单词数 (类似 Unix `wc`) 。如果不带任何 flag，则按行数、单词数、字节数的顺序输出这三项统计。使用 `--bytes` (`-c`) 、`--lines` (`-l`) 、`--words` (`-w`) 可选择特定统计项。
* `sed <expression> <path>`
  对当前磁盘上 `path` 处的文件原地应用 `sed` `expression`。要求宿主机已安装 `sed`。仅支持单个不带选项的 `sed` 表达式 (例如 `'s/foo/bar/g'`、`'/foo/d'`) ，不支持多个表达式 (`-e ... -e ...`) 或与地址组合使用的选项 (例如将 `-n` 与 `4,10p` 一起使用) 。
* `read-checksums <path>`
  读取当前磁盘上 `MergeTree` 数据分区片段的 `checksums.txt` 文件，并将其以制表符分隔、便于人类阅读的表格形式输出到 `stdout`，包含 `name`、`file_size`、`file_hash`、`uncompressed_size` 和 `uncompressed_hash` 列。最后两列仅在 compressed 文件中存在。