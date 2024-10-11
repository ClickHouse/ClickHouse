---
slug: /en/operations/utilities/clickhouse-disks
sidebar_position: 59
sidebar_label: clickhouse-disks
---

# Clickhouse-disks

A utility providing filesystem-like operations for ClickHouse disks. It can work in both interactive and not interactive modes.

## Program-wide options

* `--config-file, -C` -- path to ClickHouse config, defaults to `/etc/clickhouse-server/config.xml`.
* `--save-logs` -- Log progress of invoked commands to `/var/log/clickhouse-server/clickhouse-disks.log`.
* `--log-level` -- What [type](../server-configuration-parameters/settings#logger) of events to log, defaults to `none`.
* `--disk` -- what disk to use for `mkdir, move, read, write, remove` commands. Defaults to `default`.
* `--query, -q` -- single query that can be executed without launching interactive mode
* `--help, -h` -- print all the options and commands with description

## Default Disks
After the launch two disks are initialized. The first one is a disk `local` that is supposed to imitate local file system from which clickhouse-disks utility was launched. The second one is a disk `default` that is mounted to the local filesystem in the directory that can be found in config as a parameter `clickhouse/path` (default value is `/var/lib/clickhouse`).

## Clickhouse-disks state
For each disk that was added the utility stores current directory (as in a usual filesystem). User can change current directory and switch between disks.

State is reflected in a prompt "`disk_name`:`path_name`"

## Commands

In these documentation file all mandatory positional arguments are referred as `<parameter>`, named arguments are referred as `[--parameter value]`. All positional parameters could be mentioned as a named parameter with a corresponding name.

* `cd (change-dir, change_dir) [--disk disk] <path>`
  Change directory to path `path` on disk `disk` (default value is a current disk). No disk switching happens.
* `copy (cp) [--disk-from disk_1] [--disk-to disk_2] <path-from> <path-to>`.
  Recursively copy data from `path-from` at disk `disk_1` (default value is a current disk (parameter `disk` in a non-interactive mode))
  to `path-to` at disk `disk_2` (default value is a current disk (parameter `disk` in a non-interactive mode)).
* `current_disk_with_path (current, current_disk, current_path)`
  Print current state in format:
    `Disk: "current_disk" Path: "current path on current disk"`
* `help [<command>]`
  Print help message about command `command`. If `command` is not specified print information about all commands.
* `move (mv) <path-from> <path-to>`.
  Move file or directory from `path-from` to `path-to` within current disk.
* `remove (rm, delete) <path>`.
  Remove `path` recursively on a current disk.
* `link (ln) <path-from> <path-to>`.
  Create a hardlink from `path-from` to `path-to` on a current disk.
* `list (ls) [--recursive] <path>`
  List files at `path`s on a current disk. Non-recursive by default.
* `list-disks (list_disks, ls-disks, ls_disks)`.
  List disks names.
* `mkdir [--recursive] <path>` on a current disk.
  Create a directory. Non-recursive by default.
* `read (r) <path-from> [--path-to path]`
  Read a file from `path-from` to `path` (`stdout` if not supplied).
* `switch-disk [--path path] <disk>`
  Switch to disk `disk` on path `path` (if `path` is not specified default value is a previous path on disk `disk`).
* `write (w) [--path-from path] <path-to>`.
  Write a file from `path` (`stdin` if `path` is not supplied, input must finish by Ctrl+D) to `path-to`.
