---
description: 'Documentation for Clickhouse-disks'
sidebar_label: 'clickhouse-disks'
sidebar_position: 59
slug: /operations/utilities/clickhouse-disks
title: 'Clickhouse-disks'
---

# Clickhouse-disks

A utility providing filesystem-like operations for ClickHouse disks. It can work in both interactive and not interactive modes.

## Program-wide options {#program-wide-options}

* `--config-file, -C` -- path to ClickHouse config, defaults to `/etc/clickhouse-server/config.xml`.
* `--save-logs` -- Log progress of invoked commands to `/var/log/clickhouse-server/clickhouse-disks.log`.
* `--log-level` -- What [type](../server-configuration-parameters/settings#logger) of events to log, defaults to `none`.
* `--disk` -- what disk to use for `mkdir, move, read, write, remove` commands. Defaults to `default`.
* `--query, -q` -- single query that can be executed without launching interactive mode
* `--help, -h` -- print all the options and commands with description

## Lazy initialization {#lazy-initialization}
All disks which are available in config are initialized lazily. This means that the corresponding object for a disk is initialized only when corresponding disk is used in some command. This is done to make the utility more robust and to avoid touching of disks which are described in config but not used by a user and can fail during initialization. However, there should be a disk which is initialized at the clickhouse-disks launch. This disk is specified with parameter `--disk` through command-line (default value is `default`).

## Default Disks {#default-disks}
After launching, there are two disks that are not specified in the configuration but are available for initialization.

1. **`local` Disk**: This disk is designed to mimic the local file system from which the `clickhouse-disks` utility was launched. Its initial path is the directory from which `clickhouse-disks` was started, and it is mounted at the root directory of the file system.

2. **`default` Disk**: This disk is mounted to the local file system in the directory specified by the `clickhouse/path` parameter in the configuration (the default value is `/var/lib/clickhouse`). Its initial path is set to `/`.

## Clickhouse-disks state {#clickhouse-disks-state}
For each disk that was added the utility stores current directory (as in a usual filesystem). User can change current directory and switch between disks.

State is reflected in a prompt "`disk_name`:`path_name`"

## Commands {#commands}

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
