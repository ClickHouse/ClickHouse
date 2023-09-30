---
slug: /en/operations/utilities/clickhouse-disks
sidebar_position: 59
sidebar_label: clickhouse-disks
---

# clickhouse-disks

A utility providing filesystem-like operations for ClickHouse disks.

Program-wide options:

* `--config-file, -C` -- path to ClickHouse config, defaults to `/etc/clickhouse-server/config.xml`.
* `--save-logs` -- Log progress of invoked commands to `/var/log/clickhouse-server/clickhouse-disks.log`.
* `--log-level` -- What [type](../server-configuration-parameters/settings#server_configuration_parameters-logger) of events to log, defaults to `none`.
* `--disk` -- what disk to use for `mkdir, move, read, write, remove` commands. Defaults to `default`.

## Commands

* `copy [--disk-from d1] [--disk-to d2] <FROM_PATH> <TO_PATH>`.
  Recursively copy data from `FROM_PATH` at disk `d1` (defaults to `disk` value if not provided)
  to `TO_PATH` at disk `d2` (defaults to `disk` value if not provided).
* `move <FROM_PATH> <TO_PATH>`.
  Move file or directory from `FROM_PATH` to `TO_PATH`.
* `remove <PATH>`.
  Remove `PATH` recursively.
* `link <FROM_PATH> <TO_PATH>`.
  Create a hardlink from `FROM_PATH` to `TO_PATH`.
* `list [--recursive] <PATH>...`
  List files at `PATH`s. Non-recursive by default.
* `list-disks`.
  List disks names.
* `mkdir [--recursive] <PATH>`.
  Create a directory. Non-recursive by default.
* `read: <FROM_PATH> [<TO_PATH>]`
  Read a file from `FROM_PATH` to `TO_PATH` (`stdout` if not supplied).
* `write [FROM_PATH] <TO_PATH>`.
  Write a file from `FROM_PATH` (`stdin` if not supplied) to `TO_PATH`.
