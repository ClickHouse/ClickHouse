# Clickhouse Diagnostics Tool

## Purpose

This tool provides a means of obtaining a diagnostic bundle from a ClickHouse instance. This bundle can be provided to your nearest ClickHouse support provider in order to assist with the diagnosis of issues.

## Design Philosophy

- **No local dependencies** to run. We compile to a platform-independent binary, hence Go.
- **Minimize resource overhead**. Improvements always welcome.
- **Extendable framework**. At its core, the tool provides collectors and outputs. Collectors are independent and are responsible for collecting a specific dataset e.g. system configuration. Outputs produce the diagnostic bundle in a specific format. It should be trivial to add both for contributors. See [Collectors](#collectors) and [Outputs](#outputs) for more details.
- **Convertible output formats**. Outputs produce diagnostic bundles in different formats e.g. archive, simple report etc. Where possible, it should be possible to convert between these formats. For example, an administrator may provide a bundle as an archive to their support provider who in turn wishes to visualise this as a report or even in ClickHouse itself...
- **Something is better than nothing**. Collectors execute independently. We never fail a collection because one fails - preferring to warn the user only. There are good reasons for a collector failure e.g. insufficient permissions or missing data.
- **Execute anywhere** - Ideally, this tool is executed on a ClickHouse host. Some collectors e.g. configuration file collection or system information, rely on this. However, collectors will obtain as much information remotely from the database as possible if executed remotely from the cluster - warning where collection fails. **We do currently require ClickHouse to be running, connecting over the native port**.

We recommend reading [Permissions, Warnings & Locality](#permissions-warnings--locality).

## Usage

### Collection

The `collect` command allows the collection of a diagnostic bundle. In its simplest form, assuming ClickHouse is running locally on default ports with no password:

```bash
clickhouse-diagnostics collect
```

This will use the default collectors and the simple output. This output produces a timestamped archive bundle in `gz` format in a sub folder named after the host. This folder name can be controlled via the parameter `--id` or configured directly for the simple output parameter `output.simple.folder` (this allows a specific directory to be specified).

Collectors, Outputs and ClickHouse connection credentials can be specified as shown below:

```bash
clickhouse-diagnostics collect --password random --username default --collector=system_db,system --output=simple --id my_cluster_name
```

This collects the system database and host information from the cluster running locally. The archive bundle will be produced under a folder `my_cluster_name`.

For further details, use the in built help (the commands below are equivalent):

```bash
clickhouse-diagnostics collect --help
./clickhouse-diagnostics help collect
```

### Help & Finding parameters for collectors & outputs

Collectors and outputs have their own parameters not listed under the help for the command for the `collect` command. These can be identified using the `help` command. Specifically,

For more information about a specific collector.

```bash
Use "clickhouse-diagnostics help --collector [collector]" 
```

For more information about a specific output.

```bash
Use "clickhouse-diagnostics help --output [output]" 
```

### Convert

Coming soon to a cluster near you...

## Collectors

We currently support the following collectors. A `*` indicates this collector is enabled by default:

- `system_db*` - Collects all tables in the system database, except those which have been excluded and up to a specified row limit.
- `system*` - Collects summary OS and hardware statistics for the host.
- `config*` - Collects the ClickHouse configuration from the local filesystem. A best effort is made using process information if ClickHouse is not installed locally. `include_path` are also considered. 
- `db_logs*` - Collects the ClickHouse logs directly from the database.
- `logs*` - Collects the ClickHouse logs directly from the database.
- `summary*` - Collects summary statistics on the database based on a set of known useful queries. This represents the easiest collector to extend - contributions are welcome to this set which can be found [here](https://github.com/ClickHouse/ClickHouse/blob/master/programs/diagnostics/internal/collectors/clickhouse/queries.json).
- `file` - Collects files based on glob patterns. Does not collect directories. To preview files which will be collected try, `clickhouse-diagnostics collect --collectors=file --collector.file.file_pattern=<glob path> --output report`
- `command` - Collects the output of a user specified command. To preview output, `clickhouse-diagnostics collect --collectors=command --collector.command.command="<command>" --output report`
- `zookeeper_db` - Collects information about zookeeper using the `system.zookeeper` table, recursively iterating the zookeeper tree/table. Note: changing the default parameter values can cause extremely high load to be placed on the database. Use with caution. By default, uses the glob `/clickhouse/{task_queue}/**` to match zookeeper paths and iterates to a max depth of 8.

## Outputs

We currently support the following outputs. The `simple` output is currently the default:

- `simple` - Writes out the diagnostic bundle as files in a structured directory, optionally producing a compressed archive.
- `report` - Writes out the diagnostic bundle to the terminal as a simple report. Supports an ascii table format or markdown.
- `clickhouse` - **Under development**. This will allow a bundle to be stored in a cluster allowing visualization in common tooling e.g. Grafana.

## Simple Output

Since the `simple` output is the default we provide additional details here. 
This output produces a timestamped archive by default in `gz` format under a directory created with either the hostname of the specified collection `--id`. As shown below, a specific folder can also be specified. Compression can also be disabled, leaving just the contents of the folder:

```bash
./clickhouse-diagnostics help --output simple

Writes out the diagnostic bundle as files in a structured directory, optionally producing a compressed archive.

Usage:
  --output=simple [flags]

Flags:
      --output.simple.directory string   Directory in which to create dump. Defaults to the current directory. (default "./")
      --output.simple.format string      Format of exported files (default "csv")
      --output.simple.skip_archive       Don't compress output to an archive
```

The archive itself contains a folder for each collector. Each collector can potentially produce many discrete sets of data, known as frames. Each of these typically results in a single file within the collector's folder. For example, each query for the `summary` collector results in a correspondingly named file within the `summary` folder. 

## Permissions, Warnings & Locality

Some collectors either require specific permissions for complete collection or should be executed on a ClickHouse host. We aim to collate these requirements below:

- `system_db` - This collect aims to collect all tables in the `system` database. Some tables may fail if certain features are not enabled. Specifically,[allow_introspection_functions](https://clickhouse.com/docs/en/operations/settings/settings/#settings-allow_introspection_functions) is required to collect the `stack_traces` table. [access_management](https://clickhouse.com/docs/en/operations/settings/settings-users/#access_management-user-setting) must be set for the ClickHouse user specified for collection, to permit access to access management tables e.g. `quota_usage`.
- `db_logs`- The ClickHouse user must have access to the tables `query_log`,`query_thread_log` and `text_log`.
- `logs` - The system user under which the tool is executed must have access to the logs directory. It must therefore also be executed on the target ClickHouse server directly for this collector work. In cases where the logs directory is not a default location e.g. `/var/log/clickhouse-server` we will attempt to establish the location from the ClickHouse configuration. This requires permissions to read the configuration files - which in most cases requires specific permissions to be granted to the run user if you are not comfortable executing the tool under sudo or the `clickhouse` user.
- `summary`- This collector executes pre-recorded queries. Some of these read tables concerning access management, thus requiring the ClickHouse user to have the [access_management](https://clickhouse.com/docs/en/operations/settings/settings-users/#access_management-user-setting) permission.
- `config` - This collector reads and copies the local configuration files. It thus requires permissions to read the configuration files - which in most cases requires specific permissions to be granted to the run user if you are not comfortable executing the tool under sudo or the `clickhouse` user.

**If a collector cannot collect specific data because of either execution location or permissions, it will log a warning to the terminal.**

## Logging

All logs are output to `stderr`. `stdout` is used exclusively for outputs to print information.

## Configuration file

In addition to supporting parameters via the command line, a configuration file can be specified via the `--config`, `-f` flag. 

By default, we look for a configuration file `clickhouse-diagnostics.yml` in the same directory as the binary. If not present, we revert to command line flags.

**Values set via the command line values always take precedence over those in the configuration file.**

All parameters can be set via the configuration file and can in most cases be converted to a yaml hierarchy, where periods indicate a nesting. For example,

`--collector.system_db.row_limit=1`

becomes

```yaml
collector:
  system_db:
    row_limit: 1
```

The following exceptions exist to avoid collisions:

| Command | Parameter  | Configuration File |
|---------|------------|--------------------|
| collect | output     | collect.output     |
| collect | collectors | collect.collectors |

## FAQ

1. Does the collector need root permissions?

    No. However, to read some local files e.g. configurations, the tool should be executed as the `clickhouse` user.

2. What ClickHouse database permissions does the collector need?

    Read permissions on all system tables are required in most cases - although only specific collectors need this. [Access management permissions]((https://clickhouse.com/docs/en/operations/settings/settings-users/#access_management-user-setting)) will ensure full collection.

3. Is any processing done on logs for anonimization purposes?

    Currently no. ClickHouse should not log sensitive information to logs e.g. passwords.

4. Is sensitive information removed from configuration files e.g. passwords?

    Yes. We remove both passwords and hashed passwords. Please raise an issue if you require further information to be anonimized. We appreciate this is a sensitive topic.
