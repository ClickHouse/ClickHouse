---
description: 'This section contains descriptions of server settings i.e settings
which cannot be changed at the session or query level.'
keywords: ['global server settings']
sidebar_label: 'Server Settings'
sidebar_position: 57
slug: /operations/server-configuration-parameters/settings
title: 'Server Settings'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import SystemLogParameters from '@site/docs/operations/server-configuration-parameters/_snippets/_system-log-parameters.md';
import SettingsInfoBlock from '@theme/SettingsInfoBlock/SettingsInfoBlock';

# Server Settings

This section contains descriptions of server settings. These are settings which
cannot be changed at the session or query level.

For more information on configuration files in ClickHouse see [""Configuration Files""](/operations/configuration-files).

Other settings are described in the ""[Settings](/operations/settings/overview)"" section.
Before studying the settings, we recommend reading the [Configuration files](/operations/configuration-files)
section and note the use of substitutions (the `incl` and `optional` attributes).

## abort_on_logical_error {#abort_on_logical_error} 

<SettingsInfoBlock type="Bool" default_value="0" />Crash the server on LOGICAL_ERROR exceptions. Only for experts.

## access_control_improvements {#access_control_improvements} 

Settings for optional improvements in the access control system.

| Setting                                         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Default |
|-------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| [`on_cluster_queries_require_cluster_grant`](/operations/server-configuration-parameters/settings#access_control_improvements.on_cluster_queries_require_cluster_grant) | Sets whether `ON CLUSTER` queries require the `CLUSTER` grant.                                                                                                                                                                                                                                                                                                                                                                                                                                                   | `true`  |
| [`role_cache_expiration_time_seconds`](/operations/server-configuration-parameters/settings#access_control_improvements.role_cache_expiration_time_seconds) | Sets the number of seconds since last access, that a role is stored in the Role Cache.                                                                                                                                                                                                                                                                                                                                                                                                                           | `600`   |
| [`select_from_information_schema_requires_grant`](/operations/server-configuration-parameters/settings#access_control_improvements.select_from_information_schema_requires_grant) | Sets whether `SELECT * FROM information_schema.<table>` requires any grants and can be executed by any user. If set to true, then this query requires `GRANT SELECT ON information_schema.<table>`, just as for ordinary tables.                                                                                                                                                                                                                                                                                 | `true`  |
| [`select_from_system_db_requires_grant`](/operations/server-configuration-parameters/settings#access_control_improvements.select_from_system_db_requires_grant) | Sets whether `SELECT * FROM system.<table>` requires any grants and can be executed by any user. If set to true then this query requires `GRANT SELECT ON system.<table>` just as for non-system tables. Exceptions: a few system tables (`tables`, `columns`, `databases`, and some constant tables like `one`, `contributors`) are still accessible for everyone; and if there is a `SHOW` privilege (e.g. `SHOW USERS`) granted then the corresponding system table (i.e. `system.users`) will be accessible. | `true`  |
| [`settings_constraints_replace_previous`](/operations/server-configuration-parameters/settings#access_control_improvements.settings_constraints_replace_previous) | Sets whether a constraint in a settings profile for some setting will cancel actions of the previous constraint (defined in other profiles) for that setting, including fields which are not set by the new constraint. It also enables the `changeable_in_readonly` constraint type.                                                                                                                                                                                                                            | `true`  |
| [`table_engines_require_grant`](/operations/server-configuration-parameters/settings#access_control_improvements.table_engines_require_grant) | Sets whether creating a table with a specific table engine requires a grant.                                                                                                                                                                                                                                                                                                                                                                                                                                     | `false` |
| [`users_without_row_policies_can_read_rows`](/operations/server-configuration-parameters/settings#access_control_improvements.users_without_row_policies_can_read_rows) | Sets whether users without permissive row policies can still read rows using a `SELECT` query. For example, if there are two users A and B and a row policy is defined only for A, then if this setting is true, user B will see all rows. If this setting is false, user B will see no rows.                                                                                                                                                                                                                    | `true`  |

Example:

```xml
<access_control_improvements>
    <users_without_row_policies_can_read_rows>true</users_without_row_policies_can_read_rows>
    <on_cluster_queries_require_cluster_grant>true</on_cluster_queries_require_cluster_grant>
    <select_from_system_db_requires_grant>true</select_from_system_db_requires_grant>
    <select_from_information_schema_requires_grant>true</select_from_information_schema_requires_grant>
    <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
    <table_engines_require_grant>false</table_engines_require_grant>
    <role_cache_expiration_time_seconds>600</role_cache_expiration_time_seconds>
</access_control_improvements>
```

## access_control_path {#access_control_path} 

Path to a folder where a ClickHouse server stores user and role configurations created by SQL commands.

**See also**

- [Access Control and Account Management](/operations/access-rights#access-control-usage)

## aggregate_function_group_array_action_when_limit_is_reached {#aggregate_function_group_array_action_when_limit_is_reached} 

<SettingsInfoBlock type="GroupArrayActionWhenLimitReached" default_value="throw" />Action to execute when max array element size is exceeded in groupArray: `throw` exception, or `discard` extra values

## aggregate_function_group_array_max_element_size {#aggregate_function_group_array_max_element_size} 

<SettingsInfoBlock type="UInt64" default_value="16777215" />Max array element size in bytes for groupArray function. This limit is checked at serialization and help to avoid large state size.

## allow_feature_tier {#allow_feature_tier} 

<SettingsInfoBlock type="UInt32" default_value="0" />
Controls if the user can change settings related to the different feature tiers.

- `0` - Changes to any setting are allowed (experimental, beta, production).
- `1` - Only changes to beta and production feature settings are allowed. Changes to experimental settings are rejected.
- `2` - Only changes to production settings are allowed. Changes to experimental or beta settings are rejected.

This is equivalent to setting a readonly constraint on all `EXPERIMENTAL` / `BETA` features.

:::note
A value of `0` means that all settings can be changed.
:::


## allow_impersonate_user {#allow_impersonate_user} 

<SettingsInfoBlock type="Bool" default_value="0" />Enable/disable the IMPERSONATE feature (EXECUTE AS target_user).

## allow_implicit_no_password {#allow_implicit_no_password} 

Forbids creating a user with no password unless 'IDENTIFIED WITH no_password' is explicitly specified.

```xml
<allow_implicit_no_password>1</allow_implicit_no_password>
```

## allow_no_password {#allow_no_password} 

Sets whether an insecure password type of no_password is allowed or not.

```xml
<allow_no_password>1</allow_no_password>
```

## allow_plaintext_password {#allow_plaintext_password} 

Sets whether plaintext-password types (insecure) are allowed or not.

```xml
<allow_plaintext_password>1</allow_plaintext_password>
```

## allow_use_jemalloc_memory {#allow_use_jemalloc_memory} 

<SettingsInfoBlock type="Bool" default_value="1" />Allows to use jemalloc memory.

## allowed_disks_for_table_engines {#allowed_disks_for_table_engines} 

List of disks allowed for use with Iceberg

## async_insert_queue_flush_on_shutdown {#async_insert_queue_flush_on_shutdown} 

<SettingsInfoBlock type="Bool" default_value="1" />If true queue of asynchronous inserts is flushed on graceful shutdown

## async_insert_threads {#async_insert_threads} 

<SettingsInfoBlock type="UInt64" default_value="16" />Maximum number of threads to actually parse and insert data in background. Zero means asynchronous mode is disabled

## async_load_databases {#async_load_databases} 

<SettingsInfoBlock type="Bool" default_value="1" />
Asynchronous loading of databases and tables.

- If `true` all non-system databases with `Ordinary`, `Atomic` and `Replicated` engine will be loaded asynchronously after the ClickHouse server start up. See `system.asynchronous_loader` table, `tables_loader_background_pool_size` and `tables_loader_foreground_pool_size` server settings. Any query that tries to access a table, that is not yet loaded, will wait for exactly this table to be started up. If load job fails, query will rethrow an error (instead of shutting down the whole server in case of `async_load_databases = false`). The table that is waited for by at least one query will be loaded with higher priority. DDL queries on a database will wait for exactly that database to be started up. Also consider setting a limit `max_waiting_queries` for the total number of waiting queries.
- If `false`, all databases are loaded when the server starts.

**Example**

```xml
<async_load_databases>true</async_load_databases>
```


## async_load_system_database {#async_load_system_database} 

<SettingsInfoBlock type="Bool" default_value="0" />
Asynchronous loading of system tables. Helpful if there is a high amount of log tables and parts in the `system` database. Independent of the `async_load_databases` setting.

- If set to `true`, all system databases with `Ordinary`, `Atomic`, and `Replicated` engines will be loaded asynchronously after the ClickHouse server starts. See `system.asynchronous_loader` table, `tables_loader_background_pool_size` and `tables_loader_foreground_pool_size` server settings. Any query that tries to access a system table, that is not yet loaded, will wait for exactly this table to be started up. The table that is waited for by at least one query will be loaded with higher priority. Also consider setting the `max_waiting_queries` setting to limit the total number of waiting queries.
- If set to `false`, system database loads before server start.

**Example**

```xml
<async_load_system_database>true</async_load_system_database>
```


## asynchronous_heavy_metrics_update_period_s {#asynchronous_heavy_metrics_update_period_s} 

<SettingsInfoBlock type="UInt32" default_value="120" />Period in seconds for updating heavy asynchronous metrics.

## asynchronous_insert_log {#asynchronous_insert_log} 

Settings for the [asynchronous_insert_log](/operations/system-tables/asynchronous_insert_log) system table for logging async inserts.

<SystemLogParameters/>

**Example**

```xml
<clickhouse>
    <asynchronous_insert_log>
        <database>system</database>
        <table>asynchronous_insert_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </asynchronous_insert_log>
</clickhouse>
```

## asynchronous_metric_log {#asynchronous_metric_log} 

Enabled by default on ClickHouse Cloud deployments.

If the setting is not enabled by default on your environment, depending on how ClickHouse was installed, you can follow the instruction below to enable or disable it.

**Enabling**

To manually turn on asynchronous metric logs history collection [`system.asynchronous_metric_log`](../../operations/system-tables/asynchronous_metric_log.md), create `/etc/clickhouse-server/config.d/asynchronous_metric_log.xml` with the following content:

```xml
<clickhouse>
     <asynchronous_metric_log>
        <database>system</database>
        <table>asynchronous_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </asynchronous_metric_log>
</clickhouse>
```

**Disabling**

To disable `asynchronous_metric_log` setting, you should create the following file `/etc/clickhouse-server/config.d/disable_asynchronous_metric_log.xml` with the following content:

```xml
<clickhouse><asynchronous_metric_log remove="1" /></clickhouse>
```

<SystemLogParameters/>

## asynchronous_metrics_enable_heavy_metrics {#asynchronous_metrics_enable_heavy_metrics} 

<SettingsInfoBlock type="Bool" default_value="0" />Enable the calculation of heavy asynchronous metrics.

## asynchronous_metrics_keeper_metrics_only {#asynchronous_metrics_keeper_metrics_only} 

<SettingsInfoBlock type="Bool" default_value="0" />Make asynchronous metrics calculate the keeper-related metrics only.

## asynchronous_metrics_update_period_s {#asynchronous_metrics_update_period_s} 

<SettingsInfoBlock type="UInt32" default_value="1" />Period in seconds for updating asynchronous metrics.

## auth_use_forwarded_address {#auth_use_forwarded_address} 

Use originating address for authentication for clients connected through proxy.

:::note
This setting should be used with extra caution since forwarded addresses can be easily spoofed - servers accepting such authentication should not be accessed directly but rather exclusively through a trusted proxy.
:::

## background_buffer_flush_schedule_pool_size {#background_buffer_flush_schedule_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="16" />The maximum number of threads that will be used for performing flush operations for [Buffer-engine tables](/engines/table-engines/special/buffer) in the background.

## background_common_pool_size {#background_common_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="8" />The maximum number of threads that will be used for performing a variety of operations (mostly garbage collection) for [*MergeTree-engine](/engines/table-engines/mergetree-family) tables in the background.

## background_distributed_schedule_pool_size {#background_distributed_schedule_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="16" />The maximum number of threads that will be used for executing distributed sends.

## background_fetches_pool_size {#background_fetches_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="16" />The maximum number of threads that will be used for fetching data parts from another replica for [*MergeTree-engine](/engines/table-engines/mergetree-family) tables in the background.

## background_merges_mutations_concurrency_ratio {#background_merges_mutations_concurrency_ratio} 

<SettingsInfoBlock type="Float" default_value="2" />
Sets a ratio between the number of threads and the number of background merges and mutations that can be executed concurrently.

For example, if the ratio equals to 2 and [`background_pool_size`](/operations/server-configuration-parameters/settings#background_pool_size) is set to 16 then ClickHouse can execute 32 background merges concurrently. This is possible, because background operations could be suspended and postponed. This is needed to give small merges more execution priority.

:::note
You can only increase this ratio at runtime. To lower it you have to restart the server.

As with the [`background_pool_size`](/operations/server-configuration-parameters/settings#background_pool_size) setting [`background_merges_mutations_concurrency_ratio`](/operations/server-configuration-parameters/settings#background_merges_mutations_concurrency_ratio) could be applied from the `default` profile for backward compatibility.
:::


## background_merges_mutations_scheduling_policy {#background_merges_mutations_scheduling_policy} 

<SettingsInfoBlock type="String" default_value="round_robin" />
The policy on how to perform a scheduling for background merges and mutations. Possible values are: `round_robin` and `shortest_task_first`.

Algorithm used to select next merge or mutation to be executed by background thread pool. Policy may be changed at runtime without server restart.
Could be applied from the `default` profile for backward compatibility.

Possible values:

- `round_robin` — Every concurrent merge and mutation is executed in round-robin order to ensure starvation-free operation. Smaller merges are completed faster than bigger ones just because they have fewer blocks to merge.
- `shortest_task_first` — Always execute smaller merge or mutation. Merges and mutations are assigned priorities based on their resulting size. Merges with smaller sizes are strictly preferred over bigger ones. This policy ensures the fastest possible merge of small parts but can lead to indefinite starvation of big merges in partitions heavily overloaded by `INSERT`s.


## background_message_broker_schedule_pool_size {#background_message_broker_schedule_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="16" />The maximum number of threads that will be used for executing background operations for message streaming.

## background_move_pool_size {#background_move_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="8" />The maximum number of threads that will be used for moving data parts to another disk or volume for *MergeTree-engine tables in a background.

## background_pool_size {#background_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="16" />
Sets the number of threads performing background merges and mutations for tables with MergeTree engines.

:::note
- This setting could also be applied at server startup from the `default` profile configuration for backward compatibility at the ClickHouse server start.
- You can only increase the number of threads at runtime.
- To lower the number of threads you have to restart the server.
- By adjusting this setting, you manage CPU and disk load.
:::

:::danger
Smaller pool size utilizes less CPU and disk resources, but background processes advance slower which might eventually impact query performance.
:::

Before changing it, please also take a look at related MergeTree settings, such as:
- [`number_of_free_entries_in_pool_to_lower_max_size_of_merge`](../../operations/settings/merge-tree-settings.md#number_of_free_entries_in_pool_to_lower_max_size_of_merge).
- [`number_of_free_entries_in_pool_to_execute_mutation`](../../operations/settings/merge-tree-settings.md#number_of_free_entries_in_pool_to_execute_mutation).
- [`number_of_free_entries_in_pool_to_execute_optimize_entire_partition`](/operations/settings/merge-tree-settings#number_of_free_entries_in_pool_to_execute_optimize_entire_partition)

**Example**

```xml
<background_pool_size>16</background_pool_size>
```


## background_schedule_pool_log {#background_schedule_pool_log} 

Contains information about all background tasks that are executed via various background pools.

```xml
<background_schedule_pool_log>
    <database>system</database>
    <table>background_schedule_pool_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
    <!-- Only tasks longer than duration_threshold_milliseconds will be logged. Zero means log everything -->
    <duration_threshold_milliseconds>0</duration_threshold_milliseconds>
</background_schedule_pool_log>
```

## background_schedule_pool_max_parallel_tasks_per_type_ratio {#background_schedule_pool_max_parallel_tasks_per_type_ratio} 

<SettingsInfoBlock type="Float" default_value="0.8" />The maximum ratio of threads in the pool that can execute tasks of the same type simultaneously.

## background_schedule_pool_size {#background_schedule_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="512" />The maximum number of threads that will be used for constantly executing some lightweight periodic operations for replicated tables, Kafka streaming, and DNS cache updates.

## backup_log {#backup_log} 

Settings for the [backup_log](../../operations/system-tables/backup_log.md) system table for logging `BACKUP` and `RESTORE` operations.

<SystemLogParameters/>

**Example**

```xml
<clickhouse>
    <backup_log>
        <database>system</database>
        <table>backup_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </backup_log>
</clickhouse>
```

## backup_threads {#backup_threads} 

<SettingsInfoBlock type="NonZeroUInt64" default_value="16" />The maximum number of threads to execute `BACKUP` requests.

## backups {#backups} 

Settings for backups, used when executing the [`BACKUP` and `RESTORE`](/operations/backup/overview) statements.

The following settings can be configured by sub-tags:

<!-- SQL
WITH settings AS (
  SELECT arrayJoin([
    ('allow_concurrent_backups', 'Bool','Determines whether multiple backup operations can run concurrently on the same host.', 'true'),
    ('allow_concurrent_restores', 'Bool', 'Determines whether multiple restore operations can run concurrently on the same host.', 'true'),
    ('allowed_disk', 'String', 'Disk to backup to when using `File()`. This setting must be set in order to use `File`.', ''),
    ('allowed_path', 'String', 'Path to backup to when using `File()`. This setting must be set in order to use `File`.', ''),
    ('attempts_to_collect_metadata_before_sleep', 'UInt', 'Number of attempts to collect metadata before sleeping in case of inconsistency after comparing collected metadata.', '2'),
    ('collect_metadata_timeout', 'UInt64', 'Timeout in milliseconds for collecting metadata during backup.', '600000'),
    ('compare_collected_metadata', 'Bool', 'If true, compares the collected metadata with the existing metadata to ensure they are not changed during backup .', 'true'),
    ('create_table_timeout', 'UInt64', 'Timeout in milliseconds for creating tables during restore.', '300000'),
    ('max_attempts_after_bad_version', 'UInt64', 'Maximum number of attempts to retry after encountering a bad version error during coordinated backup/restore.', '3'),
    ('max_sleep_before_next_attempt_to_collect_metadata', 'UInt64', 'Maximum sleep time in milliseconds before the next attempt to collect metadata.', '100'),
    ('min_sleep_before_next_attempt_to_collect_metadata', 'UInt64', 'Minimum sleep time in milliseconds before the next attempt to collect metadata.', '5000'),
    ('remove_backup_files_after_failure', 'Bool', 'If the `BACKUP` command fails, ClickHouse will try to remove the files already copied to the backup before the failure,  otherwise it will leave the copied files as they are.', 'true'),
    ('sync_period_ms', 'UInt64', 'Synchronization period in milliseconds for coordinated backup/restore.', '5000'),
    ('test_inject_sleep', 'Bool', 'Testing related sleep', 'false'),
    ('test_randomize_order', 'Bool', 'If true, randomizes the order of certain operations for testing purposes.', 'false'),
    ('zookeeper_path', 'String', 'Path in ZooKeeper where backup and restore metadata is stored when using `ON CLUSTER` clause.', '/clickhouse/backups')
  ]) AS t )
SELECT concat('`', t.1, '`') AS Setting, t.2 AS Type, t.3 AS Description, concat('`', t.4, '`') AS Default FROM settings FORMAT Markdown
-->
| Setting | Type | Description | Default |
|:-|:-|:-|:-|
| [`allow_concurrent_backups`](/operations/server-configuration-parameters/settings#backups.allow_concurrent_backups) | Bool | Determines whether multiple backup operations can run concurrently on the same host. | `true` |
| [`allow_concurrent_restores`](/operations/server-configuration-parameters/settings#backups.allow_concurrent_restores) | Bool | Determines whether multiple restore operations can run concurrently on the same host. | `true` |
| [`allowed_disk`](/operations/server-configuration-parameters/settings#backups.allowed_disk) | String | Disk to backup to when using `File()`. This setting must be set in order to use `File`. | `` |
| [`allowed_path`](/operations/server-configuration-parameters/settings#backups.allowed_path) | String | Path to backup to when using `File()`. This setting must be set in order to use `File`. | `` |
| [`attempts_to_collect_metadata_before_sleep`](/operations/server-configuration-parameters/settings#backups.attempts_to_collect_metadata_before_sleep) | UInt | Number of attempts to collect metadata before sleeping in case of inconsistency after comparing collected metadata. | `2` |
| [`collect_metadata_timeout`](/operations/server-configuration-parameters/settings#backups.collect_metadata_timeout) | UInt64 | Timeout in milliseconds for collecting metadata during backup. | `600000` |
| [`compare_collected_metadata`](/operations/server-configuration-parameters/settings#backups.compare_collected_metadata) | Bool | If true, compares the collected metadata with the existing metadata to ensure they are not changed during backup . | `true` |
| [`create_table_timeout`](/operations/server-configuration-parameters/settings#backups.create_table_timeout) | UInt64 | Timeout in milliseconds for creating tables during restore. | `300000` |
| [`max_attempts_after_bad_version`](/operations/server-configuration-parameters/settings#backups.max_attempts_after_bad_version) | UInt64 | Maximum number of attempts to retry after encountering a bad version error during coordinated backup/restore. | `3` |
| [`max_sleep_before_next_attempt_to_collect_metadata`](/operations/server-configuration-parameters/settings#backups.max_sleep_before_next_attempt_to_collect_metadata) | UInt64 | Maximum sleep time in milliseconds before the next attempt to collect metadata. | `100` |
| [`min_sleep_before_next_attempt_to_collect_metadata`](/operations/server-configuration-parameters/settings#backups.min_sleep_before_next_attempt_to_collect_metadata) | UInt64 | Minimum sleep time in milliseconds before the next attempt to collect metadata. | `5000` |
| [`remove_backup_files_after_failure`](/operations/server-configuration-parameters/settings#backups.remove_backup_files_after_failure) | Bool | If the `BACKUP` command fails, ClickHouse will try to remove the files already copied to the backup before the failure,  otherwise it will leave the copied files as they are. | `true` |
| [`sync_period_ms`](/operations/server-configuration-parameters/settings#backups.sync_period_ms) | UInt64 | Synchronization period in milliseconds for coordinated backup/restore. | `5000` |
| [`test_inject_sleep`](/operations/server-configuration-parameters/settings#backups.test_inject_sleep) | Bool | Testing related sleep | `false` |
| [`test_randomize_order`](/operations/server-configuration-parameters/settings#backups.test_randomize_order) | Bool | If true, randomizes the order of certain operations for testing purposes. | `false` |
| [`zookeeper_path`](/operations/server-configuration-parameters/settings#backups.zookeeper_path) | String | Path in ZooKeeper where backup and restore metadata is stored when using `ON CLUSTER` clause. | `/clickhouse/backups` |

This setting is configured by default as:

```xml
<backups>
    ....
</backups>
```

## backups_io_thread_pool_queue_size {#backups_io_thread_pool_queue_size} 

<SettingsInfoBlock type="UInt64" default_value="0" />
The maximum number of jobs that can be scheduled on the Backups IO Thread pool. It is recommended to keep this queue unlimited due to the current S3 backup logic.

:::note
A value of `0` (default) means unlimited.
:::


## bcrypt_workfactor {#bcrypt_workfactor} 

Work factor for the `bcrypt_password` authentication type which uses the [Bcrypt algorithm](https://wildlyinaccurate.com/bcrypt-choosing-a-work-factor/).
The work factor defines the amount of computations and time needed to compute the hash and verify the password.

```xml
<bcrypt_workfactor>12</bcrypt_workfactor>
```

:::warning
For applications with high-frequency authentication,
consider alternative authentication methods due to
bcrypt's computational overhead at higher work factors.
:::

## blob_storage_log {#blob_storage_log} 

Settings for the [`blob_storage_log`](../system-tables/blob_storage_log.md) system table.

<SystemLogParameters/>

Example:

```xml
<blob_storage_log>
    <database>system</database
    <table>blob_storage_log</table
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds
    <ttl>event_date + INTERVAL 30 DAY</ttl>
</blob_storage_log>
```

## builtin_dictionaries_reload_interval {#builtin_dictionaries_reload_interval} 

The interval in seconds before reloading built-in dictionaries.

ClickHouse reloads built-in dictionaries every x seconds. This makes it possible to edit dictionaries "on the fly" without restarting the server.

**Example**

```xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

## cache_size_to_ram_max_ratio {#cache_size_to_ram_max_ratio} 

<SettingsInfoBlock type="Double" default_value="0.5" />Set cache size to RAM max ratio. Allows lowering the cache size on low-memory systems.

## cannot_allocate_thread_fault_injection_probability {#cannot_allocate_thread_fault_injection_probability} 

<SettingsInfoBlock type="Double" default_value="0" />For testing purposes.

## cgroups_memory_usage_observer_wait_time {#cgroups_memory_usage_observer_wait_time} 

<SettingsInfoBlock type="UInt64" default_value="15" />
Interval in seconds during which the server's maximum allowed memory consumption is adjusted by the corresponding threshold in cgroups.

To disable the cgroup observer, set this value to `0`.


## compiled_expression_cache_elements_size {#compiled_expression_cache_elements_size} 

<SettingsInfoBlock type="UInt64" default_value="10000" />Sets the cache size (in elements) for [compiled expressions](../../operations/caches.md).

## compiled_expression_cache_size {#compiled_expression_cache_size} 

<SettingsInfoBlock type="UInt64" default_value="134217728" />Sets the cache size (in bytes) for [compiled expressions](../../operations/caches.md).

## compression {#compression} 

Data compression settings for [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-engine tables.

:::note
We recommend not changing this if you have just started using ClickHouse.
:::

**Configuration template**:

```xml
<compression>
    <case>
      <min_part_size>...</min_part_size>
      <min_part_size_ratio>...</min_part_size_ratio>
      <method>...</method>
      <level>...</level>
    </case>
    ...
</compression>
```

**`<case>` fields**:

- `min_part_size` – The minimum size of a data part.
- `min_part_size_ratio` – The ratio of the data part size to the table size.
- `method` – Compression method. Acceptable values: `lz4`, `lz4hc`, `zstd`,`deflate_qpl`.
- `level` – Compression level. See [Codecs](/sql-reference/statements/create/table#general-purpose-codecs).

:::note
You can configure multiple `<case>` sections.
:::

**Actions when conditions are met**:

- If a data part matches a condition set, ClickHouse uses the specified compression method.
- If a data part matches multiple condition sets, ClickHouse uses the first matched condition set.

:::note
If no conditions are met for a data part, ClickHouse uses the `lz4` compression.
:::

**Example**

```xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
        <level>1</level>
    </case>
</compression>
```

## concurrent_threads_scheduler {#concurrent_threads_scheduler} 

<SettingsInfoBlock type="String" default_value="fair_round_robin" />
The policy on how to perform a scheduling of CPU slots specified by `concurrent_threads_soft_limit_num` and `concurrent_threads_soft_limit_ratio_to_cores`. Algorithm used to govern how limited number of CPU slots are distributed among concurrent queries. Scheduler may be changed at runtime without server restart.

Possible values:

- `round_robin` — Every query with setting `use_concurrency_control` = 1 allocates up to `max_threads` CPU slots. One slot per thread. On contention CPU slot are granted to queries using round-robin. Note that the first slot is granted unconditionally, which could lead to unfairness and increased latency of queries having high `max_threads` in presence of high number of queries with `max_threads` = 1.
- `fair_round_robin` — Every query with setting `use_concurrency_control` = 1 allocates up to `max_threads - 1` CPU slots. Variation of `round_robin` that does not require a CPU slot for the first thread of every query. This way queries having `max_threads` = 1 do not require any slot and could not unfairly allocate all slots. There are no slots granted unconditionally.


## concurrent_threads_soft_limit_num {#concurrent_threads_soft_limit_num} 

<SettingsInfoBlock type="UInt64" default_value="0" />
The maximum number of query processing threads, excluding threads for retrieving data from remote servers, allowed to run all queries. This is not a hard limit. In case if the limit is reached the query will still get at least one thread to run. Query can upscale to desired number of threads during execution if more threads become available.

:::note
A value of `0` (default) means unlimited.
:::


## concurrent_threads_soft_limit_ratio_to_cores {#concurrent_threads_soft_limit_ratio_to_cores} 

<SettingsInfoBlock type="UInt64" default_value="0" />Same as [`concurrent_threads_soft_limit_num`](#concurrent_threads_soft_limit_num), but with ratio to cores.

## config-file {#config-file} 

<SettingsInfoBlock type="String" default_value="config.xml" />Points to the server config file.

## config_reload_interval_ms {#config_reload_interval_ms} 

<SettingsInfoBlock type="UInt64" default_value="2000" />
How often clickhouse will reload config and check for new changes


## core_dump {#core_dump} 

Configures soft limit for core dump file size.

:::note
Hard limit is configured via system tools
:::

**Example**

```xml
<core_dump>
     <size_limit>1073741824</size_limit>
</core_dump>
```

## cpu_slot_preemption {#cpu_slot_preemption} 

<SettingsInfoBlock type="Bool" default_value="0" />
Defines how workload scheduling for CPU resources (MASTER THREAD and WORKER THREAD) is done.

- If `true` (recommended), accounting is done based on actual CPU time consumed. A fair number of CPU time would be allocated to competing workloads. Slots are allocated for a limited amount of time and re-requested after expiry. Slot requesting may block thread execution in case of CPU resource overload, i.e., preemption may happen. This ensures CPU-time fairness.
- If `false` (default), accounting is based on the number of CPU slots allocated. A fair number of CPU slots would be allocated to competing workloads. A slot is allocated when a thread starts, held continuously, and released when the thread ends execution. The number of threads allocated for query execution may only increase from 1 to `max_threads` and never decrease. This is more favorable to long-running queries and may lead to CPU starvation of short queries.

**Example**

```xml
<cpu_slot_preemption>true</cpu_slot_preemption>
```

**See Also**
- [Workload Scheduling](/operations/workload-scheduling.md)


## cpu_slot_preemption_timeout_ms {#cpu_slot_preemption_timeout_ms} 

<SettingsInfoBlock type="UInt64" default_value="1000" />
It defines how many milliseconds could a worker thread wait during preemption, i.e. while waiting for another CPU slot to be granted. After this timeout, if thread was unable to acquire a new CPU slot it will exit and the query is scaled down to a lower number of concurrently executing threads dynamically. Note that master thread never downscaled, but could be preempted indefinitely. Makes sense only when `cpu_slot_preemption` is enabled and CPU resource is defined for WORKER THREAD.

**Example**

```xml
<cpu_slot_preemption_timeout_ms>1000</cpu_slot_preemption_timeout_ms>
```

**See Also**
- [Workload Scheduling](/operations/workload-scheduling.md)


## cpu_slot_quantum_ns {#cpu_slot_quantum_ns} 

<SettingsInfoBlock type="UInt64" default_value="10000000" />
It defines how many CPU nanoseconds a thread is allowed to consume after acquired a CPU slot and before it should request another CPU slot. Makes sense only if `cpu_slot_preemption` is enabled and CPU resource is defined for MASTER THREAD or WORKER THREAD.

**Example**

```xml
<cpu_slot_quantum_ns>10000000</cpu_slot_quantum_ns>
```

**See Also**
- [Workload Scheduling](/operations/workload-scheduling.md)


## crash_log {#crash_log} 

Settings for the [crash_log](../../operations/system-tables/crash_log.md) system table operation.

The following settings can be configured by sub-tags:

| Setting                            | Description                                                                                                                                             | Default             | Note                                                                                                               |
|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------|--------------------------------------------------------------------------------------------------------------------|
| [`buffer_size_rows_flush_threshold`](/operations/server-configuration-parameters/settings#crash_log.buffer_size_rows_flush_threshold) | Threshold for amount of lines. If the threshold is reached, flushing logs to the disk is launched in background.                                        | `max_size_rows / 2` |                                                                                                                    |
| [`database`](/operations/server-configuration-parameters/settings#crash_log.database) | Name of the database.                                                                                                                                   |                     |                                                                                                                    |
| [`engine`](/operations/server-configuration-parameters/settings#crash_log.engine) | [MergeTree Engine Definition](/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-creating-a-table) for a system table. |                     | Cannot be used if `partition_by` or `order_by` defined. If not specified `MergeTree` is selected by default        |
| [`flush_interval_milliseconds`](/operations/server-configuration-parameters/settings#crash_log.flush_interval_milliseconds) | Interval for flushing data from the buffer in memory to the table.                                                                                      | `7500`              |                                                                                                                    |
| [`flush_on_crash`](/operations/server-configuration-parameters/settings#crash_log.flush_on_crash) | Sets whether logs should be dumped to the disk in case of a crash.                                                                                      | `false`             |                                                                                                                    |
| [`max_size_rows`](/operations/server-configuration-parameters/settings#crash_log.max_size_rows) | Maximal size in lines for the logs. When the amount of non-flushed logs reaches the max_size, logs are dumped to the disk.                              | `1024`           |                                                                                                                    |
| [`order_by`](/operations/server-configuration-parameters/settings#crash_log.order_by) | [Custom sorting key](/engines/table-engines/mergetree-family/mergetree#order_by) for a system table. Can't be used if `engine` defined.      |                     | If `engine` is specified for system table, `order_by` parameter should be specified directly inside 'engine'       |
| [`partition_by`](/operations/server-configuration-parameters/settings#crash_log.partition_by) | [Custom partitioning key](/engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table.                               |                     | If `engine` is specified for system table, `partition_by` parameter should be specified directly inside 'engine'   |
| [`reserved_size_rows`](/operations/server-configuration-parameters/settings#crash_log.reserved_size_rows) | Pre-allocated memory size in lines for the logs.                                                                                                        | `1024`              |                                                                                                                    |
| [`settings`](/operations/server-configuration-parameters/settings#crash_log.settings) | [Additional parameters](/engines/table-engines/mergetree-family/mergetree/#settings) that control the behavior of the MergeTree (optional).   |                     | If `engine` is specified for system table, `settings` parameter should be specified directly inside 'engine'       |
| [`storage_policy`](/operations/server-configuration-parameters/settings#crash_log.storage_policy) | Name of the storage policy to use for the table (optional).                                                                                             |                     | If `engine` is specified for system table, `storage_policy` parameter should be specified directly inside 'engine' |
| [`table`](/operations/server-configuration-parameters/settings#crash_log.table) | Name of the system table.                                                                                                                               |                     |                                                                                                                    |
| [`ttl`](/operations/server-configuration-parameters/settings#crash_log.ttl) | Specifies the table [TTL](/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl).                                              |                     | If `engine` is specified for system table, `ttl` parameter should be specified directly inside 'engine'            |

The default server configuration file `config.xml` contains the following settings section:

```xml
<crash_log>
    <database>system</database>
    <table>crash_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1024</max_size_rows>
    <reserved_size_rows>1024</reserved_size_rows>
    <buffer_size_rows_flush_threshold>512</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</crash_log>
```

## custom_cached_disks_base_directory {#custom_cached_disks_base_directory} 

This setting specifies the cache path for custom (created from SQL) cached disks.
`custom_cached_disks_base_directory` has higher priority for custom disks over `filesystem_caches_path` (found in `filesystem_caches_path.xml`),
which is used if the former one is absent.
The filesystem cache setting path must lie inside that directory,
otherwise an exception will be thrown preventing the disk from being created.

:::note
This will not affect disks created on an older version for which the server was upgraded.
In this case, an exception will not be thrown, to allow the server to successfully start.
:::

Example:

```xml
<custom_cached_disks_base_directory>/var/lib/clickhouse/caches/</custom_cached_disks_base_directory>
```

## custom_settings_prefixes {#custom_settings_prefixes} 

List of prefixes for [custom settings](/operations/settings/query-level#custom_settings). The prefixes must be separated with commas.

**Example**

```xml
<custom_settings_prefixes>custom_</custom_settings_prefixes>
```

**See Also**

- [Custom settings](/operations/settings/query-level#custom_settings)

## database_atomic_delay_before_drop_table_sec {#database_atomic_delay_before_drop_table_sec} 

<SettingsInfoBlock type="UInt64" default_value="480" />
The delay during which a dropped table can be restored using the [`UNDROP`](/sql-reference/statements/undrop.md) statement. If `DROP TABLE` ran with a `SYNC` modifier, the setting is ignored.
The default for this setting is `480` (8 minutes).


## database_catalog_drop_error_cooldown_sec {#database_catalog_drop_error_cooldown_sec} 

<SettingsInfoBlock type="UInt64" default_value="5" />In case of a failed table drop, ClickHouse will wait for this time-out before retrying the operation.

## database_catalog_drop_table_concurrency {#database_catalog_drop_table_concurrency} 

<SettingsInfoBlock type="UInt64" default_value="16" />The size of the threadpool used for dropping tables.

## database_catalog_unused_dir_cleanup_period_sec {#database_catalog_unused_dir_cleanup_period_sec} 

<SettingsInfoBlock type="UInt64" default_value="86400" />
Parameter of a task that cleans up garbage from `store/` directory.
Sets scheduling period of the task.

:::note
A value of `0` means "never". The default value corresponds to 1 day.
:::


## database_catalog_unused_dir_hide_timeout_sec {#database_catalog_unused_dir_hide_timeout_sec} 

<SettingsInfoBlock type="UInt64" default_value="3600" />
Parameter of a task that cleans up garbage from `store/` directory.
If some subdirectory is not used by clickhouse-server and this directory was not modified for last
[`database_catalog_unused_dir_hide_timeout_sec`](/operations/server-configuration-parameters/settings#database_catalog_unused_dir_hide_timeout_sec) seconds, the task will "hide" this directory by
removing all access rights. It also works for directories that clickhouse-server does not
expect to see inside `store/`.

:::note
A value of `0` means "immediately".
:::


## database_catalog_unused_dir_rm_timeout_sec {#database_catalog_unused_dir_rm_timeout_sec} 

<SettingsInfoBlock type="UInt64" default_value="2592000" />
Parameter of a task that cleans up garbage from `store/` directory.
If some subdirectory is not used by clickhouse-server and it was previously "hidden"
(see [database_catalog_unused_dir_hide_timeout_sec](/operations/server-configuration-parameters/settings#database_catalog_unused_dir_hide_timeout_sec))
and this directory was not modified for last
[`database_catalog_unused_dir_rm_timeout_sec`]/operations/server-configuration-parameters/settings#database_catalog_unused_dir_rm_timeout_sec) seconds, the task will remove this directory.
It also works for directories that clickhouse-server does not
expect to see inside `store/`.

:::note
A value of `0` means "never". The default value corresponds to 30 days.
:::


## database_replicated_allow_detach_permanently {#database_replicated_allow_detach_permanently} 

<SettingsInfoBlock type="Bool" default_value="1" />Allow detaching tables permanently in Replicated databases

## database_replicated_drop_broken_tables {#database_replicated_drop_broken_tables} 

<SettingsInfoBlock type="Bool" default_value="0" />Drop unexpected tables from Replicated databases instead of moving them to a separate local database

## dead_letter_queue {#dead_letter_queue} 

Setting for the 'dead_letter_queue' system table.

<SystemLogParameters/>

The default settings are:

```xml
<dead_letter_queue>
    <database>system</database>
    <table>dead_letter</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</dead_letter_queue>
```

## default_database {#default_database} 

<SettingsInfoBlock type="String" default_value="default" />The default database name.

## default_password_type {#default_password_type} 

Sets the password type to be automatically set for in queries like `CREATE USER u IDENTIFIED BY 'p'`.

Accepted values are:
- `plaintext_password`
- `sha256_password`
- `double_sha1_password`
- `bcrypt_password`

```xml
<default_password_type>sha256_password</default_password_type>
```

## default_profile {#default_profile} 

Default settings profile. Settings profiles are located in the file specified in the setting `user_config`.

**Example**

```xml
<default_profile>default</default_profile>
```

## default_replica_name {#default_replica_name} 

<SettingsInfoBlock type="String" default_value="{replica}" />
The replica name in ZooKeeper.

**Example**

```xml
<default_replica_name>{replica}</default_replica_name>
```


## default_replica_path {#default_replica_path} 

<SettingsInfoBlock type="String" default_value="/clickhouse/tables/{uuid}/{shard}" />
The path to the table in ZooKeeper.

**Example**

```xml
<default_replica_path>/clickhouse/tables/{uuid}/{shard}</default_replica_path>
```


## default_session_timeout {#default_session_timeout} 

Default session timeout, in seconds.

```xml
<default_session_timeout>60</default_session_timeout>
```

## dictionaries_config {#dictionaries_config} 

The path to the config file for dictionaries.

Path:

- Specify the absolute path or the path relative to the server config file.
- The path can contain wildcards \* and ?.

See also:
- "[Dictionaries](../../sql-reference/dictionaries/index.md)".

**Example**

```xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

## dictionaries_lazy_load {#dictionaries_lazy_load} 

<SettingsInfoBlock type="Bool" default_value="1" />
Lazy loading of dictionaries.

- If `true`, then each dictionary is loaded on the first use. If the loading is failed, the function that was using the dictionary throws an exception.
- If `false`, then the server loads all dictionaries at startup.

:::note
The server will wait at startup until all the dictionaries finish their loading before receiving any connections
(exception: if [`wait_dictionaries_load_at_startup`](/operations/server-configuration-parameters/settings#wait_dictionaries_load_at_startup) is set to `false`).
:::

**Example**

```xml
<dictionaries_lazy_load>true</dictionaries_lazy_load>
```


## dictionaries_lib_path {#dictionaries_lib_path} 

<SettingsInfoBlock type="String" default_value="/var/lib/clickhouse/dictionaries_lib/" />
The directory with dictionaries lib.

**Example**

```xml
<dictionaries_lib_path>/var/lib/clickhouse/dictionaries_lib/</dictionaries_lib_path>
```


## dictionary_background_reconnect_interval {#dictionary_background_reconnect_interval} 

<SettingsInfoBlock type="UInt64" default_value="1000" />Interval in milliseconds for reconnection attempts of failed MySQL and Postgres dictionaries having `background_reconnect` enabled.

## disable_insertion_and_mutation {#disable_insertion_and_mutation} 

<SettingsInfoBlock type="Bool" default_value="0" />
Disable insert/alter/delete queries. This setting will be enabled if someone needs read-only nodes to prevent insertion and mutation affect reading performance. Inserts into external engines (S3, DataLake, MySQL, PostrgeSQL, Kafka, etc) are allowed despite this setting.


## disable_internal_dns_cache {#disable_internal_dns_cache} 

<SettingsInfoBlock type="Bool" default_value="0" />Disables the internal DNS cache. Recommended for operating ClickHouse in systems with frequently changing infrastructure such as Kubernetes.

## disable_tunneling_for_https_requests_over_http_proxy {#disable_tunneling_for_https_requests_over_http_proxy} 

By default, tunneling (i.e, `HTTP CONNECT`) is used to make `HTTPS` requests over `HTTP` proxy. This setting can be used to disable it.

**no_proxy**

By default, all requests will go through the proxy. In order to disable it for specific hosts, the `no_proxy` variable must be set.
It can be set inside the `<proxy>` clause for list and remote resolvers and as an environment variable for environment resolver.
It supports IP addresses, domains, subdomains and `'*'` wildcard for full bypass. Leading dots are stripped just like curl does.

**Example**

The below configuration bypasses proxy requests to `clickhouse.cloud` and all of its subdomains (e.g, `auth.clickhouse.cloud`).
The same applies to GitLab, even though it has a leading dot. Both `gitlab.com` and `about.gitlab.com` would bypass the proxy.

```xml
<proxy>
    <no_proxy>clickhouse.cloud,.gitlab.com</no_proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

## disk_connections_hard_limit {#disk_connections_hard_limit} 

<SettingsInfoBlock type="UInt64" default_value="200000" />Exception is thrown at a creation attempt when this limit is reached. Set to 0 to turn off hard limitation. The limit applies to the disks connections.

## disk_connections_soft_limit {#disk_connections_soft_limit} 

<SettingsInfoBlock type="UInt64" default_value="5000" />Connections above this limit have significantly shorter time to live. The limit applies to the disks connections.

## disk_connections_store_limit {#disk_connections_store_limit} 

<SettingsInfoBlock type="UInt64" default_value="10000" />Connections above this limit reset after use. Set to 0 to turn connection cache off. The limit applies to the disks connections.

## disk_connections_warn_limit {#disk_connections_warn_limit} 

<SettingsInfoBlock type="UInt64" default_value="8000" />Warning massages are written to the logs if number of in-use connections are higher than this limit. The limit applies to the disks connections.

## display_secrets_in_show_and_select {#display_secrets_in_show_and_select} 

<SettingsInfoBlock type="Bool" default_value="0" />
Enables or disables showing secrets in `SHOW` and `SELECT` queries for tables, databases, table functions, and dictionaries.

User wishing to see secrets must also have
[`format_display_secrets_in_show_and_select` format setting](../settings/formats#format_display_secrets_in_show_and_select)
turned on and a
[`displaySecretsInShowAndSelect`](/sql-reference/statements/grant#displaysecretsinshowandselect) privilege.

Possible values:

- `0` — Disabled.
- `1` — Enabled.


## distributed_cache_apply_throttling_settings_from_client {#distributed_cache_apply_throttling_settings_from_client} 

<SettingsInfoBlock type="Bool" default_value="1" />Whether cache server should apply throttling settings received from client.

## distributed_cache_keep_up_free_connections_ratio {#distributed_cache_keep_up_free_connections_ratio} 

<SettingsInfoBlock type="Float" default_value="0.1" />Soft limit for number of active connection distributed cache will try to keep free. After the number of free connections goes below distributed_cache_keep_up_free_connections_ratio * max_connections, connections with oldest activity will be closed until the number goes above the limit.

## distributed_ddl {#distributed_ddl} 

Manage executing [distributed ddl queries](../../sql-reference/distributed-ddl.md) (`CREATE`, `DROP`, `ALTER`, `RENAME`) on cluster.
Works only if [ZooKeeper](/operations/server-configuration-parameters/settings#zookeeper) is enabled.

The configurable settings within `<distributed_ddl>` include:

| Setting                | Description                                                                                                                       | Default Value                          |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|
| [`cleanup_delay_period`](/operations/server-configuration-parameters/settings#distributed_ddl.cleanup_delay_period) | cleaning starts after new node event is received if the last cleaning wasn't made sooner than `cleanup_delay_period` seconds ago. | `60` seconds                           |
| [`max_tasks_in_queue`](/operations/server-configuration-parameters/settings#distributed_ddl.max_tasks_in_queue) | the maximum number of tasks that can be in the queue.                                                                             | `1,000`                                |
| [`path`](/operations/server-configuration-parameters/settings#distributed_ddl.path) | the path in Keeper for the `task_queue` for DDL queries                                                                           |                                        |
| [`pool_size`](/operations/server-configuration-parameters/settings#distributed_ddl.pool_size) | how many `ON CLUSTER` queries can be run simultaneously                                                                           |                                        |
| [`profile`](/operations/server-configuration-parameters/settings#distributed_ddl.profile) | the profile used to execute the DDL queries                                                                                       |                                        |
| [`task_max_lifetime`](/operations/server-configuration-parameters/settings#distributed_ddl.task_max_lifetime) | delete node if its age is greater than this value.                                                                                | `7 * 24 * 60 * 60` (a week in seconds) |

**Example**

```xml
<distributed_ddl>
    <!-- Path in ZooKeeper to queue with DDL queries -->
    <path>/clickhouse/task_queue/ddl</path>

    <!-- Settings from this profile will be used to execute DDL queries -->
    <profile>default</profile>

    <!-- Controls how much ON CLUSTER queries can be run simultaneously. -->
    <pool_size>1</pool_size>

    <!--
         Cleanup settings (active tasks will not be removed)
    -->

    <!-- Controls task TTL (default 1 week) -->
    <task_max_lifetime>604800</task_max_lifetime>

    <!-- Controls how often cleanup should be performed (in seconds) -->
    <cleanup_delay_period>60</cleanup_delay_period>

    <!-- Controls how many tasks could be in the queue -->
    <max_tasks_in_queue>1000</max_tasks_in_queue>
</distributed_ddl>
```

## distributed_ddl.cleanup_delay_period {#distributed_ddl.cleanup_delay_period} 

<SettingsInfoBlock type="UInt64" default_value="60" />cleaning starts after new node event is received if the last cleaning wasn't made sooner than `<cleanup_delay_period>` seconds ago.

## distributed_ddl.max_tasks_in_queue {#distributed_ddl.max_tasks_in_queue} 

<SettingsInfoBlock type="UInt64" default_value="1000" />the maximum number of tasks that can be in the queue.

## distributed_ddl.path {#distributed_ddl.path} 

<SettingsInfoBlock type="String" default_value="/clickhouse/task_queue/ddl/" />the path in Keeper for the `<task_queue>` for DDL queries

## distributed_ddl.pool_size {#distributed_ddl.pool_size} 

<SettingsInfoBlock type="Int32" default_value="1" />how many `<ON CLUSTER>` queries can be run simultaneously

## distributed_ddl.profile {#distributed_ddl.profile} 

the profile used to execute the DDL queries

## distributed_ddl.replicas_path {#distributed_ddl.replicas_path} 

<SettingsInfoBlock type="String" default_value="/clickhouse/task_queue/replicas/" />the path in Keeper for the `<task_queue>` for replicas

## distributed_ddl.task_max_lifetime {#distributed_ddl.task_max_lifetime} 

<SettingsInfoBlock type="UInt64" default_value="604800" />delete node if its age is greater than this value.

## distributed_ddl_use_initial_user_and_roles {#distributed_ddl_use_initial_user_and_roles} 

<SettingsInfoBlock type="Bool" default_value="0" />If enabled, ON CLUSTER queries will preserve and use the initiator's user and roles for execution on remote shards. This ensures consistent access control across the cluster but requires that the user and roles exist on all nodes.

## dns_allow_resolve_names_to_ipv4 {#dns_allow_resolve_names_to_ipv4} 

<SettingsInfoBlock type="Bool" default_value="1" />Allows resolve names to ipv4 addresses.

## dns_allow_resolve_names_to_ipv6 {#dns_allow_resolve_names_to_ipv6} 

<SettingsInfoBlock type="Bool" default_value="1" />Allows resolve names to ipv6 addresses.

## dns_cache_max_entries {#dns_cache_max_entries} 

<SettingsInfoBlock type="UInt64" default_value="10000" />Internal DNS cache max entries.

## dns_cache_update_period {#dns_cache_update_period} 

<SettingsInfoBlock type="Int32" default_value="15" />Internal DNS cache update period in seconds.

## dns_max_consecutive_failures {#dns_max_consecutive_failures} 

<SettingsInfoBlock type="UInt32" default_value="5" />
Stop further attempts to update a hostname's DNS cache after this number of consecutive failures. The information still remains in the DNS cache. Zero means unlimited.

**See also**

- [`SYSTEM DROP DNS CACHE`](../../sql-reference/statements/system#drop-dns-cache)


## drop_distributed_cache_pool_size {#drop_distributed_cache_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="8" />The size of the threadpool used for dropping distributed cache.

## drop_distributed_cache_queue_size {#drop_distributed_cache_queue_size} 

<SettingsInfoBlock type="UInt64" default_value="1000" />The queue size of the threadpool used for dropping distributed cache.

## enable_azure_sdk_logging {#enable_azure_sdk_logging} 

<SettingsInfoBlock type="Bool" default_value="0" />Enables logging from Azure sdk

## encryption {#encryption} 

Configures a command to obtain a key to be used by [encryption codecs](/sql-reference/statements/create/table#encryption-codecs). Key (or keys) should be written in environment variables or set in the configuration file.

Keys can be hex or string with a length equal to 16 bytes.

**Example**

Loading from config:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key>1234567812345678</key>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
Storing keys in the configuration file is not recommended. It isn't secure. You can move the keys into a separate config file on a secure disk and put a symlink to that config file to `config.d/` folder.
:::

Loading from config, when the key is in hex:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex>00112233445566778899aabbccddeeff</key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Loading key from the environment variable:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex from_env="ENVVAR"></key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Here `current_key_id` sets the current key for encryption, and all specified keys can be used for decryption.

Each of these methods can be applied for multiple keys:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
        <key_hex id="1" from_env="ENVVAR"></key_hex>
        <current_key_id>1</current_key_id>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Here `current_key_id` shows current key for encryption.

Also, users can add nonce that must be 12 bytes long (by default encryption and decryption processes use nonce that consists of zero bytes):

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce>012345678910</nonce>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Or it can be set in hex:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce_hex>abcdefabcdef</nonce_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```
:::note
Everything mentioned above can be applied for `aes_256_gcm_siv` (but the key must be 32 bytes long).
:::

## error_log {#error_log} 

It is disabled by default.

**Enabling**

To manually turn on error history collection [`system.error_log`](../../operations/system-tables/error_log.md), create `/etc/clickhouse-server/config.d/error_log.xml` with the following content:

```xml
<clickhouse>
    <error_log>
        <database>system</database>
        <table>error_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </error_log>
</clickhouse>
```

**Disabling**

To disable `error_log` setting, you should create the following file `/etc/clickhouse-server/config.d/disable_error_log.xml` with the following content:

```xml
<clickhouse>
    <error_log remove="1" />
</clickhouse>
```

<SystemLogParameters/>

## filesystem_caches_path {#filesystem_caches_path} 


This setting specifies the cache path.

**Example**

```xml
<filesystem_caches_path>/var/lib/clickhouse/filesystem_caches/</filesystem_caches_path>
```


## format_parsing_thread_pool_queue_size {#format_parsing_thread_pool_queue_size} 

<SettingsInfoBlock type="UInt64" default_value="10000" />
The maximum number of jobs that can be scheduled on thread pool for parsing input.

:::note
A value of `0` means unlimited.
:::


## format_schema_path {#format_schema_path} 

<SettingsInfoBlock type="String" default_value="/var/lib/clickhouse/format_schemas/" />
The path to the directory with the schemes for the input data, such as schemas for the [CapnProto](/interfaces/formats/CapnProto) format.

**Example**

```xml
<!-- Directory containing schema files for various input formats. -->
<format_schema_path>/var/lib/clickhouse/format_schemas/</format_schema_path>
```


## global_profiler_cpu_time_period_ns {#global_profiler_cpu_time_period_ns} 

<SettingsInfoBlock type="UInt64" default_value="10000000000" />Period for CPU clock timer of global profiler (in nanoseconds). Set 0 value to turn off the CPU clock global profiler. Recommended value is at least 10000000 (100 times a second) for single queries or 1000000000 (once a second) for cluster-wide profiling.

## global_profiler_real_time_period_ns {#global_profiler_real_time_period_ns} 

<SettingsInfoBlock type="UInt64" default_value="10000000000" />Period for real clock timer of global profiler (in nanoseconds). Set 0 value to turn off the real clock global profiler. Recommended value is at least 10000000 (100 times a second) for single queries or 1000000000 (once a second) for cluster-wide profiling.

## google_protos_path {#google_protos_path} 

<SettingsInfoBlock type="String" default_value="/usr/share/clickhouse/protos/" />
Defines a directory containing proto files for Protobuf types.

**Example**

```xml
<google_protos_path>/usr/share/clickhouse/protos/</google_protos_path>
```


## graphite {#graphite} 

Sending data to [Graphite](https://github.com/graphite-project).

Settings:

- `host` – The Graphite server.
- `port` – The port on the Graphite server.
- `interval` – The interval for sending, in seconds.
- `timeout` – The timeout for sending data, in seconds.
- `root_path` – Prefix for keys.
- `metrics` – Sending data from the [system.metrics](/operations/system-tables/metrics) table.
- `events` – Sending deltas data accumulated for the time period from the [system.events](/operations/system-tables/events) table.
- `events_cumulative` – Sending cumulative data from the [system.events](/operations/system-tables/events) table.
- `asynchronous_metrics` – Sending data from the [system.asynchronous_metrics](/operations/system-tables/asynchronous_metrics) table.

You can configure multiple `<graphite>` clauses. For instance, you can use this for sending different data at different intervals.

**Example**

```xml
<graphite>
    <host>localhost</host>
    <port>42000</port>
    <timeout>0.1</timeout>
    <interval>60</interval>
    <root_path>one_min</root_path>
    <metrics>true</metrics>
    <events>true</events>
    <events_cumulative>false</events_cumulative>
    <asynchronous_metrics>true</asynchronous_metrics>
</graphite>
```

## graphite_rollup {#graphite_rollup} 

Settings for thinning data for Graphite.

For more details, see [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md).

**Example**

```xml
<graphite_rollup_example>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup_example>
```

## hdfs.libhdfs3_conf {#hdfs.libhdfs3_conf} 

Points libhdfs3 to the right location for its config.

## hsts_max_age {#hsts_max_age} 

Expired time for HSTS in seconds.

:::note
A value of `0` means ClickHouse disables HSTS. If you set a positive number, the HSTS will be enabled and the max-age is the number you set.
:::

**Example**

```xml
<hsts_max_age>600000</hsts_max_age>
```

## http_connections_hard_limit {#http_connections_hard_limit} 

<SettingsInfoBlock type="UInt64" default_value="200000" />Exception is thrown at a creation attempt when this limit is reached. Set to 0 to turn off hard limitation. The limit applies to the http connections which do not belong to any disk or storage.

## http_connections_soft_limit {#http_connections_soft_limit} 

<SettingsInfoBlock type="UInt64" default_value="100" />Connections above this limit have significantly shorter time to live. The limit applies to the http connections which do not belong to any disk or storage.

## http_connections_store_limit {#http_connections_store_limit} 

<SettingsInfoBlock type="UInt64" default_value="1000" />Connections above this limit reset after use. Set to 0 to turn connection cache off. The limit applies to the http connections which do not belong to any disk or storage.

## http_connections_warn_limit {#http_connections_warn_limit} 

<SettingsInfoBlock type="UInt64" default_value="500" />Warning massages are written to the logs if number of in-use connections are higher than this limit. The limit applies to the http connections which do not belong to any disk or storage.

## http_handlers {#http_handlers} 

Allows using custom HTTP handlers.
To add a new http handler simply add a new `<rule>`.
Rules are checked from top to bottom as defined,
and the first match will run the handler.

The following settings can be configured by sub-tags:

| Sub-tags             | Definition                                                                                                                                        |
|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `url`                | To match the request URL, you can use the 'regex:' prefix to use regex match (optional)                                                           |
| `methods`            | To match request methods, you can use commas to separate multiple method matches (optional)                                                       |
| `headers`            | To match request headers, match each child element (child element name is header name), you can use 'regex:' prefix to use regex match (optional) |
| `handler`            | The request handler                                                                                                                               |
| `empty_query_string` | Check that there is no query string in the URL                                                                                                    |

`handler` contains the following settings, which can be configured by sub-tags:

| Sub-tags           | Definition                                                                                                                                                            |
|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `url`              | A location for redirect                                                                                                                                               |
| `type`             | Supported types: static, dynamic_query_handler, predefined_query_handler, redirect                                                                                    |
| `status`           | Use with static type, response status code                                                                                                                            |
| `query_param_name` | Use with dynamic_query_handler type, extracts and executes the value corresponding to the `<query_param_name>` value in HTTP request params                             |
| `query`            | Use with predefined_query_handler type, executes query when the handler is called                                                                                     |
| `content_type`     | Use with static type, response content-type                                                                                                                           |
| `response_content` | Use with static type, Response content sent to client, when using the prefix 'file://' or 'config://', find the content from the file or configuration send to client |

Along with a list of rules, you can specify `<defaults/>` which specifies to enable all the default handlers.

Example:

```xml
<http_handlers>
    <rule>
        <url>/</url>
        <methods>POST,GET</methods>
        <headers><pragma>no-cache</pragma></headers>
        <handler>
            <type>dynamic_query_handler</type>
            <query_param_name>query</query_param_name>
        </handler>
    </rule>

    <rule>
        <url>/predefined_query</url>
        <methods>POST,GET</methods>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT * FROM system.settings</query>
        </handler>
    </rule>

    <rule>
        <handler>
            <type>static</type>
            <status>200</status>
            <content_type>text/plain; charset=UTF-8</content_type>
            <response_content>config://http_server_default_response</response_content>
        </handler>
    </rule>
</http_handlers>
```

## http_options_response {#http_options_response} 

Used to add headers to the response in an `OPTIONS` HTTP request.
The `OPTIONS` method is used when making CORS preflight requests.

For more information, see [OPTIONS](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/OPTIONS).

Example:

```xml
<http_options_response>
     <header>
            <name>Access-Control-Allow-Origin</name>
            <value>*</value>
     </header>
     <header>
          <name>Access-Control-Allow-Headers</name>
          <value>origin, x-requested-with, x-clickhouse-format, x-clickhouse-user, x-clickhouse-key, Authorization</value>
     </header>
     <header>
          <name>Access-Control-Allow-Methods</name>
          <value>POST, GET, OPTIONS</value>
     </header>
     <header>
          <name>Access-Control-Max-Age</name>
          <value>86400</value>
     </header>
</http_options_response>
```

## http_server_default_response {#http_server_default_response} 

The page that is shown by default when you access the ClickHouse HTTP(s) server.
The default value is "Ok." (with a line feed at the end)

**Example**

Opens `https://tabix.io/` when accessing `http://localhost: http_port`.

```xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

## iceberg_catalog_threadpool_pool_size {#iceberg_catalog_threadpool_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="50" />Size of background pool for iceberg catalog

## iceberg_catalog_threadpool_queue_size {#iceberg_catalog_threadpool_queue_size} 

<SettingsInfoBlock type="UInt64" default_value="1000000" />Number of tasks which is possible to push into iceberg catalog pool

## iceberg_metadata_files_cache_max_entries {#iceberg_metadata_files_cache_max_entries} 

<SettingsInfoBlock type="UInt64" default_value="1000" />Maximum size of iceberg metadata files cache in entries. Zero means disabled.

## iceberg_metadata_files_cache_policy {#iceberg_metadata_files_cache_policy} 

<SettingsInfoBlock type="String" default_value="SLRU" />Iceberg metadata cache policy name.

## iceberg_metadata_files_cache_size {#iceberg_metadata_files_cache_size} 

<SettingsInfoBlock type="UInt64" default_value="1073741824" />Maximum size of iceberg metadata cache in bytes. Zero means disabled.

## iceberg_metadata_files_cache_size_ratio {#iceberg_metadata_files_cache_size_ratio} 

<SettingsInfoBlock type="Double" default_value="0.5" />The size of the protected queue (in case of SLRU policy) in the iceberg metadata cache relative to the cache's total size.

## ignore_empty_sql_security_in_create_view_query {#ignore_empty_sql_security_in_create_view_query} 

<SettingsInfoBlock type="Bool" default_value="1" />
If true, ClickHouse doesn't write defaults for empty SQL security statement in `CREATE VIEW` queries.

:::note
This setting is only necessary for the migration period and will become obsolete in 24.4
:::


## include_from {#include_from} 

<SettingsInfoBlock type="String" default_value="/etc/metrika.xml" />
The path to the file with substitutions. Both XML and YAML formats are supported.

For more information, see the section [Configuration files](/operations/configuration-files).

**Example**

```xml
<include_from>/etc/metrica.xml</include_from>
```


## index_mark_cache_policy {#index_mark_cache_policy} 

<SettingsInfoBlock type="String" default_value="SLRU" />Secondary index mark cache policy name.

## index_mark_cache_size {#index_mark_cache_size} 

<SettingsInfoBlock type="UInt64" default_value="5368709120" />
Maximum size of cache for index marks.

:::note

A value of `0` means disabled.

This setting can be modified at runtime and will take effect immediately.
:::


## index_mark_cache_size_ratio {#index_mark_cache_size_ratio} 

<SettingsInfoBlock type="Double" default_value="0.3" />The size of the protected queue (in case of SLRU policy) in the secondary index mark cache relative to the cache's total size.

## index_uncompressed_cache_policy {#index_uncompressed_cache_policy} 

<SettingsInfoBlock type="String" default_value="SLRU" />Secondary index uncompressed cache policy name.

## index_uncompressed_cache_size {#index_uncompressed_cache_size} 

<SettingsInfoBlock type="UInt64" default_value="0" />
Maximum size of cache for uncompressed blocks of `MergeTree` indices.

:::note
A value of `0` means disabled.

This setting can be modified at runtime and will take effect immediately.
:::


## index_uncompressed_cache_size_ratio {#index_uncompressed_cache_size_ratio} 

<SettingsInfoBlock type="Double" default_value="0.5" />The size of the protected queue (in case of SLRU policy) in the secondary index uncompressed cache relative to the cache's total size.

## interserver_http_credentials {#interserver_http_credentials} 

A username and a password used to connect to other servers during [replication](../../engines/table-engines/mergetree-family/replication.md). Additionally, the server authenticates other replicas using these credentials.
`interserver_http_credentials` must therefore be the same for all replicas in a cluster.

:::note
- By default, if `interserver_http_credentials` section is omitted, authentication is not used during replication.
- `interserver_http_credentials` settings do not relate to a ClickHouse client credentials [configuration](../../interfaces/cli.md#configuration_files).
- These credentials are common for replication via `HTTP` and `HTTPS`.
:::

The following settings can be configured by sub-tags:

- `user` — Username.
- `password` — Password.
- `allow_empty` — If `true`, then other replicas are allowed to connect without authentication even if credentials are set. If `false`, then connections without authentication are refused. Default: `false`.
- `old` — Contains old `user` and `password` used during credential rotation. Several `old` sections can be specified.

**Credentials Rotation**

ClickHouse supports dynamic interserver credentials rotation without stopping all replicas at the same time to update their configuration. Credentials can be changed in several steps.

To enable authentication, set `interserver_http_credentials.allow_empty` to `true` and add credentials. This allows connections with authentication and without it.

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>111</password>
    <allow_empty>true</allow_empty>
</interserver_http_credentials>
```

After configuring all replicas set `allow_empty` to `false` or remove this setting. It makes authentication with new credentials mandatory.

To change existing credentials, move the username and the password to `interserver_http_credentials.old` section and update `user` and `password` with new values. At this point the server uses new credentials to connect to other replicas and accepts connections with either new or old credentials.

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
    <old>
        <user>admin</user>
        <password>111</password>
    </old>
    <old>
        <user>temp</user>
        <password>000</password>
    </old>
</interserver_http_credentials>
```

When new credentials are applied to all replicas, old credentials may be removed.

## interserver_http_host {#interserver_http_host} 


The hostname that can be used by other servers to access this server.

If omitted, it is defined in the same way as the `<hostname -f>` command.

Useful for breaking away from a specific network interface.

**Example**

```xml
<interserver_http_host>example.clickhouse.com</interserver_http_host>
```


## interserver_http_port {#interserver_http_port} 

<SettingsInfoBlock type="UInt64" default_value="0" />
Port for exchanging data between ClickHouse servers.

**Example**

```xml
<interserver_http_port>9009</interserver_http_port>
```


## interserver_https_host {#interserver_https_host} 


Similar to `<interserver_http_host>`, except that this hostname can be used by other servers to access this server over `<HTTPS>`.

**Example**

```xml
<interserver_https_host>example.clickhouse.com</interserver_https_host>
```


## interserver_https_port {#interserver_https_port} 

<SettingsInfoBlock type="UInt64" default_value="0" />
Port for exchanging data between ClickHouse servers over `<HTTPS>`.

**Example**

```xml
<interserver_https_port>9010</interserver_https_port>
```


## interserver_listen_host {#interserver_listen_host} 

Restriction on hosts that can exchange data between ClickHouse servers.
If Keeper is used, the same restriction will be applied to the communication between different Keeper instances.

:::note
By default, the value is equal to the [`listen_host`](#listen_host) setting.
:::

**Example**

```xml
<interserver_listen_host>::ffff:a00:1</interserver_listen_host>
<interserver_listen_host>10.0.0.1</interserver_listen_host>
```

Type:

Default:

## io_thread_pool_queue_size {#io_thread_pool_queue_size} 

<SettingsInfoBlock type="UInt64" default_value="10000" />
The maximum number of jobs that can be scheduled on the IO Thread pool.

:::note
A value of `0` means unlimited.
:::


## jemalloc_collect_global_profile_samples_in_trace_log {#jemalloc_collect_global_profile_samples_in_trace_log} 

<SettingsInfoBlock type="Bool" default_value="0" />Store jemalloc's sampled allocations in system.trace_log

## jemalloc_enable_background_threads {#jemalloc_enable_background_threads} 

<SettingsInfoBlock type="Bool" default_value="1" />Enable jemalloc background threads. Jemalloc uses background threads to cleanup unused memory pages. Disabling it could lead to performance degradation.

## jemalloc_enable_global_profiler {#jemalloc_enable_global_profiler} 

<SettingsInfoBlock type="Bool" default_value="0" />Enable jemalloc's allocation profiler for all threads. Jemalloc will sample allocations and all deallocations for sampled allocations.
Profiles can be flushed using SYSTEM JEMALLOC FLUSH PROFILE which can be used for allocation analysis.
Samples can also be stored in system.trace_log using config jemalloc_collect_global_profile_samples_in_trace_log or with query setting jemalloc_collect_profile_samples_in_trace_log.
See [Allocation Profiling](/operations/allocation-profiling)

## jemalloc_flush_profile_interval_bytes {#jemalloc_flush_profile_interval_bytes} 

<SettingsInfoBlock type="UInt64" default_value="0" />Flushing jemalloc profile will be done after global peak memory usage increased by jemalloc_flush_profile_interval_bytes

## jemalloc_flush_profile_on_memory_exceeded {#jemalloc_flush_profile_on_memory_exceeded} 

<SettingsInfoBlock type="Bool" default_value="0" />Flushing jemalloc profile will be done on total memory exceeded errors

## jemalloc_max_background_threads_num {#jemalloc_max_background_threads_num} 

<SettingsInfoBlock type="UInt64" default_value="0" />Maximum amount of jemalloc background threads to create, set to 0 to use jemalloc's default value

## keep_alive_timeout {#keep_alive_timeout} 

<SettingsInfoBlock type="Seconds" default_value="30" />
The number of seconds that ClickHouse waits for incoming requests for HTTP protocol before closing the connection.

**Example**

```xml
<keep_alive_timeout>10</keep_alive_timeout>
```


## keeper_hosts {#keeper_hosts} 

Dynamic setting. Contains a set of [Zoo]Keeper hosts ClickHouse can potentially connect to. Doesn't expose information from `<auxiliary_zookeepers>`

## keeper_multiread_batch_size {#keeper_multiread_batch_size} 

<SettingsInfoBlock type="UInt64" default_value="10000" />
Maximum size of batch for MultiRead request to [Zoo]Keeper that support batching. If set to 0, batching is disabled. Available only in ClickHouse Cloud.


## keeper_server.socket_receive_timeout_sec {#keeper_server.socket_receive_timeout_sec} 

<SettingsInfoBlock type="UInt64" default_value="300" />Keeper socket receive timeout.

## keeper_server.socket_send_timeout_sec {#keeper_server.socket_send_timeout_sec} 

<SettingsInfoBlock type="UInt64" default_value="300" />Keeper socket send timeout.

## ldap_servers {#ldap_servers} 

List LDAP servers with their connection parameters here to:
- use them as authenticators for dedicated local users, who have an 'ldap' authentication mechanism specified instead of 'password'
- use them as remote user directories.

The following settings can be configured by sub-tags:

| Setting                        | Description                                                                                                                                                                                                                                                                                                                                                                                                                              |
|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`bind_dn`](/operations/server-configuration-parameters/settings#ldap_servers.bind_dn) | Template used to construct the DN to bind to. The resulting DN will be constructed by replacing all `\{user_name\}` substrings of the template with the actual user name during each authentication attempt.                                                                                                                                                                                                                               |
| [`enable_tls`](/operations/server-configuration-parameters/settings#ldap_servers.enable_tls) | Flag to trigger use of secure connection to the LDAP server. Specify `no` for plain text (`ldap://`) protocol (not recommended). Specify `yes` for LDAP over SSL/TLS (`ldaps://`) protocol (recommended, the default). Specify `starttls` for legacy StartTLS protocol (plain text (`ldap://`) protocol, upgraded to TLS).                                                                                                               |
| [`host`](/operations/server-configuration-parameters/settings#ldap_servers.host) | LDAP server hostname or IP, this parameter is mandatory and cannot be empty.                                                                                                                                                                                                                                                                                                                                                             |
| [`port`](/operations/server-configuration-parameters/settings#ldap_servers.port) | LDAP server port, default is 636 if `enable_tls` is set to true, `389` otherwise.                                                                                                                                                                                                                                                                                                                                                        |
| [`tls_ca_cert_dir`](/operations/server-configuration-parameters/settings#ldap_servers.tls_ca_cert_dir) | path to the directory containing CA certificates.                                                                                                                                                                                                                                                                                                                                                                                        |
| [`tls_ca_cert_file`](/operations/server-configuration-parameters/settings#ldap_servers.tls_ca_cert_file) | path to CA certificate file.                                                                                                                                                                                                                                                                                                                                                                                                             |
| [`tls_cert_file`](/operations/server-configuration-parameters/settings#ldap_servers.tls_cert_file) | path to certificate file.                                                                                                                                                                                                                                                                                                                                                                                                                |
| [`tls_cipher_suite`](/operations/server-configuration-parameters/settings#ldap_servers.tls_cipher_suite) | allowed cipher suite (in OpenSSL notation).                                                                                                                                                                                                                                                                                                                                                                                              |
| [`tls_key_file`](/operations/server-configuration-parameters/settings#ldap_servers.tls_key_file) | path to certificate key file.                                                                                                                                                                                                                                                                                                                                                                                                            |
| [`tls_minimum_protocol_version`](/operations/server-configuration-parameters/settings#ldap_servers.tls_minimum_protocol_version) | The minimum protocol version of SSL/TLS. Accepted values are: `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2` (the default).                                                                                                                                                                                                                                                                                                                |
| [`tls_require_cert`](/operations/server-configuration-parameters/settings#ldap_servers.tls_require_cert) | SSL/TLS peer certificate verification behavior. Accepted values are: `never`, `allow`, `try`, `demand` (the default).                                                                                                                                                                                                                                                                                                                    |
| [`user_dn_detection`](/operations/server-configuration-parameters/settings#ldap_servers.user_dn_detection) | Section with LDAP search parameters for detecting the actual user DN of the bound user. This is mainly used in search filters for further role mapping when the server is Active Directory. The resulting user DN will be used when replacing `\{user_dn\}` substrings wherever they are allowed. By default, user DN is set equal to bind DN, but once search is performed, it will be updated with to the actual detected user DN value. |
| [`verification_cooldown`](/operations/server-configuration-parameters/settings#ldap_servers.verification_cooldown) | A period of time, in seconds, after a successful bind attempt, during which a user will be assumed to be successfully authenticated for all consecutive requests without contacting the LDAP server. Specify `0` (the default) to disable caching and force contacting the LDAP server for each authentication request.                                                                                                                  |

Setting `user_dn_detection` can be configured with sub-tags:

| Setting         | Description                                                                                                                                                                                                                                                                                                                                    |
|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `base_dn`       | template used to construct the base DN for the LDAP search. The resulting DN will be constructed by replacing all `\{user_name\}` and '\{bind_dn\}' substrings of the template with the actual user name and bind DN during the LDAP search.                                                                                                       |
| `scope`         | scope of the LDAP search. Accepted values are: `base`, `one_level`, `children`, `subtree` (the default).                                                                                                                                                                                                                                       |
| `search_filter` | template used to construct the search filter for the LDAP search. The resulting filter will be constructed by replacing all `\{user_name\}`, `\{bind_dn\}`, and `\{base_dn\}` substrings of the template with the actual user name, bind DN, and base DN during the LDAP search. Note, that the special characters must be escaped properly in XML.  |

Example:

```xml
<my_ldap_server>
    <host>localhost</host>
    <port>636</port>
    <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
    <verification_cooldown>300</verification_cooldown>
    <enable_tls>yes</enable_tls>
    <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
    <tls_require_cert>demand</tls_require_cert>
    <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
    <tls_key_file>/path/to/tls_key_file</tls_key_file>
    <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
    <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
    <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
</my_ldap_server>
```

Example (typical Active Directory with configured user DN detection for further role mapping):

```xml
<my_ad_server>
    <host>localhost</host>
    <port>389</port>
    <bind_dn>EXAMPLE\{user_name}</bind_dn>
    <user_dn_detection>
        <base_dn>CN=Users,DC=example,DC=com</base_dn>
        <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>
    </user_dn_detection>
    <enable_tls>no</enable_tls>
</my_ad_server>
```

## license_file {#license_file} 

License file contents for ClickHouse Enterprise Edition

## license_public_key_for_testing {#license_public_key_for_testing} 

Licensing demo key, for CI use only

## listen_backlog {#listen_backlog} 

<SettingsInfoBlock type="UInt32" default_value="4096" />
Backlog (queue size of pending connections) of the listen socket. The default value of `<4096>` is the same as that of linux 5.4+).

Usually this value does not need to be changed, since:
- The default value is large enough,
- For accepting client's connections server has separate thread.

So even if you have `<TcpExtListenOverflows>` (from `<nstat>`) non-zero and this counter grows for ClickHouse server it does not mean that this value needs to be increased, since:
- Usually if `<4096>` is not enough it shows some internal ClickHouse scaling issue, so it is better to report an issue.
- It does not mean that the server can handle more connections later (and even if it could, by that moment clients may be gone or disconnected).

**Example**

```xml
<listen_backlog>4096</listen_backlog>


## listen_host {#listen_host} 

Restriction on hosts that requests can come from. If you want the server to answer all of them, specify `::`.

Examples:

```xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

## listen_reuse_port {#listen_reuse_port} 

<SettingsInfoBlock type="Bool" default_value="0" />
Allow multiple servers to listen on the same address:port. Requests will be routed to a random server by the operating system. Enabling this setting is not recommended.

**Example**

```xml
<listen_reuse_port>0</listen_reuse_port>


## listen_try {#listen_try} 

<SettingsInfoBlock type="Bool" default_value="0" />
The server will not exit if IPv6 or IPv4 networks are unavailable while trying to listen.

**Example**

```xml
<listen_try>0</listen_try>


## load_marks_threadpool_pool_size {#load_marks_threadpool_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="50" />Size of background pool for marks loading

## load_marks_threadpool_queue_size {#load_marks_threadpool_queue_size} 

<SettingsInfoBlock type="UInt64" default_value="1000000" />Number of tasks which is possible to push into prefetches pool

## logger {#logger} 

The location and format of log messages.

**Keys**:

| Key                    | Description                                                                                                                                                        |
|------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`async`](/operations/server-configuration-parameters/settings#logger.async) | When `true` (default) logging will happen asynchronously (one background thread per output channel). Otherwise it will log inside the thread calling LOG           |
| [`async_queue_max_size`](/operations/server-configuration-parameters/settings#logger.async_queue_max_size) | When using async logging, the max amount of messages that will be kept in the the queue waiting for flushing. Extra messages will be dropped                       |
| [`console`](/operations/server-configuration-parameters/settings#logger.console) | Enable logging to the console. Set to `1` or `true` to enable. Default is `1` if ClickHouse does not run in daemon mode, `0` otherwise.                            |
| [`console_log_level`](/operations/server-configuration-parameters/settings#logger.console_log_level) | Log level for console output. Defaults to `level`.                                                                                                                 |
| [`count`](/operations/server-configuration-parameters/settings#logger.count) | Rotation policy: How many historical log files ClickHouse are kept at most.                                                                                        |
| [`errorlog`](/operations/server-configuration-parameters/settings#logger.errorlog) | The path to the error log file.                                                                                                                                    |
| [`formatting.type`](/operations/server-configuration-parameters/settings#logger.formatting.type) | Log format for console output. Currently, only `json` is supported                                                                                                 |
| [`level`](/operations/server-configuration-parameters/settings#logger.level) | Log level. Acceptable values: `none` (turn logging off), `fatal`, `critical`, `error`, `warning`, `notice`, `information`,`debug`, `trace`, `test`                 |
| [`log`](/operations/server-configuration-parameters/settings#logger.log) | The path to the log file.                                                                                                                                          |
| [`rotation`](/operations/server-configuration-parameters/settings#logger.rotation) | Rotation policy: Controls when log files are rotated. Rotation can be based on size, time, or a combination of both. Examples: 100M, daily, 100M,daily. Once the log file exceeds the specified size or when the specified time interval is reached, it is renamed and archived, and a new log file is created. |
| [`shutdown_level`](/operations/server-configuration-parameters/settings#logger.shutdown_level) | Shutdown level is used to set the root logger level at server Shutdown.                                                                                            |
| [`size`](/operations/server-configuration-parameters/settings#logger.size) | Rotation policy: Maximum size of the log files in bytes. Once the log file size exceeds this threshold, it is renamed and archived, and a new log file is created. |
| [`startup_level`](/operations/server-configuration-parameters/settings#logger.startup_level) | Startup level is used to set the root logger level at server startup. After startup log level is reverted to the `level` setting                                   |
| [`stream_compress`](/operations/server-configuration-parameters/settings#logger.stream_compress) | Compress log messages using LZ4. Set to `1` or `true` to enable.                                                                                                   |
| [`syslog_level`](/operations/server-configuration-parameters/settings#logger.syslog_level) | Log level for logging to syslog.                                                                                                                                   |
| [`use_syslog`](/operations/server-configuration-parameters/settings#logger.use_syslog) | Also forward log output to syslog.                                                                                                                                 |

**Log format specifiers**

File names in `log` and `errorLog` paths support below format specifiers for the resulting file name (the directory part does not support them).

Column "Example" shows the output at `2023-07-06 18:32:07`.

| Specifier    | Description                                                                                                         | Example                  |
|--------------|---------------------------------------------------------------------------------------------------------------------|--------------------------|
| `%%`         | Literal %                                                                                                           | `%`                        |
| `%n`         | New-line character                                                                                                  |                          |
| `%t`         | Horizontal tab character                                                                                            |                          |
| `%Y`         | Year as a decimal number, e.g. 2017                                                                                 | `2023`                     |
| `%y`         | Last 2 digits of year as a decimal number (range [00,99])                                                           | `23`                       |
| `%C`         | First 2 digits of year as a decimal number (range [00,99])                                                          | `20`                       |
| `%G`         | Four-digit [ISO 8601 week-based year](https://en.wikipedia.org/wiki/ISO_8601#Week_dates), i.e. the year that contains the specified week. Normally useful only with `%V`  | `2023`       |
| `%g`         | Last 2 digits of [ISO 8601 week-based year](https://en.wikipedia.org/wiki/ISO_8601#Week_dates), i.e. the year that contains the specified week.                         | `23`         |
| `%b`         | Abbreviated month name, e.g. Oct (locale dependent)                                                                 | `Jul`                      |
| `%h`         | Synonym of %b                                                                                                       | `Jul`                      |
| `%B`         | Full month name, e.g. October (locale dependent)                                                                    | `July`                     |
| `%m`         | Month as a decimal number (range [01,12])                                                                           | `07`                       |
| `%U`         | Week of the year as a decimal number (Sunday is the first day of the week) (range [00,53])                          | `27`                       |
| `%W`         | Week of the year as a decimal number (Monday is the first day of the week) (range [00,53])                          | `27`                       |
| `%V`         | ISO 8601 week number (range [01,53])                                                                                | `27`                       |
| `%j`         | Day of the year as a decimal number (range [001,366])                                                               | `187`                      |
| `%d`         | Day of the month as a zero-padded decimal number (range [01,31]). Single digit is preceded by zero.                 | `06`                       |
| `%e`         | Day of the month as a space-padded decimal number (range [1,31]). Single digit is preceded by a space.              | `&nbsp; 6`                 |
| `%a`         | Abbreviated weekday name, e.g. Fri (locale dependent)                                                               | `Thu`                      |
| `%A`         | Full weekday name, e.g. Friday (locale dependent)                                                                   | `Thursday`                 |
| `%w`         | Weekday as a integer number with Sunday as 0 (range [0-6])                                                          | `4`                        |
| `%u`         | Weekday as a decimal number, where Monday is 1 (ISO 8601 format) (range [1-7])                                      | `4`                        |
| `%H`         | Hour as a decimal number, 24 hour clock (range [00-23])                                                             | `18`                       |
| `%I`         | Hour as a decimal number, 12 hour clock (range [01,12])                                                             | `06`                       |
| `%M`         | Minute as a decimal number (range [00,59])                                                                          | `32`                       |
| `%S`         | Second as a decimal number (range [00,60])                                                                          | `07`                       |
| `%c`         | Standard date and time string, e.g. Sun Oct 17 04:41:13 2010 (locale dependent)                                     | `Thu Jul  6 18:32:07 2023` |
| `%x`         | Localized date representation (locale dependent)                                                                    | `07/06/23`                 |
| `%X`         | Localized time representation, e.g. 18:40:20 or 6:40:20 PM (locale dependent)                                       | `18:32:07`                 |
| `%D`         | Short MM/DD/YY date, equivalent to %m/%d/%y                                                                         | `07/06/23`                 |
| `%F`         | Short YYYY-MM-DD date, equivalent to %Y-%m-%d                                                                       | `2023-07-06`               |
| `%r`         | Localized 12-hour clock time (locale dependent)                                                                     | `06:32:07 PM`              |
| `%R`         | Equivalent to "%H:%M"                                                                                               | `18:32`                    |
| `%T`         | Equivalent to "%H:%M:%S" (the ISO 8601 time format)                                                                 | `18:32:07`                 |
| `%p`         | Localized a.m. or p.m. designation (locale dependent)                                                               | `PM`                       |
| `%z`         | Offset from UTC in the ISO 8601 format (e.g. -0430), or no characters if the time zone information is not available | `+0800`                    |
| `%Z`         | Locale-dependent time zone name or abbreviation, or no characters if the time zone information is not available     | `Z AWST `                  |

**Example**

```xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server-%F-%T.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server-%F-%T.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
    <stream_compress>true</stream_compress>
</logger>
```

To print log messages only in the console:

```xml
<logger>
    <level>information</level>
    <console>true</console>
</logger>
```

**Per-level Overrides**

The log level of individual log names can be overridden. For example, to mute all messages of loggers "Backup" and "RBAC".

```xml
<logger>
    <levels>
        <logger>
            <name>Backup</name>
            <level>none</level>
        </logger>
        <logger>
            <name>RBAC</name>
            <level>none</level>
        </logger>
    </levels>
</logger>
```

**syslog**

To write log messages additionally to syslog:

```xml
<logger>
    <use_syslog>1</use_syslog>
    <syslog>
        <address>syslog.remote:10514</address>
        <hostname>myhost.local</hostname>
        <facility>LOG_LOCAL6</facility>
        <format>syslog</format>
    </syslog>
</logger>
```

Keys for `<syslog>`:

| Key        | Description                                                                                                                                                                                                                                                    |
|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `address`  | The address of syslog in format `host\[:port\]`. If omitted, the local daemon is used.                                                                                                                                                                         |
| `hostname` | The name of the host from which logs are send (optional).                                                                                                                                                                                                      |
| `facility` | The syslog [facility keyword](https://en.wikipedia.org/wiki/Syslog#Facility). Must be specified uppercase with a "LOG_" prefix, e.g. `LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3`, etc. Default: `LOG_USER` if `address` is specified, `LOG_DAEMON` otherwise.                                           |
| `format`   | Log message format. Possible values: `bsd` and `syslog.`                                                                                                                                                                                                       |

**Log formats**

You can specify the log format that will be outputted in the console log. Currently, only JSON is supported.

**Example**

Here is an example of an output JSON log:

```json
{
  "date_time_utc": "2024-11-06T09:06:09Z",
  "date_time": "1650918987.180175",
  "thread_name": "#1",
  "thread_id": "254545",
  "level": "Trace",
  "query_id": "",
  "logger_name": "BaseDaemon",
  "message": "Received signal 2",
  "source_file": "../base/daemon/BaseDaemon.cpp; virtual void SignalListener::run()",
  "source_line": "192"
}
```

To enable JSON logging support, use the following snippet:

```xml
<logger>
    <formatting>
        <type>json</type>
        <!-- Can be configured on a per-channel basis (log, errorlog, console, syslog), or globally for all channels (then just omit it). -->
        <!-- <channel></channel> -->
        <names>
            <date_time>date_time</date_time>
            <thread_name>thread_name</thread_name>
            <thread_id>thread_id</thread_id>
            <level>level</level>
            <query_id>query_id</query_id>
            <logger_name>logger_name</logger_name>
            <message>message</message>
            <source_file>source_file</source_file>
            <source_line>source_line</source_line>
        </names>
    </formatting>
</logger>
```

**Renaming keys for JSON logs**

Key names can be modified by changing tag values inside the `<names>` tag. For example, to change `DATE_TIME` to `MY_DATE_TIME`, you can use `<date_time>MY_DATE_TIME</date_time>`.

**Omitting keys for JSON logs**

Log properties can be omitted by commenting out the property. For example, if you do not want your log to print `query_id`, you can comment out the `<query_id>` tag.

## logger.async {#logger.async} 

<SettingsInfoBlock type="Bool" default_value="1" />When `<true>` (default) logging will happen asynchronously (one background thread per output channel). Otherwise it will log inside the thread calling LOG.

## logger.async_queye_max_size {#logger.async_queye_max_size} 

<SettingsInfoBlock type="UInt64" default_value="65536" />When using async logging, the max amount of messages that will be kept in the the queue waiting for flushing. Extra messages will be dropped.

## logger.console {#logger.console} 

<SettingsInfoBlock type="Bool" default_value="0" />Enable logging to the console. Set to `<1>` or `<true>` to enable. Default is `<1>` if ClickHouse does not run in daemon mode, `<0>` otherwise.

## logger.console_log_level {#logger.console_log_level} 

<SettingsInfoBlock type="String" default_value="trace" />Log level for console output. Defaults to `<level>`.

## logger.count {#logger.count} 

<SettingsInfoBlock type="UInt64" default_value="1" />Rotation policy: How many historical log files ClickHouse are kept at most.

## logger.errorlog {#logger.errorlog} 

The path to the error log file.

## logger.formatting.type {#logger.formatting.type} 

<SettingsInfoBlock type="String" default_value="json" />Log format for console output. Currently, only `<json>` is supported.

## logger.level {#logger.level} 

<SettingsInfoBlock type="String" default_value="trace" />Log level. Acceptable values: `<none>` (turn logging off), `<fatal>`, `<critical>`, `<error>`, `<warning>`, `<notice>`, `<information>`, `<debug>`, `<trace>`, `<test>`.

## logger.log {#logger.log} 

The path to the log file.

## logger.rotation {#logger.rotation} 

<SettingsInfoBlock type="String" default_value="100M" />Rotation policy: Controls when log files are rotated. Rotation can be based on size, time, or a combination of both. Examples: 100M, daily, 100M,daily. Once the log file exceeds the specified size or when the specified time interval is reached, it is renamed and archived, and a new log file is created.

## logger.shutdown_level {#logger.shutdown_level} 

Shutdown level is used to set the root logger level at server Shutdown.

## logger.size {#logger.size} 

<SettingsInfoBlock type="String" default_value="100M" />Rotation policy: Maximum size of the log files in bytes. Once the log file size exceeds this threshold, it is renamed and archived, and a new log file is created.

## logger.startup_level {#logger.startup_level} 

Startup level is used to set the root logger level at server startup. After startup log level is reverted to the `<level>` setting.

## logger.stream_compress {#logger.stream_compress} 

<SettingsInfoBlock type="Bool" default_value="0" />Compress log messages using LZ4. Set to `<1>` or `<true>` to enable.

## logger.syslog_level {#logger.syslog_level} 

<SettingsInfoBlock type="String" default_value="trace" />Log level for logging to syslog.

## logger.use_syslog {#logger.use_syslog} 

<SettingsInfoBlock type="Bool" default_value="0" />Also forward log output to syslog.

## macros {#macros} 

Parameter substitutions for replicated tables.

Can be omitted if replicated tables are not used.

For more information, see the section [Creating replicated tables](../../engines/table-engines/mergetree-family/replication.md#creating-replicated-tables).

**Example**

```xml
<macros incl="macros" optional="true" />
```

## mark_cache_policy {#mark_cache_policy} 

<SettingsInfoBlock type="String" default_value="SLRU" />Mark cache policy name.

## mark_cache_prewarm_ratio {#mark_cache_prewarm_ratio} 

<SettingsInfoBlock type="Double" default_value="0.95" />The ratio of total size of mark cache to fill during prewarm.

## mark_cache_size {#mark_cache_size} 

<SettingsInfoBlock type="UInt64" default_value="5368709120" />
Maximum size of cache for marks (index of [`MergeTree`](/engines/table-engines/mergetree-family) family of tables).

:::note
This setting can be modified at runtime and will take effect immediately.
:::


## mark_cache_size_ratio {#mark_cache_size_ratio} 

<SettingsInfoBlock type="Double" default_value="0.5" />The size of the protected queue (in case of SLRU policy) in the mark cache relative to the cache's total size.

## max_active_parts_loading_thread_pool_size {#max_active_parts_loading_thread_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="64" />The number of threads to load active set of data parts (Active ones) at startup.

## max_authentication_methods_per_user {#max_authentication_methods_per_user} 

<SettingsInfoBlock type="UInt64" default_value="100" />
The maximum number of authentication methods a user can be created with or altered to.
Changing this setting does not affect existing users. Create/alter authentication-related queries will fail if they exceed the limit specified in this setting.
Non authentication create/alter queries will succeed.

:::note
A value of `0` means unlimited.
:::


## max_backup_bandwidth_for_server {#max_backup_bandwidth_for_server} 

<SettingsInfoBlock type="UInt64" default_value="0" />The maximum read speed in bytes per second for all backups on server. Zero means unlimited.

## max_backups_io_thread_pool_free_size {#max_backups_io_thread_pool_free_size} 

<SettingsInfoBlock type="UInt64" default_value="0" />If the number of **idle** threads in the Backups IO Thread pool exceeds `max_backup_io_thread_pool_free_size`, ClickHouse will release resources occupied by idling threads and decrease the pool size. Threads can be created again if necessary.

## max_backups_io_thread_pool_size {#max_backups_io_thread_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="1000" />ClickHouse uses threads from the Backups IO Thread pool to do S3 backup IO operations. `max_backups_io_thread_pool_size` limits the maximum number of threads in the pool.

## max_build_vector_similarity_index_thread_pool_size {#max_build_vector_similarity_index_thread_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="16" />
The maximum number of threads to use for building vector indexes.

:::note
A value of `0` means all cores.
:::


## max_concurrent_insert_queries {#max_concurrent_insert_queries} 

<SettingsInfoBlock type="UInt64" default_value="0" />
Limit on total number of concurrent insert queries.

:::note

A value of `0` (default) means unlimited.

This setting can be modified at runtime and will take effect immediately. Queries that are already running will remain unchanged.
:::


## max_concurrent_queries {#max_concurrent_queries} 

<SettingsInfoBlock type="UInt64" default_value="0" />
Limit on total number of concurrently executed queries. Note that limits on `INSERT` and `SELECT` queries, and on the maximum number of queries for users must also be considered.

See also:
- [`max_concurrent_insert_queries`](/operations/server-configuration-parameters/settings#max_concurrent_insert_queries)
- [`max_concurrent_select_queries`](/operations/server-configuration-parameters/settings#max_concurrent_select_queries)
- [`max_concurrent_queries_for_all_users`](/operations/settings/settings#max_concurrent_queries_for_all_users)

:::note

A value of `0` (default) means unlimited.

This setting can be modified at runtime and will take effect immediately. Queries that are already running will remain unchanged.
:::


## max_concurrent_select_queries {#max_concurrent_select_queries} 

<SettingsInfoBlock type="UInt64" default_value="0" />
Limit on total number of concurrently select queries.

:::note

A value of `0` (default) means unlimited.

This setting can be modified at runtime and will take effect immediately. Queries that are already running will remain unchanged.
:::


## max_connections {#max_connections} 

<SettingsInfoBlock type="Int32" default_value="4096" />Max server connections.

## max_database_num_to_throw {#max_database_num_to_throw} 

<SettingsInfoBlock type="UInt64" default_value="0" />If number of databases is greater than this value, server will throw an exception. 0 means no limitation.

## max_database_num_to_warn {#max_database_num_to_warn} 

<SettingsInfoBlock type="UInt64" default_value="1000" />
If the number of attached databases exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.

**Example**

```xml
<max_database_num_to_warn>50</max_database_num_to_warn>
```


## max_database_replicated_create_table_thread_pool_size {#max_database_replicated_create_table_thread_pool_size} 

<SettingsInfoBlock type="UInt32" default_value="1" />The number of threads to create tables during replica recovery in DatabaseReplicated. Zero means number of threads equal number of cores.

## max_dictionary_num_to_throw {#max_dictionary_num_to_throw} 

<SettingsInfoBlock type="UInt64" default_value="0" />
If the number of dictionaries is greater than this value, the server will throw an exception.

Only counts tables for database engines:
- Atomic
- Ordinary
- Replicated
- Lazy

:::note
A value of `0` means no limitation.
:::

**Example**
```xml
<max_dictionary_num_to_throw>400</max_dictionary_num_to_throw>
```


## max_dictionary_num_to_warn {#max_dictionary_num_to_warn} 

<SettingsInfoBlock type="UInt64" default_value="1000" />
If the number of attached dictionaries exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.

**Example**

```xml
<max_dictionary_num_to_warn>400</max_dictionary_num_to_warn>
```


## max_distributed_cache_read_bandwidth_for_server {#max_distributed_cache_read_bandwidth_for_server} 

<SettingsInfoBlock type="UInt64" default_value="0" />The maximum total read speed from distributed cache on server in bytes per second. Zero means unlimited.

## max_distributed_cache_write_bandwidth_for_server {#max_distributed_cache_write_bandwidth_for_server} 

<SettingsInfoBlock type="UInt64" default_value="0" />The maximum total write speed to distributed cache on server in bytes per second. Zero means unlimited.

## max_entries_for_hash_table_stats {#max_entries_for_hash_table_stats} 

<SettingsInfoBlock type="UInt64" default_value="10000" />How many entries hash table statistics collected during aggregation is allowed to have

## max_fetch_partition_thread_pool_size {#max_fetch_partition_thread_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="64" />The number of threads for ALTER TABLE FETCH PARTITION.

## max_format_parsing_thread_pool_free_size {#max_format_parsing_thread_pool_free_size} 

<SettingsInfoBlock type="UInt64" default_value="0" />
Maximum number of idle standby threads to keep in the thread pool for parsing input.


## max_format_parsing_thread_pool_size {#max_format_parsing_thread_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="100" />
Maximum total number of threads to use for parsing input.


## max_io_thread_pool_free_size {#max_io_thread_pool_free_size} 

<SettingsInfoBlock type="UInt64" default_value="0" />
If the number of **idle** threads in the IO Thread pool exceeds `max_io_thread_pool_free_size`, ClickHouse will release resources occupied by idling threads and decrease the pool size. Threads can be created again if necessary.


## max_io_thread_pool_size {#max_io_thread_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="100" />
ClickHouse uses threads from the IO Thread pool to do some IO operations (e.g. to interact with S3). `max_io_thread_pool_size` limits the maximum number of threads in the pool.


## max_keep_alive_requests {#max_keep_alive_requests} 

<SettingsInfoBlock type="UInt64" default_value="10000" />
Maximal number of requests through a single keep-alive connection until it will be closed by ClickHouse server.

**Example**

```xml
<max_keep_alive_requests>10</max_keep_alive_requests>
```


## max_local_read_bandwidth_for_server {#max_local_read_bandwidth_for_server} 

<SettingsInfoBlock type="UInt64" default_value="0" />
The maximum speed of local reads in bytes per second.

:::note
A value of `0` means unlimited.
:::


## max_local_write_bandwidth_for_server {#max_local_write_bandwidth_for_server} 

<SettingsInfoBlock type="UInt64" default_value="0" />
The maximum speed of local writes in bytes per seconds.

:::note
A value of `0` means unlimited.
:::


## max_materialized_views_count_for_table {#max_materialized_views_count_for_table} 

<SettingsInfoBlock type="UInt64" default_value="0" />
A limit on the number of materialized views attached to a table.

:::note
Only directly dependent views are considered here, and the creation of one view on top of another view is not considered.
:::


## max_merges_bandwidth_for_server {#max_merges_bandwidth_for_server} 

<SettingsInfoBlock type="UInt64" default_value="0" />The maximum read speed of all merges on server in bytes per second. Zero means unlimited.

## max_mutations_bandwidth_for_server {#max_mutations_bandwidth_for_server} 

<SettingsInfoBlock type="UInt64" default_value="0" />The maximum read speed of all mutations on server in bytes per second. Zero means unlimited.

## max_named_collection_num_to_throw {#max_named_collection_num_to_throw} 

<SettingsInfoBlock type="UInt64" default_value="0" />
If number of named collections is greater than this value, server will throw an exception.

:::note
A value of `0` means no limitation.
:::

**Example**
```xml
<max_named_collection_num_to_throw>400</max_named_collection_num_to_throw>
```


## max_named_collection_num_to_warn {#max_named_collection_num_to_warn} 

<SettingsInfoBlock type="UInt64" default_value="1000" />
If the number of named collections exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.

**Example**

```xml
<max_named_collection_num_to_warn>400</max_named_collection_num_to_warn>
```


## max_open_files {#max_open_files} 

<SettingsInfoBlock type="UInt64" default_value="0" />
The maximum number of open files.

:::note
We recommend using this option in macOS since the getrlimit() function returns an incorrect value.
:::


## max_os_cpu_wait_time_ratio_to_drop_connection {#max_os_cpu_wait_time_ratio_to_drop_connection} 

<SettingsInfoBlock type="Float" default_value="0" />
Max ratio between OS CPU wait (OSCPUWaitMicroseconds metric) and busy (OSCPUVirtualTimeMicroseconds metric) times to consider dropping connections. Linear interpolation between min and max ratio is used to calculate the probability, the probability is 1 at this point.
See [Controlling behavior on server CPU overload](/operations/settings/server-overload) for more details.


## max_outdated_parts_loading_thread_pool_size {#max_outdated_parts_loading_thread_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="32" />The number of threads to load inactive set of data parts (Outdated ones) at startup.

## max_part_num_to_warn {#max_part_num_to_warn} 

<SettingsInfoBlock type="UInt64" default_value="100000" />
If the number of active parts exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.

**Example**

```xml
<max_part_num_to_warn>400</max_part_num_to_warn>
```


## max_partition_size_to_drop {#max_partition_size_to_drop} 

<SettingsInfoBlock type="UInt64" default_value="50000000000" />
Restriction on dropping partitions.

If the size of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table exceeds [`max_partition_size_to_drop`](#max_partition_size_to_drop) (in bytes), you can't drop a partition using a [DROP PARTITION](../../sql-reference/statements/alter/partition.md#drop-partitionpart) query.
This setting does not require a restart of the ClickHouse server to apply. Another way to disable the restriction is to create the `<clickhouse-path>/flags/force_drop_table` file.

:::note
The value `0` means that you can drop partitions without any restrictions.

This limitation does not restrict drop table and truncate table, see [max_table_size_to_drop](/operations/settings/settings#max_table_size_to_drop)
:::

**Example**

```xml
<max_partition_size_to_drop>0</max_partition_size_to_drop>
```


## max_parts_cleaning_thread_pool_size {#max_parts_cleaning_thread_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="128" />The number of threads for concurrent removal of inactive data parts.

## max_pending_mutations_execution_time_to_warn {#max_pending_mutations_execution_time_to_warn} 

<SettingsInfoBlock type="UInt64" default_value="86400" />
If any of the pending mutations exceeds the specified value in seconds, clickhouse server will add warning messages to `system.warnings` table.

**Example**

```xml
<max_pending_mutations_execution_time_to_warn>10000</max_pending_mutations_execution_time_to_warn>
```


## max_pending_mutations_to_warn {#max_pending_mutations_to_warn} 

<SettingsInfoBlock type="UInt64" default_value="500" />
If the number of pending mutations exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.

**Example**

```xml
<max_pending_mutations_to_warn>400</max_pending_mutations_to_warn>
```


## max_prefixes_deserialization_thread_pool_free_size {#max_prefixes_deserialization_thread_pool_free_size} 

<SettingsInfoBlock type="UInt64" default_value="0" />
If the number of **idle** threads in the prefixes deserialization Thread pool exceeds `max_prefixes_deserialization_thread_pool_free_size`, ClickHouse will release resources occupied by idling threads and decrease the pool size. Threads can be created again if necessary.


## max_prefixes_deserialization_thread_pool_size {#max_prefixes_deserialization_thread_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="100" />
ClickHouse uses threads from the prefixes deserialization Thread pool for parallel reading of metadata of columns and subcolumns from file prefixes in Wide parts in MergeTree. `max_prefixes_deserialization_thread_pool_size` limits the maximum number of threads in the pool.


## max_remote_read_network_bandwidth_for_server {#max_remote_read_network_bandwidth_for_server} 

<SettingsInfoBlock type="UInt64" default_value="0" />
The maximum speed of data exchange over the network in bytes per second for read.

:::note
A value of `0` (default) means unlimited.
:::


## max_remote_write_network_bandwidth_for_server {#max_remote_write_network_bandwidth_for_server} 

<SettingsInfoBlock type="UInt64" default_value="0" />
The maximum speed of data exchange over the network in bytes per second for write.

:::note
A value of `0` (default) means unlimited.
:::


## max_replicated_fetches_network_bandwidth_for_server {#max_replicated_fetches_network_bandwidth_for_server} 

<SettingsInfoBlock type="UInt64" default_value="0" />The maximum speed of data exchange over the network in bytes per second for replicated fetches. Zero means unlimited.

## max_replicated_sends_network_bandwidth_for_server {#max_replicated_sends_network_bandwidth_for_server} 

<SettingsInfoBlock type="UInt64" default_value="0" />The maximum speed of data exchange over the network in bytes per second for replicated sends. Zero means unlimited.

## max_replicated_table_num_to_throw {#max_replicated_table_num_to_throw} 

<SettingsInfoBlock type="UInt64" default_value="0" />
If the number of replicated tables is greater than this value, the server will throw an exception.

Only counts tables for database engines:
- Atomic
- Ordinary
- Replicated
- Lazy

:::note
A value of `0` means no limitation.
:::

**Example**
```xml
<max_replicated_table_num_to_throw>400</max_replicated_table_num_to_throw>
```


## max_server_memory_usage {#max_server_memory_usage} 

<SettingsInfoBlock type="UInt64" default_value="0" />
The maximum amount of memory the server is allowed to use, expressed in bytes.

:::note
The maximum memory consumption of the server is further restricted by setting `max_server_memory_usage_to_ram_ratio`.
:::

As a special case, a value of `0` (default) means the server may consume all available memory (excluding further restrictions imposed by `max_server_memory_usage_to_ram_ratio`).


## max_server_memory_usage_to_ram_ratio {#max_server_memory_usage_to_ram_ratio} 

<SettingsInfoBlock type="Double" default_value="0.9" />
The maximum amount of memory the server is allowed to use, expressed as a ratio to all available memory.

For example, a value of `0.9` (default) means that the server may consume 90% of the available memory.

Allows lowering the memory usage on low-memory systems.
On hosts with low RAM and swap, you may possibly need setting [`max_server_memory_usage_to_ram_ratio`](#max_server_memory_usage_to_ram_ratio) set larger than 1.

:::note
The maximum memory consumption of the server is further restricted by setting `max_server_memory_usage`.
:::


## max_session_timeout {#max_session_timeout} 

Maximum session timeout, in seconds.

Example:

```xml
<max_session_timeout>3600</max_session_timeout>
```

## max_table_num_to_throw {#max_table_num_to_throw} 

<SettingsInfoBlock type="UInt64" default_value="0" />
If number of tables is greater than this value, server will throw an exception.

The following tables are not counted:
- view
- remote
- dictionary
- system

Only counts tables for database engines:
- Atomic
- Ordinary
- Replicated
- Lazy

:::note
A value of `0` means no limitation.
:::

**Example**
```xml
<max_table_num_to_throw>400</max_table_num_to_throw>
```


## max_table_num_to_warn {#max_table_num_to_warn} 

<SettingsInfoBlock type="UInt64" default_value="5000" />
If the number of attached tables exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.

**Example**

```xml
<max_table_num_to_warn>400</max_table_num_to_warn>
```


## max_table_size_to_drop {#max_table_size_to_drop} 

<SettingsInfoBlock type="UInt64" default_value="50000000000" />
Restriction on deleting tables.

If the size of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table exceeds `max_table_size_to_drop` (in bytes), you can't delete it using a [`DROP`](../../sql-reference/statements/drop.md) query or [`TRUNCATE`](../../sql-reference/statements/truncate.md) query.

:::note
A value of `0` means that you can delete all tables without any restrictions.

This setting does not require a restart of the ClickHouse server to apply. Another way to disable the restriction is to create the `<clickhouse-path>/flags/force_drop_table` file.
:::

**Example**

```xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```


## max_temporary_data_on_disk_size {#max_temporary_data_on_disk_size} 

<SettingsInfoBlock type="UInt64" default_value="0" />
The maximum amount of storage that could be used for external aggregation, joins or sorting.
Queries that exceed this limit will fail with an exception.

:::note
A value of `0` means unlimited.
:::

See also:
- [`max_temporary_data_on_disk_size_for_user`](/operations/settings/settings#max_temporary_data_on_disk_size_for_user)
- [`max_temporary_data_on_disk_size_for_query`](/operations/settings/settings#max_temporary_data_on_disk_size_for_query)


## max_thread_pool_free_size {#max_thread_pool_free_size} 

<SettingsInfoBlock type="UInt64" default_value="1000" />
If the number of **idle** threads in the Global Thread pool is greater than [`max_thread_pool_free_size`](/operations/server-configuration-parameters/settings#max_thread_pool_free_size), then ClickHouse releases resources occupied by some threads and the pool size is decreased. Threads can be created again if necessary.

**Example**

```xml
<max_thread_pool_free_size>1200</max_thread_pool_free_size>
```


## max_thread_pool_size {#max_thread_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="10000" />
ClickHouse uses threads from the Global Thread pool to process queries. If there is no idle thread to process a query, then a new thread is created in the pool. `max_thread_pool_size` limits the maximum number of threads in the pool.

**Example**

```xml
<max_thread_pool_size>12000</max_thread_pool_size>
```


## max_unexpected_parts_loading_thread_pool_size {#max_unexpected_parts_loading_thread_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="8" />The number of threads to load inactive set of data parts (Unexpected ones) at startup.

## max_view_num_to_throw {#max_view_num_to_throw} 

<SettingsInfoBlock type="UInt64" default_value="0" />
If the number of views is greater than this value, the server will throw an exception.

Only counts tables for database engines:
- Atomic
- Ordinary
- Replicated
- Lazy

:::note
A value of `0` means no limitation.
:::

**Example**
```xml
<max_view_num_to_throw>400</max_view_num_to_throw>
```


## max_view_num_to_warn {#max_view_num_to_warn} 

<SettingsInfoBlock type="UInt64" default_value="10000" />
If the number of attached views exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.

**Example**

```xml
<max_view_num_to_warn>400</max_view_num_to_warn>
```


## max_waiting_queries {#max_waiting_queries} 

<SettingsInfoBlock type="UInt64" default_value="0" />
Limit on total number of concurrently waiting queries.
Execution of a waiting query is blocked while required tables are loading asynchronously (see [`async_load_databases`](/operations/server-configuration-parameters/settings#async_load_databases).

:::note
Waiting queries are not counted when limits controlled by the following settings are checked:

- [`max_concurrent_queries`](/operations/server-configuration-parameters/settings#max_concurrent_queries)
- [`max_concurrent_insert_queries`](/operations/server-configuration-parameters/settings#max_concurrent_insert_queries)
- [`max_concurrent_select_queries`](/operations/server-configuration-parameters/settings#max_concurrent_select_queries)
- [`max_concurrent_queries_for_user`](/operations/settings/settings#max_concurrent_queries_for_user)
- [`max_concurrent_queries_for_all_users`](/operations/settings/settings#max_concurrent_queries_for_all_users)

This correction is done to avoid hitting these limits just after server startup.
:::

:::note

A value of `0` (default) means unlimited.

This setting can be modified at runtime and will take effect immediately. Queries that are already running will remain unchanged.
:::


## memory_worker_correct_memory_tracker {#memory_worker_correct_memory_tracker} 

<SettingsInfoBlock type="Bool" default_value="0" />
Whether background memory worker should correct internal memory tracker based on the information from external sources like jemalloc and cgroups


## memory_worker_period_ms {#memory_worker_period_ms} 

<SettingsInfoBlock type="UInt64" default_value="0" />
Tick period of background memory worker which corrects memory tracker memory usages and cleans up unused pages during higher memory usage. If set to 0, default value will be used depending on the memory usage source


## memory_worker_purge_dirty_pages_threshold_ratio {#memory_worker_purge_dirty_pages_threshold_ratio} 

<SettingsInfoBlock type="Double" default_value="0.2" />
The threshold ratio for jemalloc dirty pages relative to the memory available to ClickHouse server. When dirty pages size exceeds this ratio, the background memory worker forces purging of dirty pages. If set to 0, forced purging is disabled.


## memory_worker_use_cgroup {#memory_worker_use_cgroup} 

<SettingsInfoBlock type="Bool" default_value="1" />Use current cgroup memory usage information to correct memory tracking.

## merge_tree {#merge_tree} 

Fine-tuning for tables in the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

For more information, see the MergeTreeSettings.h header file.

**Example**

```xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

## merge_workload {#merge_workload} 

<SettingsInfoBlock type="String" default_value="default" />
Used to regulate how resources are utilized and shared between merges and other workloads. Specified value is used as `workload` setting value for all background merges. Can be overridden by a merge tree setting.

**See Also**
- [Workload Scheduling](/operations/workload-scheduling.md)


## merges_mutations_memory_usage_soft_limit {#merges_mutations_memory_usage_soft_limit} 

<SettingsInfoBlock type="UInt64" default_value="0" />
Sets the limit on how much RAM is allowed to use for performing merge and mutation operations.
If ClickHouse reaches the limit set, it won't schedule any new background merge or mutation operations but will continue to execute already scheduled tasks.

:::note
A value of `0` means unlimited.
:::

**Example**

```xml
<merges_mutations_memory_usage_soft_limit>0</merges_mutations_memory_usage_soft_limit>
```


## merges_mutations_memory_usage_to_ram_ratio {#merges_mutations_memory_usage_to_ram_ratio} 

<SettingsInfoBlock type="Double" default_value="0.5" />
The default `merges_mutations_memory_usage_soft_limit` value is calculated as `memory_amount * merges_mutations_memory_usage_to_ram_ratio`.

**See also:**

- [max_memory_usage](/operations/settings/settings#max_memory_usage)
- [merges_mutations_memory_usage_soft_limit](/operations/server-configuration-parameters/settings#merges_mutations_memory_usage_soft_limit)


## metric_log {#metric_log} 

It is disabled by default.

**Enabling**

To manually turn on metrics history collection [`system.metric_log`](../../operations/system-tables/metric_log.md), create `/etc/clickhouse-server/config.d/metric_log.xml` with the following content:

```xml
<clickhouse>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </metric_log>
</clickhouse>
```

**Disabling**

To disable `metric_log` setting, you should create the following file `/etc/clickhouse-server/config.d/disable_metric_log.xml` with the following content:

```xml
<clickhouse>
    <metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters/>

## min_os_cpu_wait_time_ratio_to_drop_connection {#min_os_cpu_wait_time_ratio_to_drop_connection} 

<SettingsInfoBlock type="Float" default_value="0" />
Min ratio between OS CPU wait (OSCPUWaitMicroseconds metric) and busy (OSCPUVirtualTimeMicroseconds metric) times to consider dropping connections. Linear interpolation between min and max ratio is used to calculate the probability, the probability is 0 at this point.
See [Controlling behavior on server CPU overload](/operations/settings/server-overload) for more details.


## mlock_executable {#mlock_executable} 

<SettingsInfoBlock type="Bool" default_value="0" />
Perform `<mlockall>` after startup to lower first queries latency and to prevent clickhouse executable from being paged out under high IO load.

:::note
Enabling this option is recommended but will lead to increased startup time for up to a few seconds. Keep in mind that this setting would not work without "CAP_IPC_LOCK" capability.
:::

**Example**

```xml
<mlock_executable>false</mlock_executable>


## mlock_executable_min_total_memory_amount_bytes {#mlock_executable_min_total_memory_amount_bytes} 

<SettingsInfoBlock type="UInt64" default_value="5000000000" />The minimum memory threshold for performing `<mlockall>`

## mmap_cache_size {#mmap_cache_size} 

<SettingsInfoBlock type="UInt64" default_value="1024" />
This setting allows avoiding frequent open/close calls (which are very expensive due to consequent page faults), and to reuse mappings from several threads and queries. The setting value is the number of mapped regions (usually equal to the number of mapped files).

The amount of data in mapped files can be monitored in the following system tables with the following metrics:

- `MMappedFiles`/`MMappedFileBytes`/`MMapCacheCells` in [`system.metrics`](/operations/system-tables/metrics), [`system.metric_log`](/operations/system-tables/metric_log)
- `CreatedReadBufferMMap`/`CreatedReadBufferMMapFailed`/`MMappedFileCacheHits`/`MMappedFileCacheMisses` in [`system.events`](/operations/system-tables/events), [`system.processes`](/operations/system-tables/processes), [`system.query_log`](/operations/system-tables/query_log), [`system.query_thread_log`](/operations/system-tables/query_thread_log), [`system.query_views_log`](/operations/system-tables/query_views_log)

:::note
The amount of data in mapped files does not consume memory directly and is not accounted for in query or server memory usage — because this memory can be discarded similar to the OS page cache. The cache is dropped (the files are closed) automatically on the removal of old parts in tables of the MergeTree family, also it can be dropped manually by the `SYSTEM DROP MMAP CACHE` query.

This setting can be modified at runtime and will take effect immediately.
:::


## mutation_workload {#mutation_workload} 

<SettingsInfoBlock type="String" default_value="default" />
Used to regulate how resources are utilized and shared between mutations and other workloads. Specified value is used as `workload` setting value for all background mutations. Can be overridden by a merge tree setting.

**See Also**
- [Workload Scheduling](/operations/workload-scheduling.md)


## mysql_port {#mysql_port} 

Port for communicating with clients over MySQL protocol.

:::note
- Positive integers specify the port number to listen to
- Empty values are used to disable communication with clients over MySQL protocol.
:::

**Example**

```xml
<mysql_port>9004</mysql_port>
```

## mysql_require_secure_transport {#mysql_require_secure_transport} 

<SettingsInfoBlock type="Bool" default_value="0" />If set to true, secure communication is required with clients over [mysql_port](/operations/server-configuration-parameters/settings#mysql_port). Connection with option `<--ssl-mode=none>` will be refused. Use it with [OpenSSL](/operations/server-configuration-parameters/settings#openssl) settings.

## oom_score {#oom_score} 

<SettingsInfoBlock type="Int32" default_value="0" />On Linux systems this can control the behavior of OOM killer.

## openSSL {#openssl} 

SSL client/server configuration.

Support for SSL is provided by the `libpoco` library. The available configuration options are explained in [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h). Default values can be found in [SSLManager.cpp](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/src/SSLManager.cpp).

Keys for server/client settings:

| Option                        | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                            | Default Value                              |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------|
| `privateKeyFile`              | Path to the file with the secret key of the PEM certificate. The file may contain a key and certificate at the same time.                                                                                                                                                                                                                                                                                                                                              |                                            |
| `certificateFile`             | Path to the client/server certificate file in PEM format. You can omit it if `privateKeyFile` contains the certificate.                                                                                                                                                                                                                                                                                                                                                |                                            |
| [`cacheSessions`](/operations/server-configuration-parameters/settings#openssl.cacheSessions) | Enables or disables caching sessions. Must be used in combination with `sessionIdContext`. Acceptable values: `true`, `false`.                                                                                                                                                                                                                                                                                                                                         | `false`                                    |
| [`caConfig`](/operations/server-configuration-parameters/settings#openssl.caConfig) | Path to the file or directory that contains trusted CA certificates. If this points to a file, it must be in PEM format and can contain several CA certificates. If this points to a directory, it must contain one .pem file per CA certificate. The filenames are looked up by the CA subject name hash value. Details can be found in the man page of [SSL_CTX_load_verify_locations](https://www.openssl.org/docs/man3.0/man3/SSL_CTX_load_verify_locations.html). |                                            |
| [`cipherList`](/operations/server-configuration-parameters/settings#openssl.cipherList) | Supported OpenSSL encryptions.                                                                                                                                                                                                                                                                                                                                                                                                                                         | `ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH`  |
| [`disableProtocols`](/operations/server-configuration-parameters/settings#openssl.disableProtocols) | Protocols that are not allowed to be used.                                                                                                                                                                                                                                                                                                                                                                                                                             |                                            |
| [`extendedVerification`](/operations/server-configuration-parameters/settings#openssl.extendedVerification) | If enabled, verify that the certificate CN or SAN matches the peer hostname.                                                                                                                                                                                                                                                                                                                                                                                           | `false`                                    |
| [`fips`](/operations/server-configuration-parameters/settings#openssl.fips) | Activates OpenSSL FIPS mode. Supported if the library's OpenSSL version supports FIPS.                                                                                                                                                                                                                                                                                                                                                                                 | `false`                                    |
| [`invalidCertificateHandler`](/operations/server-configuration-parameters/settings#openssl.invalidCertificateHandler) | Class (a subclass of CertificateHandler) for verifying invalid certificates. For example: `<invalidCertificateHandler> <name>RejectCertificateHandler</name> </invalidCertificateHandler>` .                                                                                                                                                                                                                                                                           | `RejectCertificateHandler`                 |
| [`loadDefaultCAFile`](/operations/server-configuration-parameters/settings#openssl.loadDefaultCAFile) | Wether built-in CA certificates for OpenSSL will be used. ClickHouse assumes that builtin CA certificates are in the file `/etc/ssl/cert.pem` (resp. the directory `/etc/ssl/certs`) or in file (resp. directory) specified by the environment variable `SSL_CERT_FILE` (resp. `SSL_CERT_DIR`).                                                                                                                                                                        | `true`                                     |
| [`preferServerCiphers`](/operations/server-configuration-parameters/settings#openssl.preferServerCiphers) | Client-preferred server ciphers.                                                                                                                                                                                                                                                                                                                                                                                                                                       | `false`                                    |
| [`privateKeyPassphraseHandler`](/operations/server-configuration-parameters/settings#openssl.privateKeyPassphraseHandler) | Class (PrivateKeyPassphraseHandler subclass) that requests the passphrase for accessing the private key. For example: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.                                                                                                                                                                                                | `KeyConsoleHandler`                        |
| [`requireTLSv1`](/operations/server-configuration-parameters/settings#openssl.requireTLSv1) | Require a TLSv1 connection. Acceptable values: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                        | `false`                                    |
| [`requireTLSv1_1`](/operations/server-configuration-parameters/settings#openssl.requireTLSv1_1) | Require a TLSv1.1 connection. Acceptable values: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                      | `false`                                    |
| [`requireTLSv1_2`](/operations/server-configuration-parameters/settings#openssl.requireTLSv1_2) | Require a TLSv1.2 connection. Acceptable values: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                      | `false`                                    |
| [`sessionCacheSize`](/operations/server-configuration-parameters/settings#openssl.sessionCacheSize) | The maximum number of sessions that the server caches. A value of `0` means unlimited sessions.                                                                                                                                                                                                                                                                                                                                                                        | [1024\*20](https://github.com/ClickHouse/boringssl/blob/master/include/openssl/ssl.h#L1978)                            |
| [`sessionIdContext`](/operations/server-configuration-parameters/settings#openssl.sessionIdContext) | A unique set of random characters that the server appends to each generated identifier. The length of the string must not exceed `SSL_MAX_SSL_SESSION_ID_LENGTH`. This parameter is always recommended since it helps avoid problems both if the server caches the session and if the client requested caching.                                                                                                                                                        | `$\{application.name\}`                      |
| [`sessionTimeout`](/operations/server-configuration-parameters/settings#openssl.sessionTimeout) | Time for caching the session on the server in hours.                                                                                                                                                                                                                                                                                                                                                                                                                   | `2`                                        |
| [`verificationDepth`](/operations/server-configuration-parameters/settings#openssl.verificationDepth) | The maximum length of the verification chain. Verification will fail if the certificate chain length exceeds the set value.                                                                                                                                                                                                                                                                                                                                            | `9`                                        |
| [`verificationMode`](/operations/server-configuration-parameters/settings#openssl.verificationMode) | The method for checking the node's certificates. Details are in the description of the [Context](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) class. Possible values: `none`, `relaxed`, `strict`, `once`.                                                                                                                                                                                                         | `relaxed`                                  |

**Example of settings:**

```xml
<openSSL>
    <server>
        <!-- openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout /etc/clickhouse-server/server.key -out /etc/clickhouse-server/server.crt -->
        <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
        <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
        <!-- openssl dhparam -out /etc/clickhouse-server/dhparam.pem 4096 -->
        <dhParamsFile>/etc/clickhouse-server/dhparam.pem</dhParamsFile>
        <verificationMode>none</verificationMode>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
    </server>
    <client>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
        <!-- Use for self-signed: <verificationMode>none</verificationMode> -->
        <invalidCertificateHandler>
            <!-- Use for self-signed: <name>AcceptCertificateHandler</name> -->
            <name>RejectCertificateHandler</name>
        </invalidCertificateHandler>
    </client>
</openSSL>
```

## openSSL.client.caConfig {#openssl.client.caconfig} 

Path to the file or directory that contains trusted CA certificates. If this points to a file, it must be in PEM format and can contain several CA certificates. If this points to a directory, it must contain one .pem file per CA certificate. The filenames are looked up by the CA subject name hash value. Details can be found in the man page of [SSL_CTX_load_verify_locations](https://docs.openssl.org/3.0/man3/SSL_CTX_load_verify_locations/).

## openSSL.client.cacheSessions {#openssl.client.cachesessions} 

<SettingsInfoBlock type="Bool" default_value="0" />Enables or disables caching sessions. Must be used in combination with `<sessionIdContext>`. Acceptable values: `<true>`, `<false>`.

## openSSL.client.certificateFile {#openssl.client.certificatefile} 

Path to the client/server certificate file in PEM format. You can omit it if `<privateKeyFile>` contains the certificate.

## openSSL.client.cipherList {#openssl.client.cipherlist} 

<SettingsInfoBlock type="String" default_value="ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH" />Supported OpenSSL encryptions.

## openSSL.client.disableProtocols {#openssl.client.disableprotocols} 

Protocols that are not allowed to be used.

## openSSL.client.extendedVerification {#openssl.client.extendedverification} 

<SettingsInfoBlock type="Bool" default_value="0" />If enabled, verify that the certificate CN or SAN matches the peer hostname.

## openSSL.client.fips {#openssl.client.fips} 

<SettingsInfoBlock type="Bool" default_value="0" />Activates OpenSSL FIPS mode. Supported if the library's OpenSSL version supports FIPS.

## openSSL.client.invalidCertificateHandler.name {#openssl.client.invalidcertificatehandler.name} 

<SettingsInfoBlock type="String" default_value="RejectCertificateHandler" />Class (a subclass of CertificateHandler) for verifying invalid certificates. For example: `<<invalidCertificateHandler> <name>RejectCertificateHandler</name> </invalidCertificateHandler>>`.

## openSSL.client.loadDefaultCAFile {#openssl.client.loaddefaultcafile} 

<SettingsInfoBlock type="Bool" default_value="1" />Determines whether built-in CA certificates for OpenSSL will be used. ClickHouse assumes that builtin CA certificates are in the file `</etc/ssl/cert.pem>` (resp. the directory `</etc/ssl/certs>`) or in file (resp. directory) specified by the environment variable `<SSL_CERT_FILE>` (resp. `<SSL_CERT_DIR>`).

## openSSL.client.preferServerCiphers {#openssl.client.preferserverciphers} 

<SettingsInfoBlock type="Bool" default_value="0" />Client-preferred server ciphers.

## openSSL.client.privateKeyFile {#openssl.client.privatekeyfile} 

Path to the file with the secret key of the PEM certificate. The file may contain a key and certificate at the same time.

## openSSL.client.privateKeyPassphraseHandler.name {#openssl.client.privatekeypassphrasehandler.name} 

<SettingsInfoBlock type="String" default_value="KeyConsoleHandler" />Class (PrivateKeyPassphraseHandler subclass) that requests the passphrase for accessing the private key. For example: `<<privateKeyPassphraseHandler>>`, `<<name>KeyFileHandler</name>>`, `<<options><password>test</password></options>>`, `<</privateKeyPassphraseHandler>>`

## openSSL.client.requireTLSv1 {#openssl.client.requiretlsv1} 

<SettingsInfoBlock type="Bool" default_value="0" />Require a TLSv1 connection. Acceptable values: `<true>`, `<false>`.

## openSSL.client.requireTLSv1_1 {#openssl.client.requiretlsv1_1} 

<SettingsInfoBlock type="Bool" default_value="0" />Require a TLSv1.1 connection. Acceptable values: `<true>`, `<false>`.

## openSSL.client.requireTLSv1_2 {#openssl.client.requiretlsv1_2} 

<SettingsInfoBlock type="Bool" default_value="0" />Require a TLSv1.2 connection. Acceptable values: `<true>`, `<false>`.

## openSSL.client.verificationDepth {#openssl.client.verificationdepth} 

<SettingsInfoBlock type="UInt64" default_value="9" />The maximum length of the verification chain. Verification will fail if the certificate chain length exceeds the set value.

## openSSL.client.verificationMode {#openssl.client.verificationmode} 

<SettingsInfoBlock type="String" default_value="relaxed" />The method for checking the node's certificates. Details are in the description of the [Context](https://github.com/ClickHouse/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) class. Possible values: `<none>`, `<relaxed>`, `<strict>`, `<once>`.

## openSSL.server.caConfig {#openssl.server.caconfig} 

Path to the file or directory that contains trusted CA certificates. If this points to a file, it must be in PEM format and can contain several CA certificates. If this points to a directory, it must contain one .pem file per CA certificate. The filenames are looked up by the CA subject name hash value. Details can be found in the man page of [SSL_CTX_load_verify_locations](https://docs.openssl.org/3.0/man3/SSL_CTX_load_verify_locations/).

## openSSL.server.cacheSessions {#openssl.server.cachesessions} 

<SettingsInfoBlock type="Bool" default_value="0" />Enables or disables caching sessions. Must be used in combination with `<sessionIdContext>`. Acceptable values: `<true>`, `<false>`.

## openSSL.server.certificateFile {#openssl.server.certificatefile} 

Path to the client/server certificate file in PEM format. You can omit it if `<privateKeyFile>` contains the certificate.

## openSSL.server.cipherList {#openssl.server.cipherlist} 

<SettingsInfoBlock type="String" default_value="ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH" />Supported OpenSSL encryptions.

## openSSL.server.disableProtocols {#openssl.server.disableprotocols} 

Protocols that are not allowed to be used.

## openSSL.server.extendedVerification {#openssl.server.extendedverification} 

<SettingsInfoBlock type="Bool" default_value="0" />If enabled, verify that the certificate CN or SAN matches the peer hostname.

## openSSL.server.fips {#openssl.server.fips} 

<SettingsInfoBlock type="Bool" default_value="0" />Activates OpenSSL FIPS mode. Supported if the library's OpenSSL version supports FIPS.

## openSSL.server.invalidCertificateHandler.name {#openssl.server.invalidcertificatehandler.name} 

<SettingsInfoBlock type="String" default_value="RejectCertificateHandler" />Class (a subclass of CertificateHandler) for verifying invalid certificates. For example: `<<invalidCertificateHandler> <name>RejectCertificateHandler</name> </invalidCertificateHandler>>`.

## openSSL.server.loadDefaultCAFile {#openssl.server.loaddefaultcafile} 

<SettingsInfoBlock type="Bool" default_value="1" />Determines whether built-in CA certificates for OpenSSL will be used. ClickHouse assumes that builtin CA certificates are in the file `</etc/ssl/cert.pem>` (resp. the directory `</etc/ssl/certs>`) or in file (resp. directory) specified by the environment variable `<SSL_CERT_FILE>` (resp. `<SSL_CERT_DIR>`).

## openSSL.server.preferServerCiphers {#openssl.server.preferserverciphers} 

<SettingsInfoBlock type="Bool" default_value="0" />Client-preferred server ciphers.

## openSSL.server.privateKeyFile {#openssl.server.privatekeyfile} 

Path to the file with the secret key of the PEM certificate. The file may contain a key and certificate at the same time.

## openSSL.server.privateKeyPassphraseHandler.name {#openssl.server.privatekeypassphrasehandler.name} 

<SettingsInfoBlock type="String" default_value="KeyConsoleHandler" />Class (PrivateKeyPassphraseHandler subclass) that requests the passphrase for accessing the private key. For example: `<<privateKeyPassphraseHandler>>`, `<<name>KeyFileHandler</name>>`, `<<options><password>test</password></options>>`, `<</privateKeyPassphraseHandler>>`

## openSSL.server.requireTLSv1 {#openssl.server.requiretlsv1} 

<SettingsInfoBlock type="Bool" default_value="0" />Require a TLSv1 connection. Acceptable values: `<true>`, `<false>`.

## openSSL.server.requireTLSv1_1 {#openssl.server.requiretlsv1_1} 

<SettingsInfoBlock type="Bool" default_value="0" />Require a TLSv1.1 connection. Acceptable values: `<true>`, `<false>`.

## openSSL.server.requireTLSv1_2 {#openssl.server.requiretlsv1_2} 

<SettingsInfoBlock type="Bool" default_value="0" />Require a TLSv1.2 connection. Acceptable values: `<true>`, `<false>`.

## openSSL.server.sessionCacheSize {#openssl.server.sessioncachesize} 

<SettingsInfoBlock type="UInt64" default_value="20480" />The maximum number of sessions that the server caches. A value of 0 means unlimited sessions.

## openSSL.server.sessionIdContext {#openssl.server.sessionidcontext} 

<SettingsInfoBlock type="String" default_value="application.name" />A unique set of random characters that the server appends to each generated identifier. The length of the string must not exceed `<SSL_MAX_SSL_SESSION_ID_LENGTH>`. This parameter is always recommended since it helps avoid problems both if the server caches the session and if the client requested caching.

## openSSL.server.sessionTimeout {#openssl.server.sessiontimeout} 

<SettingsInfoBlock type="UInt64" default_value="2" />Time for caching the session on the server in hours.

## openSSL.server.verificationDepth {#openssl.server.verificationdepth} 

<SettingsInfoBlock type="UInt64" default_value="9" />The maximum length of the verification chain. Verification will fail if the certificate chain length exceeds the set value.

## openSSL.server.verificationMode {#openssl.server.verificationmode} 

<SettingsInfoBlock type="String" default_value="relaxed" />The method for checking the node's certificates. Details are in the description of the [Context](https://github.com/ClickHouse/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) class. Possible values: `<none>`, `<relaxed>`, `<strict>`, `<once>`.

## opentelemetry_span_log {#opentelemetry_span_log} 

Settings for the [`opentelemetry_span_log`](../system-tables/opentelemetry_span_log.md) system table.

<SystemLogParameters/>

Example:

```xml
<opentelemetry_span_log>
    <engine>
        engine MergeTree
        partition by toYYYYMM(finish_date)
        order by (finish_date, finish_time_us, trace_id)
    </engine>
    <database>system</database>
    <table>opentelemetry_span_log</table>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</opentelemetry_span_log>
```

## os_collect_psi_metrics {#os_collect_psi_metrics} 

<SettingsInfoBlock type="Bool" default_value="1" />Enable accounting PSI metrics from /proc/pressure/ files.

## os_cpu_busy_time_threshold {#os_cpu_busy_time_threshold} 

<SettingsInfoBlock type="UInt64" default_value="1000000" />Threshold of OS CPU busy time in microseconds (OSCPUVirtualTimeMicroseconds metric) to consider CPU doing some useful work, no CPU overload would be considered if busy time was below this value.

## os_threads_nice_value_distributed_cache_tcp_handler {#os_threads_nice_value_distributed_cache_tcp_handler} 

<SettingsInfoBlock type="Int32" default_value="0" />
Linux nice value for the threads of distributed cache TCP handler. Lower values mean higher CPU priority.

Requires CAP_SYS_NICE capability, otherwise no-op.

Possible values: -20 to 19.


## os_threads_nice_value_merge_mutate {#os_threads_nice_value_merge_mutate} 

<SettingsInfoBlock type="Int32" default_value="0" />
Linux nice value for merge and mutation threads. Lower values mean higher CPU priority.

Requires CAP_SYS_NICE capability, otherwise no-op.

Possible values: -20 to 19.


## os_threads_nice_value_zookeeper_client_send_receive {#os_threads_nice_value_zookeeper_client_send_receive} 

<SettingsInfoBlock type="Int32" default_value="0" />
Linux nice value for send and receive threads in ZooKeeper client. Lower values mean higher CPU priority.

Requires CAP_SYS_NICE capability, otherwise no-op.

Possible values: -20 to 19.


## page_cache_free_memory_ratio {#page_cache_free_memory_ratio} 

<SettingsInfoBlock type="Double" default_value="0.15" />Fraction of the memory limit to keep free from the userspace page cache. Analogous to Linux min_free_kbytes setting.

## page_cache_history_window_ms {#page_cache_history_window_ms} 

<SettingsInfoBlock type="UInt64" default_value="1000" />Delay before freed memory can be used by userspace page cache.

## page_cache_max_size {#page_cache_max_size} 

<SettingsInfoBlock type="UInt64" default_value="0" />Maximum size of the userspace page cache. Set to 0 to disable the cache. If greater than page_cache_min_size, the cache size will be continuously adjusted within this range, to use most of the available memory while keeping the total memory usage below the limit (max_server_memory_usage[_to_ram_ratio]).

## page_cache_min_size {#page_cache_min_size} 

<SettingsInfoBlock type="UInt64" default_value="104857600" />Minimum size of the userspace page cache.

## page_cache_policy {#page_cache_policy} 

<SettingsInfoBlock type="String" default_value="SLRU" />Userspace page cache policy name.

## page_cache_shards {#page_cache_shards} 

<SettingsInfoBlock type="UInt64" default_value="4" />Stripe userspace page cache over this many shards to reduce mutex contention. Experimental, not likely to improve performance.

## page_cache_size_ratio {#page_cache_size_ratio} 

<SettingsInfoBlock type="Double" default_value="0.5" />The size of the protected queue in the userspace page cache relative to the cache's total size.

## part_log {#part_log} 

Logging events that are associated with [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). For instance, adding or merging data. You can use the log to simulate merge algorithms and compare their characteristics. You can visualize the merge process.

Queries are logged in the [system.part_log](/operations/system-tables/part_log) table, not in a separate file. You can configure the name of this table in the `table` parameter (see below).

<SystemLogParameters/>

**Example**

```xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</part_log>
```

## parts_kill_delay_period {#parts_kill_delay_period} 

<SettingsInfoBlock type="UInt64" default_value="30" />
Period to completely remove parts for SharedMergeTree. Only available in ClickHouse Cloud


## parts_kill_delay_period_random_add {#parts_kill_delay_period_random_add} 

<SettingsInfoBlock type="UInt64" default_value="10" />
Add uniformly distributed value from 0 to x seconds to kill_delay_period to avoid thundering herd effect and subsequent DoS of ZooKeeper in case of very large number of tables. Only available in ClickHouse Cloud


## parts_killer_pool_size {#parts_killer_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="128" />
Threads for cleanup of shared merge tree outdated threads. Only available in ClickHouse Cloud


## path {#path} 

<SettingsInfoBlock type="String" default_value="/var/lib/clickhouse/" />
The path to the directory containing data.

:::note
The trailing slash is mandatory.
:::

**Example**

```xml
<path>/var/lib/clickhouse/</path>
```


## postgresql_port {#postgresql_port} 

Port for communicating with clients over PostgreSQL protocol.

:::note
- Positive integers specify the port number to listen to
- Empty values are used to disable communication with clients over PostgreSQL protocol.
:::

**Example**

```xml
<postgresql_port>9005</postgresql_port>
```

## postgresql_require_secure_transport {#postgresql_require_secure_transport} 

<SettingsInfoBlock type="Bool" default_value="0" />If set to true, secure communication is required with clients over [postgresql_port](/operations/server-configuration-parameters/settings#postgresql_port). Connection with option `<sslmode=disable>` will be refused. Use it with [OpenSSL](/operations/server-configuration-parameters/settings#openssl) settings.

## prefetch_threadpool_pool_size {#prefetch_threadpool_pool_size} 

<SettingsInfoBlock type="NonZeroUInt64" default_value="100" />Size of background pool for prefetches for remote object storages

## prefetch_threadpool_queue_size {#prefetch_threadpool_queue_size} 

<SettingsInfoBlock type="UInt64" default_value="1000000" />Number of tasks which is possible to push into prefetches pool

## prefixes_deserialization_thread_pool_thread_pool_queue_size {#prefixes_deserialization_thread_pool_thread_pool_queue_size} 

<SettingsInfoBlock type="UInt64" default_value="10000" />
The maximum number of jobs that can be scheduled on the prefixes deserialization Thread pool.

:::note
A value of `0` means unlimited.
:::


## prepare_system_log_tables_on_startup {#prepare_system_log_tables_on_startup} 

<SettingsInfoBlock type="Bool" default_value="0" />
If true, ClickHouse creates all configured `system.*_log` tables before the startup. It can be helpful if some startup scripts depend on these tables.


## primary_index_cache_policy {#primary_index_cache_policy} 

<SettingsInfoBlock type="String" default_value="SLRU" />Primary index cache policy name.

## primary_index_cache_prewarm_ratio {#primary_index_cache_prewarm_ratio} 

<SettingsInfoBlock type="Double" default_value="0.95" />The ratio of total size of mark cache to fill during prewarm.

## primary_index_cache_size {#primary_index_cache_size} 

<SettingsInfoBlock type="UInt64" default_value="5368709120" />Maximum size of cache for primary index (index of MergeTree family of tables).

## primary_index_cache_size_ratio {#primary_index_cache_size_ratio} 

<SettingsInfoBlock type="Double" default_value="0.5" />The size of the protected queue (in case of SLRU policy) in the primary index cache relative to the cache's total size.

## process_query_plan_packet {#process_query_plan_packet} 

<SettingsInfoBlock type="Bool" default_value="0" />
This setting allows reading QueryPlan packet. This packet is sent for distributed queries when serialize_query_plan is enabled.
Disabled by default to avoid possible security issues which can be caused by bugs in query plan binary deserialization.

**Example**

```xml
<process_query_plan_packet>true</process_query_plan_packet>
```


## processors_profile_log {#processors_profile_log} 

Settings for the [`processors_profile_log`](../system-tables/processors_profile_log.md) system table.

<SystemLogParameters/>

The default settings are:

```xml
<processors_profile_log>
    <database>system</database>
    <table>processors_profile_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</processors_profile_log>
```

## prometheus {#prometheus} 

Exposing metrics data for scraping from [Prometheus](https://prometheus.io).

Settings:

- `endpoint` – HTTP endpoint for scraping metrics by prometheus server. Start from '/'.
- `port` – Port for `endpoint`.
- `metrics` – Expose metrics from the [system.metrics](/operations/system-tables/metrics) table.
- `events` – Expose metrics from the [system.events](/operations/system-tables/events) table.
- `asynchronous_metrics` – Expose current metrics values from the [system.asynchronous_metrics](/operations/system-tables/asynchronous_metrics) table.
- `errors` - Expose the number of errors by error codes occurred since the last server restart. This information could be obtained from the [system.errors](/operations/system-tables/errors) as well.

**Example**

```xml
<clickhouse>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <!-- highlight-start -->
    <prometheus>
        <endpoint>/metrics</endpoint>
        <port>9363</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
        <errors>true</errors>
    </prometheus>
    <!-- highlight-end -->
</clickhouse>
```

Check (replace `127.0.0.1` with the IP addr or hostname of your ClickHouse server):
```bash
curl 127.0.0.1:9363/metrics
```

## prometheus.keeper_metrics_only {#prometheus.keeper_metrics_only} 

<SettingsInfoBlock type="Bool" default_value="0" />Expose the keeper related metrics

## proxy {#proxy} 

Define proxy servers for HTTP and HTTPS requests, currently supported by S3 storage, S3 table functions, and URL functions.

There are three ways to define proxy servers:
- environment variables
- proxy lists
- remote proxy resolvers.

Bypassing proxy servers for specific hosts is also supported with the use of `no_proxy`.

**Environment variables**

The `http_proxy` and `https_proxy` environment variables allow you to specify a
proxy server for a given protocol. If you have it set on your system, it should work seamlessly.

This is the simplest approach if a given protocol has
only one proxy server and that proxy server doesn't change.

**Proxy lists**

This approach allows you to specify one or more
proxy servers for a protocol. If more than one proxy server is defined,
ClickHouse uses the different proxies on a round-robin basis, balancing the
load across the servers. This is the simplest approach if there is more than
one proxy server for a protocol and the list of proxy servers doesn't change.

**Configuration template**

```xml
<proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```
Select a parent field in the tabs below to view their children:

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>

| Field     | Description                         |
|-----------|-------------------------------------|
| `<http>`  | A list of one or more HTTP proxies  |
| `<https>` | A list of one or more HTTPS proxies |

  </TabItem>
  <TabItem value="http_https" label="<http> and <https>">

| Field   | Description          |
|---------|----------------------|
| `<uri>` | The URI of the proxy |

  </TabItem>
</Tabs>

**Remote proxy resolvers**

It's possible that the proxy servers change dynamically. In that
case, you can define the endpoint of a resolver. ClickHouse sends
an empty GET request to that endpoint, the remote resolver should return the proxy host.
ClickHouse will use it to form the proxy URI using the following template: `\{proxy_scheme\}://\{proxy_host\}:{proxy_port}`

**Configuration template**

```xml
<proxy>
    <http>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>80</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </http>

    <https>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>3128</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </https>

</proxy>
```

Select a parent field in the tabs below to view their children:

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>

| Field    | Description                      |
|----------|----------------------------------|
| `<http>` | A list of one or more resolvers* |
| `<https>` | A list of one or more resolvers* |

  </TabItem>
  <TabItem value="http_https" label="<http> and <https>">

| Field       | Description                                   |
|-------------|-----------------------------------------------|
| `<resolver>` | The endpoint and other details for a resolver |

:::note
You can have multiple `<resolver>` elements, but only the first
`<resolver>` for a given protocol is used. Any other `<resolver>`
elements for that protocol are ignored. That means load balancing
(if needed) should be implemented by the remote resolver.
:::

  </TabItem>
  <TabItem value="resolver" label="<resolver>">

| Field               | Description                                                                                                                                                                            |
|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `<endpoint>`        | The URI of the proxy resolver                                                                                                                                                          |
| `<proxy_scheme>`    | The protocol of the final proxy URI. This can be either `http` or `https`.                                                                                                             |
| `<proxy_port>`      | The port number of the proxy resolver                                                                                                                                                  |
| `<proxy_cache_time>` | The time in seconds that values from the resolver should be cached by ClickHouse. Setting this value to `0` causes ClickHouse to contact the resolver for every HTTP or HTTPS request. |

  </TabItem>
</Tabs>

**Precedence**

Proxy settings are determined in the following order:

| Order | Setting                |
|-------|------------------------|
| 1.    | Remote proxy resolvers |
| 2.    | Proxy lists            |
| 3.    | Environment variables  |

ClickHouse will check the highest priority resolver type for the request protocol. If it is not defined,
it will check the next highest priority resolver type, until it reaches the environment resolver.
This also allows a mix of resolver types can be used.

## query_cache {#query_cache} 

[Query cache](../query-cache.md) configuration.

The following settings are available:

| Setting                   | Description                                                                            | Default Value |
|---------------------------|----------------------------------------------------------------------------------------|---------------|
| [`max_entries`](/operations/server-configuration-parameters/settings#query_cache.max_entries) | The maximum number of `SELECT` query results stored in the cache.                      | `1024`        |
| [`max_entry_size_in_bytes`](/operations/server-configuration-parameters/settings#query_cache.max_entry_size_in_bytes) | The maximum size in bytes `SELECT` query results may have to be saved in the cache.    | `1048576`     |
| [`max_entry_size_in_rows`](/operations/server-configuration-parameters/settings#query_cache.max_entry_size_in_rows) | The maximum number of rows `SELECT` query results may have to be saved in the cache.   | `30000000`    |
| [`max_size_in_bytes`](/operations/server-configuration-parameters/settings#query_cache.max_size_in_bytes) | The maximum cache size in bytes. `0` means the query cache is disabled.                | `1073741824`  |

:::note
- Changed settings take effect immediately.
- Data for the query cache is allocated in DRAM. If memory is scarce, make sure to set a small value for `max_size_in_bytes` or disable the query cache altogether.
:::

**Example**

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

## query_cache.max_entries {#query_cache.max_entries} 

<SettingsInfoBlock type="UInt64" default_value="1024" />The maximum number of SELECT query results stored in the cache.

## query_cache.max_entry_size_in_bytes {#query_cache.max_entry_size_in_bytes} 

<SettingsInfoBlock type="UInt64" default_value="1048576" />The maximum size in bytes SELECT query results may have to be saved in the cache.

## query_cache.max_entry_size_in_rows {#query_cache.max_entry_size_in_rows} 

<SettingsInfoBlock type="UInt64" default_value="30000000" />The maximum number of rows SELECT query results may have to be saved in the cache.

## query_cache.max_size_in_bytes {#query_cache.max_size_in_bytes} 

<SettingsInfoBlock type="UInt64" default_value="1073741824" />The maximum cache size in bytes. 0 means the query cache is disabled.

## query_condition_cache_policy {#query_condition_cache_policy} 

<SettingsInfoBlock type="String" default_value="SLRU" />Query condition cache policy name.

## query_condition_cache_size {#query_condition_cache_size} 

<SettingsInfoBlock type="UInt64" default_value="104857600" />
Maximum size of the query condition cache.
:::note
This setting can be modified at runtime and will take effect immediately.
:::


## query_condition_cache_size_ratio {#query_condition_cache_size_ratio} 

<SettingsInfoBlock type="Double" default_value="0.5" />The size of the protected queue (in case of SLRU policy) in the query condition cache relative to the cache's total size.

## query_log {#query_log} 

Setting for logging queries received with the [log_queries=1](../../operations/settings/settings.md) setting.

Queries are logged in the [system.query_log](/operations/system-tables/query_log) table, not in a separate file. You can change the name of the table in the `table` parameter (see below).

<SystemLogParameters/>

If the table does not exist, ClickHouse will create it. If the structure of the query log changed when the ClickHouse server was updated, the table with the old structure is renamed, and a new table is created automatically.

**Example**

```xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_log>
```

## query_masking_rules {#query_masking_rules} 

Regexp-based rules, which will be applied to queries as well as all log messages before storing them in server logs,
[`system.query_log`](/operations/system-tables/query_log), [`system.text_log`](/operations/system-tables/text_log), [`system.processes`](/operations/system-tables/processes) tables, and in logs sent to the client. That allows preventing
sensitive data leakage from SQL queries such as names, emails, personal identifiers or credit card numbers to logs.

**Example**

```xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

**Config fields**:

| Setting   | Description                                                                   |
|-----------|-------------------------------------------------------------------------------|
| [`name`](/operations/server-configuration-parameters/settings#query_masking_rules.name) | name for the rule (optional)                                                  |
| [`regexp`](/operations/server-configuration-parameters/settings#query_masking_rules.regexp) | RE2 compatible regular expression (mandatory)                                 |
| [`replace`](/operations/server-configuration-parameters/settings#query_masking_rules.replace) | substitution string for sensitive data (optional, by default - six asterisks) |

The masking rules are applied to the whole query (to prevent leaks of sensitive data from malformed / non-parseable queries).

The [`system.events`](/operations/system-tables/events) table has counter `QueryMaskingRulesMatch` which has an overall number of query masking rules matches.

For distributed queries each server has to be configured separately, otherwise, subqueries passed to other
nodes will be stored without masking.

## query_metric_log {#query_metric_log} 

It is disabled by default.

**Enabling**

To manually turn on metrics history collection [`system.query_metric_log`](../../operations/system-tables/query_metric_log.md), create `/etc/clickhouse-server/config.d/query_metric_log.xml` with the following content:

```xml
<clickhouse>
    <query_metric_log>
        <database>system</database>
        <table>query_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_metric_log>
</clickhouse>
```

**Disabling**

To disable `query_metric_log` setting, you should create the following file `/etc/clickhouse-server/config.d/disable_query_metric_log.xml` with the following content:

```xml
<clickhouse>
    <query_metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters/>

## query_thread_log {#query_thread_log} 

Setting for logging threads of queries received with the [log_query_threads=1](/operations/settings/settings#log_query_threads) setting.

Queries are logged in the [system.query_thread_log](/operations/system-tables/query_thread_log) table, not in a separate file. You can change the name of the table in the `table` parameter (see below).

<SystemLogParameters/>

If the table does not exist, ClickHouse will create it. If the structure of the query thread log changed when the ClickHouse server was updated, the table with the old structure is renamed, and a new table is created automatically.

**Example**

```xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_thread_log>
```

## query_views_log {#query_views_log} 

Setting for logging views (live, materialized etc) dependant of queries received with the [log_query_views=1](/operations/settings/settings#log_query_views) setting.

Queries are logged in the [system.query_views_log](/operations/system-tables/query_views_log) table, not in a separate file. You can change the name of the table in the `table` parameter (see below).

<SystemLogParameters/>

If the table does not exist, ClickHouse will create it. If the structure of the query views log changed when the ClickHouse server was updated, the table with the old structure is renamed, and a new table is created automatically.

**Example**

```xml
<query_views_log>
    <database>system</database>
    <table>query_views_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_views_log>
```

## remap_executable {#remap_executable} 

<SettingsInfoBlock type="Bool" default_value="0" />
Setting to reallocate memory for machine code ("text") using huge pages.

:::note
This feature is highly experimental.
:::

**Example**

```xml
<remap_executable>false</remap_executable>


## remote_servers {#remote_servers} 

Configuration of clusters used by the [Distributed](../../engines/table-engines/special/distributed.md) table engine and by the `cluster` table function.

**Example**

```xml
<remote_servers incl="clickhouse_remote_servers" />
```

For the value of the `incl` attribute, see the section "[Configuration files](/operations/configuration-files)".

**See Also**

- [skip_unavailable_shards](../../operations/settings/settings.md#skip_unavailable_shards)
- [Cluster Discovery](../../operations/cluster-discovery.md)
- [Replicated database engine](../../engines/database-engines/replicated.md)

## remote_url_allow_hosts {#remote_url_allow_hosts} 

List of hosts which are allowed to be used in URL-related storage engines and table functions.

When adding a host with the `\<host\>` xml tag:
- it should be specified exactly as in the URL, as the name is checked before DNS resolution. For example: `<host>clickhouse.com</host>`
- if the port is explicitly specified in the URL, then host:port is checked as a whole. For example: `<host>clickhouse.com:80</host>`
- if the host is specified without a port, then any port of the host is allowed. For example: if `<host>clickhouse.com</host>` is specified then `clickhouse.com:20` (FTP), `clickhouse.com:80` (HTTP), `clickhouse.com:443` (HTTPS) etc are allowed.
- if the host is specified as an IP address, then it is checked as specified in the URL. For example: `[2a02:6b8:a::a]`.
- if there are redirects and support for redirects is enabled, then every redirect (the location field) is checked.

For example:

```sql
<remote_url_allow_hosts>
    <host>clickhouse.com</host>
</remote_url_allow_hosts>
```

## replica_group_name {#replica_group_name} 

Replica group name for database Replicated.

The cluster created by Replicated database will consist of replicas in the same group.
DDL queries will only wait for the replicas in the same group.

Empty by default.

**Example**

```xml
<replica_group_name>backups</replica_group_name>
```

## replicated_fetches_http_connection_timeout {#replicated_fetches_http_connection_timeout} 

<SettingsInfoBlock type="Seconds" default_value="0" />HTTP connection timeout for part fetch requests. Inherited from default profile `http_connection_timeout` if not set explicitly.

## replicated_fetches_http_receive_timeout {#replicated_fetches_http_receive_timeout} 

<SettingsInfoBlock type="Seconds" default_value="0" />HTTP receive timeout for fetch part requests. Inherited from default profile `http_receive_timeout` if not set explicitly.

## replicated_fetches_http_send_timeout {#replicated_fetches_http_send_timeout} 

<SettingsInfoBlock type="Seconds" default_value="0" />HTTP send timeout for part fetch requests. Inherited from default profile `http_send_timeout` if not set explicitly.

## replicated_merge_tree {#replicated_merge_tree} 

Fine-tuning for tables in the [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/mergetree.md). This setting has a higher priority.

For more information, see the MergeTreeSettings.h header file.

**Example**

```xml
<replicated_merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</replicated_merge_tree>
```

## restore_threads {#restore_threads} 

<SettingsInfoBlock type="NonZeroUInt64" default_value="16" />The maximum number of threads to execute RESTORE requests.

## s3_credentials_provider_max_cache_size {#s3_credentials_provider_max_cache_size} 

<SettingsInfoBlock type="UInt64" default_value="100" />The maximum number of S3 credentials providers that can be cached

## s3_max_redirects {#s3_max_redirects} 

<SettingsInfoBlock type="UInt64" default_value="10" />Max number of S3 redirects hops allowed.

## s3_retry_attempts {#s3_retry_attempts} 

<SettingsInfoBlock type="UInt64" default_value="500" />Setting for Aws::Client::RetryStrategy, Aws::Client does retries itself, 0 means no retries

## s3queue_disable_streaming {#s3queue_disable_streaming} 

<SettingsInfoBlock type="Bool" default_value="0" />Disable streaming in S3Queue even if the table is created and there are attached materiaized views

## s3queue_log {#s3queue_log} 

Settings for the `s3queue_log` system table.

<SystemLogParameters/>

The default settings are:

```xml
<s3queue_log>
    <database>system</database>
    <table>s3queue_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</s3queue_log>
```

## send_crash_reports {#send_crash_reports} 

Settings for sending of crash reports to the ClickHouse core developers team.

Enabling it, especially in pre-production environments, is highly appreciated.

Keys:

| Key                   | Description                                                                                                                          |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| [`enabled`](/operations/server-configuration-parameters/settings#send_crash_reports.enabled) | Boolean flag to enable the feature, `true` by default. Set to `false` to avoid sending crash reports.                                |
| [`endpoint`](/operations/server-configuration-parameters/settings#send_crash_reports.endpoint) | You can override the endpoint URL for sending crash reports.                                                                         |
| [`send_logical_errors`](/operations/server-configuration-parameters/settings#send_crash_reports.send_logical_errors) | `LOGICAL_ERROR` is like an `assert`, it is a bug in ClickHouse. This boolean flag enables sending this exceptions (Default: `true`). |

**Recommended usage**

```xml
<send_crash_reports>
    <enabled>true</enabled>
</send_crash_reports>
```

## series_keeper_path {#series_keeper_path} 

<SettingsInfoBlock type="String" default_value="/clickhouse/series" />
Path in Keeper with auto-incremental numbers, generated by the `generateSerialID` function. Each series will be a node under this path.


## show_addresses_in_stack_traces {#show_addresses_in_stack_traces} 

<SettingsInfoBlock type="Bool" default_value="1" />If it is set true will show addresses in stack traces

## shutdown_wait_backups_and_restores {#shutdown_wait_backups_and_restores} 

<SettingsInfoBlock type="Bool" default_value="1" />If set to true ClickHouse will wait for running backups and restores to finish before shutdown.

## shutdown_wait_unfinished {#shutdown_wait_unfinished} 

<SettingsInfoBlock type="UInt64" default_value="5" />Delay in seconds to wait for unfinished queries

## shutdown_wait_unfinished_queries {#shutdown_wait_unfinished_queries} 

<SettingsInfoBlock type="Bool" default_value="0" />If set true ClickHouse will wait for running queries finish before shutdown.

## skip_binary_checksum_checks {#skip_binary_checksum_checks} 

<SettingsInfoBlock type="Bool" default_value="0" />Skips ClickHouse binary checksum integrity checks

## skip_check_for_incorrect_settings {#skip_check_for_incorrect_settings} 

<SettingsInfoBlock type="Bool" default_value="0" />
If set to true, server settings will not be checked for correctness.

**Example**

```xml
<skip_check_for_incorrect_settings>1</skip_check_for_incorrect_settings>


## ssh_server {#ssh_server} 

The public part of the host key will be written to the known_hosts file
on the SSH client side on the first connect.

Host Key Configurations are inactive by default.
Uncomment the host key configurations, and provide the path to the respective ssh key to active them:

Example:

```xml
<ssh_server>
    <host_rsa_key>path_to_the_ssh_key</host_rsa_key>
    <host_ecdsa_key>path_to_the_ssh_key</host_ecdsa_key>
    <host_ed25519_key>path_to_the_ssh_key</host_ed25519_key>
</ssh_server>
```

## startup_mv_delay_ms {#startup_mv_delay_ms} 

<SettingsInfoBlock type="UInt64" default_value="0" />Debug parameter to simulate materizlied view creation delay

## startup_scripts.throw_on_error {#startup_scripts.throw_on_error} 

<SettingsInfoBlock type="Bool" default_value="0" />If set to true, the server will not start if an error occurs during script execution.

## storage_configuration {#storage_configuration} 

Allows for multi-disk configuration of storage.

Storage configuration follows the structure:

```xml
<storage_configuration>
    <disks>
        <!-- configuration -->
    </disks>
    <policies>
        <!-- configuration -->
    </policies>
</storage_configuration>
```

### Configuration of disks {#configuration-of-disks}

Configuration of `disks` follows the structure given below:

```xml
<storage_configuration>
    <disks>
        <disk_name_1>
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>
        ...
    </disks>
</storage_configuration>
```

The sub-tags above define the following settings for `disks`:

| Setting                 | Description                                                                                           |
|-------------------------|-------------------------------------------------------------------------------------------------------|
| `<disk_name_N>`         | The name of the disk, which should be unique.                                                         |
| `path`                  | The path to which server data will be stored (`data` and `shadow` catalogues). It should end with `/` |
| `keep_free_space_bytes` | Size of the reserved free space on disk.                                                              |

:::note
The order of the disks does not matter.
:::

### Configuration of policies {#configuration-of-policies}

The sub-tags above define the following settings for `policies`:

| Setting                      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `policy_name_N`              | Name of the policy. Policy names must be unique.                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `volume_name_N`              | The volume name. Volume names must be unique.                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `disk`                       | The disk located inside the volume.                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `max_data_part_size_bytes`   | The maximum size of a chunk of data that can reside on any of the disks in this volume. If the merge results in a chunk size expected to be larger than max_data_part_size_bytes, the chunk will be written to the next volume. Basically this feature allows you to store new / small chunks on a hot (SSD) volume and move them to a cold (HDD) volume when they reach a large size. Do not use this option if the policy has only one volume.                                                                 |
| `move_factor`                | The share of available free space on the volume. If the space becomes less, the data will start transferring to the next volume, if there is one. For transfer, chunks are sorted by size from larger to smaller (descending) and chunks whose total size is sufficient to meet the `move_factor` condition are selected, if the total size of all chunks is insufficient, all chunks will be moved.                                                                                                             |
| `perform_ttl_move_on_insert` | Disables moving data with expired TTL on insertion. By default (if enabled), if we insert a piece of data that has already expired according to the move on life rule, it is immediately moved to the volume / disk specified in the move rule. This can significantly slow down insertion in case the target volume / disk is slow (e.g. S3). If disabled, the expired portion of the data is written to the default volume and then immediately moved to the volume specified in the rule for the expired TTL. |
| `load_balancing`             | Disk balancing policy, `round_robin` or `least_used`.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `least_used_ttl_ms`          | Sets the timeout (in milliseconds) to update the available space on all disks (`0` - always update, `-1` - never update, default value is `60000`). Note, if the disk is only used by ClickHouse and will not be subject to file system resizing on the fly, you can use the `-1` value. In all other cases this is not recommended, as it will eventually lead to incorrect space allocation.                                                                                                                   |
| `prefer_not_to_merge`        | Disables merging parts of the data on this volume. Note: this is potentially harmful and can cause slowdown. When this setting is enabled (don't do this), merging data on this volume is prohibited (which is bad). This allows control of how ClickHouse interacts with slow disks. We recommend not to use this at all.                                                                                                                                                                                       |
| `volume_priority`            | Defines the priority (order) in which volumes are filled. The smaller the value, the higher the priority. The parameter values must be natural numbers and cover the range from 1 to N (N is the largest parameter value specified) with no gaps.                                                                                                                                                                                                                                                                |

For the `volume_priority`:
- If all volumes have this parameter, they are prioritized in the specified order.
- If only _some_ volumes have it, volumes that do not have it have the lowest priority. Those that do have it are prioritized according to the tag value, the priority of the rest is determined by the order of description in the configuration file relative to each other.
- If _no_ volumes are given this parameter, their order is determined by the order of the description in the configuration file.
- The priority of volumes may not be identical.

## storage_connections_hard_limit {#storage_connections_hard_limit} 

<SettingsInfoBlock type="UInt64" default_value="200000" />Exception is thrown at a creation attempt when this limit is reached. Set to 0 to turn off hard limitation. The limit applies to the storages connections.

## storage_connections_soft_limit {#storage_connections_soft_limit} 

<SettingsInfoBlock type="UInt64" default_value="100" />Connections above this limit have significantly shorter time to live. The limit applies to the storages connections.

## storage_connections_store_limit {#storage_connections_store_limit} 

<SettingsInfoBlock type="UInt64" default_value="1000" />Connections above this limit reset after use. Set to 0 to turn connection cache off. The limit applies to the storages connections.

## storage_connections_warn_limit {#storage_connections_warn_limit} 

<SettingsInfoBlock type="UInt64" default_value="500" />Warning massages are written to the logs if number of in-use connections are higher than this limit. The limit applies to the storages connections.

## storage_metadata_write_full_object_key {#storage_metadata_write_full_object_key} 

<SettingsInfoBlock type="Bool" default_value="1" />Write disk metadata files with VERSION_FULL_OBJECT_KEY format. This is enabled by default. The setting is deprecated.

## storage_shared_set_join_use_inner_uuid {#storage_shared_set_join_use_inner_uuid} 

<SettingsInfoBlock type="Bool" default_value="1" />If enabled, an inner UUID is generated during the creation of SharedSet and SharedJoin. ClickHouse Cloud only

## table_engines_require_grant {#table_engines_require_grant} 

If set to true, users require a grant to create a table with a specific engine e.g. `GRANT TABLE ENGINE ON TinyLog to user`.

:::note
By default, for backward compatibility creating table with a specific table engine ignores grant, however you can change this behaviour by setting this to true.
:::

## tables_loader_background_pool_size {#tables_loader_background_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="0" />
Sets the number of threads performing asynchronous load jobs in background pool. The background pool is used for loading tables asynchronously after server start in case there are no queries waiting for the table. It could be beneficial to keep low number of threads in background pool if there are a lot of tables. It will reserve CPU resources for concurrent query execution.

:::note
A value of `0` means all available CPUs will be used.
:::


## tables_loader_foreground_pool_size {#tables_loader_foreground_pool_size} 

<SettingsInfoBlock type="UInt64" default_value="0" />
Sets the number of threads performing load jobs in foreground pool. The foreground pool is used for loading table synchronously before server start listening on a port and for loading tables that are waited for. Foreground pool has higher priority than background pool. It means that no job starts in background pool while there are jobs running in foreground pool.

:::note
A value of `0` means all available CPUs will be used.
:::


## tcp_close_connection_after_queries_num {#tcp_close_connection_after_queries_num} 

<SettingsInfoBlock type="UInt64" default_value="0" />Maximum number of queries allowed per TCP connection before the connection is closed. Set to 0 for unlimited queries.

## tcp_close_connection_after_queries_seconds {#tcp_close_connection_after_queries_seconds} 

<SettingsInfoBlock type="UInt64" default_value="0" />Maximum lifetime of a TCP connection in seconds before it is closed. Set to 0 for unlimited connection lifetime.

## tcp_port {#tcp_port} 

Port for communicating with clients over the TCP protocol.

**Example**

```xml
<tcp_port>9000</tcp_port>
```

## tcp_port_secure {#tcp_port_secure} 

TCP port for secure communication with clients. Use it with [OpenSSL](#openssl) settings.

**Default value**

```xml
<tcp_port_secure>9440</tcp_port_secure>
```

## tcp_ssh_port {#tcp_ssh_port} 

Port for the SSH server which allows the user to connect and execute queries in an interactive fashion using the embedded client over the PTY.

Example:

```xml
<tcp_ssh_port>9022</tcp_ssh_port>
```

## temporary_data_in_cache {#temporary_data_in_cache} 


With this option, temporary data will be stored in the cache for the particular disk.
In this section, you should specify the disk name with the type `cache`.
In that case, the cache and temporary data will share the same space, and the disk cache can be evicted to create temporary data.

:::note
Only one option can be used to configure temporary data storage: `tmp_path` ,`tmp_policy`, `temporary_data_in_cache`.
:::

**Example**

Both the cache for `local_disk`, and temporary data will be stored in `/tiny_local_cache` on the filesystem, managed by `tiny_local_cache`.

```xml
<clickhouse>
<storage_configuration>
<disks>
<local_disk>
<type>local</type>
<path>/local_disk/</path>
</local_disk>

<!-- highlight-start -->
<tiny_local_cache>
<type>cache</type>
<disk>local_disk</disk>
<path>/tiny_local_cache/</path>
<max_size_rows>10M</max_size_rows>
<max_file_segment_size>1M</max_file_segment_size>
<cache_on_write_operations>1</cache_on_write_operations>
</tiny_local_cache>
<!-- highlight-end -->
</disks>
</storage_configuration>

<!-- highlight-start -->
<temporary_data_in_cache>tiny_local_cache</temporary_data_in_cache>
<!-- highlight-end -->
</clickhouse>
```


## temporary_data_in_distributed_cache {#temporary_data_in_distributed_cache} 

<SettingsInfoBlock type="Bool" default_value="0" />Store temporary data in the distributed cache.

## text_index_dictionary_block_cache_max_entries {#text_index_dictionary_block_cache_max_entries} 

<SettingsInfoBlock type="UInt64" default_value="1000000" />Size of cache for text index dictionary block in entries. Zero means disabled.

## text_index_dictionary_block_cache_policy {#text_index_dictionary_block_cache_policy} 

<SettingsInfoBlock type="String" default_value="SLRU" />Text index dictionary block cache policy name.

## text_index_dictionary_block_cache_size {#text_index_dictionary_block_cache_size} 

<SettingsInfoBlock type="UInt64" default_value="1073741824" />Size of cache for text index dictionary blocks. Zero means disabled.

:::note
This setting can be modified at runtime and will take effect immediately.
:::

## text_index_dictionary_block_cache_size_ratio {#text_index_dictionary_block_cache_size_ratio} 

<SettingsInfoBlock type="Double" default_value="0.5" />The size of the protected queue (in case of SLRU policy) in the text index dictionary block cache relative to the cache's total size.

## text_index_header_cache_max_entries {#text_index_header_cache_max_entries} 

<SettingsInfoBlock type="UInt64" default_value="100000" />Size of cache for text index header in entries. Zero means disabled.

## text_index_header_cache_policy {#text_index_header_cache_policy} 

<SettingsInfoBlock type="String" default_value="SLRU" />Text index header cache policy name.

## text_index_header_cache_size {#text_index_header_cache_size} 

<SettingsInfoBlock type="UInt64" default_value="1073741824" />Size of cache for text index headers. Zero means disabled.

:::note
This setting can be modified at runtime and will take effect immediately.
:::

## text_index_header_cache_size_ratio {#text_index_header_cache_size_ratio} 

<SettingsInfoBlock type="Double" default_value="0.5" />The size of the protected queue (in case of SLRU policy) in the text index header cache relative to the cache's total size.

## text_index_postings_cache_max_entries {#text_index_postings_cache_max_entries} 

<SettingsInfoBlock type="UInt64" default_value="1000000" />Size of cache for text index posting list in entries. Zero means disabled.

## text_index_postings_cache_policy {#text_index_postings_cache_policy} 

<SettingsInfoBlock type="String" default_value="SLRU" />Text index posting list cache policy name.

## text_index_postings_cache_size {#text_index_postings_cache_size} 

<SettingsInfoBlock type="UInt64" default_value="2147483648" />Size of cache for text index posting lists. Zero means disabled.

:::note
This setting can be modified at runtime and will take effect immediately.
:::

## text_index_postings_cache_size_ratio {#text_index_postings_cache_size_ratio} 

<SettingsInfoBlock type="Double" default_value="0.5" />The size of the protected queue (in case of SLRU policy) in the text index posting list cache relative to the cache's total size.

## text_log {#text_log} 

Settings for the [text_log](/operations/system-tables/text_log) system table for logging text messages.

<SystemLogParameters/>

Additionally:

| Setting | Description                                                                                                                                                                                                 | Default Value       |
|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------|
| [`level`](/operations/server-configuration-parameters/settings#text_log.level) | Maximum Message Level (by default `Trace`) which will be stored in a table.                                                                                                                                 | `Trace`             |

**Example**

```xml
<clickhouse>
    <text_log>
        <level>notice</level>
        <database>system</database>
        <table>text_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <partition_by>event_date</partition_by> -->
        <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    </text_log>
</clickhouse>
```

## thread_pool_queue_size {#thread_pool_queue_size} 

<SettingsInfoBlock type="UInt64" default_value="10000" />
The maximum number of jobs that can be scheduled on the Global Thread pool. Increasing queue size leads to larger memory usage. It is recommended to keep this value equal to [`max_thread_pool_size`](/operations/server-configuration-parameters/settings#max_thread_pool_size).

:::note
A value of `0` means unlimited.
:::

**Example**

```xml
<thread_pool_queue_size>12000</thread_pool_queue_size>
```


## threadpool_local_fs_reader_pool_size {#threadpool_local_fs_reader_pool_size} 

<SettingsInfoBlock type="NonZeroUInt64" default_value="100" />The number of threads in the thread pool for reading from local filesystem when `local_filesystem_read_method = 'pread_threadpool'`.

## threadpool_local_fs_reader_queue_size {#threadpool_local_fs_reader_queue_size} 

<SettingsInfoBlock type="UInt64" default_value="1000000" />The maximum number of jobs that can be scheduled on the thread pool for reading from local filesystem.

## threadpool_remote_fs_reader_pool_size {#threadpool_remote_fs_reader_pool_size} 

<SettingsInfoBlock type="NonZeroUInt64" default_value="250" />Number of threads in the Thread pool used for reading from remote filesystem when `remote_filesystem_read_method = 'threadpool'`.

## threadpool_remote_fs_reader_queue_size {#threadpool_remote_fs_reader_queue_size} 

<SettingsInfoBlock type="UInt64" default_value="1000000" />The maximum number of jobs that can be scheduled on the thread pool for reading from remote filesystem.

## threadpool_writer_pool_size {#threadpool_writer_pool_size} 

<SettingsInfoBlock type="NonZeroUInt64" default_value="100" />Size of background pool for write requests to object storages

## threadpool_writer_queue_size {#threadpool_writer_queue_size} 

<SettingsInfoBlock type="UInt64" default_value="1000000" />Number of tasks which is possible to push into background pool for write requests to object storages

## throw_on_unknown_workload {#throw_on_unknown_workload} 

<SettingsInfoBlock type="Bool" default_value="0" />
Defines behaviour on access to unknown WORKLOAD with query setting 'workload'.

- If `true`, RESOURCE_ACCESS_DENIED exception is thrown from a query that is trying to access unknown workload. Useful to enforce resource scheduling for all queries after WORKLOAD hierarchy is established and contains WORKLOAD default.
- If `false` (default), unlimited access w/o resource scheduling is provided to a query with 'workload' setting pointing to unknown WORKLOAD. This is important during setting up hierarchy of WORKLOAD, before WORKLOAD default is added.

**Example**

```xml
<throw_on_unknown_workload>true</throw_on_unknown_workload>
```

**See Also**
- [Workload Scheduling](/operations/workload-scheduling.md)


## timezone {#timezone} 

The server's time zone.

Specified as an IANA identifier for the UTC timezone or geographic location (for example, Africa/Abidjan).

The time zone is necessary for conversions between String and DateTime formats when DateTime fields are output to text format (printed on the screen or in a file), and when getting DateTime from a string. Besides, the time zone is used in functions that work with the time and date if they didn't receive the time zone in the input parameters.

**Example**

```xml
<timezone>Asia/Istanbul</timezone>
```

**See also**

- [session_timezone](../settings/settings.md#session_timezone)

## tmp_path {#tmp_path} 

<SettingsInfoBlock type="String" default_value="/var/lib/clickhouse/tmp/" />
Path on the local filesystem to store temporary data for processing large queries.

:::note
- Only one option can be used to configure temporary data storage: tmp_path, tmp_policy, temporary_data_in_cache.
- The trailing slash is mandatory.
:::

**Example**

```xml
<tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
```


## tmp_policy {#tmp_policy} 


Policy for storage with temporary data. All files with `tmp` prefix will be removed at start.

:::note
Recommendations for using object storage as `tmp_policy`:
- Use separate `bucket:path` on each server
- Use `metadata_type=plain`
- You may also want to set TTL for this bucket
:::

:::note
- Only one option can be used to configure temporary data storage: `tmp_path` ,`tmp_policy`, `temporary_data_in_cache`.
- `move_factor`, `keep_free_space_bytes`,`max_data_part_size_bytes` and are ignored.
- Policy should have exactly *one volume*

For more information see the [MergeTree Table Engine](/engines/table-engines/mergetree-family/mergetree) documentation.
:::

**Example**

When `/disk1` is full, temporary data will be stored on `/disk2`.

```xml
<clickhouse>
<storage_configuration>
<disks>
<disk1>
<path>/disk1/</path>
</disk1>
<disk2>
<path>/disk2/</path>
</disk2>
</disks>

<policies>
<!-- highlight-start -->
<tmp_two_disks>
<volumes>
<main>
<disk>disk1</disk>
<disk>disk2</disk>
</main>
</volumes>
</tmp_two_disks>
<!-- highlight-end -->
</policies>
</storage_configuration>

<!-- highlight-start -->
<tmp_policy>tmp_two_disks</tmp_policy>
<!-- highlight-end -->
</clickhouse>
```


## top_level_domains_list {#top_level_domains_list} 

Defines a list of custom top level domains to add where each entry is, of the format `<name>/path/to/file</name>`.

For example:

```xml
<top_level_domains_lists>
    <public_suffix_list>/path/to/public_suffix_list.dat</public_suffix_list>
</top_level_domains_lists>
```

See also:
- function [`cutToFirstSignificantSubdomainCustom`](../../sql-reference/functions/url-functions.md/#cutToFirstSignificantSubdomainCustom) and variations thereof,
  which accepts a custom TLD list name, returning the part of the domain that includes top-level subdomains up to the first significant subdomain.

## top_level_domains_path {#top_level_domains_path} 

<SettingsInfoBlock type="String" default_value="/var/lib/clickhouse/top_level_domains/" />
The directory with top level domains.

**Example**

```xml
<top_level_domains_path>/var/lib/clickhouse/top_level_domains/</top_level_domains_path>
```


## total_memory_profiler_sample_max_allocation_size {#total_memory_profiler_sample_max_allocation_size} 

<SettingsInfoBlock type="UInt64" default_value="0" />Collect random allocations of size less or equal than specified value with probability equal to `total_memory_profiler_sample_probability`. 0 means disabled. You may want to set 'max_untracked_memory' to 0 to make this threshold to work as expected.

## total_memory_profiler_sample_min_allocation_size {#total_memory_profiler_sample_min_allocation_size} 

<SettingsInfoBlock type="UInt64" default_value="0" />Collect random allocations of size greater or equal than specified value with probability equal to `total_memory_profiler_sample_probability`. 0 means disabled. You may want to set 'max_untracked_memory' to 0 to make this threshold to work as expected.

## total_memory_profiler_step {#total_memory_profiler_step} 

<SettingsInfoBlock type="UInt64" default_value="0" />Whenever server memory usage becomes larger than every next step in number of bytes the memory profiler will collect the allocating stack trace. Zero means disabled memory profiler. Values lower than a few megabytes will slow down server.

## total_memory_tracker_sample_probability {#total_memory_tracker_sample_probability} 

<SettingsInfoBlock type="Double" default_value="0" />
Allows to collect random allocations and de-allocations and writes them in the [system.trace_log](../../operations/system-tables/trace_log.md) system table with `trace_type` equal to a `MemorySample` with the specified probability. The probability is for every allocation or deallocations, regardless of the size of the allocation. Note that sampling happens only when the amount of untracked memory exceeds the untracked memory limit (default value is `4` MiB). It can be lowered if [total_memory_profiler_step](/operations/server-configuration-parameters/settings#total_memory_profiler_step) is lowered. You can set `total_memory_profiler_step` equal to `1` for extra fine-grained sampling.

Possible values:

- Positive double.
- `0` — Writing of random allocations and de-allocations in the `system.trace_log` system table is disabled.


## trace_log {#trace_log} 

Settings for the [trace_log](/operations/system-tables/trace_log) system table operation.

<SystemLogParameters/>

The default server configuration file `config.xml` contains the following settings section:

```xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
    <symbolize>false</symbolize>
</trace_log>
```

## uncompressed_cache_policy {#uncompressed_cache_policy} 

<SettingsInfoBlock type="String" default_value="SLRU" />Uncompressed cache policy name.

## uncompressed_cache_size {#uncompressed_cache_size} 

<SettingsInfoBlock type="UInt64" default_value="0" />
Maximum size (in bytes) for uncompressed data used by table engines from the MergeTree family.

There is one shared cache for the server. Memory is allocated on demand. The cache is used if the option `use_uncompressed_cache` is enabled.

The uncompressed cache is advantageous for very short queries in individual cases.

:::note
A value of `0` means disabled.

This setting can be modified at runtime and will take effect immediately.
:::


## uncompressed_cache_size_ratio {#uncompressed_cache_size_ratio} 

<SettingsInfoBlock type="Double" default_value="0.5" />The size of the protected queue (in case of SLRU policy) in the uncompressed cache relative to the cache's total size.

## url_scheme_mappers {#url_scheme_mappers} 

Configuration for translating shortened or symbolic URL prefixes into full URLs.

Example:

```xml
<url_scheme_mappers>
    <s3>
        <to>https://{bucket}.s3.amazonaws.com</to>
    </s3>
    <gs>
        <to>https://storage.googleapis.com/{bucket}</to>
    </gs>
    <oss>
        <to>https://{bucket}.oss.aliyuncs.com</to>
    </oss>
</url_scheme_mappers>
```

## use_minimalistic_part_header_in_zookeeper {#use_minimalistic_part_header_in_zookeeper} 

Storage method for data part headers in ZooKeeper. This setting only applies to the [`MergeTree`](/engines/table-engines/mergetree-family) family. It can be specified:

**Globally in the [merge_tree](#merge_tree) section of the `config.xml` file**

ClickHouse uses the setting for all the tables on the server. You can change the setting at any time. Existing tables change their behaviour when the setting changes.

**For each table**

When creating a table, specify the corresponding [engine setting](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). The behaviour of an existing table with this setting does not change, even if the global setting changes.

**Possible values**

- `0` — Functionality is turned off.
- `1` — Functionality is turned on.

If [`use_minimalistic_part_header_in_zookeeper = 1`](#use_minimalistic_part_header_in_zookeeper), then [replicated](../../engines/table-engines/mergetree-family/replication.md) tables store the headers of the data parts compactly using a single `znode`. If the table contains many columns, this storage method significantly reduces the volume of the data stored in Zookeeper.

:::note
After applying `use_minimalistic_part_header_in_zookeeper = 1`, you can't downgrade the ClickHouse server to a version that does not support this setting. Be careful when upgrading ClickHouse on servers in a cluster. Don't upgrade all the servers at once. It is safer to test new versions of ClickHouse in a test environment, or on just a few servers of a cluster.

Data part headers already stored with this setting can't be restored to their previous (non-compact) representation.
:::

## user_defined_executable_functions_config {#user_defined_executable_functions_config} 

The path to the config file for executable user defined functions.

Path:

- Specify the absolute path or the path relative to the server config file.
- The path can contain wildcards \* and ?.

See also:
- "[Executable User Defined Functions](/sql-reference/functions/udf#executable-user-defined-functions).".

**Example**

```xml
<user_defined_executable_functions_config>*_function.xml</user_defined_executable_functions_config>
```

## user_defined_path {#user_defined_path} 

The directory with user defined files. Used for SQL user defined functions [SQL User Defined Functions](/sql-reference/functions/udf).

**Example**

```xml
<user_defined_path>/var/lib/clickhouse/user_defined/</user_defined_path>
```

## user_directories {#user_directories} 

Section of the configuration file that contains settings:
- Path to configuration file with predefined users.
- Path to folder where users created by SQL commands are stored.
- ZooKeeper node path where users created by SQL commands are stored and replicated.

If this section is specified, the path from [users_config](/operations/server-configuration-parameters/settings#users_config) and [access_control_path](../../operations/server-configuration-parameters/settings.md#access_control_path) won't be used.

The `user_directories` section can contain any number of items, the order of the items means their precedence (the higher the item the higher the precedence).

**Examples**

```xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <local_directory>
        <path>/var/lib/clickhouse/access/</path>
    </local_directory>
</user_directories>
```

Users, roles, row policies, quotas, and profiles can be also stored in ZooKeeper:

```xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <replicated>
        <zookeeper_path>/clickhouse/access/</zookeeper_path>
    </replicated>
</user_directories>
```

You can also define sections `memory` — means storing information only in memory, without writing to disk, and `ldap` — means storing information on an LDAP server.

To add an LDAP server as a remote user directory of users that are not defined locally, define a single `ldap` section with the following settings:

| Setting  | Description                                                                                                                                                                                                                                                                                                                                                                    |
|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`roles`](/operations/server-configuration-parameters/settings#user_directories.roles) | section with a list of locally defined roles that will be assigned to each user retrieved from the LDAP server. If no roles are specified, user will not be able to perform any actions after authentication. If any of the listed roles is not defined locally at the time of authentication, the authentication attempt will fail as if the provided password was incorrect. |
| [`server`](/operations/server-configuration-parameters/settings#user_directories.server) | one of LDAP server names defined in `ldap_servers` config section. This parameter is mandatory and cannot be empty.                                                                                                                                                                                                                                                            |

**Example**

```xml
<ldap>
    <server>my_ldap_server</server>
        <roles>
            <my_local_role1 />
            <my_local_role2 />
        </roles>
</ldap>
```

## user_files_path {#user_files_path} 

<SettingsInfoBlock type="String" default_value="/var/lib/clickhouse/user_files/" />
The directory with user files. Used in the table function [file()](/sql-reference/table-functions/file), [fileCluster()](/sql-reference/table-functions/fileCluster).

**Example**

```xml
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```


## user_scripts_path {#user_scripts_path} 

<SettingsInfoBlock type="String" default_value="/var/lib/clickhouse/user_scripts/" />
The directory with user scripts files. Used for Executable user defined functions [Executable User Defined Functions](/sql-reference/functions/udf#executable-user-defined-functions).

**Example**

```xml
<user_scripts_path>/var/lib/clickhouse/user_scripts/</user_scripts_path>
```


## users_config {#users_config} 

Path to the file that contains:

- User configurations.
- Access rights.
- Settings profiles.
- Quota settings.

**Example**

```xml
<users_config>users.xml</users_config>
```

## validate_tcp_client_information {#validate_tcp_client_information} 

<SettingsInfoBlock type="Bool" default_value="0" />Determines whether validation of client information is enabled when a query packet is received.

By default, it is `false`:

```xml
<validate_tcp_client_information>false</validate_tcp_client_information>
```

## vector_similarity_index_cache_max_entries {#vector_similarity_index_cache_max_entries} 

<SettingsInfoBlock type="UInt64" default_value="10000000" />Size of cache for vector similarity index in entries. Zero means disabled.

## vector_similarity_index_cache_policy {#vector_similarity_index_cache_policy} 

<SettingsInfoBlock type="String" default_value="SLRU" />Vector similarity index cache policy name.

## vector_similarity_index_cache_size {#vector_similarity_index_cache_size} 

<SettingsInfoBlock type="UInt64" default_value="5368709120" />Size of cache for vector similarity indexes. Zero means disabled.

:::note
This setting can be modified at runtime and will take effect immediately.
:::

## vector_similarity_index_cache_size_ratio {#vector_similarity_index_cache_size_ratio} 

<SettingsInfoBlock type="Double" default_value="0.5" />The size of the protected queue (in case of SLRU policy) in the vector similarity index cache relative to the cache's total size.

## wait_dictionaries_load_at_startup {#wait_dictionaries_load_at_startup} 

<SettingsInfoBlock type="Bool" default_value="1" />
This setting allows to specify behavior if `dictionaries_lazy_load` is `false`.
(If `dictionaries_lazy_load` is `true` this setting doesn't affect anything.)

If `wait_dictionaries_load_at_startup` is `false`, then the server
will start loading all the dictionaries at startup and it will receive connections in parallel with that loading.
When a dictionary is used in a query for the first time then the query will wait until the dictionary is loaded if it's not loaded yet.
Setting `wait_dictionaries_load_at_startup` to `false` can make ClickHouse start faster, however some queries can be executed slower
(because they will have to wait for some dictionaries to be loaded).

If `wait_dictionaries_load_at_startup` is `true`, then the server will wait at startup
until all the dictionaries finish their loading (successfully or not) before receiving any connections.

**Example**

```xml
<wait_dictionaries_load_at_startup>true</wait_dictionaries_load_at_startup>
```


## workload_path {#workload_path} 

The directory used as a storage for all `CREATE WORKLOAD` and `CREATE RESOURCE` queries. By default `/workload/` folder under server working directory is used.

**Example**

```xml
<workload_path>/var/lib/clickhouse/workload/</workload_path>
```

**See Also**
- [Workload Hierarchy](/operations/workload-scheduling.md#workloads)
- [workload_zookeeper_path](#workload_zookeeper_path)

## workload_zookeeper_path {#workload_zookeeper_path} 

The path to a ZooKeeper node, which is used as a storage for all `CREATE WORKLOAD` and `CREATE RESOURCE` queries. For consistency all SQL definitions are stored as a value of this single znode. By default ZooKeeper is not used and definitions are stored on [disk](#workload_path).

**Example**

```xml
<workload_zookeeper_path>/clickhouse/workload/definitions.sql</workload_zookeeper_path>
```

**See Also**
- [Workload Hierarchy](/operations/workload-scheduling.md#workloads)
- [workload_path](#workload_path)

## zookeeper {#zookeeper} 

Contains settings that allow ClickHouse to interact with a [ZooKeeper](http://zookeeper.apache.org/) cluster. ClickHouse uses ZooKeeper for storing metadata of replicas when using replicated tables. If replicated tables are not used, this section of parameters can be omitted.

The following settings can be configured by sub-tags:

| Setting                                    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|--------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`node`](/operations/server-configuration-parameters/settings#zookeeper.node) | ZooKeeper endpoint. You can set multiple endpoints. Eg. `<node index="1"><host>example_host</host><port>2181</port></node>`. The `index` attribute specifies the node order when trying to connect to the ZooKeeper cluster.                                                                                                                                                                                                                                                                                            |
| [`operation_timeout_ms`](/operations/server-configuration-parameters/settings#zookeeper.operation_timeout_ms) | Maximum timeout for one operation in milliseconds.                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| [`session_timeout_ms`](/operations/server-configuration-parameters/settings#zookeeper.session_timeout_ms) | Maximum timeout for the client session in milliseconds.                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `root` (optional)                          | The znode that is used as the root for znodes used by the ClickHouse server.                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `fallback_session_lifetime.min` (optional) | Minimum limit for the lifetime of a zookeeper session to the fallback node when primary is unavailable (load-balancing). Set in seconds. Default: 3 hours.                                                                                                                                                                                                                                                                                                                                                              |
| `fallback_session_lifetime.max` (optional) | Maximum limit for the lifetime of a zookeeper session to the fallback node when primary is unavailable (load-balancing). Set in seconds. Default: 6 hours.                                                                                                                                                                                                                                                                                                                                                              |
| `identity` (optional)                      | User and password required by ZooKeeper to access requested znodes.                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `use_compression` (optional)               | Enables compression in Keeper protocol if set to true.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |

There is also the `zookeeper_load_balancing` setting (optional) which lets you select the algorithm for ZooKeeper node selection:

| Algorithm Name                   | Description                                                                                                                    |
|----------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| `random`                         | randomly selects one of ZooKeeper nodes.                                                                                       |
| `in_order`                       | selects the first ZooKeeper node, if it's not available then the second, and so on.                                            |
| `nearest_hostname`               | selects a ZooKeeper node with a hostname that is most similar to the server's hostname, hostname is compared with name prefix. |
| `hostname_levenshtein_distance`  | just like nearest_hostname, but it compares hostname in a levenshtein distance manner.                                         |
| `first_or_random`                | selects the first ZooKeeper node, if it's not available then randomly selects one of remaining ZooKeeper nodes.                |
| `round_robin`                    | selects the first ZooKeeper node, if reconnection happens selects the next.                                                    |

**Example configuration**

```xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <session_timeout_ms>30000</session_timeout_ms>
    <operation_timeout_ms>10000</operation_timeout_ms>
    <!-- Optional. Chroot suffix. Should exist. -->
    <root>/path/to/zookeeper/node</root>
    <!-- Optional. Zookeeper digest ACL string. -->
    <identity>user:password</identity>
    <!--<zookeeper_load_balancing>random / in_order / nearest_hostname / hostname_levenshtein_distance / first_or_random / round_robin</zookeeper_load_balancing>-->
    <zookeeper_load_balancing>random</zookeeper_load_balancing>
</zookeeper>
```

**See Also**

- [Replication](../../engines/table-engines/mergetree-family/replication.md)
- [ZooKeeper Programmer's Guide](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)
- [Optional secured communication between ClickHouse and Zookeeper](/operations/ssl-zookeeper)

## zookeeper_log {#zookeeper_log} 

Settings for the [`zookeeper_log`](/operations/system-tables/zookeeper_log) system table.

The following settings can be configured by sub-tags:

<SystemLogParameters/>

**Example**

```xml
<clickhouse>
    <zookeeper_log>
        <database>system</database>
        <table>zookeeper_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <ttl>event_date + INTERVAL 1 WEEK DELETE</ttl>
    </zookeeper_log>
</clickhouse>
```