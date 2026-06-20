---
slug: /sql-reference/statements/create/dictionary/sources/executable-pool
title: 'Executable Pool dictionary source'
sidebar_position: 4
sidebar_label: 'Executable Pool'
description: 'Configure an executable pool as a dictionary source in ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Executable pool allows loading data from a pool of processes.
This source does not work with dictionary layouts that need to load all data from source.

Executable pool works if the dictionary [is stored](../layouts/#ways-to-store-dictionaries-in-memory) using one of the following layouts:
- `cache`
- `complex_key_cache`
- `ssd_cache`
- `complex_key_ssd_cache`
- `direct`
- `complex_key_direct`

Executable pool will spawn a pool of processes with the specified command and keep them running until they exit. The program should read data from STDIN while it is available and output the result to STDOUT. It can wait for the next block of data on STDIN. ClickHouse will not close STDIN after processing a block of data, but will pipe another chunk of data when needed. The executable script should be ready for this way of data processing — it should poll STDIN and flush data to STDOUT early.

Example of settings:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
SOURCE(EXECUTABLE_POOL(
    command 'while read key; do printf "$key\tData for key $key\n"; done'
    format 'TabSeparated'
    pool_size 10
    max_command_execution_time 10
    implicit_key false
))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<source>
    <executable_pool>
        <command><command>while read key; do printf "$key\tData for key $key\n"; done</command</command>
        <format>TabSeparated</format>
        <pool_size>10</pool_size>
        <max_command_execution_time>10<max_command_execution_time>
        <implicit_key>false</implicit_key>
    </executable_pool>
</source>
```

</TabItem>
</Tabs>

Setting fields:

| Setting | Description |
|---------|-------------|
| `command` | The absolute path to the executable file, or the file name (if the program directory is written to `PATH`). |
| `format` | The file format. All the formats described in [Formats](/sql-reference/formats) are supported. |
| `pool_size` | Size of pool. If 0 is specified as `pool_size` then there is no pool size restrictions. Default value is `16`. |
| `command_termination_timeout` | Executable script should contain main read-write loop. After dictionary is destroyed, pipe is closed, and executable file will have `command_termination_timeout` seconds to shutdown before ClickHouse will send SIGTERM signal to child process. Specified in seconds. Default value is `10`. Optional. |
| `max_command_execution_time` | Maximum executable script command execution time for processing block of data. Specified in seconds. Default value is `10`. Optional. |
| `command_read_timeout` | Timeout for reading data from command stdout in milliseconds. Default value `10000`. Optional. |
| `command_write_timeout` | Timeout for writing data to command stdin in milliseconds. Default value `10000`. Optional. |
| `implicit_key` | The executable source file can return only values, and the correspondence to the requested keys is determined implicitly by the order of rows in the result. Default value is `false`. Optional. |
| `execute_direct` | If `execute_direct` = `1`, then `command` will be searched inside user_scripts folder specified by [user_scripts_path](/operations/server-configuration-parameters/settings#user_scripts_path). Additional script arguments can be specified using whitespace separator. Example: `script_name arg1 arg2`. If `execute_direct` = `0`, `command` is passed as argument for `bin/sh -c`. Default value is `1`. Optional. |
| `send_chunk_header` | Controls whether to send row count before sending a chunk of data to process. Default value is `false`. Optional. |

That dictionary source can be configured only via XML configuration. Creating dictionaries with executable source via DDL is disabled, otherwise, the DB user would be able to execute arbitrary binary on ClickHouse node.
