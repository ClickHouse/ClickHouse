---
slug: /sql-reference/statements/create/dictionary/sources/executable-file
title: 'Executable File dictionary source'
sidebar_position: 3
sidebar_label: 'Executable File'
description: 'Configure an executable file as a dictionary source in ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Working with executable files depends on [how the dictionary is stored in memory](../layouts/). If the dictionary is stored using `cache` and `complex_key_cache`, ClickHouse requests the necessary keys by sending a request to the executable file's STDIN. Otherwise, ClickHouse starts the executable file and treats its output as dictionary data.

Example of settings:

<Tabs>
<TabItem value="ddl" label="DDL" default>

```sql
SOURCE(EXECUTABLE(
    command 'cat /opt/dictionaries/os.tsv'
    format 'TabSeparated'
    implicit_key false
))
```

</TabItem>
<TabItem value="xml" label="Configuration file">

```xml
<source>
    <executable>
        <command>cat /opt/dictionaries/os.tsv</command>
        <format>TabSeparated</format>
        <implicit_key>false</implicit_key>
    </executable>
</source>
```

</TabItem>
</Tabs>

Setting fields:

| Setting | Description |
|---------|-------------|
| `command` | The absolute path to the executable file, or the file name (if the command's directory is in the `PATH`). |
| `format` | The file format. All the formats described in [Formats](/sql-reference/formats) are supported. |
| `command_termination_timeout` | The executable script should contain a main read-write loop. After the dictionary is destroyed, the pipe is closed, and the executable file will have `command_termination_timeout` seconds to shutdown before ClickHouse will send a SIGTERM signal to the child process. Specified in seconds. Default value is `10`. Optional. |
| `command_read_timeout` | Timeout for reading data from command stdout in milliseconds. Default value `10000`. Optional. |
| `command_write_timeout` | Timeout for writing data to command stdin in milliseconds. Default value `10000`. Optional. |
| `implicit_key` | The executable source file can return only values, and the correspondence to the requested keys is determined implicitly by the order of rows in the result. Default value is `false`. |
| `execute_direct` | If `execute_direct` = `1`, then `command` will be searched inside user_scripts folder specified by [user_scripts_path](/operations/server-configuration-parameters/settings#user_scripts_path). Additional script arguments can be specified using a whitespace separator. Example: `script_name arg1 arg2`. If `execute_direct` = `0`, `command` is passed as argument for `bin/sh -c`. Default value is `0`. Optional. |
| `send_chunk_header` | Controls whether to send row count before sending a chunk of data to process. Default value is `false`. Optional. |

That dictionary source can be configured only via XML configuration. Creating dictionaries with executable source via DDL is disabled; otherwise, the DB user would be able to execute arbitrary binaries on the ClickHouse node.
