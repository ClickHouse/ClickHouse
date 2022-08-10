---
sidebar_position: 43
sidebar_label: Files
---

# Functions for Working with Files {#functions-for-working-with-files}

## file {#file}

Reads file as a String. The file content is not parsed, so any information is read as one string and placed into the specified column.

**Syntax**

``` sql
file(path)
```

**Arguments**

-   `path` — The relative path to the file from [user_files_path](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_files_path). Path to file support following wildcards: `*`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc', 'def'` — strings.

**Example**

Inserting data from files a.txt and b.txt into a table as strings:

Query:

``` sql
INSERT INTO table SELECT file('a.txt'), file('b.txt');
```

**See Also**

-   [user_files_path](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_files_path)
-   [file](../table-functions/file.md)
