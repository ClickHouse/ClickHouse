---
slug: /en/sql-reference/functions/files
sidebar_position: 75
sidebar_label: Files
---

## file

Reads a file as string and loads the data into the specified column. The file content is not interpreted.

Also see table function [file](../table-functions/file.md).

**Syntax**

``` sql
file(path[, default])
```

**Arguments**

- `path` — The path of the file relative to [user_files_path](../../operations/server-configuration-parameters/settings.md#user_files_path). Supports wildcards `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` are numbers and `'abc', 'def'` are strings.
- `default` — The value returned if the file does not exist or cannot be accessed. Supported data types: [String](../data-types/string.md) and [NULL](../../sql-reference/syntax.md#null-literal).

**Example**

Inserting data from files a.txt and b.txt into a table as strings:

``` sql
INSERT INTO table SELECT file('a.txt'), file('b.txt');
```
