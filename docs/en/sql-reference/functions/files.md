---
toc_priority: 43
toc_title: Files
---

# Functions for Working with Files {#functions-for-working-with-files}

## file {#file}

Reads file as a String. The file can contain subqueries, condition, names of columns and any other information that will be read as one line.

**Syntax**

``` sql
file(path)
```

**Arguments**

-   `path` — The relative path to the file from [user_files_path](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_files_path). Path to file support following wildcards in read-only mode: `*`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc', 'def'` — strings.

**Example**

Inserting data from files a.txt and b.txt into a table as different rows.

Query:

``` sql
INSERT INTO table SELECT file('a.txt'), file('b.txt');
```

**See Also**

-   [user_files_path](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_files_path)
-   [file](../table-functions/file.md)

