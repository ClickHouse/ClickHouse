---
toc_priority: 68
toc_title: Files
---

# Functions for Working with Files {#functions-for-working-with-files}

## file() {#file}

Reads file as a String. The file can contain subqueries, condition, names of columns  and any other information that will be read as one line.

**Syntax**

``` sql
file(filename)
```

**Arguments**

-   `filename` — The name of the file to read. The file must be located in the user's directory specified in `user_files_path` settings.

**Example**

Inserting data from files a.txt and b.txt in the table as separate rows.

Query:

``` sql
INSERT INTO table SELECT file('a.txt'), file('b.txt')
```

**See Also**

-   [user_files_path][../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_files_path](#)

