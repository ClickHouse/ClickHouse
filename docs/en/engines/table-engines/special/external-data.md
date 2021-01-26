---
toc_priority: 45
toc_title: External Data
---

# External Data for Query Processing {#external-data-for-query-processing}

ClickHouse allows sending a server the data that is needed for processing a query, together with a `SELECT` query. This data is put in a temporary table (see the section “Temporary tables”) and can be used in the query (for example, in `IN` operators).

For example, if you have a text file with important user identifiers, you can upload it to the server along with a query that uses filtration by this list.

If you need to run more than one query with a large volume of external data, don’t use this feature. It is better to upload the data to the DB ahead of time.

External data can be uploaded using the command-line client (in non-interactive mode), or using the HTTP interface.

In the command-line client, you can specify a parameters section in the format

``` bash
--external --file=... [--name=...] [--format=...] [--types=...|--structure=...]
```

You may have multiple sections like this, for the number of tables being transmitted.

**–external** – Marks the beginning of a clause.
**–file** – Path to the file with the table dump, or -, which refers to stdin.
Only a single table can be retrieved from stdin.

The following parameters are optional: **–name**– Name of the table. If omitted, \_data is used.
**–format** – Data format in the file. If omitted, TabSeparated is used.

One of the following parameters is required:**–types** – A list of comma-separated column types. For example: `UInt64,String`. The columns will be named \_1, \_2, …
**–structure**– The table structure in the format`UserID UInt64`, `URL String`. Defines the column names and types.

The files specified in ‘file’ will be parsed by the format specified in ‘format’, using the data types specified in ‘types’ or ‘structure’. The table will be uploaded to the server and accessible there as a temporary table with the name in ‘name’.

Examples:

``` bash
$ echo -ne "1\n2\n3\n" | clickhouse-client --query="SELECT count() FROM test.visits WHERE TraficSourceID IN _data" --external --file=- --types=Int8
849897
$ cat /etc/passwd | sed 's/:/\t/g' | clickhouse-client --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external --file=- --name=passwd --structure='login String, unused String, uid UInt16, gid UInt16, comment String, home String, shell String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

When using the HTTP interface, external data is passed in the multipart/form-data format. Each table is transmitted as a separate file. The table name is taken from the file name. The `query_string` is passed the parameters `name_format`, `name_types`, and `name_structure`, where `name` is the name of the table that these parameters correspond to. The meaning of the parameters is the same as when using the command-line client.

Example:

``` bash
$ cat /etc/passwd | sed 's/:/\t/g' > passwd.tsv

$ curl -F 'passwd=@passwd.tsv;' 'http://localhost:8123/?query=SELECT+shell,+count()+AS+c+FROM+passwd+GROUP+BY+shell+ORDER+BY+c+DESC&passwd_structure=login+String,+unused+String,+uid+UInt16,+gid+UInt16,+comment+String,+home+String,+shell+String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

For distributed query processing, the temporary tables are sent to all the remote servers.

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/external_data/) <!--hide-->
