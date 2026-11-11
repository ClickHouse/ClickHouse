---
title: 'Using a clickhouse-local database'
sidebarTitle: 'Using clickhouse-local database'
slug: /chdb/guides/clickhouse-local
description: 'Learn how to use a clickhouse-local database with chDB'
keywords: ['chdb', 'clickhouse-local']
doc_type: 'guide'
---

[clickhouse-local](/concepts/features/tools-and-utilities/clickhouse-local) is a CLI with an embedded version of ClickHouse.
It gives users the power of ClickHouse without having to install a server.
In this guide, we will learn how to use a clickhouse-local database from chDB.

## Setup {#setup}

Let's first create a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

And now we'll install chDB.
Make sure you have version 2.0.2 or higher:

```bash
pip install "chdb>=2.0.2"
```

And now we're going to install [ipython](https://ipython.org/):

```bash
pip install ipython
```

We're going to use `ipython` to run the commands in the rest of the guide, which you can launch by running:

```bash
ipython
```

## Installing clickhouse-local {#installing-clickhouse-local}

Downloading and installing clickhouse-local is the same as [downloading and installing ClickHouse](/get-started/setup/install).
We can do this by running the following command:

```bash
curl https://clickhouse.com/ | sh
```

To launch clickhouse-local with the data being persisted to a directory, we need to pass in a `--path`:

```bash
./clickhouse -m --path demo.chdb
```

## Ingesting data into clickhouse-local {#ingesting-data-into-clickhouse-local}

The default database only stores data in memory, so we'll need to create a named database to make sure any data we ingest is persisted to disk.

```sql
CREATE DATABASE foo;
```

Let's create a table and insert some random numbers:

```sql
CREATE TABLE foo.randomNumbers
ORDER BY number AS
SELECT rand() AS number
FROM numbers(10_000_000);
```

Let's write a query to see what data we've got:

```sql
SELECT quantilesExact(0, 0.5, 0.75, 0.99)(number) AS quants
FROM foo.randomNumbers
```

```response
┌─quants────────────────────────────────┐
│ [69,2147776478,3221525118,4252096960] │
└───────────────────────────────────────┘
```

Once you've done that, make sure you `exit;` from the CLI as only one process can hold a lock on this directory.
If we don't do that, we'll get the following error when we try to connect to the database from chDB:

```text
ChdbError: Code: 76. DB::Exception: Cannot lock file demo.chdb/status. Another server instance in same directory is already running. (CANNOT_OPEN_FILE)
```

## Connecting to a clickhouse-local database {#connecting-to-a-clickhouse-local-database}

Go back to the `ipython` shell and import the `session` module from chDB:

```python
from chdb import session as chs
```

Initialize a session pointing to `demo.chdb`:

```python
sess = chs.Session("demo.chdb")
```

We can then run the same query that returns the quantiles of numbers:

```python
sess.query("""
SELECT quantilesExact(0, 0.5, 0.75, 0.99)(number) AS quants
FROM foo.randomNumbers
""", "Vertical")

Row 1:
──────
quants: [0,9976599,2147776478,4209286886]
```

We can also insert data into this database from chDB:

```python
sess.query("""
INSERT INTO foo.randomNumbers
SELECT rand() AS number FROM numbers(10_000_000)
""")

Row 1:
──────
quants: [0,9976599,2147776478,4209286886]
```

We can then re-run the quantiles query from chDB or clickhouse-local.
