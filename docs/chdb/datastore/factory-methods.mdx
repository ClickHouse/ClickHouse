---
title: 'DataStore factory methods'
sidebarTitle: 'Factory methods'
slug: /chdb/datastore/factory-methods
description: 'Create DataStore instances from files, databases, cloud storage, and data lakes'
keywords: ['chdb', 'datastore', 'factory', 'from_file', 'from_s3', 'uri', 'mysql', 'postgresql']
doc_type: 'reference'
---

DataStore provides over 20 factory methods to create instances from various data sources including local files, databases, cloud storage, and data lakes.

## Universal URI Interface {#uri}

The `uri()` method is the recommended universal entry point that auto-detects the source type:

```python
from chdb.datastore import DataStore

# Local files
ds = DataStore.uri("data.csv")
ds = DataStore.uri("/path/to/data.parquet")

# Cloud storage
ds = DataStore.uri("s3://bucket/data.parquet?nosign=true")
ds = DataStore.uri("https://example.com/data.csv")

# Databases
ds = DataStore.uri("mysql://user:pass@host:3306/db/table")
ds = DataStore.uri("postgresql://user:pass@host:5432/db/table")
```

### URI Syntax Reference {#uri-syntax}

| Source Type | URI Format | Example |
|-------------|------------|---------|
| Local file | `path/to/file` | `data.csv`, `/abs/path/data.parquet` |
| S3 | `s3://bucket/path` | `s3://mybucket/data.parquet?nosign=true` |
| GCS | `gs://bucket/path` | `gs://mybucket/data.csv` |
| Azure | `az://container/path` | `az://mycontainer/data.parquet` |
| HTTP/HTTPS | `https://url` | `https://example.com/data.csv` |
| MySQL | `mysql://user:pass@host:port/db/table` | `mysql://root:pass@localhost:3306/mydb/users` |
| PostgreSQL | `postgresql://user:pass@host:port/db/table` | `postgresql://postgres:pass@localhost:5432/mydb/users` |
| SQLite | `sqlite:///path?table=name` | `sqlite:///data.db?table=users` |
| ClickHouse | `clickhouse://host:port/db/table` | `clickhouse://localhost:9000/default/hits` |

---

## File Sources {#file-sources}

### `from_file` {#from-file}

Create DataStore from a local or remote file with automatic format detection.

```python
DataStore.from_file(path, format=None, compression=None, **kwargs)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | str | *required* | File path (local or URL) |
| `format` | str | `None` | File format (auto-detected if None) |
| `compression` | str | `None` | Compression type (auto-detected if None) |

**Supported formats:** CSV, TSV, Parquet, JSON, JSONLines, ORC, Avro, Arrow

**Examples:**

```python
from chdb.datastore import DataStore

# Auto-detect format from extension
ds = DataStore.from_file("data.csv")
ds = DataStore.from_file("data.parquet")
ds = DataStore.from_file("data.json")

# Explicit format
ds = DataStore.from_file("data.txt", format="CSV")

# With compression
ds = DataStore.from_file("data.csv.gz", compression="gzip")
```

### Pandas-compatible read functions {#pandas-read}

```python
from chdb import datastore as pd

# CSV files
ds = pd.read_csv("data.csv")
ds = pd.read_csv("data.csv", sep=";", header=0, nrows=1000)

# Parquet files (recommended for large datasets)
ds = pd.read_parquet("data.parquet")
ds = pd.read_parquet("data.parquet", columns=['col1', 'col2'])

# JSON files
ds = pd.read_json("data.json")
ds = pd.read_json("data.jsonl", lines=True)

# Excel files
ds = pd.read_excel("data.xlsx", sheet_name="Sheet1")
```

---

## Cloud Storage {#cloud-storage}

### `from_s3` {#from-s3}

Create DataStore from Amazon S3.

```python
DataStore.from_s3(url, access_key_id=None, secret_access_key=None, format=None, **kwargs)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | str | *required* | S3 URL (s3://bucket/path) |
| `access_key_id` | str | `None` | AWS access key ID |
| `secret_access_key` | str | `None` | AWS secret access key |
| `format` | str | `None` | File format (auto-detected) |

**Examples:**

```python
from chdb.datastore import DataStore

# Anonymous access (public bucket)
ds = DataStore.from_s3("s3://bucket/data.parquet")

# With credentials
ds = DataStore.from_s3(
    "s3://bucket/data.parquet",
    access_key_id="AKIAIOSFODNN7EXAMPLE",
    secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

# Using URI with query parameters
ds = DataStore.uri("s3://bucket/data.parquet?nosign=true")
ds = DataStore.uri("s3://bucket/data.parquet?access_key_id=KEY&secret_access_key=SECRET")
```

### `from_gcs` {#from-gcs}

Create DataStore from Google Cloud Storage.

```python
DataStore.from_gcs(url, credentials_path=None, **kwargs)
```

**Examples:**

```python
ds = DataStore.from_gcs("gs://bucket/data.parquet")
ds = DataStore.from_gcs("gs://bucket/data.parquet", credentials_path="/path/to/creds.json")
```

### `from_azure` {#from-azure}

Create DataStore from Azure Blob Storage.

```python
DataStore.from_azure(url, account_name=None, account_key=None, **kwargs)
```

**Examples:**

```python
ds = DataStore.from_azure(
    "az://container/data.parquet",
    account_name="myaccount",
    account_key="mykey"
)
```

### `from_hdfs` {#from-hdfs}

Create DataStore from HDFS.

```python
DataStore.from_hdfs(url, **kwargs)
```

**Examples:**

```python
ds = DataStore.from_hdfs("hdfs://namenode:8020/path/data.parquet")
```

### `from_url` {#from-url}

Create DataStore from HTTP/HTTPS URL.

```python
DataStore.from_url(url, format=None, **kwargs)
```

**Examples:**

```python
ds = DataStore.from_url("https://example.com/data.csv")
ds = DataStore.from_url("https://raw.githubusercontent.com/user/repo/main/data.parquet")
```

---

## Databases {#databases}

### `from_mysql` {#from-mysql}

Create DataStore from MySQL database.

```python
DataStore.from_mysql(host, database, table, user, password, port=3306, **kwargs)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | str | *required* | MySQL host |
| `database` | str | *required* | Database name |
| `table` | str | *required* | Table name |
| `user` | str | *required* | Username |
| `password` | str | *required* | Password |
| `port` | int | `3306` | Port number |

**Examples:**

```python
ds = DataStore.from_mysql(
    host="localhost",
    database="mydb",
    table="users",
    user="root",
    password="password"
)

# Using URI
ds = DataStore.uri("mysql://root:password@localhost:3306/mydb/users")
```

### `from_postgresql` {#from-postgresql}

Create DataStore from PostgreSQL database.

```python
DataStore.from_postgresql(host, database, table, user, password, port=5432, **kwargs)
```

**Examples:**

```python
ds = DataStore.from_postgresql(
    host="localhost",
    database="mydb",
    table="users",
    user="postgres",
    password="password"
)

# Using URI
ds = DataStore.uri("postgresql://postgres:password@localhost:5432/mydb/users")
```

### `from_clickhouse` {#from-clickhouse}

Create DataStore from ClickHouse server.

```python
DataStore.from_clickhouse(host, database, table, user=None, password=None, port=9000, **kwargs)
```

**Examples:**

```python
ds = DataStore.from_clickhouse(
    host="localhost",
    database="default",
    table="hits",
    user="default",
    password=""
)

# Connection-level mode (explore databases)
ds = DataStore.from_clickhouse(
    host="analytics.company.com",
    user="analyst",
    password="secret"
)
ds.databases()                  # List databases
ds.tables("production")         # List tables
result = ds.sql("SELECT * FROM production.users LIMIT 10")
```

### `from_mongodb` {#from-mongodb}

Create DataStore from MongoDB.

```python
DataStore.from_mongodb(uri, database, collection, **kwargs)
```

**Examples:**

```python
ds = DataStore.from_mongodb(
    uri="mongodb://localhost:27017",
    database="mydb",
    collection="users"
)
```

### `from_sqlite` {#from-sqlite}

Create DataStore from SQLite database.

```python
DataStore.from_sqlite(database_path, table, **kwargs)
```

**Examples:**

```python
ds = DataStore.from_sqlite("data.db", table="users")

# Using URI
ds = DataStore.uri("sqlite:///data.db?table=users")
```

---

## Data Lakes {#data-lakes}

### `from_iceberg` {#from-iceberg}

Create DataStore from Apache Iceberg table.

```python
DataStore.from_iceberg(path, **kwargs)
```

**Examples:**

```python
ds = DataStore.from_iceberg("/path/to/iceberg_table")
ds = DataStore.uri("iceberg://catalog/namespace/table")
```

### `from_delta` {#from-delta}

Create DataStore from Delta Lake table.

```python
DataStore.from_delta(path, **kwargs)
```

**Examples:**

```python
ds = DataStore.from_delta("/path/to/delta_table")
ds = DataStore.uri("deltalake:///path/to/delta_table")
```

### `from_hudi` {#from-hudi}

Create DataStore from Apache Hudi table.

```python
DataStore.from_hudi(path, **kwargs)
```

**Examples:**

```python
ds = DataStore.from_hudi("/path/to/hudi_table")
ds = DataStore.uri("hudi:///path/to/hudi_table")
```

---

## In-Memory Sources {#in-memory}

### `from_df` / `from_dataframe` {#from-df}

Create DataStore from pandas DataFrame.

```python
DataStore.from_df(df, name=None)
DataStore.from_dataframe(df, name=None)  # alias
```

**Examples:**

```python
import pandas
from chdb.datastore import DataStore

pdf = pandas.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
ds = DataStore.from_df(pdf)
```

### `DataFrame` Constructor {#dataframe-constructor}

Create DataStore using pandas-like constructor.

```python
from chdb import datastore as pd

# From dictionary
ds = pd.DataFrame({
    'name': ['Alice', 'Bob'],
    'age': [25, 30]
})

# From pandas DataFrame
import pandas
pdf = pandas.DataFrame({'a': [1, 2, 3]})
ds = pd.DataFrame(pdf)
```

---

## Special Sources {#special-sources}

### `from_numbers` {#from-numbers}

Create DataStore with sequential numbers (useful for testing).

```python
DataStore.from_numbers(count, **kwargs)
```

**Examples:**

```python
ds = DataStore.from_numbers(1000000)  # 1M rows with 'number' column
result = ds.filter(ds['number'] % 2 == 0).head(10)  # Even numbers
```

### `from_random` {#from-random}

Create DataStore with random data.

```python
DataStore.from_random(rows, columns, **kwargs)
```

**Examples:**

```python
ds = DataStore.from_random(rows=1000, columns=5)
```

### `run_sql` {#run-sql}

Create DataStore from raw SQL query.

```python
DataStore.run_sql(query)
```

**Examples:**

```python
ds = DataStore.run_sql("""
    SELECT number, number * 2 as doubled
    FROM numbers(100)
    WHERE number % 10 = 0
""")
```

---

## Summary Table {#summary}

| Method | Source Type | Example |
|--------|-------------|---------|
| `uri()` | Universal | `DataStore.uri("s3://bucket/data.parquet")` |
| `from_file()` | Local/Remote files | `DataStore.from_file("data.csv")` |
| `read_csv()` | CSV files | `pd.read_csv("data.csv")` |
| `read_parquet()` | Parquet files | `pd.read_parquet("data.parquet")` |
| `from_s3()` | Amazon S3 | `DataStore.from_s3("s3://bucket/path")` |
| `from_gcs()` | Google Cloud Storage | `DataStore.from_gcs("gs://bucket/path")` |
| `from_azure()` | Azure Blob | `DataStore.from_azure("az://container/path")` |
| `from_hdfs()` | HDFS | `DataStore.from_hdfs("hdfs://host/path")` |
| `from_url()` | HTTP/HTTPS | `DataStore.from_url("https://example.com/data.csv")` |
| `from_mysql()` | MySQL | `DataStore.from_mysql(host, db, table, user, pass)` |
| `from_postgresql()` | PostgreSQL | `DataStore.from_postgresql(host, db, table, user, pass)` |
| `from_clickhouse()` | ClickHouse | `DataStore.from_clickhouse(host, db, table)` |
| `from_mongodb()` | MongoDB | `DataStore.from_mongodb(uri, db, collection)` |
| `from_sqlite()` | SQLite | `DataStore.from_sqlite("data.db", table)` |
| `from_iceberg()` | Apache Iceberg | `DataStore.from_iceberg("/path/to/table")` |
| `from_delta()` | Delta Lake | `DataStore.from_delta("/path/to/table")` |
| `from_hudi()` | Apache Hudi | `DataStore.from_hudi("/path/to/table")` |
| `from_df()` | pandas DataFrame | `DataStore.from_df(pandas_df)` |
| `DataFrame()` | Dictionary/DataFrame | `pd.DataFrame({'a': [1, 2, 3]})` |
| `from_numbers()` | Sequential numbers | `DataStore.from_numbers(1000000)` |
| `from_random()` | Random data | `DataStore.from_random(rows=1000, columns=5)` |
| `run_sql()` | Raw SQL | `DataStore.run_sql("SELECT * FROM ...")` |
