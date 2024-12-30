---
slug: /ja/operations/system-tables/resources
---
# resources

ローカルサーバー上に存在する[リソース](/docs/ja/operations/workload-scheduling.md#workload_entity_storage)に関する情報を含みます。このテーブルには各リソースごとに行が含まれています。

例：

``` sql
SELECT *
FROM system.resources
FORMAT Vertical
```

``` text
Row 1:
──────
name:         io_read
read_disks:   ['s3']
write_disks:  []
create_query: CREATE RESOURCE io_read (READ DISK s3)

Row 2:
──────
name:         io_write
read_disks:   []
write_disks:  ['s3']
create_query: CREATE RESOURCE io_write (WRITE DISK s3)
```

カラム:

- `name` (`String`) - リソース名。
- `read_disks` (`Array(String)`) - 読み取り操作にこのリソースを使用するディスク名の配列。
- `write_disks` (`Array(String)`) - 書き込み操作にこのリソースを使用するディスク名の配列。
- `create_query` (`String`) - リソースの定義。
