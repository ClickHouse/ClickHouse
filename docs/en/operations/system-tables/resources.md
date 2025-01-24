---
slug: /en/operations/system-tables/resources
---
# resources

Contains information for [resources](/docs/en/operations/workload-scheduling.md#workload_entity_storage) residing on the local server. The table contains a row for every resource.

Example:

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

Columns:

- `name` (`String`) - Resource name.
- `read_disks` (`Array(String)`) - The array of disk names that uses this resource for read operations.
- `write_disks` (`Array(String)`) - The array of disk names that uses this resource for write operations.
- `create_query` (`String`) - The definition of the resource.
