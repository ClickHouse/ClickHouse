---
sidebar_position: 60
sidebar_label: Set
---

# Set Table Engine

A data set that is always in RAM. It is intended for use on the right side of the `IN` operator (see the section “IN operators”).

You can use `INSERT` to insert data in the table. New elements will be added to the data set, while duplicates will be ignored.
But you can’t perform `SELECT` from the table. The only way to retrieve data is by using it in the right half of the `IN` operator.

Data is always located in RAM. For `INSERT`, the blocks of inserted data are also written to the directory of tables on the disk. When starting the server, this data is loaded to RAM. In other words, after restarting, the data remains in place.

For a rough server restart, the block of data on the disk might be lost or damaged. In the latter case, you may need to manually delete the file with damaged data.

### Limitations and Settings {#join-limitations-and-settings}

When creating a table, the following settings are applied:

-   [persistent](../../../operations/settings/settings.md#persistent)

[Original article](https://clickhouse.com/docs/en/operations/table_engines/special/set/) <!--hide-->
