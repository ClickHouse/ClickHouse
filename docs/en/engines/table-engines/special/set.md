---
description: 'A data set that is always in RAM. It is intended for use on the right
  side of the `IN` operator.'
sidebar_label: 'Set'
sidebar_position: 60
slug: /engines/table-engines/special/set
title: 'Set Table Engine'
---

# Set Table Engine

:::note
In ClickHouse Cloud, if your service was created with a version earlier than 25.4, you will need to set the compatibility to at least 25.4 using  `SET compatibility=25.4`.
:::

A data set that is always in RAM. It is intended for use on the right side of the `IN` operator (see the section "IN operators").

You can use `INSERT` to insert data in the table. New elements will be added to the data set, while duplicates will be ignored.
But you can't perform `SELECT` from the table. The only way to retrieve data is by using it in the right half of the `IN` operator.

Data is always located in RAM. For `INSERT`, the blocks of inserted data are also written to the directory of tables on the disk. When starting the server, this data is loaded to RAM. In other words, after restarting, the data remains in place.

For a rough server restart, the block of data on the disk might be lost or damaged. In the latter case, you may need to manually delete the file with damaged data.

### Limitations and Settings {#join-limitations-and-settings}

When creating a table, the following settings are applied:

#### persistent {#persistent}

Disables persistency for the Set and [Join](/engines/table-engines/special/join) table engines.

Reduces the I/O overhead. Suitable for scenarios that pursue performance and do not require persistence.

Possible values:

- 1 — Enabled.
- 0 — Disabled.

Default value: `1`.
