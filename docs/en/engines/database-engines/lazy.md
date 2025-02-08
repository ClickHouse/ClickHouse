---
slug: /en/engines/database-engines/lazy
sidebar_label: Lazy
sidebar_position: 20
---

# Lazy

Keeps tables in RAM only `expiration_time_in_seconds` seconds after last access. Can be used only with \*Log tables.

It's optimized for storing many small \*Log tables, for which there is a long time interval between accesses.

## Creating a Database {#creating-a-database}

    CREATE DATABASE testlazy ENGINE = Lazy(expiration_time_in_seconds);
