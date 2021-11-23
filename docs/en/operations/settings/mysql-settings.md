---
toc_priority: 66
toc_title: MySQL Settings
---

# MySQL Settings {#mysql-settings}

Used in the [MySQL])(../../engines/table-engines/integrations/mysql.md) engine.

Default settings are not very efficient, since they do not even reuse connections. These settings allow you to increase the number of queries run by the server per second.

## connection_auto_close {#connection-auto-close}

Auto-close connection after query execution, i.e. disables connection reuse.

Possible values:

-   1 — The connection reuse is disabled.
-   0 — The connection reuse is enabled.

Default value: `1`.

## connection_max_tries {#connection-max-tries}

Number of retries for pool with failover.

Possible values:

-   Positive integer.
-   0 — There are no retries for pool with failover.

Default value: `3`.

## connection_pool_size {#connection-pool-size}

Size of connection pool (if all connections are in use, the query will wait until some connection will be freed).

Possible values:

-   Positive integer.

Default value: `16`.
