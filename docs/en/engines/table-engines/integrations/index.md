---
slug: /en/engines/table-engines/integrations/
sidebar_position: 40
sidebar_label:  Integrations
---

# Table Engines for Integrations

ClickHouse provides various means for integrating with external systems, including table engines. Like with all other table engines, the configuration is done using `CREATE TABLE` or `ALTER TABLE` queries. Then from a user perspective, the configured integration looks like a normal table, but queries to it are proxied to the external system. This transparent querying is one of the key advantages of this approach over alternative integration methods, like dictionaries or table functions, which require the use of custom query methods on each use.
