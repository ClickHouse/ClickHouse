---
description: 'Overview of domain types in ClickHouse, which extend base types with
  additional features'
sidebar_label: 'Domains'
sidebar_position: 56
slug: /sql-reference/data-types/domains/
title: 'Domains'
doc_type: 'reference'
---

# Domains

Domains are special-purpose types that add extra features on top of existing base types, while leaving the on-wire and on-disk format of the underlying data type intact. Currently, ClickHouse does not support user-defined domains.

You can use domains anywhere corresponding base type can be used, for example:

- Create a column of a domain type
- Read/write values from/to domain column
- Use it as an index if a base type can be used as an index
- Call functions with values of domain column

### Extra Features of Domains {#extra-features-of-domains}

- Explicit column type name in `SHOW CREATE TABLE` or `DESCRIBE TABLE`
- Input from human-friendly format with `INSERT INTO domain_table(domain_column) VALUES(...)`
- Output to human-friendly format for `SELECT domain_column FROM domain_table`
- Loading data from an external source in the human-friendly format: `INSERT INTO domain_table FORMAT CSV ...`

### Limitations {#limitations}

- Can't convert index column of base type to domain type via `ALTER TABLE`.
- Can't implicitly convert string values into domain values when inserting data from another column or table.
- Domain adds no constrains on stored values.
