---
description: 'How to use data catalogs with iceberg'
sidebar_label: 'Schema evolution'
sidebar_position: 90
slug: /sql-reference/table-functions/iceberg-schema-evolution
title: 'Iceberg schema evolution'
doc_type: 'reference'
---

# Iceberg schema evolution {#schema-evolution}

At the moment, with the help of CH, you can read iceberg tables, the schema of which has changed over time.
We currently support reading tables where columns have been added and removed, and their order has changed.
You can also change a column where a value is required to one where NULL is allowed.
Additionally, we support permitted type casting for simple types, namely:

* int -> long
* float -> double
* decimal(P, S) -> decimal(P', S) where P' > P.

Currently, it is not possible to change nested structures or the types of elements within arrays and maps.
