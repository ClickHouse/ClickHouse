---
description: 'When writing to a `Null` table, data is ignored. When reading from a
  `Null` table, the response is empty.'
sidebar_label: 'Null'
sidebar_position: 50
slug: /engines/table-engines/special/null
title: 'Null table engine'
doc_type: 'reference'
---

# Null table engine 

When writing data to a `Null` table, data is ignored.
When reading from a `Null` table, the response is empty.

The `Null` table engine is useful for data transformations where you no longer need the original data after it has been transformed.
For this purpose you can create a materialized view on a `Null` table.
The data written to the table will be consumed by the view, but the original raw data will be discarded.
