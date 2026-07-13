---
slug: /guides/developer/on-the-fly-mutations
sidebarTitle: 'On-the-fly mutation'
title: 'On-the-fly Mutations'
keywords: ['On-the-fly mutation']
description: 'Provides a description of on-the-fly mutations'
doc_type: 'guide'
---

## On-the-fly mutations 

When on-the-fly mutations are enabled, updated rows are marked as updated immediately and subsequent `SELECT` queries will automatically return with the changed values. When on-the-fly mutations are not enabled, you may have to wait for your mutations to be applied via a background process to see the changed values.

On-the-fly mutations can be enabled for `MergeTree`-family tables by enabling the query-level setting `apply_mutations_on_fly`.

```sql
SET apply_mutations_on_fly = 1;
```

## Example 

Let's create a table and run some mutations:
```sql
CREATE TABLE test_on_fly_mutations (id UInt64, v String)
ENGINE = MergeTree ORDER BY id;

-- Disable background materialization of mutations to showcase
-- default behavior when on-the-fly mutations are not enabled
SYSTEM STOP MERGES test_on_fly_mutations;
SET mutations_sync = 0;

-- Insert some rows in our new table
INSERT INTO test_on_fly_mutations VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- Update the values of the rows
ALTER TABLE test_on_fly_mutations UPDATE v = 'd' WHERE id = 1;
ALTER TABLE test_on_fly_mutations DELETE WHERE v = 'd';
ALTER TABLE test_on_fly_mutations UPDATE v = 'e' WHERE id = 2;
ALTER TABLE test_on_fly_mutations DELETE WHERE v = 'e';
```

Let's check the result of the updates via a `SELECT` query:

```sql
-- Explicitly disable on-the-fly-mutations
SET apply_mutations_on_fly = 0;

SELECT id, v FROM test_on_fly_mutations ORDER BY id;
```

Note that the values of the rows have not yet been updated when we query the new table:

```response
┌─id─┬─v─┐
│  1 │ a │
│  2 │ b │
│  3 │ c │
└────┴───┘
```

Let's now see what happens when we enable on-the-fly mutations:

```sql
-- Enable on-the-fly mutations
SET apply_mutations_on_fly = 1;

SELECT id, v FROM test_on_fly_mutations ORDER BY id;
```

The `SELECT` query now returns the correct result immediately, without having to wait for the mutations to be applied:

```response
┌─id─┬─v─┐
│  3 │ c │
└────┴───┘
```

## Performance impact 

When on-the-fly mutations are enabled, mutations are not materialized immediately but will only be applied during `SELECT` queries. However, please note that mutations are still being materialized asynchronously in the background, which is a heavy process.

If the number of submitted mutations constantly exceeds the number of mutations that are processed in the background over some time interval, the queue of unmaterialized mutations that have to be applied will continue to grow. This will result in the eventual degradation of `SELECT` query performance.

We suggest enabling the setting `apply_mutations_on_fly` together with other `MergeTree`-level settings such as `number_of_mutations_to_throw` and `number_of_mutations_to_delay` to restrict the infinite growth of unmaterialized mutations.

## Support for subqueries and non-deterministic functions 

On-the-fly mutations have limited support with subqueries and non-deterministic functions. Only scalar subqueries with a result that have a reasonable size (controlled by the setting `mutations_max_literal_size_to_replace`) are supported. Only constant non-deterministic functions are supported (e.g. the function `now()`).

These behaviours are controlled by the following settings:

- `mutations_execute_nondeterministic_on_initiator` - if true, non-deterministic functions are executed on the initiator replica and are replaced as literals in `UPDATE` and `DELETE` queries. Default value: `false`.
- `mutations_execute_subqueries_on_initiator` - if true, scalar subqueries are executed on the initiator replica and are replaced as literals in `UPDATE` and `DELETE` queries. Default value: `false`.
- `mutations_max_literal_size_to_replace` - The maximum size of serialized literals in bytes to replace in `UPDATE` and `DELETE` queries. Default value: `16384` (16 KiB).
