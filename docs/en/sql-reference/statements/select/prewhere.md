---
toc_title: PREWHERE
---

# PREWHERE Clause {#prewhere-clause}

Prewhere is an optimization to apply filtering more efficiently. It is enabled by default even if `PREWHERE` clause is not specified explicitly. It works by automatically moving part of [WHERE](../../../sql-reference/statements/select/where.md) condition to prewhere stage. The role of `PREWHERE` clause is only to control this optimization if you think that you know how to do it better than it happens by default.

With prewhere optimization, at first only the columns necessary for executing prewhere expression are read. Then the other columns are read that are needed for running the rest of the query, but only those blocks where the prewhere expression is `true` at least for some rows. If there are a lot of blocks where prewhere expression is `false` for all rows and prewhere needs less columns than other parts of query, this often allows to read a lot less data from disk for query execution.

## Controlling Prewhere Manually {#controlling-prewhere-manually}

The clause has the same meaning as the `WHERE` clause. The difference is in which data is read from the table. When manually controlling `PREWHERE` for filtration conditions that are used by a minority of the columns in the query, but that provide strong data filtration. This reduces the volume of data to read.

A query may simultaneously specify `PREWHERE` and `WHERE`. In this case, `PREWHERE` precedes `WHERE`.

If the [optimize_move_to_prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) setting is set to 0, heuristics to automatically move parts of expressions from `WHERE` to `PREWHERE` are disabled.

If query has [FINAL](from.md#select-from-final) modifier, the `PREWHERE` optimization is not always correct. It is enabled only if both settings [optimize_move_to_prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) and [optimize_move_to_prewhere_if_final](../../../operations/settings/settings.md#optimize_move_to_prewhere_if_final) are turned on.

!!! note "Attention"
     The `PREWHERE` section is executed before `FINAL`, so the results of `FROM ... FINAL` queries may be skewed when using `PREWHERE` with fields not in the `ORDER BY` section of a table.

## Limitations {#limitations}

`PREWHERE` is only supported by tables from the [*MergeTree](../../../engines/table-engines/mergetree-family/index.md) family.
