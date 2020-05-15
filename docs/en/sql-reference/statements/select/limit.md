---
toc_title: LIMIT
---

# LIMIT Clause {#limit-clause}

`LIMIT m` allows to select the first `m` rows from the result.

`LIMIT n, m` allows to select the `m` rows from the result after skipping the first `n` rows. The `LIMIT m OFFSET n` syntax is equivalent.

`n` and `m` must be non-negative integers.

If there is no [ORDER BY](order-by.md) clause that explicitly sorts results, the choice of rows for the result may be arbitrary and non-deterministic.
