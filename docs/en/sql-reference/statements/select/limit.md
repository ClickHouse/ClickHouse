### LIMIT Clause {#limit-clause}

`LIMIT m` allows you to select the first `m` rows from the result.

`LIMIT n, m` allows you to select the first `m` rows from the result after skipping the first `n` rows. The `LIMIT m OFFSET n` syntax is also supported.

`n` and `m` must be non-negative integers.

If there is no [ORDER BY](order-by.md) clause that explicitly sorts results, the result may be arbitrary and nondeterministic.
