# Notes

## Q11
The `FRACTION` parameter in the `HAVING` clause is defined as `0.0001 / SF` (spec section 2.4.11.3). The current query uses `0.0001`, which is correct for SF = 1. For other scale factors, adjust accordingly, e.g. `0.00001` for SF = 10, `0.000001` for SF = 100.

# List of known problems
## Q6
The query doesn't work out-of-the-box due to https://github.com/ClickHouse/ClickHouse/issues/70136. The alternative formulation with a minor fix works.

Original:
```sql
SELECT
    sum(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE (l_shipdate >= date '1994-01-01')
    AND (l_shipdate < date '1994-01-01' + INTERVAL 1 YEAR)
    AND (l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01)
    AND (l_quantity < 24);
```

Alternative:
```sql
SELECT
    sum(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE (l_shipdate >= date '1994-01-01')
    AND (l_shipdate < date '1994-01-01' + INTERVAL 1 YEAR)
    AND (l_discount BETWEEN 0.06::Decimal(12,2) - 0.01::Decimal(12,2) AND 0.06::Decimal(12,2) + 0.01::Decimal(12,2))
    AND (l_quantity < 24);
```
