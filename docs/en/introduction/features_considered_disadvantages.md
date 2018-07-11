# ClickHouse features that can be considered disadvantages

1. No transactions.
2. For aggregation, query results must fit in the RAM on a single server. However, the volume of source data for a query may be indefinitely large.
3. Lack of full-fledged UPDATE/DELETE implementation.

