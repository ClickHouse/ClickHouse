ClickHouse features that can be considered disadvantages
--------------------------------------------------------

#. No transactions.
#. For aggregation, query results must fit in the RAM on a single server. However, the volume of source data for a query may be indefinitely large.
#. Lack of full-fledged UPDATE/DELETE implementation.
