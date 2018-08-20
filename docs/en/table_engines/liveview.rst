LiveView
----------------

Used for implementing live views (for more information, see ``CREATE LIVE VIEW``). It does not store any data permanently,
but stores the ``SELECT`` query and caches its result. The result of the query is updated automatically when
changes to the table specified in the ``SELECT`` are made. In addition to ``SELECT`` query for reading the current result
of the query, the table also supports ``WATCH`` query to constantly receive the updated query result.
