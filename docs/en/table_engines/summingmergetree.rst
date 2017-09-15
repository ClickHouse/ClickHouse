SummingMergeTree
----------------

This engine differs from ``MergeTree`` in that it totals data while merging.

.. code-block:: sql

  SummingMergeTree(EventDate, (OrderID, EventDate, BannerID, ...), 8192)

The columns to total are implicit. When merging, all rows with the same primary key value (in the example, OrderId, EventDate, BannerID, ...) have their values totaled in numeric columns that are not part of the primary key.

.. code-block:: sql

  SummingMergeTree(EventDate, (OrderID, EventDate, BannerID, ...), 8192, (Shows, Clicks, Cost, ...))

The columns to total are set explicitly (the last parameter - Shows, Clicks, Cost, ...). When merging, all rows with the same primary key value have their values totaled in the specified columns. The specified columns also must be numeric and must not be part of the primary key.

If the values were null in all of these columns, the row is deleted. (The exception is cases when the data part would not have any rows left in it.)

For the other rows that are not part of the primary key, the first value that occurs is selected when merging.

Summation is not performed for a read operation. If it is necessary, write the appropriate GROUP BY.

In addition, a table can have nested data structures that are processed in a special way.
If the name of a nested table ends in 'Map' and it contains at least two columns that meet the following criteria:
* for the first table, numeric ((U)IntN, Date, DateTime), we'll refer to it as 'key'
* for other tables, arithmetic ((U)IntN, Float32/64), we'll refer to it as '(values...)'

Then this nested table is interpreted as a mapping of key => (values...), and when merging its rows, the elements of two data sets are merged by 'key' with a summation of the corresponding (values...).

Examples:

.. code-block:: text

  [(1, 100)] + [(2, 150)] -> [(1, 100), (2, 150)]
  [(1, 100)] + [(1, 150)] -> [(1, 250)]
  [(1, 100)] + [(1, 150), (2, 150)] -> [(1, 250), (2, 150)]
  [(1, 100), (2, 150)] + [(1, -100)] -> [(2, 150)]

For aggregating Map use function sumMap(key, value).

For nested data structures, you don't need to specify the columns as a list of columns for totaling.

This table engine is not particularly useful. Remember that when saving just pre-aggregated data, you lose some of the system's advantages.
