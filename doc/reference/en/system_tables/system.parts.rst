system.parts
------------
Contains information about parts of a table in the MergeTree family.

Columns:

.. code-block:: text

  database String            - Name of the database where the table that this part belongs to is located.
  table String               - Name of the table that this part belongs to.
  engine String              - Name of the table engine, without parameters.
  partition String           - Name of the partition, in the format YYYYMM.
  name String                - Name of the part.
  replicated UInt8           - Whether the part belongs to replicated data.
  active UInt8               - Whether the part is used in a table, or is no longer needed and will be deleted soon. Inactive parts remain after merging.
  marks UInt64               - Number of marks - multiply by the index granularity (usually 8192) to get the approximate number of rows in the part.
  bytes UInt64               - Number of bytes when compressed.
  modification_time DateTime - Time the directory with the part was modified. Usually corresponds to the part's creation time.
  remove_time DateTime       - For inactive parts only - the time when the part became inactive.
  refcount UInt32            - The number of places where the part is used. A value greater than 2 indicates that this part participates in queries or merges.
