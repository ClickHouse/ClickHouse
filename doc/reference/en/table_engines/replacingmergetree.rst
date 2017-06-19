ReplacingMergeTree
------------------

This engine differs from ``MergeTree`` in that it can deduplicate data by primary key while merging.

For ReplacingMergeTree mode, last parameter is optional name of 'version' column. While merging, for all rows with same primary key, only one row is selected: last row, if version column was not specified, or last row with maximum version value, if specified.

Version column must have type of UInt family or ``Date`` or ``DateTime``.

.. code-block:: sql

  ReplacingMergeTree(EventDate, (OrderID, EventDate, BannerID, ...), 8192, ver)

Please note, that data is deduplicated only while merging process. Merges are processed in background. Exact time of merge is unspecified and you could not rely on it. Some part of data could be not merged at all. While you could trigger extra merge with OPTIMIZE query, it is not recommended, as OPTIMIZE will read and write vast amount of data.

This table engine is suitable for background removal of duplicate data to save space, but not suitable to guarantee of deduplication.

*Developed for special purposes of not Yandex.Metrica department.*

