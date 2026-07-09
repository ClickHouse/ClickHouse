---
description: 'la fonction de table timeSeriesTags renvoie la table de tags utilisée par la table `db_name.time_series_table`
  dont le moteur de table est TimeSeries.'
sidebar_label: 'timeSeriesTags'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesTags
title: 'timeSeriesTags'
doc_type: 'reference'
---

`timeSeriesTags(db_name.time_series_table)` - Renvoie la [table de tags](../../engines/table-engines/integrations/time-series.md#tags-table)
utilisée par la table `db_name.time_series_table` dont le moteur de table est [TimeSeries](../../engines/table-engines/integrations/time-series.md) :

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS tags_table
```

La fonction fonctionne également si *tags* est une table interne :

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

Les requêtes suivantes sont équivalentes :

```sql
SELECT * FROM timeSeriesTags(db_name.time_series_table);
SELECT * FROM timeSeriesTags('db_name.time_series_table');
SELECT * FROM timeSeriesTags('db_name', 'time_series_table');
```