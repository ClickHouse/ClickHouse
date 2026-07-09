---
description: 'timeSeriesMetrics renvoie la table des métriques utilisée par la table `db_name.time_series_table`
  dont le moteur de table est TimeSeries.'
sidebar_label: 'timeSeriesMetrics'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesMetrics
title: 'timeSeriesMetrics'
doc_type: 'référence'
---

`timeSeriesMetrics(db_name.time_series_table)` - renvoie la table des [métriques](../../engines/table-engines/integrations/time-series.md#metrics-table)
utilisée par la table `db_name.time_series_table` dont le moteur de table est [TimeSeries](../../engines/table-engines/integrations/time-series.md) :

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS metrics_table
```

La fonction fonctionne aussi si la table *metrics* est de type inner :

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

Les requêtes suivantes sont équivalentes :

```sql
SELECT * FROM timeSeriesMetrics(db_name.time_series_table);
SELECT * FROM timeSeriesMetrics('db_name.time_series_table');
SELECT * FROM timeSeriesMetrics('db_name', 'time_series_table');
```