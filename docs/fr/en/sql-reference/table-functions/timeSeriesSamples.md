---
description: 'timeSeriesSamples renvoie la table d’échantillons utilisée par la table `db_name.time_series_table`
  dont le moteur est TimeSeries.'
sidebar_label: 'timeSeriesSamples'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesSamples
title: 'timeSeriesSamples'
doc_type: 'référence'
---

`timeSeriesSamples(db_name.time_series_table)` - Renvoie la table des [échantillons](../../engines/table-engines/integrations/time-series.md#samples-table)
utilisée par la table `db_name.time_series_table` dont le moteur est [TimeSeries](../../engines/table-engines/integrations/time-series.md):

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries SAMPLES samples_table
```

La fonction fonctionne également si la table *samples* est une table interne :

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries SAMPLES INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

Les requêtes suivantes sont équivalentes :

```sql
SELECT * FROM timeSeriesSamples(db_name.time_series_table);
SELECT * FROM timeSeriesSamples('db_name.time_series_table');
SELECT * FROM timeSeriesSamples('db_name', 'time_series_table');
```

:::note
La fonction `timeSeriesSamples` a pour alias `timeSeriesData`, conservé pour des raisons de compatibilité descendante.
:::