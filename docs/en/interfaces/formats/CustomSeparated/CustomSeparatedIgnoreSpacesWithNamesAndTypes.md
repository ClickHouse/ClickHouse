---
description: 'Documentation for the CustomSeparatedIgnoreSpacesWithNamesAndTypes format'
keywords: ['CustomSeparatedIgnoreSpacesWithNamesAndTypes']
slug: /interfaces/formats/CustomSeparatedIgnoreSpacesWithNamesAndTypes
title: 'CustomSeparatedIgnoreSpacesWithNamesAndTypes'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     |        |       |

## Description {#description}

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following txt file, named as `football.txt`:

```text
row('date'; 'season'; 'home_team'; 'away_team'; 'home_team_goals'; 'away_team_goals'), row('Date'; 'Int16'; 'LowCardinality(String)'; 'LowCardinality(String)'; 'Int8'; 'Int8'), row('2022-04-30'; 2021; 'Sutton United'; 'Bradford City'; 1; 4), row( '2022-04-30'; 2021; 'Swindon Town'; 'Barrow'; 2; 1), row( '2022-04-30'; 2021; 'Tranmere Rovers'; 'Oldham Athletic'; 2; 0), row('2022-05-02'; 2021; 'Salford City'; 'Mansfield Town'; 2; 2), row('2022-05-02'; 2021; 'Port Vale'; 'Newport County'; 1; 2), row('2022-05-07'; 2021; 'Barrow'; 'Northampton Town'; 1; 3), row('2022-05-07'; 2021; 'Bradford City'; 'Carlisle United'; 2; 0), row('2022-05-07'; 2021; 'Bristol Rovers'; 'Scunthorpe United'; 7; 0), row('2022-05-07'; 2021; 'Exeter City'; 'Port Vale'; 0; 1), row('2022-05-07'; 2021; 'Harrogate Town A.F.C.'; 'Sutton United'; 0; 2), row('2022-05-07'; 2021; 'Hartlepool United'; 'Colchester United'; 0; 2), row('2022-05-07'; 2021; 'Leyton Orient'; 'Tranmere Rovers'; 0; 1), row('2022-05-07'; 2021; 'Mansfield Town'; 'Forest Green Rovers'; 2; 2), row('2022-05-07'; 2021; 'Newport County'; 'Rochdale'; 0; 2), row('2022-05-07'; 2021; 'Oldham Athletic'; 'Crawley Town'; 3; 3), row('2022-05-07'; 2021; 'Stevenage Borough'; 'Salford City'; 4; 2), row('2022-05-07'; 2021; 'Walsall'; 'Swindon Town'; 0; 3)
```

Configure the custom delimiter settings:

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.txt' FORMAT CustomSeparatedIgnoreSpacesWithNamesAndTypes;
```

## Format settings {#format-settings}