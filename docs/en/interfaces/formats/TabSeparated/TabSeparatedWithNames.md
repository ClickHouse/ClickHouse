---
alias: ['TSVWithNames']
description: 'Documentation for the TabSeparatedWithNames format'
input_format: true
keywords: ['TabSeparatedWithNames']
output_format: true
slug: /interfaces/formats/TabSeparatedWithNames
title: 'TabSeparatedWithNames'
doc_type: 'reference'
---

| Input | Output | Alias                          |
|-------|--------|--------------------------------|
|     ✔    |     ✔     | `TSVWithNames`, `RawWithNames` |

## Description {#description}

Differs from the [`TabSeparated`](./TabSeparated.md) format in that the column names are written in the first row.

During parsing, the first row is expected to contain the column names. You can use column names to determine their position and to check their correctness.

:::note
If setting [`input_format_with_names_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from the input data will be mapped to the columns of the table by their names, columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Otherwise, the first row will be skipped.
:::

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using the following tsv file, named as `football.tsv`:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparatedWithNames;
```

### Reading data {#reading-data}

Read data using the `TabSeparatedWithNames` format:

```sql
SELECT *
FROM football
FORMAT TabSeparatedWithNames
```

The output will be in tab separated format:

```tsv
date    season  home_team       away_team       home_team_goals away_team_goals
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

## Format settings {#format-settings}