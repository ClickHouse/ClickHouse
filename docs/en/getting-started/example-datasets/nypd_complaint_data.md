---
slug: /en/getting-started/example-datasets/nypd_complaint_data
sidebar_label: NYPD Complaint Data
description: "Ingest and query Tab Separated Value data in 5 steps"
title: NYPD Complaint Data
---

Tab separated value, or TSV, files are common and may include field headings as the first line of the file. ClickHouse can ingest TSVs, and also can query TSVs without ingesting the files.  This guide covers both of these cases. If you need to query or ingest CSV files, the same techniques work, simply substitute `TSV` with `CSV` in your format arguments.

While working through this guide you will:
- **Investigate**: Query the structure and content of the TSV file.
- **Determine the target ClickHouse schema**: Choose proper data types and map the existing data to those types.
- **Create a ClickHouse table**.
- **Preprocess and stream** the data to ClickHouse.
- **Run some queries** against ClickHouse.

The dataset used in this guide comes from the NYC Open Data team, and contains data about "all valid felony, misdemeanor, and violation crimes reported to the New York City Police Department (NYPD)". At the time of writing, the data file is 166MB, but it is updated regularly.

**Source**: [data.cityofnewyork.us](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Current-Year-To-Date-/5uac-w243)
**Terms of use**: https://www1.nyc.gov/home/terms-of-use.page

## Prerequisites
- Download the dataset by visiting the [NYPD Complaint Data Current (Year To Date)](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Current-Year-To-Date-/5uac-w243) page, clicking the Export button, and choosing **TSV for Excel**.
- Install [ClickHouse server and client](../../getting-started/install.md).
- [Launch](../../getting-started/install.md#launch) ClickHouse server, and connect with `clickhouse-client`

### A note about the commands described in this guide
There are two types of commands in this guide:
- Some of the commands are querying the TSV files, these are run at the command prompt.
- The rest of the commands are querying ClickHouse, and these are run in the `clickhouse-client` or Play UI.

:::note
The examples in this guide assume that you have saved the TSV file to `${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv`, please adjust the commands if needed.
:::

## Familiarize yourself with the TSV file

Before starting to work with the ClickHouse database familiarize yourself with the data.

### Look at the fields in the source TSV file

This is an example of a command to query a TSV file, but don't run it yet.
```sh
clickhouse-local --query \
"describe file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')"
```

Sample response
```response
CMPLNT_NUM                  Nullable(Float64)
ADDR_PCT_CD                 Nullable(Float64)
BORO_NM                     Nullable(String)
CMPLNT_FR_DT                Nullable(String)
CMPLNT_FR_TM                Nullable(String)
```

:::tip
Most of the time the above command will let you know which fields in the input data are numeric, and which are strings, and which are tuples.  This is not always the case.  Because ClickHouse is routineley used with datasets containing billions of records there is a default number (100) of rows examined to [infer the schema](/en/integrations/data-formats/json/inference) in order to avoid parsing billions of rows to infer the schema. The response below may not match what you see, as the dataset is updated several times each year. Looking at the Data Dictionary you can see that CMPLNT_NUM is specified as text, and not numeric.  By overriding the default of 100 rows for inference with the setting `SETTINGS input_format_max_rows_to_read_for_schema_inference=2000`
you can get a better idea of the content.

Note: as of version 22.5 the default is now 25,000 rows for inferring the schema, so only change the setting if you are on an older version or if you need more than 25,000 rows to be sampled.
:::

Run this command at your command prompt.  You will be using `clickhouse-local` to query the data in the TSV file you downloaded.
```sh
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"describe file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')"
```

Result:
```response
CMPLNT_NUM        Nullable(String)
ADDR_PCT_CD       Nullable(Float64)
BORO_NM           Nullable(String)
CMPLNT_FR_DT      Nullable(String)
CMPLNT_FR_TM      Nullable(String)
CMPLNT_TO_DT      Nullable(String)
CMPLNT_TO_TM      Nullable(String)
CRM_ATPT_CPTD_CD  Nullable(String)
HADEVELOPT        Nullable(String)
HOUSING_PSA       Nullable(Float64)
JURISDICTION_CODE Nullable(Float64)
JURIS_DESC        Nullable(String)
KY_CD             Nullable(Float64)
LAW_CAT_CD        Nullable(String)
LOC_OF_OCCUR_DESC Nullable(String)
OFNS_DESC         Nullable(String)
PARKS_NM          Nullable(String)
PATROL_BORO       Nullable(String)
PD_CD             Nullable(Float64)
PD_DESC           Nullable(String)
PREM_TYP_DESC     Nullable(String)
RPT_DT            Nullable(String)
STATION_NAME      Nullable(String)
SUSP_AGE_GROUP    Nullable(String)
SUSP_RACE         Nullable(String)
SUSP_SEX          Nullable(String)
TRANSIT_DISTRICT  Nullable(Float64)
VIC_AGE_GROUP     Nullable(String)
VIC_RACE          Nullable(String)
VIC_SEX           Nullable(String)
X_COORD_CD        Nullable(Float64)
Y_COORD_CD        Nullable(Float64)
Latitude          Nullable(Float64)
Longitude         Nullable(Float64)
Lat_Lon           Tuple(Nullable(Float64), Nullable(Float64))
New Georeferenced Column Nullable(String)
```

At this point you should check that the columns in the TSV file match the names and types specified in the **Columns in this Dataset** section of the [dataset web page](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Current-Year-To-Date-/5uac-w243).  The data types are not very specific, all numeric fields are set to `Nullable(Float64)`, and all other fields are `Nullable(String)`.  When you create a ClickHouse table to store the data you can specify more appropriate and performant types.

### Determine the proper schema

In order to figure out what types should be used for the fields it is necessary to know what the data looks like. For example, the field `JURISDICTION_CODE` is a numeric: should it be a `UInt8`, or an `Enum`, or is `Float64` appropriate?

```sql
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select JURISDICTION_CODE, count() FROM
 file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
 GROUP BY JURISDICTION_CODE
 ORDER BY JURISDICTION_CODE
 FORMAT PrettyCompact"
```

Result:
```response
┌─JURISDICTION_CODE─┬─count()─┐
│                 0 │  188875 │
│                 1 │    4799 │
│                 2 │   13833 │
│                 3 │     656 │
│                 4 │      51 │
│                 6 │       5 │
│                 7 │       2 │
│                 9 │      13 │
│                11 │      14 │
│                12 │       5 │
│                13 │       2 │
│                14 │      70 │
│                15 │      20 │
│                72 │     159 │
│                87 │       9 │
│                88 │      75 │
│                97 │     405 │
└───────────────────┴─────────┘
```

The query response shows that the `JURISDICTION_CODE` fits well in a `UInt8`.

Similarly, look at some of the `String` fields and see if they are well suited to being `DateTime` or [`LowCardinality(String)`](../../sql-reference/data-types/lowcardinality.md) fields.

For example, the field `PARKS_NM` is described as "Name of NYC park, playground or greenspace of occurrence, if applicable (state parks are not included)".  The names of parks in New York City may be a good candidate for a `LowCardinality(String)`:

```sh
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select count(distinct PARKS_NM) FROM
 file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
 FORMAT PrettyCompact"
```

Result:
```response
┌─uniqExact(PARKS_NM)─┐
│                 319 │
└─────────────────────┘
```

Have a look at some of the park names:
```sql
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select distinct PARKS_NM FROM
 file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
 LIMIT 10
 FORMAT PrettyCompact"
```

Result:
```response
┌─PARKS_NM───────────────────┐
│ (null)                     │
│ ASSER LEVY PARK            │
│ JAMES J WALKER PARK        │
│ BELT PARKWAY/SHORE PARKWAY │
│ PROSPECT PARK              │
│ MONTEFIORE SQUARE          │
│ SUTTON PLACE PARK          │
│ JOYCE KILMER PARK          │
│ ALLEY ATHLETIC PLAYGROUND  │
│ ASTORIA PARK               │
└────────────────────────────┘
```

The dataset in use at the time of writing has only a few hundred distinct parks and playgrounds in the `PARK_NM` column.  This is a small number based on the [LowCardinality](../../sql-reference/data-types/lowcardinality.md#lowcardinality-dscr) recommendation to stay below 10,000 distinct strings in a `LowCardinality(String)` field.

### DateTime fields
Based on the **Columns in this Dataset** section of the [dataset web page](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Current-Year-To-Date-/5uac-w243) there are date and time fields for the start and end of the reported event.  Looking at the min and max of the `CMPLNT_FR_DT` and `CMPLT_TO_DT` gives an idea of whether or not the fields are always populated:

```sh title="CMPLNT_FR_DT"
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select min(CMPLNT_FR_DT), max(CMPLNT_FR_DT) FROM
file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
FORMAT PrettyCompact"
```

Result:
```response
┌─min(CMPLNT_FR_DT)─┬─max(CMPLNT_FR_DT)─┐
│ 01/01/1973        │ 12/31/2021        │
└───────────────────┴───────────────────┘
```

```sh title="CMPLNT_TO_DT"
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select min(CMPLNT_TO_DT), max(CMPLNT_TO_DT) FROM
file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
FORMAT PrettyCompact"
```

Result:
```response
┌─min(CMPLNT_TO_DT)─┬─max(CMPLNT_TO_DT)─┐
│                   │ 12/31/2021        │
└───────────────────┴───────────────────┘
```

```sh title="CMPLNT_FR_TM"
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select min(CMPLNT_FR_TM), max(CMPLNT_FR_TM) FROM
file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
FORMAT PrettyCompact"
```

Result:
```response
┌─min(CMPLNT_FR_TM)─┬─max(CMPLNT_FR_TM)─┐
│ 00:00:00          │ 23:59:00          │
└───────────────────┴───────────────────┘
```

```sh title="CMPLNT_TO_TM"
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select min(CMPLNT_TO_TM), max(CMPLNT_TO_TM) FROM
file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
FORMAT PrettyCompact"
```

Result:
```response
┌─min(CMPLNT_TO_TM)─┬─max(CMPLNT_TO_TM)─┐
│ (null)            │ 23:59:00          │
└───────────────────┴───────────────────┘
```

## Make a plan

Based on the above investigation:
- `JURISDICTION_CODE` should be cast as `UInt8`.
- `PARKS_NM` should be cast to `LowCardinality(String)`
- `CMPLNT_FR_DT` and `CMPLNT_FR_TM` are always populated (possibly with a default time of `00:00:00`)
- `CMPLNT_TO_DT` and `CMPLNT_TO_TM` may be empty
- Dates and times are stored in separate fields in the source
- Dates are `mm/dd/yyyy` format
- Times are `hh:mm:ss` format
- Dates and times can be concatenated into DateTime types
- There are some dates before January 1st 1970, which means we need a 64 bit DateTime

:::note
There are many more changes to be made to the types, they all can be determined by following the same investigation steps.  Look at the number of distinct strings in a field, the min and max of the numerics, and make your decisions.  The table schema that is given later in the guide has many low cardinality strings and unsigned integer fields and very few floating point numerics.
:::

## Concatenate the date and time fields

To concatenate the date and time fields `CMPLNT_FR_DT` and `CMPLNT_FR_TM` into a single `String` that can be cast to a `DateTime`, select the two fields joined by the concatenation operator: `CMPLNT_FR_DT || ' ' || CMPLNT_FR_TM`.  The `CMPLNT_TO_DT` and `CMPLNT_TO_TM` fields are handled similarly.

```sh
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select CMPLNT_FR_DT || ' ' || CMPLNT_FR_TM AS complaint_begin FROM
file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
LIMIT 10
FORMAT PrettyCompact"
```

Result:
```response
┌─complaint_begin─────┐
│ 07/29/2010 00:01:00 │
│ 12/01/2011 12:00:00 │
│ 04/01/2017 15:00:00 │
│ 03/26/2018 17:20:00 │
│ 01/01/2019 00:00:00 │
│ 06/14/2019 00:00:00 │
│ 11/29/2021 20:00:00 │
│ 12/04/2021 00:35:00 │
│ 12/05/2021 12:50:00 │
│ 12/07/2021 20:30:00 │
└─────────────────────┘
```

## Convert the date and time String to a DateTime64 type

Earlier in the guide we discovered that there are dates in the TSV file before January 1st 1970, which means that we need a 64 bit DateTime type for the dates.  The dates also need to be converted from `MM/DD/YYYY` to `YYYY/MM/DD` format.  Both of these can be done with [`parseDateTime64BestEffort()`](../../sql-reference/functions/type-conversion-functions.md#parsedatetime64besteffort).

```sh
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"WITH (CMPLNT_FR_DT || ' ' || CMPLNT_FR_TM) AS CMPLNT_START,
      (CMPLNT_TO_DT || ' ' || CMPLNT_TO_TM) AS CMPLNT_END
select parseDateTime64BestEffort(CMPLNT_START) AS complaint_begin,
       parseDateTime64BestEffortOrNull(CMPLNT_END) AS complaint_end
FROM file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
ORDER BY complaint_begin ASC
LIMIT 25
FORMAT PrettyCompact"
```

Lines 2 and 3 above contain the concatenation from the previous step, and lines 4 and 5 above parse the strings into `DateTime64`.  As the complaint end time is not guaranteed to exist `parseDateTime64BestEffortOrNull` is used.

Result:
```response
┌─────────complaint_begin─┬───────────complaint_end─┐
│ 1925-01-01 10:00:00.000 │ 2021-02-12 09:30:00.000 │
│ 1925-01-01 11:37:00.000 │ 2022-01-16 11:49:00.000 │
│ 1925-01-01 15:00:00.000 │ 2021-12-31 00:00:00.000 │
│ 1925-01-01 15:00:00.000 │ 2022-02-02 22:00:00.000 │
│ 1925-01-01 19:00:00.000 │ 2022-04-14 05:00:00.000 │
│ 1955-09-01 19:55:00.000 │ 2022-08-01 00:45:00.000 │
│ 1972-03-17 11:40:00.000 │ 2022-03-17 11:43:00.000 │
│ 1972-05-23 22:00:00.000 │ 2022-05-24 09:00:00.000 │
│ 1972-05-30 23:37:00.000 │ 2022-05-30 23:50:00.000 │
│ 1972-07-04 02:17:00.000 │                    ᴺᵁᴸᴸ │
│ 1973-01-01 00:00:00.000 │                    ᴺᵁᴸᴸ │
│ 1975-01-01 00:00:00.000 │                    ᴺᵁᴸᴸ │
│ 1976-11-05 00:01:00.000 │ 1988-10-05 23:59:00.000 │
│ 1977-01-01 00:00:00.000 │ 1977-01-01 23:59:00.000 │
│ 1977-12-20 00:01:00.000 │                    ᴺᵁᴸᴸ │
│ 1981-01-01 00:01:00.000 │                    ᴺᵁᴸᴸ │
│ 1981-08-14 00:00:00.000 │ 1987-08-13 23:59:00.000 │
│ 1983-01-07 00:00:00.000 │ 1990-01-06 00:00:00.000 │
│ 1984-01-01 00:01:00.000 │ 1984-12-31 23:59:00.000 │
│ 1985-01-01 12:00:00.000 │ 1987-12-31 15:00:00.000 │
│ 1985-01-11 09:00:00.000 │ 1985-12-31 12:00:00.000 │
│ 1986-03-16 00:05:00.000 │ 2022-03-16 00:45:00.000 │
│ 1987-01-07 00:00:00.000 │ 1987-01-09 00:00:00.000 │
│ 1988-04-03 18:30:00.000 │ 2022-08-03 09:45:00.000 │
│ 1988-07-29 12:00:00.000 │ 1990-07-27 22:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```
:::note
The dates shown as `1925` above are from errors in the data.  There are several records in the original data with dates in the years `1019` - `1022` that should be `2019` - `2022`.  They are being stored as Jan 1st 1925 as that is the earliest date with a 64 bit DateTime.
:::

## Create a table

The decisions made above on the data types used for the columns are reflected in the table schema
below. We also need to decide on the `ORDER BY` and `PRIMARY KEY` used for the table.  At least one
of `ORDER BY` or `PRIMARY KEY` must be specified.  Here are some guidelines on deciding on the
columns to includes in `ORDER BY`, and more information is in the *Next Steps* section at the end
of this document.

### Order By and Primary Key clauses

- The `ORDER BY` tuple should include fields that are used in query filters
- To maximize compression on disk the `ORDER BY` tuple should be ordered by ascending cardinality
- If it exists, the `PRIMARY KEY` tuple must be a subset of the `ORDER BY` tuple
- If only `ORDER BY` is specified, then the same tuple will be used as `PRIMARY KEY`
- The primary key index is created using the `PRIMARY KEY` tuple if specified, otherwise the `ORDER BY` tuple
- The `PRIMARY KEY` index is kept in main memory

Looking at the dataset and the questions that might be answered by querying it we might
decide that we would look at the types of crimes reported over time in the five boroughs of
New York City.  These fields might be then included in the `ORDER BY`:

| Column      | Description (from the data dictionary)                 |
| ----------- | --------------------------------------------------- |
| OFNS_DESC   | Description of offense corresponding with key code     |
| RPT_DT      | Date event was reported to police                      |
| BORO_NM     | The name of the borough in which the incident occurred |


Querying the TSV file for the cardinality of the three candidate columns:

```bash
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select formatReadableQuantity(uniq(OFNS_DESC)) as cardinality_OFNS_DESC,
        formatReadableQuantity(uniq(RPT_DT)) as cardinality_RPT_DT,
        formatReadableQuantity(uniq(BORO_NM)) as cardinality_BORO_NM
  FROM
  file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
  FORMAT PrettyCompact"
```

Result:
```response
┌─cardinality_OFNS_DESC─┬─cardinality_RPT_DT─┬─cardinality_BORO_NM─┐
│ 60.00                 │ 306.00             │ 6.00                │
└───────────────────────┴────────────────────┴─────────────────────┘
```
Ordering by cardinality, the `ORDER BY` becomes:

```
ORDER BY ( BORO_NM, OFNS_DESC, RPT_DT )
```
:::note
The table below will use more easily read column names, the above names will be mapped to
```
ORDER BY ( borough, offense_description, date_reported )
```
:::

Putting together the changes to data types and the `ORDER BY` tuple gives this table structure:

```sql
CREATE TABLE NYPD_Complaint (
    complaint_number     String,
    precinct             UInt8,
    borough              LowCardinality(String),
    complaint_begin      DateTime64(0,'America/New_York'),
    complaint_end        DateTime64(0,'America/New_York'),
    was_crime_completed  String,
    housing_authority    String,
    housing_level_code   UInt32,
    jurisdiction_code    UInt8,
    jurisdiction         LowCardinality(String),
    offense_code         UInt8,
    offense_level        LowCardinality(String),
    location_descriptor  LowCardinality(String),
    offense_description  LowCardinality(String),
    park_name            LowCardinality(String),
    patrol_borough       LowCardinality(String),
    PD_CD                UInt16,
    PD_DESC              String,
    location_type        LowCardinality(String),
    date_reported        Date,
    transit_station      LowCardinality(String),
    suspect_age_group    LowCardinality(String),
    suspect_race         LowCardinality(String),
    suspect_sex          LowCardinality(String),
    transit_district     UInt8,
    victim_age_group     LowCardinality(String),
    victim_race          LowCardinality(String),
    victim_sex           LowCardinality(String),
    NY_x_coordinate      UInt32,
    NY_y_coordinate      UInt32,
    Latitude             Float64,
    Longitude            Float64
) ENGINE = MergeTree
  ORDER BY ( borough, offense_description, date_reported )
```

### Finding the primary key of a table

The ClickHouse `system` database, specifically `system.table` has all of the information about the table you
just created.  This query shows the `ORDER BY` (sorting key), and the `PRIMARY KEY`:
```sql
SELECT
    partition_key,
    sorting_key,
    primary_key,
    table
FROM system.tables
WHERE table = 'NYPD_Complaint'
FORMAT Vertical
```

Response
```response
Query id: 6a5b10bf-9333-4090-b36e-c7f08b1d9e01

Row 1:
──────
partition_key:
sorting_key:   borough, offense_description, date_reported
primary_key:   borough, offense_description, date_reported
table:         NYPD_Complaint

1 row in set. Elapsed: 0.001 sec.
```

## Preprocess and Import Data {#preprocess-import-data}

We will use `clickhouse-local` tool for data preprocessing and `clickhouse-client` to upload it.

### `clickhouse-local` arguments used

:::tip
`table='input'` appears in the arguments to clickhouse-local below.  clickhouse-local takes the provided input (`cat ${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv`) and inserts the input into a table.  By default the table is named `table`.  In this guide the name of the table is set to `input` to make the data flow clearer. The final argument to clickhouse-local is a query that selects from the table (`FROM input`) which is then piped to `clickhouse-client` to populate the table `NYPD_Complaint`.
:::

```sql
cat ${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv \
  | clickhouse-local --table='input' --input-format='TSVWithNames' \
  --input_format_max_rows_to_read_for_schema_inference=2000 \
  --query "
    WITH (CMPLNT_FR_DT || ' ' || CMPLNT_FR_TM) AS CMPLNT_START,
     (CMPLNT_TO_DT || ' ' || CMPLNT_TO_TM) AS CMPLNT_END
    SELECT
      CMPLNT_NUM                                  AS complaint_number,
      ADDR_PCT_CD                                 AS precinct,
      BORO_NM                                     AS borough,
      parseDateTime64BestEffort(CMPLNT_START)     AS complaint_begin,
      parseDateTime64BestEffortOrNull(CMPLNT_END) AS complaint_end,
      CRM_ATPT_CPTD_CD                            AS was_crime_completed,
      HADEVELOPT                                  AS housing_authority_development,
      HOUSING_PSA                                 AS housing_level_code,
      JURISDICTION_CODE                           AS jurisdiction_code,
      JURIS_DESC                                  AS jurisdiction,
      KY_CD                                       AS offense_code,
      LAW_CAT_CD                                  AS offense_level,
      LOC_OF_OCCUR_DESC                           AS location_descriptor,
      OFNS_DESC                                   AS offense_description,
      PARKS_NM                                    AS park_name,
      PATROL_BORO                                 AS patrol_borough,
      PD_CD,
      PD_DESC,
      PREM_TYP_DESC                               AS location_type,
      toDate(parseDateTimeBestEffort(RPT_DT))     AS date_reported,
      STATION_NAME                                AS transit_station,
      SUSP_AGE_GROUP                              AS suspect_age_group,
      SUSP_RACE                                   AS suspect_race,
      SUSP_SEX                                    AS suspect_sex,
      TRANSIT_DISTRICT                            AS transit_district,
      VIC_AGE_GROUP                               AS victim_age_group,
      VIC_RACE                                    AS victim_race,
      VIC_SEX                                     AS victim_sex,
      X_COORD_CD                                  AS NY_x_coordinate,
      Y_COORD_CD                                  AS NY_y_coordinate,
      Latitude,
      Longitude
    FROM input" \
  | clickhouse-client --query='INSERT INTO NYPD_Complaint FORMAT TSV'
```

## Validate the Data {#validate-data}

:::note
The dataset changes once or more per year, your counts may not match what is in this document.
:::

Query:

```sql
SELECT count()
FROM NYPD_Complaint
```

Result:

```text
┌─count()─┐
│  208993 │
└─────────┘

1 row in set. Elapsed: 0.001 sec.
```

The size of the dataset in ClickHouse is just 12% of the original TSV file, compare the size of the original TSV file with the size of the table:

Query:

```sql
SELECT formatReadableSize(total_bytes)
FROM system.tables
WHERE name = 'NYPD_Complaint'
```

Result:
```text
┌─formatReadableSize(total_bytes)─┐
│ 8.63 MiB                        │
└─────────────────────────────────┘
```


## Run Some Queries {#run-queries}

### Query 1. Compare the number of complaints by month

Query:

```sql
SELECT
    dateName('month', date_reported) AS month,
    count() AS complaints,
    bar(complaints, 0, 50000, 80)
FROM NYPD_Complaint
GROUP BY month
ORDER BY complaints DESC
```

Result:
```response
Query id: 7fbd4244-b32a-4acf-b1f3-c3aa198e74d9

┌─month─────┬─complaints─┬─bar(count(), 0, 50000, 80)───────────────────────────────┐
│ March     │      34536 │ ███████████████████████████████████████████████████████▎ │
│ May       │      34250 │ ██████████████████████████████████████████████████████▋  │
│ April     │      32541 │ ████████████████████████████████████████████████████     │
│ January   │      30806 │ █████████████████████████████████████████████████▎       │
│ February  │      28118 │ ████████████████████████████████████████████▊            │
│ November  │       7474 │ ███████████▊                                             │
│ December  │       7223 │ ███████████▌                                             │
│ October   │       7070 │ ███████████▎                                             │
│ September │       6910 │ ███████████                                              │
│ August    │       6801 │ ██████████▊                                              │
│ June      │       6779 │ ██████████▋                                              │
│ July      │       6485 │ ██████████▍                                              │
└───────────┴────────────┴──────────────────────────────────────────────────────────┘

12 rows in set. Elapsed: 0.006 sec. Processed 208.99 thousand rows, 417.99 KB (37.48 million rows/s., 74.96 MB/s.)
```

### Query 2. Compare total number of complaints by Borough

Query:

```sql
SELECT
    borough,
    count() AS complaints,
    bar(complaints, 0, 125000, 60)
FROM NYPD_Complaint
GROUP BY borough
ORDER BY complaints DESC
```

Result:
```response
Query id: 8cdcdfd4-908f-4be0-99e3-265722a2ab8d

┌─borough───────┬─complaints─┬─bar(count(), 0, 125000, 60)──┐
│ BROOKLYN      │      57947 │ ███████████████████████████▋ │
│ MANHATTAN     │      53025 │ █████████████████████████▍   │
│ QUEENS        │      44875 │ █████████████████████▌       │
│ BRONX         │      44260 │ █████████████████████▏       │
│ STATEN ISLAND │       8503 │ ████                         │
│ (null)        │        383 │ ▏                            │
└───────────────┴────────────┴──────────────────────────────┘

6 rows in set. Elapsed: 0.008 sec. Processed 208.99 thousand rows, 209.43 KB (27.14 million rows/s., 27.20 MB/s.)
```

## Next Steps

[A Practical Introduction to Sparse Primary Indexes in ClickHouse](/docs/en/guides/best-practices/sparse-primary-indexes.md) discusses the differences in ClickHouse indexing compared to traditional relational databases, how ClickHouse builds and uses a sparse primary index, and indexing best practices.
