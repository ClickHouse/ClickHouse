# system.time_zones {#system-time_zones}

Contains a list of time zones that are supported by the ClickHouse server. This list of timezones might vary depending on the version of ClickHouse.

Columns:

-   `time_zone` (String) — List of supported time zones.

**Example**

``` sql
SELECT * FROM system.time_zones LIMIT 10
```

``` text
┌─time_zone──────────┐
│ Africa/Abidjan     │
│ Africa/Accra       │
│ Africa/Addis_Ababa │
│ Africa/Algiers     │
│ Africa/Asmara      │
│ Africa/Asmera      │
│ Africa/Bamako      │
│ Africa/Bangui      │
│ Africa/Banjul      │
│ Africa/Bissau      │
└────────────────────┘
```

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/time_zones) <!--hide-->
