# system.time_zones {#system-time_zones}

包含 ClickHouse 服务器支持的时区列表. 此时区列表可能因 ClickHouse 的版本而异

列信息:

-   `time_zone` (String) — List of supported time zones.

**示例**

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

[原始文章](https://clickhouse.com/docs/en/operations/system-tables/time_zones) <!--hide-->
