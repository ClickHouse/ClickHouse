---
slug: /en/engines/table-engines/integrations/hive
sidebar_position: 4
sidebar_label: maxminddb 
---

# MaxMindDB Engine

this engine allows integrating ClickHouse with [MaxMindDB](www.maxmind.com).


## Creating a Table

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    key_column_name [key_type],
    value_column_name [value_type]
    ...
) ENGINE = MaxMindDB(mmdb_file_path) PRIMARY KEY(key_column_name)
```

Engine parameters:
- `key_type`: The data type of the key column, which is used to look up values in the MaxMindDB. Common types include `String`, `IPv4`, `IPv6`.
- `key_column_name`: The name of the key column in the table schema.
- `value_type`: The data type of the value column, which stores the data retrieved from the MaxMindDB. Its nested type must be `String`.
- `value_column_name`: The name of the value column in the table schema.
- `mmdb_file_path`: The file path to the MaxMindDB file (usually with a .mmdb extension) that you want to integrate with ClickHouse. This file contains the geolocation data used for IP address lookups.

Example:
``` sql 
CREATE TABLE test_mmdb
(
    key String,
    value String
)
ENGINE = MaxMindDB('/path/to/mmdb/file')
PRIMARY KEY key
```

## Supported operations
Currently MaxMindDB Engine only supports select query.

### Selects
Because MaxMindDB doesn't support full-scan or range-scan, select query must contain where conditions with functions equal or in.

```sql
select * from test_mmdb where key in ('1.2.2.3', '1.2.2.2') \G
```

```
Row 1:
──────
key:   1.2.2.2
value: {
  "continent": {
    "code": "AS",
    "geoname_id": 6255147,
    "names": {
      "de": "Asien",
      "en": "Asia",
      "es": "Asia",
      "fr": "Asie",
      "ja": "アジア",
      "pt-BR": "Ásia",
      "ru": "Азия",
      "zh-CN": "亚洲"
    }
  },
  "country": {
    "geoname_id": 1814991,
    "iso_code": "CN",
    "names": {
      "de": "China",
      "en": "China",
      "es": "China",
      "fr": "Chine",
      "ja": "中国",
      "pt-BR": "China",
      "ru": "Китай",
      "zh-CN": "中国"
    }
  },
  "registered_country": {
    "geoname_id": 1814991,
    "iso_code": "CN",
    "names": {
      "de": "China",
      "en": "China",
      "es": "China",
      "fr": "Chine",
      "ja": "中国",
      "pt-BR": "China",
      "ru": "Китай",
      "zh-CN": "中国"
    }
  }
}

Row 2:
──────
key:   1.2.2.3
value: {
  "continent": {
    "code": "AS",
    "geoname_id": 6255147,
    "names": {
      "de": "Asien",
      "en": "Asia",
      "es": "Asia",
      "fr": "Asie",
      "ja": "アジア",
      "pt-BR": "Ásia",
      "ru": "Азия",
      "zh-CN": "亚洲"
    }
  },
  "country": {
    "geoname_id": 1814991,
    "iso_code": "CN",
    "names": {
      "de": "China",
      "en": "China",
      "es": "China",
      "fr": "Chine",
      "ja": "中国",
      "pt-BR": "China",
      "ru": "Китай",
      "zh-CN": "中国"
    }
  },
  "registered_country": {
    "geoname_id": 1814991,
    "iso_code": "CN",
    "names": {
      "de": "China",
      "en": "China",
      "es": "China",
      "fr": "Chine",
      "ja": "中国",
      "pt-BR": "China",
      "ru": "Китай",
      "zh-CN": "中国"
    }
  }
}
```
