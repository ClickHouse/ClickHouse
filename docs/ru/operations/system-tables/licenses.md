---
slug: /ru/operations/system-tables/licenses
---
# system.licenses {#system-tables_system.licenses}

Содержит информацию о лицензиях сторонних библиотек, которые находятся в директории [contrib](https://github.com/ClickHouse/ClickHouse/tree/master/contrib) исходных кодов ClickHouse.

Столбцы:

- `library_name` ([String](../../sql-reference/data-types/string.md)) — Название библиотеки, к которой относится лицензия.
- `license_type` ([String](../../sql-reference/data-types/string.md)) — Тип лицензии, например, Apache, MIT.
- `license_path` ([String](../../sql-reference/data-types/string.md)) — Путь к файлу с текстом лицензии.
- `license_text` ([String](../../sql-reference/data-types/string.md)) — Текст лицензии.

**Пример**

``` sql
SELECT library_name, license_type, license_path FROM system.licenses LIMIT 15
```

``` text
┌─library_name───────┬─license_type─┬─license_path────────────────────────┐
│ aws-c-common       │ Apache       │ /contrib/aws-c-common/LICENSE       │
│ base64             │ BSD 2-clause │ /contrib/aklomp-base64/LICENSE      │
│ brotli             │ MIT          │ /contrib/brotli/LICENSE             │
│ [...]              │ [...]        │ [...]                               │
└────────────────────┴──────────────┴─────────────────────────────────────┘

```
