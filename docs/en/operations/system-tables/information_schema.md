# information_schema {#information-schema}

`INFORMATION_SCHEMA` (`information_schema`) is a system database that contains views. Using these views, you can get information about the metadata of database objects.

The structure and composition of system tables may change in different versions of the product, but the support of the `information_schema` makes it possible to change the structure of system tables without changing the method of access to metadata. Metadata requests do not depend on the DBMS used.

``` sql
SHOW TABLES FROM INFORMATION_SCHEMA;
```

``` text
┌─name─────┐
│ COLUMNS  │
│ SCHEMATA │
│ TABLES   │
│ VIEWS    │
└──────────┘
```

`INFORMATION_SCHEMA` contains the following views:

-   `SCHEMATA` — The view that can be used to get all the current database schemas.

``` sql
SELECT * FROM information_schema.schemata WHERE schema_name ILIKE 'information_schema' LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
catalog_name:                  INFORMATION_SCHEMA
schema_name:                   INFORMATION_SCHEMA
schema_owner:                  default
default_character_set_catalog: ᴺᵁᴸᴸ
default_character_set_schema:  ᴺᵁᴸᴸ
default_character_set_name:    ᴺᵁᴸᴸ
sql_path:                      ᴺᵁᴸᴸ
```

-   `TABLES` — The view that can be used to get all tables in the current database.

``` sql
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE (table_schema = currentDatabase() OR table_schema = '') AND table_name NOT LIKE '%inner%' LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
table_catalog: default
table_schema:  default
table_name:    describe_example
table_type:    BASE TABLE
```

-   `COLUMNS` — The view that can be used to get a list of table columns in the current database.

``` sql
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE (table_schema=currentDatabase() OR table_schema='') AND table_name NOT LIKE '%inner%' LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
table_catalog:            default
table_schema:             default
table_name:               describe_example
column_name:              id
ordinal_position:         1
column_default:
is_nullable:              0
data_type:                UInt64
character_maximum_length: ᴺᵁᴸᴸ
character_octet_length:   ᴺᵁᴸᴸ
numeric_precision:        64
numeric_precision_radix:  2
numeric_scale:            0
datetime_precision:       ᴺᵁᴸᴸ
character_set_catalog:    ᴺᵁᴸᴸ
character_set_schema:     ᴺᵁᴸᴸ
character_set_name:       ᴺᵁᴸᴸ
collation_catalog:        ᴺᵁᴸᴸ
collation_schema:         ᴺᵁᴸᴸ
collation_name:           ᴺᵁᴸᴸ
domain_catalog:           ᴺᵁᴸᴸ
domain_schema:            ᴺᵁᴸᴸ
domain_name:              ᴺᵁᴸᴸ
```

-   `VIEWS` — The view that can be used to get a list of all views in the current database.

``` sql
CREATE VIEW v (n Nullable(Int32), f Float64) AS SELECT n, f FROM t;
CREATE MATERIALIZED VIEW mv ENGINE = Null AS SELECT * FROM system.one;
SELECT * FROM information_schema.views WHERE table_schema = currentDatabase();
```

``` text
Row 1:
──────
table_catalog:              default
table_schema:               default
table_name:                 mv
view_definition:            SELECT * FROM system.one
check_option:               NONE
is_updatable:               NO
is_insertable_into:         YES
is_trigger_updatable:       NO
is_trigger_deletable:       NO
is_trigger_insertable_into: NO
```

**See Also**

-   [View](../../sql-reference/statements/create/view.md).
-   [system.tables](../../operations/system-tables/tables.md).
-   [system.columns](../../operations/system-tables/columns.md).
-   [system.views](../../operations/system-tables/views.md).
