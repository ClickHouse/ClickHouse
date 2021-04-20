---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: IPv6
---

## IPv6 {#ipv6}

`IPv6` est un domaine basé sur `FixedString(16)` tapez et sert de remplacement typé pour stocker des valeurs IPv6. Il fournit un stockage compact avec le format d'entrée-sortie convivial et les informations de type de colonne sur l'inspection.

### Utilisation De Base {#basic-usage}

``` sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY url;

DESCRIBE TABLE hits;
```

``` text
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ url  │ String │              │                    │         │                  │
│ from │ IPv6   │              │                    │         │                  │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┘
```

Ou vous pouvez utiliser `IPv6` domaine comme l'un des principaux:

``` sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY from;
```

`IPv6` le domaine prend en charge l'entrée personnalisée en tant que chaînes IPv6:

``` sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '2a02:aa08:e000:3100::2')('https://clickhouse.tech', '2001:44c8:129:2632:33:0:252:2')('https://clickhouse.tech/docs/en/', '2a02:e980:1e::1');

SELECT * FROM hits;
```

``` text
┌─url────────────────────────────────┬─from──────────────────────────┐
│ https://clickhouse.tech          │ 2001:44c8:129:2632:33:0:252:2 │
│ https://clickhouse.tech/docs/en/ │ 2a02:e980:1e::1               │
│ https://wikipedia.org              │ 2a02:aa08:e000:3100::2        │
└────────────────────────────────────┴───────────────────────────────┘
```

Les valeurs sont stockées sous forme binaire compacte:

``` sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

``` text
┌─toTypeName(from)─┬─hex(from)────────────────────────┐
│ IPv6             │ 200144C8012926320033000002520002 │
└──────────────────┴──────────────────────────────────┘
```

Les valeurs de domaine ne sont pas implicitement convertibles en types autres que `FixedString(16)`.
Si vous voulez convertir `IPv6` valeur à une chaîne, vous devez le faire explicitement avec `IPv6NumToString()` fonction:

``` sql
SELECT toTypeName(s), IPv6NumToString(from) as s FROM hits LIMIT 1;
```

``` text
┌─toTypeName(IPv6NumToString(from))─┬─s─────────────────────────────┐
│ String                            │ 2001:44c8:129:2632:33:0:252:2 │
└───────────────────────────────────┴───────────────────────────────┘
```

Ou coulé à un `FixedString(16)` valeur:

``` sql
SELECT toTypeName(i), CAST(from as FixedString(16)) as i FROM hits LIMIT 1;
```

``` text
┌─toTypeName(CAST(from, 'FixedString(16)'))─┬─i───────┐
│ FixedString(16)                           │  ��� │
└───────────────────────────────────────────┴─────────┘
```

[Article Original](https://clickhouse.tech/docs/en/data_types/domains/ipv6) <!--hide-->
