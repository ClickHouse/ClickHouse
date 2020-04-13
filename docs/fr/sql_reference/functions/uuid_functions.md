---
machine_translated: true
machine_translated_rev: f865c9653f9df092694258e0ccdd733c339112f5
toc_priority: 53
toc_title: Travailler avec UUID
---

# Fonctions pour travailler avec UUID {#functions-for-working-with-uuid}

Les fonctions pour travailler avec UUID sont listées ci-dessous.

## generateUUIDv4 {#uuid-function-generate}

Génère le [UUID](../../sql_reference/data_types/uuid.md) de [la version 4](https://tools.ietf.org/html/rfc4122#section-4.4).

``` sql
generateUUIDv4()
```

**Valeur renvoyée**

La valeur de type UUID.

**Exemple d'utilisation**

Cet exemple montre la création d'une table avec la colonne de type UUID et l'insertion d'une valeur dans la table.

``` sql
CREATE TABLE t_uuid (x UUID) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4()

SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┐
│ f4bf890f-f9dc-4332-ad5c-0c18e73f28e9 │
└──────────────────────────────────────┘
```

## toUUID (x) {#touuid-x}

Convertit la valeur de type de chaîne en type UUID.

``` sql
toUUID(String)
```

**Valeur renvoyée**

La valeur de type UUID.

**Exemple d'utilisation**

``` sql
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid
```

``` text
┌─────────────────────────────────uuid─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0 │
└──────────────────────────────────────┘
```

## UUIDStringToNum {#uuidstringtonum}

Accepte une chaîne contenant 36 caractères dans le format `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, et le renvoie comme un ensemble d'octets dans un [FixedString (16)](../../sql_reference/data_types/fixedstring.md).

``` sql
UUIDStringToNum(String)
```

**Valeur renvoyée**

FixedString (16)

**Exemples d'utilisation**

``` sql
SELECT
    '612f3c40-5d3b-217e-707b-6a546a3d7b29' AS uuid,
    UUIDStringToNum(uuid) AS bytes
```

``` text
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## UUIDNumToString {#uuidnumtostring}

Accepte un [FixedString (16)](../../sql_reference/data_types/fixedstring.md) valeur, et renvoie une chaîne contenant 36 caractères au format texte.

``` sql
UUIDNumToString(FixedString(16))
```

**Valeur renvoyée**

Chaîne.

**Exemple d'utilisation**

``` sql
SELECT
    'a/<@];!~p{jTj={)' AS bytes,
    UUIDNumToString(toFixedString(bytes, 16)) AS uuid
```

``` text
┌─bytes────────────┬─uuid─────────────────────────────────┐
│ a/<@];!~p{jTj={) │ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │
└──────────────────┴──────────────────────────────────────┘
```

## Voir aussi {#see-also}

-   [dictGetUUID](ext_dict_functions.md#ext_dict_functions-other)

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/uuid_function/) <!--hide-->
