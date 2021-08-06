---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 53
toc_title: Trabajando con UUID
---

# Funciones para trabajar con UUID {#functions-for-working-with-uuid}

Las funciones para trabajar con UUID se enumeran a continuación.

## GenerateUUIDv4 {#uuid-function-generate}

Genera el [UUID](../../sql-reference/data-types/uuid.md) de [versión 4](https://tools.ietf.org/html/rfc4122#section-4.4).

``` sql
generateUUIDv4()
```

**Valor devuelto**

El valor de tipo UUID.

**Ejemplo de uso**

En este ejemplo se muestra la creación de una tabla con la columna de tipo UUID e insertar un valor en la tabla.

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

## paraUUID (x) {#touuid-x}

Convierte el valor de tipo de cadena en tipo UUID.

``` sql
toUUID(String)
```

**Valor devuelto**

El valor de tipo UUID.

**Ejemplo de uso**

``` sql
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid
```

``` text
┌─────────────────────────────────uuid─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0 │
└──────────────────────────────────────┘
```

## UUIDStringToNum {#uuidstringtonum}

Acepta una cadena que contiene 36 caracteres en el formato `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, y lo devuelve como un conjunto de bytes en un [Cadena fija (16)](../../sql-reference/data-types/fixedstring.md).

``` sql
UUIDStringToNum(String)
```

**Valor devuelto**

Cadena fija (16)

**Ejemplos de uso**

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

Acepta un [Cadena fija (16)](../../sql-reference/data-types/fixedstring.md) valor, y devuelve una cadena que contiene 36 caracteres en formato de texto.

``` sql
UUIDNumToString(FixedString(16))
```

**Valor devuelto**

Cadena.

**Ejemplo de uso**

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

## Ver también {#see-also}

-   [dictGetUUID](ext-dict-functions.md#ext_dict_functions-other)

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/uuid_function/) <!--hide-->
