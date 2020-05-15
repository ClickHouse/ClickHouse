---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 46
toc_title: GenerateRandom
---

# Generaterandom {#table_engines-generate}

El motor de tabla GenerateRandom produce datos aleatorios para el esquema de tabla determinado.

Ejemplos de uso:

-   Se usa en la prueba para poblar una tabla grande reproducible.
-   Generar entrada aleatoria para pruebas de fuzzing.

## Uso En El Servidor De Clickhouse {#usage-in-clickhouse-server}

``` sql
ENGINE = GenerateRandom(random_seed, max_string_length, max_array_length)
```

El `max_array_length` y `max_string_length` parámetros especifican la longitud máxima de todos
columnas y cadenas de matriz correspondientemente en los datos generados.

Generar motor de tabla sólo admite `SELECT` consulta.

Es compatible con todos [Tipos de datos](../../../sql-reference/data-types/index.md) que se pueden almacenar en una tabla excepto `LowCardinality` y `AggregateFunction`.

**Ejemplo:**

**1.** Configurar el `generate_engine_table` tabla:

``` sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** Consultar los datos:

``` sql
SELECT * FROM generate_engine_table LIMIT 3
```

``` text
┌─name─┬──────value─┐
│ c4xJ │ 1412771199 │
│ r    │ 1791099446 │
│ 7#$  │  124312908 │
└──────┴────────────┘
```

## Detalles De La implementación {#details-of-implementation}

-   No soportado:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   `INSERT`
    -   Indice
    -   Replicación

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/generate/) <!--hide-->
