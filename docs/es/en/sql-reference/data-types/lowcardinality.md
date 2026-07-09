---
description: 'Documentación sobre la optimización LowCardinality para columnas String'
sidebar_label: 'LowCardinality(T)'
sidebar_position: 42
slug: /sql-reference/data-types/lowcardinality
title: 'LowCardinality(T)'
doc_type: 'reference'
---

Modifica la representación interna de otros tipos de datos para codificarlos mediante diccionario.

<div id="syntax">
  ## Sintaxis
</div>

```sql
LowCardinality(data_type)
```

**Parámetros**

* `data_type` — [String](../../sql-reference/data-types/string.md), [FixedString](../../sql-reference/data-types/fixedstring.md), [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md) y tipos numéricos, excepto [Decimal](../../sql-reference/data-types/decimal.md). `LowCardinality` no es eficiente para algunos tipos de datos; consulte la descripción del ajuste [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types).

<div id="description">
  ## Descripción
</div>

`LowCardinality` es una superestructura que modifica el método de almacenamiento de datos y las reglas de procesamiento de datos. ClickHouse aplica [codificación por diccionario](https://en.wikipedia.org/wiki/Dictionary_coder) a las columnas `LowCardinality`. Trabajar con datos codificados por diccionario aumenta significativamente el rendimiento de las consultas [SELECT](../../sql-reference/statements/select/index.md) en muchas aplicaciones.

La eficiencia del uso del tipo de dato `LowCardinality` depende de la diversidad de los datos. Si un diccionario contiene menos de 10.000 valores distintos, ClickHouse suele mostrar una mayor eficiencia en la lectura y el almacenamiento de datos. Si un diccionario contiene más de 100.000 valores distintos, ClickHouse puede tener un peor rendimiento en comparación con el uso de tipos de datos convencionales.

Considere usar `LowCardinality` en lugar de [Enum](../../sql-reference/data-types/enum.md) al trabajar con cadenas. `LowCardinality` ofrece más flexibilidad y, a menudo, la misma eficiencia o incluso una mayor.

<div id="example">
  ## Ejemplo
</div>

Cree una tabla con una columna `LowCardinality`:

```sql
CREATE TABLE lc_t
(
    `id` UInt16,
    `strings` LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY id
```

<div id="related-settings-and-functions">
  ## Configuración y funciones relacionadas
</div>

Configuración:

* [low&#95;cardinality&#95;max&#95;dictionary&#95;size](../../operations/settings/settings.md#low_cardinality_max_dictionary_size)
* [low&#95;cardinality&#95;use&#95;single&#95;dictionary&#95;for&#95;part](../../operations/settings/settings.md#low_cardinality_use_single_dictionary_for_part)
* [low&#95;cardinality&#95;allow&#95;in&#95;native&#95;format](../../operations/settings/settings.md#low_cardinality_allow_in_native_format)
* [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types)
* [output&#95;format&#95;arrow&#95;low&#95;cardinality&#95;as&#95;dictionary](/es/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary)

Funciones:

* [toLowCardinality](../../sql-reference/functions/type-conversion-functions.md#toLowCardinality)

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Optimización de ClickHouse con esquemas y códecs](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
* Blog: [Cómo trabajar con datos de series temporales en ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
* [Optimización de String (presentación en video en ruso)](https://youtu.be/rqf-ILRgBdY?list=PL0Z2YDlm0b3iwXCpEFiOOYmwXzVmjJfEt). [Diapositivas en inglés](https://github.com/ClickHouse/clickhouse-presentations/raw/master/meetup19/string_optimization.pdf)