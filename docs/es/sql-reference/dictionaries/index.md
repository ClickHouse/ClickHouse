---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: Diccionario
toc_priority: 35
toc_title: "Implantaci\xF3n"
---

# Diccionario {#dictionaries}

Un diccionario es un mapeo (`key -> attributes`) que es conveniente para varios tipos de listas de referencia.

ClickHouse admite funciones especiales para trabajar con diccionarios que se pueden usar en consultas. Es más fácil y más eficiente usar diccionarios con funciones que un `JOIN` con tablas de referencia.

[NULL](../../sql-reference/syntax.md#null-literal) los valores no se pueden almacenar en un diccionario.

Soporta ClickHouse:

-   [Diccionarios incorporados](internal-dicts.md#internal_dicts) con una específica [conjunto de funciones](../../sql-reference/functions/ym-dict-functions.md).
-   [Diccionarios complementarios (externos)](external-dictionaries/external-dicts.md#dicts-external-dicts) con un [conjunto de funciones](../../sql-reference/functions/ext-dict-functions.md).

[Artículo Original](https://clickhouse.tech/docs/en/query_language/dicts/) <!--hide-->
