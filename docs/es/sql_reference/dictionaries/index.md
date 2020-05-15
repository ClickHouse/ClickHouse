---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_folder_title: Dictionaries
toc_priority: 35
toc_title: "Implantaci\xF3n"
---

# Diccionario {#dictionaries}

Un diccionario es un mapeo (`key -> attributes`) que es conveniente para varios tipos de listas de referencia.

ClickHouse admite funciones especiales para trabajar con diccionarios que se pueden usar en consultas. Es más fácil y más eficiente usar diccionarios con funciones que un `JOIN` con tablas de referencia.

[NULL](../syntax.md#null) los valores no se pueden almacenar en un diccionario.

Soporta ClickHouse:

-   [Diccionarios incorporados](internal_dicts.md#internal_dicts) con una específica [conjunto de funciones](../../sql_reference/functions/ym_dict_functions.md).
-   [Diccionarios complementarios (externos)](external_dictionaries/external_dicts.md) con un [neto de funciones](../../sql_reference/functions/ext_dict_functions.md).

[Artículo Original](https://clickhouse.tech/docs/en/query_language/dicts/) <!--hide-->
