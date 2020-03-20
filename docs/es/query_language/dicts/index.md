# Diccionario {#dictionaries}

Un diccionario es un mapeo (`key -> attributes`) que es conveniente para varios tipos de listas de referencia.

ClickHouse admite funciones especiales para trabajar con diccionarios que se pueden usar en consultas. Es más fácil y más eficiente usar diccionarios con funciones que un `JOIN` con tablas de referencia.

[NULO](../syntax.md#null) no se pueden almacenar en un diccionario.

Soporta ClickHouse:

-   [Diccionarios incorporados](internal_dicts.md#internal_dicts) con una específica [conjunto de funciones](../functions/ym_dict_functions.md).
-   [Diccionarios complementarios (externos)](external_dicts.md) con un [conjunto de funciones](../functions/ext_dict_functions.md).

[Artículo Original](https://clickhouse.tech/docs/es/query_language/dicts/) <!--hide-->
