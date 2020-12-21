---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: Cadena fija (N)
---

# Cuerda fija {#fixedstring}

Una cadena de longitud fija de `N` bytes (ni caracteres ni puntos de código).

Para declarar una columna de `FixedString` tipo, utilice la siguiente sintaxis:

``` sql
<column_name> FixedString(N)
```

Donde `N` es un número natural.

El `FixedString` tipo es eficiente cuando los datos tienen la longitud de `N` byte. En todos los demás casos, es probable que reduzca la eficiencia.

Ejemplos de los valores que se pueden almacenar eficientemente en `FixedString`escrito columnas:

-   La representación binaria de direcciones IP (`FixedString(16)` para IPv6).
-   Language codes (ru_RU, en_US … ).
-   Currency codes (USD, RUB … ).
-   Representación binaria de hashes (`FixedString(16)` para MD5, `FixedString(32)` para SHA256).

Para almacenar valores UUID, utilice el [UUID](uuid.md) tipo de datos.

Al insertar los datos, ClickHouse:

-   Complementa una cadena con bytes nulos si la cadena contiene menos de `N` byte.
-   Lanza el `Too large value for FixedString(N)` excepción si la cadena contiene más de `N` byte.

Al seleccionar los datos, ClickHouse no elimina los bytes nulos al final de la cadena. Si utiliza el `WHERE` cláusula, debe agregar bytes nulos manualmente para que coincida con el `FixedString` valor. En el ejemplo siguiente se muestra cómo utilizar el `WHERE` cláusula con `FixedString`.

Consideremos la siguiente tabla con el único `FixedString(2)` columna:

``` text
┌─name──┐
│ b     │
└───────┘
```

Consulta `SELECT * FROM FixedStringTable WHERE a = 'b'` no devuelve ningún dato como resultado. Debemos complementar el patrón de filtro con bytes nulos.

``` sql
SELECT * FROM FixedStringTable
WHERE a = 'b\0'
```

``` text
┌─a─┐
│ b │
└───┘
```

Este comportamiento difiere de MySQL para el `CHAR` tipo (donde las cadenas se rellenan con espacios y los espacios se eliminan para la salida).

Tenga en cuenta que la longitud del `FixedString(N)` el valor es constante. El [longitud](../../sql-reference/functions/array-functions.md#array_functions-length) función devuelve `N` incluso si el `FixedString(N)` sólo se rellena con bytes nulos, pero el valor [vaciar](../../sql-reference/functions/string-functions.md#empty) función devuelve `1` en este caso.

[Artículo Original](https://clickhouse.tech/docs/en/data_types/fixedstring/) <!--hide-->
