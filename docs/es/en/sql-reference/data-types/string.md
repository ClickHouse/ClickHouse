---
description: 'Documentación del tipo de dato String en ClickHouse'
sidebar_label: 'String'
sidebar_position: 8
slug: /sql-reference/data-types/string
title: 'String'
doc_type: 'reference'
---

Cadenas de longitud arbitraria. La longitud no tiene límite. El valor puede contener cualquier conjunto de bytes, incluidos bytes nulos.
El tipo String sustituye a los tipos VARCHAR, BLOB, CLOB y otros de distintos DBMS.

Al crear tablas, se pueden especificar parámetros numéricos para los campos de texto (p. ej., `VARCHAR(255)`), pero ClickHouse los ignora.

Alias:

* `String` — `LONGTEXT`, `MEDIUMTEXT`, `TINYTEXT`, `TEXT`, `LONGBLOB`, `MEDIUMBLOB`, `TINYBLOB`, `BLOB`, `VARCHAR`, `CHAR`, `CHAR LARGE OBJECT`, `CHAR VARYING`, `CHARACTER LARGE OBJECT`, `CHARACTER VARYING`, `NCHAR LARGE OBJECT`, `NCHAR VARYING`, `NATIONAL CHARACTER LARGE OBJECT`, `NATIONAL CHARACTER VARYING`, `NATIONAL CHAR VARYING`, `NATIONAL CHARACTER`, `NATIONAL CHAR`, `BINARY LARGE OBJECT`, `BINARY VARYING`,

<div id="encodings">
  ## Codificaciones
</div>

ClickHouse no maneja el concepto de codificaciones. Las cadenas pueden contener un conjunto arbitrario de bytes, que se almacenan y se devuelven tal cual.
Si necesita almacenar texto, recomendamos usar codificación UTF-8. Como mínimo, si su terminal usa UTF-8 (como se recomienda), podrá leer y escribir sus valores sin necesidad de realizar conversiones.
Del mismo modo, ciertas funciones para trabajar con cadenas tienen variantes específicas que funcionan partiendo de la suposición de que la cadena contiene un conjunto de bytes que representa texto codificado en UTF-8.
Por ejemplo, la función [length](/es/sql-reference/functions/array-functions#length) calcula la longitud de la cadena en bytes, mientras que la función [lengthUTF8](../functions/string-functions.md#lengthUTF8) calcula la longitud de la cadena en puntos de código Unicode, asumiendo que el valor está codificado en UTF-8.