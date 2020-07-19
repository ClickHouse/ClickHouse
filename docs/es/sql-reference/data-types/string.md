---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: Cadena
---

# Cadena {#string}

Cuerdas de una longitud arbitraria. La longitud no está limitada. El valor puede contener un conjunto arbitrario de bytes, incluidos los bytes nulos.
El tipo String reemplaza los tipos VARCHAR, BLOB, CLOB y otros de otros DBMS.

## Codificación {#encodings}

ClickHouse no tiene el concepto de codificaciones. Las cadenas pueden contener un conjunto arbitrario de bytes, que se almacenan y salen tal cual.
Si necesita almacenar textos, le recomendamos que utilice la codificación UTF-8. Como mínimo, si su terminal usa UTF-8 (según lo recomendado), puede leer y escribir sus valores sin realizar conversiones.
Del mismo modo, ciertas funciones para trabajar con cadenas tienen variaciones separadas que funcionan bajo el supuesto de que la cadena contiene un conjunto de bytes que representan un texto codificado en UTF-8.
Por ejemplo, el ‘length’ función calcula la longitud de cadena en bytes, mientras que la ‘lengthUTF8’ función calcula la longitud de la cadena en puntos de código Unicode, suponiendo que el valor está codificado UTF-8.

[Artículo Original](https://clickhouse.tech/docs/en/data_types/string/) <!--hide-->
