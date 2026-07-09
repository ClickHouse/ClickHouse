---
description: 'Descripción general de los tipos de dominio en ClickHouse, que amplían los tipos base con
  funcionalidades adicionales'
sidebar_label: 'Dominios'
sidebar_position: 56
slug: /sql-reference/data-types/domains/
title: 'Dominios'
doc_type: 'reference'
---

Los dominios son tipos de propósito especial que agregan funcionalidades adicionales a los tipos base existentes, sin alterar el formato on-wire ni on-disk del tipo de dato subyacente. Actualmente, ClickHouse no admite dominios definidos por el usuario.

Puedes usar dominios en cualquier lugar donde se pueda usar el tipo base correspondiente, por ejemplo:

* Crear una columna de tipo de dominio
* Leer/escribir valores desde/hacia una columna de dominio
* Usarlo como índice si un tipo base puede usarse como índice
* Llamar a funciones con valores de una columna de dominio

<div id="extra-features-of-domains">
  ### Características adicionales de los dominios
</div>

* Nombre explícito del tipo de columna en `SHOW CREATE TABLE` o `DESCRIBE TABLE`
* Entrada en un formato legible para humanos con `INSERT INTO domain_table(domain_column) VALUES(...)`
* Salida en un formato legible para humanos para `SELECT domain_column FROM domain_table`
* Carga de datos desde una fuente externa en un formato legible para humanos: `INSERT INTO domain_table FORMAT CSV ...`

<div id="limitations">
  ### Limitaciones
</div>

* No se puede convertir una columna de índice de tipo base en un tipo de dominio mediante `ALTER TABLE`.
* No se pueden convertir implícitamente valores de cadena en valores de dominio al insertar datos desde otra columna o tabla.
* El dominio no añade restricciones a los valores almacenados.