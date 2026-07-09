---
description: 'Crea una tabla temporal de la estructura especificada con el motor de tabla Null.
  La función se utiliza para simplificar la escritura de pruebas y demostraciones.'
sidebar_label: 'función null'
sidebar_position: 140
slug: /sql-reference/table-functions/null
title: 'null'
doc_type: 'reference'
---

Crea una tabla temporal de la estructura especificada con el motor de tabla [Null](../../engines/table-engines/special/null.md). De acuerdo con las propiedades del motor `Null`, los datos de la tabla se ignoran y la propia tabla se elimina inmediatamente después de ejecutar la consulta. La función se utiliza para simplificar la escritura de pruebas y demostraciones.

<div id="syntax">
  ## Sintaxis
</div>

```sql
null('structure')
```

<div id="argument">
  ## Argumento
</div>

* `structure` — Una lista de columnas y sus tipos. [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## Valor devuelto
</div>

Una tabla temporal con el motor `Null` y la estructura especificada.

<div id="example">
  ## Ejemplo
</div>

Consulta con la función `null`:

```sql
INSERT INTO function null('x UInt64') SELECT * FROM numbers_mt(1000000000);
```

puede reemplazar tres consultas:

```sql
CREATE TABLE t (x UInt64) ENGINE = Null;
INSERT INTO t SELECT * FROM numbers_mt(1000000000);
DROP TABLE IF EXISTS t;
```

<div id="related">
  ## Relacionado
</div>

* [Motor de tabla Null](../../engines/table-engines/special/null.md)