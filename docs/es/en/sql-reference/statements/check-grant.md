---
description: 'Documentación de CHECK GRANT'
sidebar_label: 'CHECK GRANT'
sidebar_position: 56
slug: /sql-reference/statements/check-grant
title: 'Sentencia CHECK GRANT'
doc_type: 'reference'
---

La consulta `CHECK GRANT` se utiliza para comprobar si al usuario o rol actual se le ha concedido un privilegio específico.

<div id="syntax">
  ## Sintaxis
</div>

La sintaxis básica de la consulta es la siguiente:

```sql
CHECK GRANT privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*}
```

* `privilege` — Tipo de privilegio.

<div id="examples">
  ## Ejemplos
</div>

Si anteriormente se le concedió el privilegio al usuario, la respuesta `check_grant` será `1`. De lo contrario, la respuesta `check_grant` será `0`.

Si `table_1.col1` existe y al usuario actual se le ha concedido el privilegio `SELECT`/`SELECT(con)` o un rol (con el privilegio), la respuesta es `1`.

```sql
CHECK GRANT SELECT(col1) ON table_1;
```

```text
┌─result─┐
│      1 │
└────────┘
```

Si `table_2.col2` no existe, o si al usuario actual no se le ha concedido el privilegio `SELECT`/`SELECT(con)` ni un rol (con privilegio), la respuesta es `0`.

```sql
CHECK GRANT SELECT(col2) ON table_2;
```

```text
┌─result─┐
│      0 │
└────────┘
```

<div id="wildcard">
  ## Comodín
</div>

Al especificar privilegios, puede usar el asterisco (`*`) en lugar del nombre de una tabla o de una base de datos. Consulte [WILDCARD GRANTS](../../sql-reference/statements/grant.md#wildcard-grants) para conocer las reglas sobre comodines.