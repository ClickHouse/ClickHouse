---
description: 'Documentación de ROW POLICY'
sidebar_label: 'ROW POLICY'
sidebar_position: 41
slug: /sql-reference/statements/create/row-policy
title: 'CREATE ROW POLICY'
doc_type: 'reference'
---

Crea una [ROW POLICY](../../../guides/sre/user-management/index.md#row-policy-management), es decir, un filtro que determina qué filas de una tabla puede leer un usuario.

:::tip
Las ROW POLICIES solo tienen sentido para usuarios con acceso readonly. Si un usuario puede modificar una tabla o copiar particiones entre tablas, las restricciones de las ROW POLICIES dejan de ser efectivas.
:::

Sintaxis:

```sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name1 [ON CLUSTER cluster_name1] ON [db1.]table1|db1.*
        [, policy_name2 [ON CLUSTER cluster_name2] ON [db2.]table2|db2.* ...]
    [IN access_storage_type]
    [FOR SELECT] USING condition
    [AS {PERMISSIVE | RESTRICTIVE}]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
```

<div id="using-clause">
  ## Cláusula USING
</div>

Permite especificar una condición para filtrar filas. Un usuario verá una fila si la condición se evalúa como distinta de cero para esa fila.

<div id="to-clause">
  ## Cláusula TO
</div>

En la sección `TO` puedes proporcionar una lista de usuarios y roles a los que debe aplicarse esta política. Por ejemplo, `CREATE ROW POLICY ... TO accountant, john@localhost`.

La palabra clave `ALL` significa todos los usuarios de ClickHouse, incluido el usuario actual. La palabra clave `ALL EXCEPT` permite excluir algunos usuarios de la lista de todos los usuarios; por ejemplo, `CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

<div id="as-clause">
  ## Cláusula AS
</div>

Se permite tener más de una política habilitada en la misma tabla para el mismo usuario al mismo tiempo. Por lo tanto, necesitamos una forma de combinar las condiciones de varias políticas.

De forma predeterminada, las políticas se combinan mediante el operador booleano `OR`. Por ejemplo, las siguientes políticas:

```sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 TO peter, antonio
```

permiten al usuario `peter` ver filas con `b=1` o `c=2`.

La cláusula `AS` especifica cómo deben combinarse las políticas con otras políticas. Las políticas pueden ser permisivas o restrictivas. De forma predeterminada, las políticas son permisivas, lo que significa que se combinan mediante el operador booleano `OR`.

Como alternativa, una política puede definirse como restrictiva. Las políticas restrictivas se combinan mediante el operador booleano `AND`.

Esta es la fórmula general:

```text
row_is_visible = (one or more of the permissive policies' conditions are non-zero) AND
                 (all of the restrictive policies's conditions are non-zero)
```

Por ejemplo, las siguientes políticas:

```sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

permiten que el usuario `peter` vea filas solo si se cumplen `b=1` y `c=2` a la vez.

Las políticas de la base de datos se combinan con las políticas de la tabla.

Por ejemplo, las siguientes políticas:

```sql
CREATE ROW POLICY pol1 ON mydb.* USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

permitir que el usuario `peter` vea las filas de table1 solo si se cumplen `b=1` AND `c=2`, aunque
en cualquier otra tabla de mydb solo se aplicaría al usuario la política `b=1`.

<div id="on-cluster-clause">
  ## Cláusula ON CLUSTER
</div>

Permite crear ROW POLICIES en un clúster; consulta [DDL distribuido](../../../sql-reference/distributed-ddl.md).

<div id="examples">
  ## Ejemplos
</div>

`CREATE ROW POLICY filter1 ON mydb.mytable USING a<1000 TO accountant, john@localhost`

`CREATE ROW POLICY filter2 ON mydb.mytable USING a<1000 AND b=5 TO ALL EXCEPT mira`

`CREATE ROW POLICY filter3 ON mydb.mytable USING 1 TO admin`

`CREATE ROW POLICY filter4 ON mydb.* USING 1 TO admin`