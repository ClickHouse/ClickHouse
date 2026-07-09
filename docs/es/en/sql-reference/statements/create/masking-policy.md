---
description: 'Documentación sobre la política de enmascaramiento'
sidebar_label: 'POLÍTICA DE ENMASCARAMIENTO'
sidebar_position: 42
slug: /sql-reference/statements/create/masking-policy
title: 'CREATE MASKING POLICY'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

Crea una política de enmascaramiento que permite transformar o enmascarar dinámicamente los valores de las columnas para usuarios o roles específicos cuando consultan una tabla.

:::tip
Las políticas de enmascaramiento proporcionan seguridad de datos a nivel de columna al transformar datos confidenciales en tiempo de consulta sin modificar los datos almacenados.
:::

Sintaxis:

```sql
CREATE MASKING POLICY [IF NOT EXISTS | OR REPLACE] policy_name ON [database.]table
    UPDATE column1 = expression1 [, column2 = expression2 ...]
    [WHERE condition]
    TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}
    [PRIORITY priority_number]
```

<div id="update-clause">
  ## Cláusula UPDATE
</div>

La cláusula `UPDATE` especifica qué columnas se deben enmascarar y cómo transformarlas. Puede enmascarar varias columnas en una sola política.

Ejemplos:

* Enmascaramiento simple: `UPDATE email = '***masked***'`
* Enmascaramiento parcial: `UPDATE email = concat(substring(email, 1, 3), '***@***.***')`
* Enmascaramiento basado en hash: `UPDATE email = concat('masked_', substring(hex(cityHash64(email)), 1, 8))`
* Varias columnas: `UPDATE email = '***@***.***', phone = '***-***-****'`

<div id="where-clause">
  ## Cláusula WHERE
</div>

La cláusula `WHERE` opcional permite aplicar el enmascaramiento de forma condicional en función de los valores de las filas. El enmascaramiento solo se aplicará a las filas que cumplan la condición.

Ejemplo:

```sql
CREATE MASKING POLICY mask_high_salaries ON employees
UPDATE salary = 0
WHERE salary > 100000
TO analyst;
```

<div id="to-clause">
  ## Cláusula `TO`
</div>

En la sección `TO`, especifique a qué usuarios y roles debe aplicarse la política.

* `TO user1, user2`: Aplicar a usuarios/roles específicos
* `TO ALL`: Aplicar a todos los usuarios
* `TO ALL EXCEPT user1, user2`: Aplicar a todos los usuarios excepto a los indicados

:::note
A diferencia de las políticas de fila, las políticas de enmascaramiento no afectan a los usuarios a los que no se les aplique la política. Si no se aplica ninguna política de enmascaramiento a un usuario, este verá los datos originales.
:::

<div id="priority-clause">
  ## Cláusula PRIORITY
</div>

Cuando varias políticas de enmascaramiento afectan a la misma columna para un usuario, la cláusula `PRIORITY` determina el orden de aplicación. Las políticas se aplican de mayor a menor prioridad.

La prioridad predeterminada es 0. Las políticas con la misma prioridad se aplican en un orden indefinido.

Ejemplo:

```sql
-- Applied second (lower priority)
CREATE MASKING POLICY mask1 ON users
UPDATE email = 'low@priority.com'
TO analyst
PRIORITY 1;

-- Applied first (higher priority)
CREATE MASKING POLICY mask2 ON users
UPDATE email = 'high@priority.com'
TO analyst
PRIORITY 10;

-- analyst sees 'low@priority.com' because it's applied last
```

:::note Consideraciones sobre el rendimiento

* Las políticas de enmascaramiento pueden afectar al rendimiento de las consultas según la complejidad de la expresión.
* Algunas optimizaciones pueden deshabilitarse en tablas con políticas de enmascaramiento activas.
  :::