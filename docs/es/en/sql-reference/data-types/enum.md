---
description: 'Documentación del tipo de dato Enum en ClickHouse, que representa
  un conjunto de valores constantes con nombre'
sidebar_label: 'Enum'
sidebar_position: 20
slug: /sql-reference/data-types/enum
title: 'Enum'
doc_type: 'reference'
---

Tipo enumerado compuesto por valores con nombre.

Los valores con nombre pueden declararse como pares `'string' = integer` o como nombres `'string'`. ClickHouse almacena solo números, pero permite operar con los valores a través de sus nombres.

ClickHouse admite:

* `Enum` de 8 bits. Puede contener hasta 256 valores enumerados en el intervalo `[-128, 127]`.
* `Enum` de 16 bits. Puede contener hasta 65536 valores enumerados en el intervalo `[-32768, 32767]`.

ClickHouse elige automáticamente el tipo de `Enum` cuando se insertan los datos. También puede usar los tipos `Enum8` o `Enum16` para asegurarse del tamaño de almacenamiento.

<div id="usage-examples">
  ## Ejemplos de uso
</div>

Aquí creamos una tabla con una columna de tipo `Enum8('hello' = 1, 'world' = 2)`:

```sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world' = 2)
)
ENGINE = TinyLog
```

Del mismo modo, puedes omitir los números. ClickHouse asignará automáticamente números consecutivos. De forma predeterminada, los números se asignan a partir de 1.

```sql
CREATE TABLE t_enum
(
    x Enum('hello', 'world')
)
ENGINE = TinyLog
```

También puede especificar un número inicial permitido para el primer nombre.

```sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world')
)
ENGINE = TinyLog
```

```sql
CREATE TABLE t_enum
(
    x Enum8('hello' = -129, 'world')
)
ENGINE = TinyLog
```

```text
Exception on server:
Code: 69. DB::Exception: Value -129 for element 'hello' exceeds range of Enum8.
```

La columna `x` solo puede almacenar los valores que figuran en la definición del tipo: `'hello'` o `'world'`. Si intentas guardar cualquier otro valor, ClickHouse generará una excepción. El tamaño de 8 bits para este `Enum` se elige automáticamente.

```sql
INSERT INTO t_enum VALUES ('hello'), ('world'), ('hello')
```

```text
Ok.
```

```sql
INSERT INTO t_enum VALUES('a')
```

```text
Exception on client:
Code: 49. DB::Exception: Unknown element 'a' for type Enum('hello' = 1, 'world' = 2)
```

Cuando consultas datos de la tabla, ClickHouse devuelve los valores de texto de `Enum`.

```sql
SELECT * FROM t_enum
```

```text
┌─x─────┐
│ hello │
│ world │
│ hello │
└───────┘
```

Si necesita ver los equivalentes numéricos de las filas, debe convertir el valor `Enum` a un tipo entero.

```sql
SELECT CAST(x, 'Int8') FROM t_enum
```

```text
┌─CAST(x, 'Int8')─┐
│               1 │
│               2 │
│               1 │
└─────────────────┘
```

Para crear un valor Enum en una consulta, también debes usar `CAST`.

```sql
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))
```

```text
┌─toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))─┐
│ Enum8('a' = 1, 'b' = 2)                             │
└─────────────────────────────────────────────────────┘
```

<div id="general-rules-and-usage">
  ## Reglas generales y uso
</div>

A cada uno de los valores se le asigna un número en el intervalo `-128 ... 127` para `Enum8` o en el intervalo `-32768 ... 32767` para `Enum16`. Todas las cadenas y todos los números deben ser distintos. Se permite una cadena vacía. Si se especifica este tipo (en una definición de tabla), los números pueden estar en cualquier orden. Sin embargo, el orden no importa.

Ni la cadena ni el valor numérico de un `Enum` pueden ser [NULL](../../sql-reference/syntax.md).

Un `Enum` puede formar parte del tipo [Nullable](../../sql-reference/data-types/nullable.md). Así, si crea una tabla con la consulta

```sql
CREATE TABLE t_enum_nullable
(
    x Nullable( Enum8('hello' = 1, 'world' = 2) )
)
ENGINE = TinyLog
```

puede almacenar no solo `'hello'` y `'world'`, sino también `NULL`.

```sql
INSERT INTO t_enum_nullable VALUES('hello'),('world'),(NULL)
```

En RAM, una columna `Enum` se almacena de la misma forma que `Int8` o `Int16` de los valores numéricos correspondientes.

Al leer en formato de texto, ClickHouse interpreta el valor como una cadena y busca la cadena correspondiente en el conjunto de valores del Enum. Si no la encuentra, se lanza una excepción. Al leer en formato de texto, se lee la cadena y se busca el valor numérico correspondiente. Se lanzará una excepción si no se encuentra.
Al escribir en formato de texto, escribe el valor como la cadena correspondiente. Si los datos de la columna contienen valores no válidos (números que no pertenecen al conjunto válido), se lanza una excepción. Al leer y escribir en formato binario, funciona de la misma forma que con los tipos de datos Int8 e Int16.
El valor predeterminado implícito es el valor con el número más bajo.

Durante `ORDER BY`, `GROUP BY`, `IN`, `DISTINCT`, etc., los Enum se comportan de la misma forma que los números correspondientes. Por ejemplo, ORDER BY los ordena numéricamente. Los operadores de igualdad y comparación funcionan igual con los Enum que con los valores numéricos subyacentes.

Los valores Enum no se pueden comparar con números. Los Enum se pueden comparar con una cadena constante. Si la cadena con la que se comparan no es un valor válido para el Enum, se lanzará una excepción. Se admite el operador IN con el Enum en el lado izquierdo y un conjunto de cadenas en el lado derecho. Las cadenas son los valores del Enum correspondiente.

La mayoría de las operaciones numéricas y de cadena no están definidas para los valores Enum; por ejemplo, sumar un número a un Enum o concatenar una cadena a un Enum.
Sin embargo, el Enum tiene una función `toString` natural que devuelve su valor de cadena.

Los valores Enum también se pueden convertir a tipos numéricos mediante la función `toT`, donde T es un tipo numérico. Cuando T corresponde al tipo numérico subyacente del enum, esta conversión no tiene coste.
El tipo Enum se puede cambiar sin coste usando ALTER, si solo cambia el conjunto de valores. Es posible tanto añadir como eliminar miembros del Enum usando ALTER (eliminarlos es seguro solo si el valor eliminado nunca se ha usado en la tabla). Como medida de seguridad, cambiar el valor numérico de un miembro del Enum definido previamente lanzará una excepción.

Con ALTER, es posible cambiar un Enum8 a un Enum16 o viceversa, igual que al cambiar un Int8 a Int16.

<div id="add-enum-values">
  ## AÑADIR VALORES A ENUM
</div>

Existe una sintaxis abreviada para añadir nuevos valores a enum con ALTER [MODIFY COLUMN ADD ENUM VALUES](../../sql-reference/statements/alter/column.md#modify-column-add-enum-values)

```sql
CREATE TABLE enum
(
    x Enum('One' = 1, 'Two', 'Three')
) ENGINE = Memory;
ALTER TABLE enum MODIFY COLUMN x ADD ENUM VALUES ('Zero' = 0, 'Four' = 4);
SHOW CREATE TABLE enum;
```

```text
┌─statement────────────────────────────────────────────────────────────────┐
│CREATE TABLE default.enum                                                 │
│(                                                                         │
│    `x` Enum8('Zero' = 0, 'One' = 1, 'Two' = 2, 'Three' = 3, 'Four' = 4)  │
│)                                                                         │
│ENGINE = Memory                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```