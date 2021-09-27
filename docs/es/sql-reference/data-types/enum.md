---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 50
toc_title: Enum
---

# Enum {#enum}

Tipo enumerado que consta de valores con nombre.

Los valores con nombre deben declararse como `'string' = integer` par. ClickHouse almacena solo números, pero admite operaciones con los valores a través de sus nombres.

Soporta ClickHouse:

-   de 8 bits `Enum`. Puede contener hasta 256 valores enumerados en el `[-128, 127]` gama.
-   de 16 bits `Enum`. Puede contener hasta 65536 valores enumerados en el `[-32768, 32767]` gama.

ClickHouse elige automáticamente el tipo de `Enum` cuando se insertan datos. También puede utilizar `Enum8` o `Enum16` para estar seguro en el tamaño de almacenamiento.

## Ejemplos de uso {#usage-examples}

Aquí creamos una tabla con un `Enum8('hello' = 1, 'world' = 2)` tipo columna:

``` sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world' = 2)
)
ENGINE = TinyLog
```

Columna `x` sólo puede almacenar valores que se enumeran en la definición de tipo: `'hello'` o `'world'`. Si intenta guardar cualquier otro valor, ClickHouse generará una excepción. Tamaño de 8 bits para esto `Enum` se elige automáticamente.

``` sql
INSERT INTO t_enum VALUES ('hello'), ('world'), ('hello')
```

``` text
Ok.
```

``` sql
INSERT INTO t_enum values('a')
```

``` text
Exception on client:
Code: 49. DB::Exception: Unknown element 'a' for type Enum('hello' = 1, 'world' = 2)
```

Al consultar datos de la tabla, ClickHouse genera los valores de cadena de `Enum`.

``` sql
SELECT * FROM t_enum
```

``` text
┌─x─────┐
│ hello │
│ world │
│ hello │
└───────┘
```

Si necesita ver los equivalentes numéricos de las filas, debe `Enum` valor a tipo entero.

``` sql
SELECT CAST(x, 'Int8') FROM t_enum
```

``` text
┌─CAST(x, 'Int8')─┐
│               1 │
│               2 │
│               1 │
└─────────────────┘
```

Para crear un valor Enum en una consulta, también debe usar `CAST`.

``` sql
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))
```

``` text
┌─toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))─┐
│ Enum8('a' = 1, 'b' = 2)                             │
└─────────────────────────────────────────────────────┘
```

## Reglas generales y uso {#general-rules-and-usage}

A cada uno de los valores se le asigna un número en el rango `-128 ... 127` para `Enum8` o en el rango `-32768 ... 32767` para `Enum16`. Todas las cadenas y números deben ser diferentes. Se permite una cadena vacía. Si se especifica este tipo (en una definición de tabla), los números pueden estar en un orden arbitrario. Sin embargo, el orden no importa.

Ni la cadena ni el valor numérico en un `Enum` puede ser [NULL](../../sql-reference/syntax.md).

Un `Enum` puede estar contenido en [NULL](nullable.md) tipo. Entonces, si crea una tabla usando la consulta

``` sql
CREATE TABLE t_enum_nullable
(
    x Nullable( Enum8('hello' = 1, 'world' = 2) )
)
ENGINE = TinyLog
```

puede almacenar no sólo `'hello'` y `'world'`, pero `NULL`, también.

``` sql
INSERT INTO t_enum_nullable Values('hello'),('world'),(NULL)
```

En RAM, un `Enum` columna se almacena de la misma manera que `Int8` o `Int16` de los valores numéricos correspondientes.

Al leer en forma de texto, ClickHouse analiza el valor como una cadena y busca la cadena correspondiente del conjunto de valores Enum. Si no se encuentra, se lanza una excepción. Al leer en formato de texto, se lee la cadena y se busca el valor numérico correspondiente. Se lanzará una excepción si no se encuentra.
Al escribir en forma de texto, escribe el valor como la cadena correspondiente. Si los datos de columna contienen elementos no utilizados (números que no son del conjunto válido), se produce una excepción. Al leer y escribir en forma binaria, funciona de la misma manera que para los tipos de datos Int8 e Int16.
El valor predeterminado es el valor con el número más bajo.

Durante `ORDER BY`, `GROUP BY`, `IN`, `DISTINCT` y así sucesivamente, las enumeraciones se comportan de la misma manera que los números correspondientes. Por ejemplo, ORDER BY los ordena numéricamente. Los operadores de igualdad y comparación funcionan de la misma manera en enumeraciones que en los valores numéricos subyacentes.

Los valores de Enum no se pueden comparar con los números. Las enumeraciones se pueden comparar con una cadena constante. Si la cadena en comparación con no es un valor válido para el Enum, se lanzará una excepción. El operador IN es compatible con el Enum en el lado izquierdo y un conjunto de cadenas en el lado derecho. Las cadenas son los valores del Enum correspondiente.

Most numeric and string operations are not defined for Enum values, e.g. adding a number to an Enum or concatenating a string to an Enum.
Sin embargo, el Enum tiene un `toString` función que devuelve su valor de cadena.

Los valores de Enum también se pueden convertir a tipos numéricos utilizando el `toT` función, donde T es un tipo numérico. Cuando T corresponde al tipo numérico subyacente de la enumeración, esta conversión es de costo cero.
El tipo Enum se puede cambiar sin costo usando ALTER, si solo se cambia el conjunto de valores. Es posible agregar y eliminar miembros del Enum usando ALTER (eliminar es seguro solo si el valor eliminado nunca se ha usado en la tabla). Como salvaguardia, al cambiar el valor numérico de un miembro Enum definido previamente se producirá una excepción.

Usando ALTER, es posible cambiar un Enum8 a un Enum16 o viceversa, al igual que cambiar un Int8 a Int16.

[Artículo Original](https://clickhouse.tech/docs/en/data_types/enum/) <!--hide-->
