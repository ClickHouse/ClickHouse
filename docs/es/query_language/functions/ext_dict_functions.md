---
machine_translated: true
---

# Funciones para trabajar con diccionarios externos {#ext_dict_functions}

Para obtener información sobre cómo conectar y configurar diccionarios externos, consulte [Diccionarios externos](../dicts/external_dicts.md).

## dictGet {#dictget}

Recupera un valor de un diccionario externo.

``` sql
dictGet('dict_name', 'attr_name', id_expr)
dictGetOrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**Parámetros**

-   `dict_name` — Nombre del diccionario. [Literal de cadena](../syntax.md#syntax-string-literal).
-   `attr_name` — Nombre de la columna del diccionario. [Literal de cadena](../syntax.md#syntax-string-literal).
-   `id_expr` — Valor clave. [Expresion](../syntax.md#syntax-expressions) devolviendo un [UInt64](../../data_types/int_uint.md) o [Tupla](../../data_types/tuple.md)valor -type dependiendo de la configuración del diccionario.
-   `default_value_expr` — Valor devuelto si el diccionario no contiene una fila `id_expr` Nivel de Cifrado WEP [Expresion](../syntax.md#syntax-expressions) devolviendo el valor en el tipo de datos configurado para `attr_name` atributo.

**Valor devuelto**

-   Si ClickHouse analiza el atributo correctamente en el [tipo de datos del atributo](../../query_language/dicts/external_dicts_dict_structure.md#ext_dict_structure-attributes), funciones devuelven el valor del atributo de diccionario que corresponde a `id_expr`.

-   Si no hay la clave, correspondiente a `id_expr` es el diccionario, entonces:

        - `dictGet` returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.
        - `dictGetOrDefault` returns the value passed as the `default_value_expr` parameter.

ClickHouse produce una excepción si no puede analizar el valor del atributo o si el valor no coincide con el tipo de datos del atributo.

**Ejemplo**

Crear un archivo de texto `ext-dict-text.csv` que contiene los siguientes:

``` text
1,1
2,2
```

La primera columna es `id` la segunda columna es `c1`.

Configurar el diccionario externo:

``` xml
<yandex>
    <dictionary>
        <name>ext-dict-test</name>
        <source>
            <file>
                <path>/path-to/ext-dict-test.csv</path>
                <format>CSV</format>
            </file>
        </source>
        <layout>
            <flat />
        </layout>
        <structure>
            <id>
                <name>id</name>
            </id>
            <attribute>
                <name>c1</name>
                <type>UInt32</type>
                <null_value></null_value>
            </attribute>
        </structure>
        <lifetime>0</lifetime>
    </dictionary>
</yandex>
```

Realizar la consulta:

``` sql
SELECT
    dictGetOrDefault('ext-dict-test', 'c1', number + 1, toUInt32(number * 10)) AS val,
    toTypeName(val) AS type
FROM system.numbers
LIMIT 3
```

``` text
┌─val─┬─type───┐
│   1 │ UInt32 │
│   2 │ UInt32 │
│  20 │ UInt32 │
└─────┴────────┘
```

**Ver también**

-   [Diccionarios externos](../dicts/external_dicts.md)

## dictHas {#dicthas}

Comprueba si hay una clave en un diccionario.

``` sql
dictHas('dict_name', id_expr)
```

**Parámetros**

-   `dict_name` — Nombre del diccionario. [Literal de cadena](../syntax.md#syntax-string-literal).
-   `id_expr` — Valor clave. [Expresion](../syntax.md#syntax-expressions) devolviendo un [UInt64](../../data_types/int_uint.md)-tipo de valor.

**Valor devuelto**

-   0, si no hay clave.
-   1, si hay una llave.

Tipo: `UInt8`.

## dictGetHierarchy {#dictgethierarchy}

Crea una matriz, que contiene todos los padres de una clave [diccionario jerárquico](../dicts/external_dicts_dict_hierarchical.md).

**Sintaxis**

``` sql
dictGetHierarchy('dict_name', key)
```

**Parámetros**

-   `dict_name` — Nombre del diccionario. [Literal de cadena](../syntax.md#syntax-string-literal).
-   `key` — Valor clave. [Expresion](../syntax.md#syntax-expressions) devolviendo un [UInt64](../../data_types/int_uint.md)-tipo de valor.

**Valor devuelto**

-   Padres por la llave.

Tipo: [Matriz (UInt64)](../../data_types/array.md).

## DictIsIn {#dictisin}

Comprueba el antecesor de una clave a través de toda la cadena jerárquica en el diccionario.

``` sql
dictIsIn('dict_name', child_id_expr, ancestor_id_expr)
```

**Parámetros**

-   `dict_name` — Nombre del diccionario. [Literal de cadena](../syntax.md#syntax-string-literal).
-   `child_id_expr` — Clave a comprobar. [Expresion](../syntax.md#syntax-expressions) devolviendo un [UInt64](../../data_types/int_uint.md)-tipo de valor.
-   `ancestor_id_expr` — Presunto ancestro de la `child_id_expr` Nivel de Cifrado WEP [Expresion](../syntax.md#syntax-expressions) devolviendo un [UInt64](../../data_types/int_uint.md)-tipo de valor.

**Valor devuelto**

-   0, si `child_id_expr` no es un niño de `ancestor_id_expr`.
-   1, si `child_id_expr` es un niño de `ancestor_id_expr` o si `child_id_expr` es una `ancestor_id_expr`.

Tipo: `UInt8`.

## Otras funciones {#ext_dict_functions-other}

ClickHouse admite funciones especializadas que convierten los valores de atributo de diccionario a un tipo de datos específico, independientemente de la configuración del diccionario.

Función:

-   `dictGetInt8`, `dictGetInt16`, `dictGetInt32`, `dictGetInt64`
-   `dictGetUInt8`, `dictGetUInt16`, `dictGetUInt32`, `dictGetUInt64`
-   `dictGetFloat32`, `dictGetFloat64`
-   `dictGetDate`
-   `dictGetDateTime`
-   `dictGetUUID`
-   `dictGetString`

Todas estas funciones tienen el `OrDefault` modificación. Por ejemplo, `dictGetDateOrDefault`.

Sintaxis:

``` sql
dictGet[Type]('dict_name', 'attr_name', id_expr)
dictGet[Type]OrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**Parámetros**

-   `dict_name` — Nombre del diccionario. [Literal de cadena](../syntax.md#syntax-string-literal).
-   `attr_name` — Nombre de la columna del diccionario. [Literal de cadena](../syntax.md#syntax-string-literal).
-   `id_expr` — Valor clave. [Expresion](../syntax.md#syntax-expressions) devolviendo un [UInt64](../../data_types/int_uint.md)-tipo de valor.
-   `default_value_expr` — Valor que se devuelve si el diccionario no contiene una fila `id_expr` Nivel de Cifrado WEP [Expresion](../syntax.md#syntax-expressions) devolviendo un valor en el tipo de datos configurado para `attr_name` atributo.

**Valor devuelto**

-   Si ClickHouse analiza el atributo correctamente en el [tipo de datos del atributo](../../query_language/dicts/external_dicts_dict_structure.md#ext_dict_structure-attributes), funciones devuelven el valor del atributo de diccionario que corresponde a `id_expr`.

-   Si no se solicita `id_expr` es el diccionario entonces:

        - `dictGet[Type]` returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.
        - `dictGet[Type]OrDefault` returns the value passed as the `default_value_expr` parameter.

ClickHouse produce una excepción si no puede analizar el valor del atributo o si el valor no coincide con el tipo de datos del atributo.

[Artículo Original](https://clickhouse.tech/docs/es/query_language/functions/ext_dict_functions/) <!--hide-->
