---
description: 'El motor `Dictionary` muestra los datos del diccionario como una tabla
  de ClickHouse.'
sidebar_label: 'Diccionario'
sidebar_position: 20
slug: /engines/table-engines/special/dictionary
title: 'Motor de tabla Diccionario'
doc_type: 'reference'
---

El motor `Dictionary` muestra los datos del [diccionario](../../../sql-reference/statements/create/dictionary/overview.md) como una tabla de ClickHouse.

<div id="example">
  ## Ejemplo
</div>

Por ejemplo, considere un diccionario de `products` con la siguiente configuración:

```xml
<dictionaries>
    <dictionary>
        <name>products</name>
        <source>
            <odbc>
                <table>products</table>
                <connection_string>DSN=some-db-server</connection_string>
            </odbc>
        </source>
        <lifetime>
            <min>300</min>
            <max>360</max>
        </lifetime>
        <layout>
            <flat/>
        </layout>
        <structure>
            <id>
                <name>product_id</name>
            </id>
            <attribute>
                <name>title</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
    </dictionary>
</dictionaries>
```

Consulta los datos del diccionario:

```sql
SELECT
    name,
    type,
    key,
    attribute.names,
    attribute.types,
    bytes_allocated,
    element_count,
    source
FROM system.dictionaries
WHERE name = 'products'
```

```text
┌─name─────┬─type─┬─key────┬─attribute.names─┬─attribute.types─┬─bytes_allocated─┬─element_count─┬─source──────────┐
│ products │ Flat │ UInt64 │ ['title']       │ ['String']      │        23065376 │        175032 │ ODBC: .products │
└──────────┴──────┴────────┴─────────────────┴─────────────────┴─────────────────┴───────────────┴─────────────────┘
```

Puede usar las funciones [dictGet*](/es/sql-reference/functions/ext-dict-functions) para obtener los datos del diccionario en este formato.

Esta vista no resulta útil cuando necesita obtener datos sin procesar o realizar una operación `JOIN`. En esos casos, puede usar el motor `Dictionary`, que muestra los datos del diccionario en una tabla.

Sintaxis:

```sql
CREATE TABLE %table_name% (%fields%) engine = Dictionary(%dictionary_name%)`
```

Ejemplo de uso:

```sql
CREATE TABLE products (product_id UInt64, title String) ENGINE = Dictionary(products);
```

Bien

Echa un vistazo a lo que hay en la tabla.

```sql
SELECT * FROM products LIMIT 1;
```

```text
┌────product_id─┬─title───────────┐
│        152689 │ Some item       │
└───────────────┴─────────────────┘
```

**Véase también**

* [Función Diccionario](/es/sql-reference/table-functions/dictionary)