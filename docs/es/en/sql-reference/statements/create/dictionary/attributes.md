---
description: 'Configuración de la clave y los atributos del diccionario'
sidebar_label: 'Atributos'
sidebar_position: 2
slug: /sql-reference/statements/create/dictionary/attributes
title: 'Atributos del diccionario'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';

<CloudDetails />

La cláusula `structure` describe la clave del diccionario y los campos disponibles para las consultas.

Descripción en XML:

```xml
<dictionary>
    <structure>
        <id>
            <name>Id</name>
        </id>

        <attribute>
            <!-- Attribute parameters -->
        </attribute>

        ...

    </structure>
</dictionary>
```

Los atributos se describen mediante los elementos:

* `<id>` — Columna clave
* `<attribute>` — Columna de datos: puede haber varios atributos.

Consulta DDL:

```sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

Los atributos se describen en el cuerpo de la consulta:

* `PRIMARY KEY` — columna clave
* `AttrName AttrType` — columna de datos. Puede haber varios atributos.

<div id="key">
  ## Clave
</div>

ClickHouse admite los siguientes tipos de claves:

* Clave numérica. `UInt64`. Se define en la etiqueta `<id>` o mediante la palabra clave `PRIMARY KEY`.
* Clave compuesta. Conjunto de valores de distintos tipos. Se define en la etiqueta `<key>` o mediante la palabra clave `PRIMARY KEY`.

Una estructura XML puede contener `<id>` o `<key>`. La consulta DDL debe contener una sola `PRIMARY KEY`.

:::note
No describa la clave como un atributo.
:::

<div id="numeric-key">
  ### Clave numérica
</div>

Tipo: `UInt64`.

Ejemplo de configuración:

```xml
<id>
    <name>Id</name>
</id>
```

Campos de configuración:

* `name` – El nombre de la columna con claves.

Para la consulta DDL:

```sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

* `PRIMARY KEY` – El nombre de la columna con las claves.

<div id="composite-key">
  ### Clave compuesta
</div>

La clave puede ser una `tuple` de campos de cualquier tipo. El [layout](./layouts/) en este caso debe ser `complex_key_hashed` o `complex_key_cache`.

:::tip
Una clave compuesta puede constar de un solo elemento. Esto permite usar una cadena como clave, por ejemplo.
:::

La estructura de la clave se define en el elemento `<key>`. Los campos de la clave se especifican en el mismo formato que los [atributos](#attributes) del diccionario. Ejemplo:

```xml
<structure>
    <key>
        <attribute>
            <name>field1</name>
            <type>String</type>
        </attribute>
        <attribute>
            <name>field2</name>
            <type>UInt32</type>
        </attribute>
        ...
    </key>
...
```

or

```sql
CREATE DICTIONARY (
    field1 String,
    field2 UInt32
    ...
)
PRIMARY KEY field1, field2
...
```

Para una consulta a la función `dictGet*`, se pasa una tupla como clave. Ejemplo: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

Cuando la clave compuesta consta de un único atributo, el valor de la clave puede pasarse directamente, sin necesidad de envolverlo en `tuple`. Por ejemplo, tanto `dictGetString('dict_name', 'attr_name', 'key')` como `dictGetString('dict_name', 'attr_name', tuple('key'))` son válidos.

<div id="attributes">
  ## Atributos
</div>

Ejemplo de configuración:

```xml
<structure>
    ...
    <attribute>
        <name>Name</name>
        <type>ClickHouseDataType</type>
        <null_value></null_value>
        <expression>rand64()</expression>
        <hierarchical>true</hierarchical>
        <injective>true</injective>
        <is_object_id>true</is_object_id>
    </attribute>
</structure>
```

o

```sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

Campos de configuración:

| Etiqueta                                           | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Obligatorio |
| -------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `name`                                             | Nombre de la columna.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | Sí          |
| `type`                                             | Tipo de dato de ClickHouse: [UInt8](../../../data-types/int-uint.md), [UInt16](../../../data-types/int-uint.md), [UInt32](../../../data-types/int-uint.md), [UInt64](../../../data-types/int-uint.md), [Int8](../../../data-types/int-uint.md), [Int16](../../../data-types/int-uint.md), [Int32](../../../data-types/int-uint.md), [Int64](../../../data-types/int-uint.md), [Float32](../../../data-types/float.md), [Float64](../../../data-types/float.md), [UUID](../../../data-types/uuid.md), [Decimal32](../../../data-types/decimal.md), [Decimal64](../../../data-types/decimal.md), [Decimal128](../../../data-types/decimal.md), [Decimal256](../../../data-types/decimal.md),[Date](../../../data-types/date.md), [Date32](../../../data-types/date32.md), [DateTime](../../../data-types/datetime.md), [DateTime64](../../../data-types/datetime64.md), [String](../../../data-types/string.md), [Array](../../../data-types/array.md).<br />ClickHouse intenta convertir el valor del diccionario al tipo de dato especificado. Por ejemplo, en MySQL, el campo puede ser `TEXT`, `VARCHAR` o `BLOB` en la tabla de origen de MySQL, pero puede cargarse como `String` en ClickHouse.<br />Actualmente, [Nullable](../../../data-types/nullable.md) es compatible con los diccionarios [Flat](./layouts/flat), [Hashed](./layouts/hashed), [ComplexKeyHashed](./layouts/hashed#complex_key_hashed), [Direct](./layouts/direct), [ComplexKeyDirect](./layouts/direct#complex_key_direct), [RangeHashed](./layouts/range-hashed), Polygon, [Cache](./layouts/cache), [ComplexKeyCache](./layouts/cache), [SSDCache](./layouts/ssd-cache), [SSDComplexKeyCache](./layouts/ssd-cache#complex_key_ssd_cache). En los diccionarios [IPTrie](./layouts/ip-trie), los tipos `Nullable` no son compatibles. | Sí          |
| `null_value`                                       | Valor predeterminado para un elemento inexistente.<br />En el ejemplo, es una cadena vacía. El valor [NULL](../../../syntax.md#null) solo puede usarse para los tipos `Nullable` (consulte la línea anterior con la descripción de los tipos).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Sí          |
| `expression`                                       | [Expresión](../../../syntax.md#expressions) que ClickHouse ejecuta sobre el valor.<br />La expresión puede ser un nombre de columna en la base de datos SQL remota. Por lo tanto, puede usarla para crear un alias para la columna remota.<br /><br />Valor predeterminado: sin expresión.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | No          |
| <a name="hierarchical-dict-attr" /> `hierarchical` | Si es `true`, el atributo contiene el valor de una clave padre para la clave actual. Consulte [Hierarchical Dictionaries](./layouts/hierarchical).<br /><br />Valor predeterminado: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | No          |
| `injective`                                        | Indicador que muestra si la imagen `id -> attribute` es [inyectiva](https://en.wikipedia.org/wiki/Injective_function).<br />Si es `true`, ClickHouse puede colocar automáticamente, después de la cláusula `GROUP BY`, las solicitudes a diccionarios con atributos inyectivos. Normalmente, esto reduce significativamente la cantidad de esas solicitudes.<br /><br />Valor predeterminado: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | No          |
| `is_object_id`                                     | Indicador que muestra si la consulta se ejecuta para un documento de MongoDB mediante `ObjectID`.<br /><br />Valor predeterminado: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |             |