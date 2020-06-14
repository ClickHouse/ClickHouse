---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: Clave y campos del diccionario
---

# Clave y campos del diccionario {#dictionary-key-and-fields}

El `<structure>` cláusula describe la clave del diccionario y los campos disponibles para consultas.

Descripción XML:

``` xml
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

Los atributos se describen en los elementos:

-   `<id>` — [Columna clave](external-dicts-dict-structure.md#ext_dict_structure-key).
-   `<attribute>` — [Columna de datos](external-dicts-dict-structure.md#ext_dict_structure-attributes). Puede haber un número múltiple de atributos.

Consulta DDL:

``` sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

Los atributos se describen en el cuerpo de la consulta:

-   `PRIMARY KEY` — [Columna clave](external-dicts-dict-structure.md#ext_dict_structure-key)
-   `AttrName AttrType` — [Columna de datos](external-dicts-dict-structure.md#ext_dict_structure-attributes). Puede haber un número múltiple de atributos.

## Clave {#ext_dict_structure-key}

ClickHouse admite los siguientes tipos de claves:

-   Tecla numérica. `UInt64`. Definido en el `<id>` etiqueta o usando `PRIMARY KEY` palabra clave.
-   Clave compuesta. Conjunto de valores de diferentes tipos. Definido en la etiqueta `<key>` o `PRIMARY KEY` palabra clave.

Una estructura xml puede contener `<id>` o `<key>`. La consulta DDL debe contener `PRIMARY KEY`.

!!! warning "Advertencia"
    No debe describir la clave como un atributo.

### Tecla numérica {#ext_dict-numeric-key}

Tipo: `UInt64`.

Ejemplo de configuración:

``` xml
<id>
    <name>Id</name>
</id>
```

Campos de configuración:

-   `name` – The name of the column with keys.

Para consulta DDL:

``` sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

-   `PRIMARY KEY` – The name of the column with keys.

### Clave compuesta {#composite-key}

La clave puede ser un `tuple` de cualquier tipo de campo. El [diseño](external-dicts-dict-layout.md) en este caso debe ser `complex_key_hashed` o `complex_key_cache`.

!!! tip "Consejo"
    Una clave compuesta puede consistir en un solo elemento. Esto hace posible usar una cadena como clave, por ejemplo.

La estructura clave se establece en el elemento `<key>`. Los campos clave se especifican en el mismo formato que el diccionario [atributo](external-dicts-dict-structure.md). Ejemplo:

``` xml
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

o

``` sql
CREATE DICTIONARY (
    field1 String,
    field2 String
    ...
)
PRIMARY KEY field1, field2
...
```

Para una consulta al `dictGet*` función, una tupla se pasa como la clave. Ejemplo: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

## Atributo {#ext_dict_structure-attributes}

Ejemplo de configuración:

``` xml
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

``` sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

Campos de configuración:

| Etiqueta                                             | Descripci                                                                                                                                                                                                                                                                                                                                                                                 | Requerir |
|------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `name`                                               | Nombre de columna.                                                                                                                                                                                                                                                                                                                                                                        | Sí       |
| `type`                                               | Tipo de datos ClickHouse.<br/>ClickHouse intenta convertir el valor del diccionario al tipo de datos especificado. Por ejemplo, para MySQL, el campo podría ser `TEXT`, `VARCHAR`, o `BLOB` en la tabla fuente de MySQL, pero se puede cargar como `String` en ClickHouse.<br/>[NULL](../../../sql-reference/data-types/nullable.md) no es compatible.                                    | Sí       |
| `null_value`                                         | Valor predeterminado para un elemento no existente.<br/>En el ejemplo, es una cadena vacía. No se puede utilizar `NULL` en este campo.                                                                                                                                                                                                                                                    | Sí       |
| `expression`                                         | [Expresion](../../syntax.md#syntax-expressions) que ClickHouse ejecuta en el valor.<br/>La expresión puede ser un nombre de columna en la base de datos SQL remota. Por lo tanto, puede usarlo para crear un alias para la columna remota.<br/><br/>Valor predeterminado: sin expresión.                                                                                                  | No       |
| <a name="hierarchical-dict-attr"></a> `hierarchical` | Si `true` el atributo contiene el valor de una clave primaria para la clave actual. Ver [Diccionarios jerárquicos](external-dicts-dict-hierarchical.md).<br/><br/>Valor predeterminado: `false`.                                                                                                                                                                                          | No       |
| `injective`                                          | Indicador que muestra si el `id -> attribute` la imagen es [inyectivo](https://en.wikipedia.org/wiki/Injective_function).<br/>Si `true`, ClickHouse puede colocar automáticamente después de la `GROUP BY` cláusula las solicitudes a los diccionarios con inyección. Por lo general, reduce significativamente la cantidad de tales solicitudes.<br/><br/>Valor predeterminado: `false`. | No       |
| `is_object_id`                                       | Indicador que muestra si la consulta se ejecuta para un documento MongoDB mediante `ObjectID`.<br/><br/>Valor predeterminado: `false`.                                                                                                                                                                                                                                                    | No       |

## Ver también {#see-also}

-   [Funciones para trabajar con diccionarios externos](../../../sql-reference/functions/ext-dict-functions.md).

[Artículo Original](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_structure/) <!--hide-->
