---
description: 'Documentación sobre funciones para trabajar con diccionarios'
sidebar_label: 'Diccionario'
slug: /sql-reference/functions/ext-dict-functions
title: 'Funciones para trabajar con diccionarios'
doc_type: 'reference'
---

:::note
En los diccionarios creados con [DDL queries](../statements/create/dictionary/overview.md), el parámetro `dict_name` debe especificarse por completo, como `<database>.<dict_name>`. De lo contrario, se usa la base de datos actual.
:::

Para obtener información sobre cómo conectar y configurar diccionarios, consulte [Diccionario](../statements/create/dictionary/overview.md).

<div id="example-dictionary">
  ## Diccionarios de ejemplo
</div>

Los ejemplos de esta sección usan los siguientes diccionarios. Puede crearlos en ClickHouse
para ejecutar los ejemplos de las funciones que se describen a continuación.

<details>
  <summary>Diccionario de ejemplo para las funciones dictGet&lt;T&gt; y dictGet&lt;T&gt;OrDefault</summary>

  ```sql
  -- Crear una tabla con todos los tipos de datos necesarios
  CREATE TABLE all_types_test (
      `id` UInt32,
      
      -- Tipo String
      `String_value` String,
      
      -- Tipos enteros sin signo
      `UInt8_value` UInt8,
      `UInt16_value` UInt16,
      `UInt32_value` UInt32,
      `UInt64_value` UInt64,
      
      -- Tipos enteros con signo
      `Int8_value` Int8,
      `Int16_value` Int16,
      `Int32_value` Int32,
      `Int64_value` Int64,
      
      -- Tipos de coma flotante
      `Float32_value` Float32,
      `Float64_value` Float64,
      
      -- Tipos de fecha y hora
      `Date_value` Date,
      `DateTime_value` DateTime,
      
      -- Tipos de red
      `IPv4_value` IPv4,
      `IPv6_value` IPv6,
      
      -- Tipo UUID
      `UUID_value` UUID
  ) ENGINE = MergeTree() 
  ORDER BY id;
  ```

  ```sql
  -- Insertar datos de prueba
  INSERT INTO all_types_test VALUES
  (
      1,                              -- id
      'ClickHouse',                   -- String
      100,                            -- UInt8
      5000,                           -- UInt16
      1000000,                        -- UInt32
      9223372036854775807,            -- UInt64
      -100,                           -- Int8
      -5000,                          -- Int16
      -1000000,                       -- Int32
      -9223372036854775808,           -- Int64
      123.45,                         -- Float32
      987654.123456,                  -- Float64
      '2024-01-15',                   -- Date
      '2024-01-15 10:30:00',          -- DateTime
      '192.168.1.1',                  -- IPv4
      '2001:db8::1',                  -- IPv6
      '550e8400-e29b-41d4-a716-446655440000' -- UUID
  )
  ```

  ```sql
  -- Crear diccionario
  CREATE DICTIONARY all_types_dict
  (
      id UInt32,
      String_value String,
      UInt8_value UInt8,
      UInt16_value UInt16,
      UInt32_value UInt32,
      UInt64_value UInt64,
      Int8_value Int8,
      Int16_value Int16,
      Int32_value Int32,
      Int64_value Int64,
      Float32_value Float32,
      Float64_value Float64,
      Date_value Date,
      DateTime_value DateTime,
      IPv4_value IPv4,
      IPv6_value IPv6,
      UUID_value UUID
  )
  PRIMARY KEY id
  SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'all_types_test' DB 'default'))
  LAYOUT(HASHED())
  LIFETIME(MIN 300 MAX 600);
  ```
</details>

<details>
  <summary>Diccionario de ejemplo para dictGetAll</summary>

  Cree una tabla para almacenar los datos del diccionario regexp tree:

  ```sql
  CREATE TABLE regexp_os(
      id UInt64,
      parent_id UInt64,
      regexp String,
      keys Array(String),
      values Array(String)
  )
  ENGINE = Memory;
  ```

  Inserte los datos en la tabla:

  ```sql
  INSERT INTO regexp_os 
  SELECT *
  FROM s3(
      'https://datasets-documentation.s3.eu-west-3.amazonaws.com/' ||
      'user_agent_regex/regexp_os.csv'
  );
  ```

  Cree el diccionario regexp tree:

  ```sql
  CREATE DICTIONARY regexp_tree
  (
      regexp String,
      os_replacement String DEFAULT 'Other',
      os_v1_replacement String DEFAULT '0',
      os_v2_replacement String DEFAULT '0',
      os_v3_replacement String DEFAULT '0',
      os_v4_replacement String DEFAULT '0'
  )
  PRIMARY KEY regexp
  SOURCE(CLICKHOUSE(TABLE 'regexp_os'))
  LIFETIME(MIN 0 MAX 0)
  LAYOUT(REGEXP_TREE);
  ```
</details>

<details>
  <summary>Ejemplo de diccionario con clave de rango</summary>

  Cree la tabla de entrada:

  ```sql
  CREATE TABLE range_key_dictionary_source_table
  (
      key UInt64,
      start_date Date,
      end_date Date,
      value String,
      value_nullable Nullable(String)
  )
  ENGINE = TinyLog();
  ```

  Inserte los datos en la tabla de entrada:

  ```sql
  INSERT INTO range_key_dictionary_source_table VALUES(1, toDate('2019-05-20'), toDate('2019-05-20'), 'First', 'First');
  INSERT INTO range_key_dictionary_source_table VALUES(2, toDate('2019-05-20'), toDate('2019-05-20'), 'Second', NULL);
  INSERT INTO range_key_dictionary_source_table VALUES(3, toDate('2019-05-20'), toDate('2019-05-20'), 'Third', 'Third');
  ```

  Cree el diccionario:

  ```sql
  CREATE DICTIONARY range_key_dictionary
  (
      key UInt64,
      start_date Date,
      end_date Date,
      value String,
      value_nullable Nullable(String)
  )
  PRIMARY KEY key
  SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'range_key_dictionary_source_table'))
  LIFETIME(MIN 1 MAX 1000)
  LAYOUT(RANGE_HASHED())
  RANGE(MIN start_date MAX end_date);
  ```
</details>

<details>
  <summary>Ejemplo de diccionario con clave compleja</summary>

  Cree la tabla de origen:

  ```sql
  CREATE TABLE dict_mult_source
  (
  id UInt32,
  c1 UInt32,
  c2 String
  ) ENGINE = Memory;
  ```

  Inserte los datos en la tabla de origen:

  ```sql
  INSERT INTO dict_mult_source VALUES
  (1, 1, '1'),
  (2, 2, '2'),
  (3, 3, '3');
  ```

  Cree el diccionario:

  ```sql
  CREATE DICTIONARY ext_dict_mult
  (
      id UInt32,
      c1 UInt32,
      c2 String
  )
  PRIMARY KEY id
  SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'dict_mult_source' DB 'default'))
  LAYOUT(FLAT())
  LIFETIME(MIN 0 MAX 0);
  ```
</details>

<details>
  <summary>Ejemplo de diccionario jerárquico</summary>

  Cree la tabla de origen:

  ```sql
  CREATE TABLE hierarchy_source
  (
    id UInt64,
    parent_id UInt64,
    name String
  ) ENGINE = Memory;
  ```

  Inserte los datos en la tabla de origen:

  ```sql
  INSERT INTO hierarchy_source VALUES
  (0, 0, 'Root'),
  (1, 0, 'Level 1 - Node 1'),
  (2, 1, 'Level 2 - Node 2'),
  (3, 1, 'Level 2 - Node 3'),
  (4, 2, 'Level 3 - Node 4'),
  (5, 2, 'Level 3 - Node 5'),
  (6, 3, 'Level 3 - Node 6');

  -- 0 (Raíz)
  -- └── 1 (Nivel 1 - Nodo 1)
  --     ├── 2 (Nivel 2 - Nodo 2)
  --     │   ├── 4 (Nivel 3 - Nodo 4)
  --     │   └── 5 (Nivel 3 - Nodo 5)
  --     └── 3 (Nivel 2 - Nodo 3)
  --         └── 6 (Nivel 3 - Nodo 6)
  ```

  Cree el diccionario:

  ```sql
  CREATE DICTIONARY hierarchical_dictionary
  (
      id UInt64,
      parent_id UInt64 HIERARCHICAL,
      name String
  )
  PRIMARY KEY id
  SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'hierarchy_source' DB 'default'))
  LAYOUT(HASHED())
  LIFETIME(MIN 300 MAX 600);
  ```
</details>

<div id="passing-keys">
  ## Pasar claves a las funciones de diccionario
</div>

El argumento de clave (`id_expr`) de funciones como `dictGet`, `dictGetOrDefault`, `dictGetOrNull` y `dictHas` depende de la clave del diccionario:

* Para un diccionario con una **clave simple** (`UInt64`), pase directamente el valor de la clave:

```sql
SELECT dictGet('simple_key_dictionary', 'attr_name', toUInt64(1));
```

* Para un diccionario con una **clave compuesta (compleja)** formada por más de un atributo, pase los valores de la clave como una tupla:

```sql
SELECT dictGet('complex_key_dictionary', 'attr_name', ('value_for_field1', 42));
```

* Cuando la **clave compuesta consta de un solo atributo**, el valor de la clave puede pasarse directamente, sin encapsularlo en `tuple`. Ambos ejemplos siguientes son válidos y equivalentes:

```sql
SELECT dictGet('complex_key_dictionary', 'attr_name', 'key');
SELECT dictGet('complex_key_dictionary', 'attr_name', tuple('key'));
```

Esto también se aplica a los diccionarios `ip_trie`, cuya clave es un solo atributo. La dirección IP que se quiere buscar puede pasarse directamente:

```sql
SELECT dictGet('ip_trie_dictionary', 'attr_name', toIPv4('202.79.32.10'));
```

{/* 
  El contenido interno de las etiquetas siguientes se reemplaza durante la build del framework de documentación con 
  documentación generada a partir de system.functions. No modifique ni elimine las etiquetas.
  Consulte: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }