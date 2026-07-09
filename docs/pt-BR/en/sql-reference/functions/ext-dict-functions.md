---
description: 'Documentação sobre funções para trabalhar com dicionários'
sidebar_label: 'Dicionários'
slug: /sql-reference/functions/ext-dict-functions
title: 'Funções para trabalhar com dicionários'
doc_type: 'reference'
---

:::note
Para dicionários criados com [consultas DDL](../statements/create/dictionary/overview.md), o parâmetro `dict_name` deve ser especificado completamente, como `<database>.<dict_name>`. Caso contrário, o banco de dados atual será usado.
:::

Para informações sobre como conectar e configurar dicionários, consulte [Dicionários](../statements/create/dictionary/overview.md).

<div id="example-dictionary">
  ## Dicionários de exemplo
</div>

Os exemplos desta seção usam os dicionários a seguir. Você pode criá-los no ClickHouse
para executar os exemplos das funções descritas abaixo.

<details>
  <summary>Dicionário de exemplo para as funções dictGet&lt;T&gt; e dictGet&lt;T&gt;OrDefault</summary>

  ```sql
  -- Criar tabela com todos os tipos de dados necessários
  CREATE TABLE all_types_test (
      `id` UInt32,
      
      -- Tipo String
      `String_value` String,
      
      -- Tipos inteiros sem sinal
      `UInt8_value` UInt8,
      `UInt16_value` UInt16,
      `UInt32_value` UInt32,
      `UInt64_value` UInt64,
      
      -- Tipos inteiros com sinal
      `Int8_value` Int8,
      `Int16_value` Int16,
      `Int32_value` Int32,
      `Int64_value` Int64,
      
      -- Tipos de ponto flutuante
      `Float32_value` Float32,
      `Float64_value` Float64,
      
      -- Tipos de data/hora
      `Date_value` Date,
      `DateTime_value` DateTime,
      
      -- Tipos de rede
      `IPv4_value` IPv4,
      `IPv6_value` IPv6,
      
      -- Tipo UUID
      `UUID_value` UUID
  ) ENGINE = MergeTree() 
  ORDER BY id;
  ```

  ```sql
  -- Inserir dados de teste
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
  -- Criar dicionário
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
  <summary>Dicionário de exemplo para dictGetAll</summary>

  Crie uma tabela para armazenar os dados do dicionário regexp tree:

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

  Insira os dados na tabela:

  ```sql
  INSERT INTO regexp_os 
  SELECT *
  FROM s3(
      'https://datasets-documentation.s3.eu-west-3.amazonaws.com/' ||
      'user_agent_regex/regexp_os.csv'
  );
  ```

  Crie o dicionário regexp tree:

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
  <summary>Exemplo de dicionário com chave de intervalo</summary>

  Crie a tabela de entrada:

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

  Insira os dados na tabela de entrada:

  ```sql
  INSERT INTO range_key_dictionary_source_table VALUES(1, toDate('2019-05-20'), toDate('2019-05-20'), 'First', 'First');
  INSERT INTO range_key_dictionary_source_table VALUES(2, toDate('2019-05-20'), toDate('2019-05-20'), 'Second', NULL);
  INSERT INTO range_key_dictionary_source_table VALUES(3, toDate('2019-05-20'), toDate('2019-05-20'), 'Third', 'Third');
  ```

  Crie o dicionário:

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
  <summary>Exemplo de dicionário com chave complexa</summary>

  Crie a tabela de origem:

  ```sql
  CREATE TABLE dict_mult_source
  (
  id UInt32,
  c1 UInt32,
  c2 String
  ) ENGINE = Memory;
  ```

  Insira os dados na tabela de origem:

  ```sql
  INSERT INTO dict_mult_source VALUES
  (1, 1, '1'),
  (2, 2, '2'),
  (3, 3, '3');
  ```

  Crie o dicionário:

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
  <summary>Exemplo de dicionário hierárquico</summary>

  Crie a tabela de origem:

  ```sql
  CREATE TABLE hierarchy_source
  (
    id UInt64,
    parent_id UInt64,
    name String
  ) ENGINE = Memory;
  ```

  Insira os dados na tabela de origem:

  ```sql
  INSERT INTO hierarchy_source VALUES
  (0, 0, 'Root'),
  (1, 0, 'Level 1 - Node 1'),
  (2, 1, 'Level 2 - Node 2'),
  (3, 1, 'Level 2 - Node 3'),
  (4, 2, 'Level 3 - Node 4'),
  (5, 2, 'Level 3 - Node 5'),
  (6, 3, 'Level 3 - Node 6');

  -- 0 (Raiz)
  -- └── 1 (Nível 1 - Nó 1)
  --     ├── 2 (Nível 2 - Nó 2)
  --     │   ├── 4 (Nível 3 - Nó 4)
  --     │   └── 5 (Nível 3 - Nó 5)
  --     └── 3 (Nível 2 - Nó 3)
  --         └── 6 (Nível 3 - Nó 6)
  ```

  Crie o dicionário:

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
  ## Passando chaves para funções de dicionário
</div>

O argumento de chave (`id_expr`) de funções como `dictGet`, `dictGetOrDefault`, `dictGetOrNull` e `dictHas` depende da chave do dicionário:

* Para um dicionário com uma **chave simples** (`UInt64`), passe o valor da chave diretamente:

```sql
SELECT dictGet('simple_key_dictionary', 'attr_name', toUInt64(1));
```

* Para um dicionário com uma **chave composta (complexa)** com mais de um atributo, passe os valores da chave como uma tupla:

```sql
SELECT dictGet('complex_key_dictionary', 'attr_name', ('value_for_field1', 42));
```

* Quando a **chave composta consiste em um único atributo**, o valor da chave pode ser passado diretamente, sem precisar envolvê-lo em `tuple`. Ambos os exemplos a seguir são válidos e equivalentes:

```sql
SELECT dictGet('complex_key_dictionary', 'attr_name', 'key');
SELECT dictGet('complex_key_dictionary', 'attr_name', tuple('key'));
```

Isso também se aplica aos dicionários `ip_trie`, cuja chave é um único atributo. O endereço IP a ser consultado pode ser informado diretamente:

```sql
SELECT dictGet('ip_trie_dictionary', 'attr_name', toIPv4('202.79.32.10'));
```

{/* 
  O conteúdo interno das tags abaixo é substituído durante a compilação do framework de documentação por 
  documentação gerada a partir de system.functions. Não modifique nem remova as tags.
  Consulte: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }