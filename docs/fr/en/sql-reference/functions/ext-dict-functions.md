---
description: 'Documentation des fonctions pour travailler avec les dictionnaires'
sidebar_label: 'Dictionnaires'
slug: /sql-reference/functions/ext-dict-functions
title: 'Fonctions pour travailler avec les dictionnaires'
doc_type: 'reference'
---

:::note
Pour les dictionnaires créés avec les [requêtes DDL](../statements/create/dictionary/overview.md), le paramètre `dict_name` doit être entièrement spécifié, sous la forme `<database>.<dict_name>`. Sinon, la base de données courante est utilisée.
:::

Pour plus d&#39;informations sur la connexion aux dictionnaires et leur configuration, consultez [Dictionnaires](../statements/create/dictionary/overview.md).

<div id="example-dictionary">
  ## Exemples de dictionnaires
</div>

Les exemples de cette section utilisent les dictionnaires suivants. Vous pouvez les créer dans ClickHouse
pour exécuter les exemples des fonctions décrites ci-dessous.

<details>
  <summary>Dictionnaire d’exemple pour les fonctions dictGet&lt;T&gt; et dictGet&lt;T&gt;OrDefault</summary>

  ```sql
  -- Créer une table avec tous les types de données requis
  CREATE TABLE all_types_test (
      `id` UInt32,
      
      -- Type String
      `String_value` String,
      
      -- Types d’entiers non signés
      `UInt8_value` UInt8,
      `UInt16_value` UInt16,
      `UInt32_value` UInt32,
      `UInt64_value` UInt64,
      
      -- Types d’entiers signés
      `Int8_value` Int8,
      `Int16_value` Int16,
      `Int32_value` Int32,
      `Int64_value` Int64,
      
      -- Types à virgule flottante
      `Float32_value` Float32,
      `Float64_value` Float64,
      
      -- Types de date/heure
      `Date_value` Date,
      `DateTime_value` DateTime,
      
      -- Types réseau
      `IPv4_value` IPv4,
      `IPv6_value` IPv6,
      
      -- Type UUID
      `UUID_value` UUID
  ) ENGINE = MergeTree() 
  ORDER BY id;
  ```

  ```sql
  -- Insérer des données de test
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
  -- Créer le dictionnaire
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
  <summary>Dictionnaire d’exemple pour dictGetAll</summary>

  Créez une table pour stocker les données du dictionnaire regexp tree :

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

  Insérez les données dans la table :

  ```sql
  INSERT INTO regexp_os 
  SELECT *
  FROM s3(
      'https://datasets-documentation.s3.eu-west-3.amazonaws.com/' ||
      'user_agent_regex/regexp_os.csv'
  );
  ```

  Créez le dictionnaire regexp tree :

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
  <summary>Exemple de dictionnaire avec clé de plage</summary>

  Créez la table d&#39;entrée :

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

  Insérez les données dans la table d&#39;entrée :

  ```sql
  INSERT INTO range_key_dictionary_source_table VALUES(1, toDate('2019-05-20'), toDate('2019-05-20'), 'First', 'First');
  INSERT INTO range_key_dictionary_source_table VALUES(2, toDate('2019-05-20'), toDate('2019-05-20'), 'Second', NULL);
  INSERT INTO range_key_dictionary_source_table VALUES(3, toDate('2019-05-20'), toDate('2019-05-20'), 'Third', 'Third');
  ```

  Créez le dictionnaire :

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
  <summary>Exemple de dictionnaire à clé composite</summary>

  Créez la table source :

  ```sql
  CREATE TABLE dict_mult_source
  (
  id UInt32,
  c1 UInt32,
  c2 String
  ) ENGINE = Memory;
  ```

  Insérez les données dans la table source :

  ```sql
  INSERT INTO dict_mult_source VALUES
  (1, 1, '1'),
  (2, 2, '2'),
  (3, 3, '3');
  ```

  Créez le dictionnaire :

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
  <summary>Exemple de dictionnaire hiérarchique</summary>

  Créez la table source :

  ```sql
  CREATE TABLE hierarchy_source
  (
    id UInt64,
    parent_id UInt64,
    name String
  ) ENGINE = Memory;
  ```

  Insérez les données dans la table source :

  ```sql
  INSERT INTO hierarchy_source VALUES
  (0, 0, 'Root'),
  (1, 0, 'Level 1 - Node 1'),
  (2, 1, 'Level 2 - Node 2'),
  (3, 1, 'Level 2 - Node 3'),
  (4, 2, 'Level 3 - Node 4'),
  (5, 2, 'Level 3 - Node 5'),
  (6, 3, 'Level 3 - Node 6');

  -- 0 (Racine)
  -- └── 1 (Niveau 1 - Nœud 1)
  --     ├── 2 (Niveau 2 - Nœud 2)
  --     │   ├── 4 (Niveau 3 - Nœud 4)
  --     │   └── 5 (Niveau 3 - Nœud 5)
  --     └── 3 (Niveau 2 - Nœud 3)
  --         └── 6 (Niveau 3 - Nœud 6)
  ```

  Créez le dictionnaire :

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
  ## Passage des clés aux fonctions de dictionnaire
</div>

L’argument de clé (`id_expr`) de fonctions telles que `dictGet`, `dictGetOrDefault`, `dictGetOrNull` et `dictHas` dépend de la clé du dictionnaire :

* Pour un dictionnaire avec une **clé simple** (`UInt64`), passez directement la valeur de la clé :

```sql
SELECT dictGet('simple_key_dictionary', 'attr_name', toUInt64(1));
```

* Pour un dictionnaire avec une **clé composite (complexe)** comportant plus d’un attribut, transmettez les valeurs de la clé sous forme de tuple :

```sql
SELECT dictGet('complex_key_dictionary', 'attr_name', ('value_for_field1', 42));
```

* Lorsque la **clé composite ne comporte qu’un seul attribut**, la valeur de clé peut être passée directement, sans être encapsulée dans `tuple`. Les deux exemples suivants sont valides et équivalents :

```sql
SELECT dictGet('complex_key_dictionary', 'attr_name', 'key');
SELECT dictGet('complex_key_dictionary', 'attr_name', tuple('key'));
```

Cela s’applique également aux dictionnaires `ip_trie`, dont la clé est constituée d’un seul attribut. L’adresse IP à rechercher peut être transmise directement :

```sql
SELECT dictGet('ip_trie_dictionary', 'attr_name', toIPv4('202.79.32.10'));
```

{/* 
  Le contenu interne des balises ci-dessous est remplacé lors du processus de build du framework de documentation par 
  la documentation générée depuis system.functions. Veuillez ne pas modifier ni supprimer les balises.
  Voir : https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }