---
description: 'Permet de se connecter à des bases de données sur un serveur PostgreSQL distant.'
sidebar_label: 'PostgreSQL'
sidebar_position: 40
slug: /engines/database-engines/postgresql
title: 'PostgreSQL'
doc_type: 'guide'
---

Permet de se connecter à des bases de données sur un serveur [PostgreSQL](https://www.postgresql.org) distant. Prend en charge les opérations de lecture et d’écriture (requêtes `SELECT` et `INSERT`) pour l’échange de données entre ClickHouse et PostgreSQL.

Offre un accès en temps réel à la liste des tables et à leur structure sur le serveur PostgreSQL distant à l’aide des requêtes `SHOW TABLES` et `DESCRIBE TABLE`.

Prend en charge les modifications de la structure des tables (`ALTER TABLE ... ADD|DROP COLUMN`). Si le paramètre `use_table_cache` (voir les paramètres du moteur ci-dessous) est défini sur `1`, la structure de la table est mise en cache et les modifications ne sont pas vérifiées, mais elle peut être mise à jour avec les requêtes `DETACH` et `ATTACH`.

<div id="creating-a-database">
  ## Créer une base de données
</div>

```sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('host:port', 'database', 'user', 'password'[, `schema`, `use_table_cache`]);
```

**Paramètres du moteur**

* `host:port` — Adresse du serveur PostgreSQL.
* `database` — Nom de la base de données distante.
* `user` — Utilisateur PostgreSQL.
* `password` — Mot de passe de l’utilisateur.
* `schema` — Schéma PostgreSQL.
* `use_table_cache` — Définit si la structure de la table de la base de données est mise en cache ou non. Facultatif. Valeur par défaut : `0`.

<div id="data_types-support">
  ## Prise en charge des types de données
</div>

| PostgreSQL       | ClickHouse                                                                     |
| ---------------- | ------------------------------------------------------------------------------ |
| DATE             | [Date](../../sql-reference/data-types/date.md)                                 |
| TIMESTAMP        | [DateTime](../../sql-reference/data-types/datetime.md)                         |
| REAL             | [Float32](../../sql-reference/data-types/float.md)                             |
| DOUBLE           | [Float64](../../sql-reference/data-types/float.md)                             |
| DECIMAL, NUMERIC | [Decimal](../../sql-reference/data-types/decimal.md) (voir la note ci-dessous) |
| SMALLINT         | [Int16](../../sql-reference/data-types/int-uint.md)                            |
| INTEGER          | [Int32](../../sql-reference/data-types/int-uint.md)                            |
| BIGINT           | [Int64](../../sql-reference/data-types/int-uint.md)                            |
| SERIAL           | [UInt32](../../sql-reference/data-types/int-uint.md)                           |
| BIGSERIAL        | [UInt64](../../sql-reference/data-types/int-uint.md)                           |
| TEXT, CHAR       | [String](../../sql-reference/data-types/string.md)                             |
| INTEGER          | Nullable([Int32](../../sql-reference/data-types/int-uint.md))                  |
| ARRAY            | [Array](../../sql-reference/data-types/array.md)                               |

:::note
Le type PostgreSQL `numeric(p, 0)` avec une `precision` `p` supérieure à 76 (le maximum pris en charge par `Decimal256`) — par exemple `numeric(78, 0)`, couramment utilisé pour stocker des entiers sur 256 bits — est converti en [`Int256`](../../sql-reference/data-types/int-uint.md) plutôt qu&#39;en `Decimal`. Les valeurs qui ne tiennent pas dans la plage de `Int256` sont rejetées avec une erreur.
:::

<div id="examples-of-use">
  ## Exemples d’utilisation
</div>

Base de données dans ClickHouse échangeant des données avec le serveur PostgreSQL :

```sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword', 'schema_name',1);
```

```sql
SHOW DATABASES;
```

```text
┌─name──────────┐
│ default       │
│ test_database │
│ system        │
└───────────────┘
```

```sql
SHOW TABLES FROM test_database;
```

```text
┌─name───────┐
│ test_table │
└────────────┘
```

Lecture des données depuis la table PostgreSQL :

```sql
SELECT * FROM test_database.test_table;
```

```text
┌─id─┬─value─┐
│  1 │     2 │
└────┴───────┘
```

Écriture de données dans la table PostgreSQL :

```sql
INSERT INTO test_database.test_table VALUES (3,4);
SELECT * FROM test_database.test_table;
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

Supposons que la structure de la table ait été modifiée dans PostgreSQL :

```sql
postgre> ALTER TABLE test_table ADD COLUMN data Text
```

Comme le paramètre `use_table_cache` a été défini sur `1` lors de la création de la base de données, la structure de la table dans ClickHouse a été mise en cache et n’a donc pas été modifiée :

```sql
DESCRIBE TABLE test_database.test_table;
```

```text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
└────────┴───────────────────┘
```

Après avoir détaché la table puis l’avoir rattachée, la structure a été mise à jour :

```sql
DETACH TABLE test_database.test_table;
ATTACH TABLE test_database.test_table;
DESCRIBE TABLE test_database.test_table;
```

```text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
│ data   │ Nullable(String)  │
└────────┴───────────────────┘
```

<div id="related-content">
  ## Contenu associé
</div>

* Blog : [ClickHouse et PostgreSQL - le duo idéal pour les données - partie 1](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
* Blog : [ClickHouse et PostgreSQL - le duo idéal pour les données - partie 2](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)