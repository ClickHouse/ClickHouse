---
alias: []
description: 'Documentation sur le format MySQLDump'
input_format: true
keywords: ['MySQLDump']
output_format: false
slug: /interfaces/formats/MySQLDump
title: 'MySQLDump'
doc_type: 'référence'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✗      |       |

<div id="description">
  ## Description
</div>

ClickHouse prend en charge la lecture des [dumps](https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html) MySQL.

Il lit toutes les données des requêtes `INSERT` issues d&#39;une même table dans le dump.
S&#39;il y a plusieurs tables, il lit par défaut les données de la première.

:::note
Ce format prend en charge l&#39;inférence de schéma : si le dump contient une requête `CREATE` pour la table spécifiée, la structure en est déduite ; sinon, le schéma est déduit des données des requêtes `INSERT`.
:::

<div id="example-usage">
  ## Exemple d’utilisation
</div>

Soit le fichier dump SQL suivant :

```sql title="dump.sql"
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test` (
  `x` int DEFAULT NULL,
  `y` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test` VALUES (1,NULL),(2,NULL),(3,NULL),(3,NULL),(4,NULL),(5,NULL),(6,7);
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test 3` (
  `y` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test 3` VALUES (1);
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test2` (
  `x` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test2` VALUES (1),(2),(3);
```

Vous pouvez exécuter les requêtes suivantes :

```sql title="Query"
DESCRIBE TABLE file(dump.sql, MySQLDump) 
SETTINGS input_format_mysql_dump_table_name = 'test2'
```

```response title="Response"
┌─name─┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ x    │ Nullable(Int32) │              │                    │         │                  │                │
└──────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql title="Query"
SELECT *
FROM file(dump.sql, MySQLDump)
SETTINGS input_format_mysql_dump_table_name = 'test2'
```

```response title="Response"
┌─x─┐
│ 1 │
│ 2 │
│ 3 │
└───┘
```

<div id="format-settings">
  ## Paramètres de format
</div>

Vous pouvez spécifier le nom de la table à partir de laquelle lire les données à l’aide du paramètre [`input_format_mysql_dump_table_name`](/fr/operations/settings/settings-formats.md/#input_format_mysql_dump_table_name).
Si le paramètre `input_format_mysql_dump_map_columns` est défini sur `1` et que le dump contient une requête `CREATE` pour la table spécifiée ou des noms de colonnes dans la requête `INSERT`, les colonnes des données d’entrée seront associées aux colonnes de la table par leur nom.
Les colonnes dont le nom est inconnu seront ignorées si le paramètre [`input_format_skip_unknown_fields`](/fr/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) est défini sur `1`.