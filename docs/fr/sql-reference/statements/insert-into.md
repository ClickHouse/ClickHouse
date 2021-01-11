---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: INSERT INTO
---

## INSERT {#insert}

L'ajout de données.

Format de requête de base:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

La requête peut spécifier une liste de colonnes à insérer `[(c1, c2, c3)]`. Dans ce cas, le reste des colonnes sont remplis avec:

-   Les valeurs calculées à partir `DEFAULT` expressions spécifiées dans la définition de la table.
-   Zéros et chaînes vides, si `DEFAULT` les expressions ne sont pas définies.

Si [strict_insert_defaults=1](../../operations/settings/settings.md), les colonnes qui n'ont pas `DEFAULT` défini doit être répertorié dans la requête.

Les données peuvent être transmises à L'INSERT dans n'importe quel [format](../../interfaces/formats.md#formats) soutenu par ClickHouse. Le format doit être spécifié explicitement dans la requête:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

For example, the following query format is identical to the basic version of INSERT … VALUES:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse supprime tous les espaces et un saut de ligne (s'il y en a un) avant les données. Lors de la formation d'une requête, nous recommandons de placer les données sur une nouvelle ligne après les opérateurs de requête (ceci est important si les données commencent par des espaces).

Exemple:

``` sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

Vous pouvez insérer des données séparément de la requête à l'aide du client de ligne de commande ou de L'interface HTTP. Pour plus d'informations, consultez la section “[Interface](../../interfaces/index.md#interfaces)”.

### Contraintes {#constraints}

Si la table a [contraintes](create.md#constraints), their expressions will be checked for each row of inserted data. If any of those constraints is not satisfied — server will raise an exception containing constraint name and expression, the query will be stopped.

### Insertion des résultats de `SELECT` {#insert_query_insert-select}

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

Les colonnes sont mappées en fonction de leur position dans la clause SELECT. Cependant, leurs noms dans L'expression SELECT et la table pour INSERT peuvent différer. Si nécessaire, la coulée de type est effectuée.

Aucun des formats de données à l'exception des Valeurs permettent de définir des valeurs d'expressions telles que `now()`, `1 + 2` et ainsi de suite. Le format des valeurs permet une utilisation limitée des expressions, mais ce n'est pas recommandé, car dans ce cas, un code inefficace est utilisé pour leur exécution.

Les autres requêtes de modification des parties de données ne sont pas prises en charge: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
Cependant, vous pouvez supprimer les anciennes données en utilisant `ALTER TABLE ... DROP PARTITION`.

`FORMAT` la clause doit être spécifié à la fin de la requête si `SELECT` la clause contient la fonction de table [entrée()](../table-functions/input.md).

### Considérations De Performance {#performance-considerations}

`INSERT` trie les données d'entrée par la clé primaire et les divise en partitions par une clé de partition. Si vous insérez des données dans plusieurs partitions à la fois, cela peut réduire considérablement les performances de l' `INSERT` requête. Pour éviter cela:

-   Ajoutez des données en lots assez importants, tels que 100 000 lignes à la fois.
-   Groupez les données par une clé de partition avant de les télécharger sur ClickHouse.

Les performances ne diminueront pas si:

-   Les données sont ajoutées en temps réel.
-   Vous téléchargez des données qui sont généralement triées par heure.

[Article Original](https://clickhouse.tech/docs/en/query_language/insert_into/) <!--hide-->
