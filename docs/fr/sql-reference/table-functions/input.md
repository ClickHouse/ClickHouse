---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: "entr\xE9e"
---

# entrée {#input}

`input(structure)` - fonction de table qui permet effectivement convertir et insérer des données envoyées à la
serveur avec une structure donnée à la table avec une autre structure.

`structure` - structure de données envoyées au serveur dans le format suivant `'column1_name column1_type, column2_name column2_type, ...'`.
Exemple, `'id UInt32, name String'`.

Cette fonction peut être utilisée uniquement dans `INSERT SELECT` requête et une seule fois mais se comporte autrement comme une fonction de table ordinaire
(par exemple, il peut être utilisé dans la sous-requête, etc.).

Les données peuvent être envoyées de quelque manière que ce soit comme pour ordinaire `INSERT` requête et passé dans tout disponible [format](../../interfaces/formats.md#formats)
qui doit être spécifié à la fin de la requête (contrairement à l'ordinaire `INSERT SELECT`).

La caractéristique principale de cette fonction est que lorsque le serveur reçoit des données du client il les convertit simultanément
selon la liste des expressions dans le `SELECT` clause et insère dans la table cible. Table temporaire
avec toutes les données transférées n'est pas créé.

**Exemple**

-   Laissez le `test` le tableau a la structure suivante `(a String, b String)`
    et les données `data.csv` a une structure différente `(col1 String, col2 Date, col3 Int32)`. Requête pour insérer
    les données de l' `data.csv` dans le `test` table avec conversion simultanée ressemble à ceci:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

-   Si `data.csv` contient les données de la même structure `test_structure` comme la table `test` puis ces deux requêtes sont égales:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```

[Article Original](https://clickhouse.tech/docs/en/query_language/table_functions/input/) <!--hide-->
