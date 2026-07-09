---
description: 'Fonction de table qui permet de convertir et d’insérer efficacement des données
  envoyées au serveur avec une structure donnée dans une table ayant une structure différente.'
sidebar_label: 'input'
sidebar_position: 95
slug: /sql-reference/table-functions/input
title: 'input'
doc_type: 'reference'
---

`input(structure)` - fonction de table qui permet de convertir et d’insérer efficacement des données envoyées au
serveur avec une structure donnée dans une table ayant une structure différente.

`structure` - structure des données envoyées au serveur au format suivant `'column1_name column1_type, column2_name column2_type, ...'`.
Par exemple, `'id UInt32, name String'`.

Cette fonction ne peut être utilisée que dans une requête `INSERT SELECT`, et une seule fois, mais se comporte par ailleurs comme une fonction de table ordinaire
(par exemple, elle peut être utilisée dans une sous-requête, etc.).

Les données peuvent être envoyées comme pour une requête `INSERT` ordinaire et transmises dans n’importe quel [format](/fr/sql-reference/formats)
disponible, qui doit être spécifié à la fin de la requête (contrairement à une requête `INSERT SELECT` ordinaire).

La principale fonctionnalité de cette fonction est que lorsque le serveur reçoit des données du client, il les convertit simultanément
selon la liste des expressions de la clause `SELECT` et les insère dans la table cible. Aucune table temporaire
contenant toutes les données transférées n’est créée.

<div id="examples">
  ## Exemples
</div>

* Supposons que la table `test` ait la structure suivante `(a String, b String)`
  et que les données de `data.csv` aient une structure différente `(col1 String, col2 Date, col3 Int32)`. La requête permettant d’insérer
  les données de `data.csv` dans la table `test` avec conversion simultanée se présente comme suit :

{/* */ }

```bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

* Si `data.csv` contient des données ayant la même structure `test_structure` que la table `test`, alors ces deux requêtes sont équivalentes :

{/* */ }

```bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```