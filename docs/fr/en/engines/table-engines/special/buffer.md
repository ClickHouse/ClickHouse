---
description: 'Met les données en tampon dans la RAM, puis les vidage périodiquement
  vers une autre table. Lors de la lecture, les données sont lues simultanément
  depuis le tampon et l''autre table.'
sidebar_label: 'Buffer'
sidebar_position: 120
slug: /engines/table-engines/special/buffer
title: 'Moteur de table Buffer'
doc_type: 'référence'
---

Met les données en tampon dans la RAM, puis les vidage périodiquement vers une autre table. Lors de la lecture, les données sont lues simultanément depuis le tampon et l&#39;autre table.

:::note
Une alternative recommandée au moteur de table Buffer consiste à activer les [insertions asynchrones](/fr/guides/best-practices/asyncinserts.md).
:::

```sql
Buffer(database, table, num_layers, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes [,flush_time [,flush_rows [,flush_bytes]]])
```

<div id="engine-parameters">
  ### Paramètres du moteur
</div>

<div id="database">
  #### `database`
</div>

`database` – Nom de la base de données. Vous pouvez utiliser `currentDatabase()` ou toute autre expression constante qui renvoie une chaîne de caractères.

<div id="table">
  #### `table`
</div>

`table` – Table dans laquelle vider les données.

<div id="num_layers">
  #### `num_layers`
</div>

`num_layers` – Niveau de parallélisme. Concrètement, la table sera représentée par `num_layers` tampons indépendants.

<div id="min_time-max_time-min_rows-max_rows-min_bytes-and-max_bytes">
  #### `min_time`, `max_time`, `min_rows`, `max_rows`, `min_bytes`, and `max_bytes`
</div>

Conditions de vidage des données du tampon.

<div id="optional-engine-parameters">
  ### Paramètres optionnels du moteur
</div>

<div id="flush_time-flush_rows-and-flush_bytes">
  #### `flush_time`, `flush_rows`, et `flush_bytes`
</div>

Conditions de vidage des données du tampon en arrière-plan (si elles sont omises ou valent zéro, cela signifie qu&#39;il n&#39;y a pas de paramètres `flush*`).

Les données sont vidées du tampon et écrites dans la table de destination si toutes les conditions `min*` sont remplies, ou si au moins une condition `max*` est remplie.

De plus, si au moins une condition `flush*` est remplie, un vidage est déclenché en arrière-plan. Cela diffère de `max*`, car `flush*` vous permet de configurer séparément les vidages en arrière-plan afin d&#39;éviter d&#39;ajouter de la latence aux requêtes `INSERT` sur les tables Buffer.

<div id="min_time-max_time-and-flush_time">
  #### `min_time`, `max_time` et `flush_time`
</div>

Condition relative à la durée, en secondes, écoulée depuis la première écriture dans le tampon.

<div id="min_rows-max_rows-and-flush_rows">
  #### `min_rows`, `max_rows`, and `flush_rows`
</div>

Condition relative au nombre de lignes dans le tampon.

<div id="min_bytes-max_bytes-and-flush_bytes">
  #### `min_bytes`, `max_bytes`, and `flush_bytes`
</div>

Condition relative au nombre d’octets dans le tampon.

Pendant l’opération d’écriture, les données sont insérées dans un ou plusieurs tampons choisis aléatoirement (configurés avec `num_layers`). Ou, si la part de données à insérer est suffisamment volumineuse (supérieure à `max_rows` ou `max_bytes`), elle est écrite directement dans la table de destination, sans passer par le tampon.

Les conditions de vidage des données sont calculées séparément pour chacun des `num_layers` tampons. Par exemple, si `num_layers = 16` et `max_bytes = 100000000`, la consommation maximale de RAM est de 1,6 Go.

Exemple :

```sql
CREATE TABLE merge.hits_buffer AS merge.hits ENGINE = Buffer(merge, hits, 1, 10, 100, 10000, 1000000, 10000000, 100000000)
```

Création d&#39;une table `merge.hits_buffer` avec la même structure que `merge.hits` en utilisant le moteur Buffer. Lors de l&#39;écriture dans cette table, les données sont mises en mémoire tampon dans la RAM, puis écrites ultérieurement dans la table &#39;merge.hits&#39;. Un seul tampon est créé, et les données sont vidées si l&#39;une des conditions suivantes est remplie :

* 100 secondes se sont écoulées depuis la dernière vidange (`max_time`) ou
* 1 million de lignes ont été écrites (`max_rows`) ou
* 100 Mo de données ont été écrits (`max_bytes`) ou
* 10 secondes se sont écoulées (`min_time`) et 10 000 lignes (`min_rows`) et 10 Mo (`min_bytes`) de données ont été écrites

Par exemple, si une seule ligne a été écrite, elle sera vidée après 100 secondes, quoi qu&#39;il arrive. En revanche, si de nombreuses lignes ont été écrites, les données seront vidées plus tôt.

Lorsque le serveur est arrêté, avec `DROP TABLE` ou `DETACH TABLE`, les données mises en mémoire tampon sont également vidées dans la table de destination.

Vous pouvez définir des chaînes vides entre guillemets simples pour la base de données et le nom de la table. Cela indique l&#39;absence de table de destination. Dans ce cas, lorsque les conditions de vidange des données sont atteintes, le tampon est simplement effacé. Cela peut être utile pour conserver une fenêtre de données en mémoire.

Lors de la lecture depuis une table Buffer, les données sont traitées à la fois depuis le tampon et depuis la table de destination (s&#39;il y en a une).
Notez que la table Buffer ne prend pas en charge d&#39;index. En d&#39;autres termes, les données du tampon sont entièrement parcourues, ce qui peut être lent pour les tampons volumineux. (Pour les données d&#39;une table subordonnée, l&#39;index qu&#39;elle prend en charge sera utilisé.)

Si l&#39;ensemble de colonnes de la table Buffer ne correspond pas à celui d&#39;une table subordonnée, un sous-ensemble des colonnes présentes dans les deux tables est inséré.

Si les types ne correspondent pas pour l&#39;une des colonnes de la table Buffer et d&#39;une table subordonnée, un message d&#39;erreur est consigné dans le journal du serveur, et le tampon est vidé.
Il en va de même si la table subordonnée n&#39;existe pas au moment où le tampon est vidé.

:::note
L&#39;exécution de ALTER sur la table Buffer dans les releases publiées avant le 26 oct. 2021 provoquera une erreur `Block structure mismatch` (voir [#15117](https://github.com/ClickHouse/ClickHouse/issues/15117) et [#30565](https://github.com/ClickHouse/ClickHouse/pull/30565)) ; la seule option consiste donc à supprimer la table Buffer, puis à la recréer. Vérifiez que cette erreur est corrigée dans votre release avant d&#39;essayer d&#39;exécuter ALTER sur la table Buffer.
:::

Si le serveur redémarre anormalement, les données du tampon sont perdues.

`FINAL` et `SAMPLE` ne fonctionnent pas correctement pour les tables Buffer. Ces conditions sont transmises à la table de destination, mais ne sont pas utilisées pour traiter les données du tampon. Si ces fonctionnalités sont nécessaires, nous recommandons d&#39;utiliser la table Buffer uniquement pour l&#39;écriture, tout en lisant depuis la table de destination.

Lors de l&#39;ajout de données à une table Buffer, l&#39;un des tampons est verrouillé. Cela entraîne des délais si une opération de lecture est effectuée simultanément sur la table.

Les données insérées dans une table Buffer peuvent se retrouver dans la table subordonnée dans un ordre différent et dans des blocks différents. Pour cette raison, une table Buffer est difficile à utiliser pour écrire correctement dans une CollapsingMergeTree. Pour éviter les problèmes, vous pouvez définir `num_layers` sur 1.

Si la table de destination est répliquée, certaines caractéristiques attendues des tables répliquées sont perdues lors de l&#39;écriture dans une table Buffer. Les modifications aléatoires de l&#39;ordre des lignes et de la taille des data parts font que la déduplication des données cesse de fonctionner, ce qui signifie qu&#39;il n&#39;est pas possible d&#39;obtenir une écriture &#39;exactly once&#39; fiable dans des tables répliquées.

En raison de ces inconvénients, nous ne pouvons recommander l&#39;utilisation d&#39;une table Buffer que dans de rares cas.

Une table Buffer est utilisée lorsqu&#39;un trop grand nombre d&#39;INSERT est reçu depuis un grand nombre de serveurs sur une période donnée, et que les données ne peuvent pas être mises en mémoire tampon avant l&#39;insertion, ce qui signifie que les INSERT ne peuvent pas s&#39;exécuter assez rapidement.

Notez qu’il n’est pas judicieux d’insérer des données ligne par ligne, même pour les tables Buffer. Cela ne permet d’atteindre que quelques milliers de lignes par seconde, tandis que l’insertion de blocs de données plus volumineux peut dépasser le million de lignes par seconde.