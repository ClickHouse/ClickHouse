---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: Tampon
---

# Tampon {#buffer}

Met en mémoire tampon les données à écrire dans la RAM, les vidant périodiquement dans une autre table. Pendant l'opération de lecture, les données sont lues à partir de la mémoire tampon, et l'autre simultanément.

``` sql
Buffer(database, table, num_layers, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes)
```

Les paramètres du moteur:

-   `database` – Database name. Instead of the database name, you can use a constant expression that returns a string.
-   `table` – Table to flush data to.
-   `num_layers` – Parallelism layer. Physically, the table will be represented as `num_layers` indépendant de tampons. Valeur recommandée: 16.
-   `min_time`, `max_time`, `min_rows`, `max_rows`, `min_bytes`, et `max_bytes` – Conditions for flushing data from the buffer.

Les données sont vidées du tampon et écrites dans la table de destination si toutes les `min*` conditions ou au moins un `max*` conditions sont remplies.

-   `min_time`, `max_time` – Condition for the time in seconds from the moment of the first write to the buffer.
-   `min_rows`, `max_rows` – Condition for the number of rows in the buffer.
-   `min_bytes`, `max_bytes` – Condition for the number of bytes in the buffer.

Pendant l'opération d'écriture, les données sont insérées dans un `num_layers` nombre aléatoire de tampons. Ou, si la partie de données à insérer est suffisamment grande (supérieure à `max_rows` ou `max_bytes`), il est écrit directement dans la table de destination, en omettant le tampon.

Les conditions de purger les données sont calculées séparément pour chacun des `num_layers` tampon. Par exemple, si `num_layers = 16` et `max_bytes = 100000000`, la consommation maximale de RAM est de 1,6 Go.

Exemple:

``` sql
CREATE TABLE merge.hits_buffer AS merge.hits ENGINE = Buffer(merge, hits, 16, 10, 100, 10000, 1000000, 10000000, 100000000)
```

La création d'un ‘merge.hits\_buffer’ table avec la même structure que ‘merge.hits’ et en utilisant le moteur tampon. Lors de l'écriture dans cette table, les données sont mises en mémoire tampon dans la RAM ‘merge.hits’ table. 16 tampons sont créés. Les données dans chacun d'entre eux est rincé si 100 secondes sont écoulées, ou un million de lignes ont été écrites, ou 100 MO de données ont été écrits; ou si, simultanément, 10 secondes et 10 000 lignes et 10 MO de données ont été écrites. Par exemple, si une ligne a été écrite, après 100 secondes, il sera vidé, n'importe quoi. Mais si plusieurs lignes ont été écrites, les données seront vidées plus tôt.

Lorsque le serveur est arrêté, avec DROP TABLE ou DETACH TABLE, les données du tampon sont également vidées vers la table de destination.

Vous pouvez définir des chaînes vides entre guillemets simples pour le nom de la base de données et de la table. Cela indique l'absence d'une table de destination. Dans ce cas, lorsque les conditions de vidage des données sont atteintes, le tampon est simplement effacé. Cela peut être utile pour garder une fenêtre de données dans la mémoire.

Lors de la lecture à partir d'une table tampon, les données sont traitées à la fois à partir du tampon et de la table de destination (s'il y en a une).
Notez que les tables de tampon ne prennent pas en charge un index. En d'autres termes, les données dans le tampon sont entièrement analysées, ce qui peut être lent pour les grands tampons. (Pour les données dans une table subordonnée, l'index qu'il prend en charge sera utilisé.)

Si l'ensemble de colonnes de la table tampon ne correspond pas à l'ensemble de colonnes d'une table subordonnée, un sous-ensemble de colonnes existant dans les deux tables est inséré.

Si les types ne correspondent pas à l'une des colonnes de la table tampon et à une table subordonnée, un message d'erreur est entré dans le journal du serveur et le tampon est effacé.
La même chose se produit si la table subordonnée n'existe pas lorsque le tampon est vidé.

Si vous devez exécuter ALTER pour une table subordonnée et la table tampon, nous vous recommandons de supprimer d'abord la table tampon, d'exécuter ALTER pour la table subordonnée, puis de créer à nouveau la table tampon.

Si le serveur est redémarré anormalement, les données dans le tampon sont perdues.

FINAL et SAMPLE ne fonctionnent pas correctement pour les tables tampon. Ces conditions sont transmises à la table de destination, mais ne sont pas utilisées pour traiter les données dans le tampon. Si ces fonctionnalités sont nécessaires, nous vous recommandons d'utiliser uniquement la table tampon pour l'écriture, lors de la lecture à partir de la table de destination.

Lors de l'ajout de données à un Tampon, un des tampons est verrouillé. Cela entraîne des retards si une opération de lecture est effectuée simultanément à partir de la table.

Les données insérées dans une table tampon peuvent se retrouver dans la table subordonnée dans un ordre différent et dans des blocs différents. Pour cette raison, une table tampon est difficile à utiliser pour écrire correctement dans un CollapsingMergeTree. Pour éviter les problèmes, vous pouvez définir ‘num\_layers’ 1.

Si la table de destination est répliquée, certaines caractéristiques attendues des tables répliquées sont perdues lors de l'écriture dans une table tampon. Les modifications aléatoires apportées à l'ordre des lignes et des tailles des parties de données provoquent l'arrêt de la déduplication des données, ce qui signifie qu'il n'est pas possible d'avoir un ‘exactly once’ Ecrire dans des tables répliquées.

En raison de ces inconvénients, nous ne pouvons recommander l'utilisation d'une table tampon que dans de rares cas.

Une table tampon est utilisée lorsque trop D'insertions sont reçues d'un grand nombre de serveurs sur une unité de temps et que les données ne peuvent pas être mises en mémoire tampon avant l'insertion, ce qui signifie que les insertions ne peuvent pas s'exécuter assez rapidement.

Notez qu'il n'est pas judicieux d'insérer des données d'une ligne de temps, même pour Tampon tables. Cela ne produira qu'une vitesse de quelques milliers de lignes par seconde, tandis que l'insertion de blocs de données plus grands peut produire plus d'un million de lignes par seconde (voir la section “Performance”).

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/buffer/) <!--hide-->
